-- =============================================================================
-- Bronze 레이어: 원천 데이터 수집
-- =============================================================================

-- 랜딩 존에서 원천 거래 데이터 수집
CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.transactions
COMMENT "랜딩 존 CSV 파일에서 수집한 원천 거래 데이터"
AS SELECT 
    CAST(transactionID AS LONG) AS transactionID,
    CAST(customerID AS LONG) AS customerID,
    CAST(franchiseID AS LONG) AS franchiseID,
    CAST(dateTime AS TIMESTAMP) AS dateTime,
    product,
    CAST(quantity AS LONG) AS quantity,
    CAST(unitPrice AS LONG) AS unitPrice,
    CAST(totalPrice AS LONG) AS totalPrice,
    paymentMethod,
    CAST(cardNumber AS LONG) AS cardNumber,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bakehouse_incoming/landing_zone/sales_transactions/',
    format => 'csv',
    header => 'true',
    inferSchema => 'false'
);

-- CDC 작업 추적이 포함된 가맹점 데이터 수집
CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.franchises
COMMENT "CDC 작업 추적이 포함된 원천 가맹점 데이터"
AS SELECT 
    CAST(franchiseID AS LONG) AS franchiseID,
    name, city, district, zipcode, country, size,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(supplierID AS LONG) AS supplierID,
    _operation, _change_timestamp,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bakehouse_incoming/landing_zone/sales_franchises/',
    format => 'csv',
    header => 'true',
    inferSchema => 'false'
);

-- =============================================================================
-- Silver 레이어: 데이터 품질 및 CDC 처리
-- =============================================================================
-- 데이터 품질 제약 조건이 적용된 검증된 거래 데이터
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.transactions (
    CONSTRAINT valid_transaction_id EXPECT (transactionID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_customer_id EXPECT (customerID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_franchise_id EXPECT (franchiseID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
    CONSTRAINT positive_price EXPECT (totalPrice > 0) ON VIOLATION DROP ROW,
    CONSTRAINT valid_payment_method EXPECT (paymentMethod IN ('Credit Card', 'Debit Card', 'Cash', 'Mobile Payment'))
)
COMMENT "품질 제약 조건이 적용된 검증된 거래 데이터"
AS SELECT
    transactionID, customerID, franchiseID, dateTime,
    DATE(dateTime) AS transactionDate,
    product, quantity, unitPrice, totalPrice, paymentMethod, cardNumber,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM ${catalog}.bronze.transactions;
-- SCD Type 2 가맹점 차원 테이블
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.franchises
COMMENT "SCD Type 2 이력 추적이 포함된 가맹점 차원 테이블"
CLUSTER BY AUTO;
CREATE FLOW franchises_cdc AS AUTO CDC INTO ${catalog}.silver.franchises
FROM STREAM ${catalog}.bronze.franchises
KEYS (franchiseID)
APPLY AS DELETE WHEN _operation = 'DELETE'
SEQUENCE BY _change_timestamp
COLUMNS * EXCEPT (_source_file, _ingested_at, _operation)
STORED AS SCD TYPE 2;

-- =============================================================================
-- Gold 레이어: 비즈니스 집계
-- =============================================================================
-- 일별 매출 요약
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.daily_sales_summary
COMMENT "가맹점별로 집계된 일별 매출 지표"
AS SELECT
    t.transactionDate,
    t.franchiseID,
    f.name AS franchise_name,
    f.country,
    COUNT(DISTINCT t.transactionID) AS total_transactions,
    COUNT(DISTINCT t.customerID) AS unique_customers,
    SUM(t.quantity) AS total_items_sold,
    SUM(t.totalPrice) AS total_revenue,
    AVG(t.totalPrice) AS avg_transaction_value
FROM ${catalog}.silver.transactions t
LEFT JOIN ${catalog}.silver.franchises f ON t.franchiseID = f.franchiseID
WHERE f.__END_AT IS NULL
GROUP BY t.transactionDate, t.franchiseID, f.name, f.country;