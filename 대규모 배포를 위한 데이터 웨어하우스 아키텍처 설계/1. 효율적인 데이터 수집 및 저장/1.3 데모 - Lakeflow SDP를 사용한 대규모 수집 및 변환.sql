-- Databricks notebook source
-- DBTITLE 1,제목
-- MAGIC %md
-- MAGIC # 1.3 데모 - Lakeflow SDP를 사용한 대규모 수집 및 변환
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,학습 목표 및 시나리오
-- MAGIC %md
-- MAGIC ## 학습 목표 및 시나리오
-- MAGIC
-- MAGIC **학습 목표:**
-- MAGIC 1. Lakeflow SDP로 스트리밍 테이블 구성하여 원본 데이터 수집
-- MAGIC 2. `EXPECT` 제약 조건으로 데이터 품질 기대값 구현
-- MAGIC 3. SCD Type 2로 CDC 이벤트 처리
-- MAGIC 4. Gold 계층의 비즈니스 집계를 위한 구체화된 뷰 생성
-- MAGIC 5. 증분 처리 시연
-- MAGIC
-- MAGIC **시나리오:** 랜딩 존의 CSV 파일에서 베이커리 프랜차이즈 데이터를 수집하고 Bronze -> Silver -> Gold 계층으로 변환합니다. Auto Loader 스트리밍 수집, 데이터 품질 적용, SCD Type 2 이력 추적, 비즈니스 집계를 포함합니다.

-- COMMAND ----------

-- DBTITLE 1,설정 안내
-- MAGIC %md
-- MAGIC ## 설정 안내
-- MAGIC
-- MAGIC - **컴퓨팅:** Serverless SQL Warehouse (`shared_warehouse`) 사용 권장
-- MAGIC - **데이터 설정:** 사전에 **0 - Required Setup** 노트북 실행 필요
-- MAGIC - **주의:** 이 노트북의 SQL 셀은 Lakeflow SDP 파이프라인을 정의하므로 대화형으로 실행할 수 없습니다. Databricks UI에서 파이프라인을 생성하고 실행해야 합니다.

-- COMMAND ----------

-- DBTITLE 1,파이프라인 아키텍처
-- MAGIC %md
-- MAGIC ## 파이프라인 아키텍처
-- MAGIC
-- MAGIC **랜딩 존** (CSV 파일) -> **Bronze** (Streaming Table) -> **Silver** (ST / DQ + SCD Type 2) -> **Gold** (Materialized View)
-- MAGIC
-- MAGIC - **랜딩 존:** transactions.csv, franchises.csv, franchises_cdc.csv
-- MAGIC - **Bronze:** transactions, franchises (각각 Streaming Table)
-- MAGIC - **Silver:** transactions (DQ Expectations 포함), franchises (SCD Type 2)
-- MAGIC - **Gold:** daily_sales_summary, franchise_performance (각각 Materialized View)

-- COMMAND ----------

-- DBTITLE 1,Bronze 계층
-- MAGIC %md
-- MAGIC ## Bronze 계층: 원본 데이터 수집
-- MAGIC
-- MAGIC 소스 시스템의 원본 데이터를 있는 그대로 수집합니다. 주요 특징:
-- MAGIC - **추가 전용(Append-only)** - 전체 이력 보존
-- MAGIC - **스키마 추론** - 컬럼 타입 자동 감지
-- MAGIC - **내결함성** - 처리된 파일 추적
-- MAGIC - Auto Loader (`STREAM read_files()`)로 증분 수집, 정확히 한 번(Exactly-once) 보장

-- COMMAND ----------

-- DBTITLE 1,bronze.transactions SQL
-- Bronze: 랜딩 존에서 원본 트랜잭션 데이터 수집
CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.transactions
COMMENT "Raw transaction data ingested from landing zone CSV files"
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

-- COMMAND ----------

-- DBTITLE 1,bronze.franchises SQL
-- Bronze: 프랜차이즈 데이터 수집
CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.franchises
COMMENT "Raw franchise data with CDC operation tracking"
AS SELECT 
    CAST(franchiseID AS LONG) AS franchiseID,
    name,
    city,
    district,
    zipcode,
    country,
    size,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(supplierID AS LONG) AS supplierID,
    _operation,
    _change_timestamp,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/bakehouse_incoming/landing_zone/sales_franchises/',
    format => 'csv',
    header => 'true',
    inferSchema => 'false'
);

-- COMMAND ----------

-- DBTITLE 1,Silver 계층
-- MAGIC %md
-- MAGIC ## Silver 계층: 데이터 품질 및 CDC 처리
-- MAGIC
-- MAGIC - **트랜잭션**: `EXPECT` 제약 조건으로 데이터 검증, 잘못된 레코드 드롭/추적
-- MAGIC - **프랜차이즈**: `AUTO CDC INTO`와 SCD Type 2로 전체 변경 이력 유지
-- MAGIC
-- MAGIC | 제약 조건 액션 | 동작 |
-- MAGIC |----------------|------|
-- MAGIC | `EXPECT` (조건) | 위반을 메트릭으로 추적하지만 모든 행 유지 |
-- MAGIC | `EXPECT ... ON VIOLATION DROP ROW` | 위반 행 제거 |
-- MAGIC | `EXPECT ... ON VIOLATION FAIL UPDATE` | 위반 시 전체 갱신 실패 |
-- MAGIC
-- MAGIC > **참고:** `DROP ROW`로 Silver에서 제거된 레코드도 Bronze에 원본 그대로 보존됩니다. 위반 횟수는 파이프라인 이벤트 로그에서 추적됩니다.

-- COMMAND ----------

-- DBTITLE 1,silver.transactions SQL
-- Silver: 데이터 품질 기대값으로 트랜잭션 데이터 정제 및 검증
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.transactions (
    -- Critical constraints - drop invalid rows
    CONSTRAINT valid_transaction_id EXPECT (transactionID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_customer_id EXPECT (customerID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_franchise_id EXPECT (franchiseID IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
    CONSTRAINT positive_price EXPECT (totalPrice > 0) ON VIOLATION DROP ROW,
    -- Warning constraints - track but keep rows
    CONSTRAINT valid_payment_method EXPECT (paymentMethod IN ('Credit Card', 'Debit Card', 'Cash', 'Mobile Payment')),
    CONSTRAINT reasonable_unit_price EXPECT (unitPrice BETWEEN 1 AND 1000)
)
COMMENT "Validated transaction data with quality constraints applied"
-- 쿼리에서 자주 사용되는 필터 성능 향상을 위해 Liquid Clustering 활성화
CLUSTER BY (transactionDate, customerID)
AS SELECT
    transactionID,
    customerID,
    franchiseID,
    dateTime,
    DATE(dateTime) AS transactionDate,
    product,
    quantity,
    unitPrice,
    totalPrice,
    paymentMethod,
    cardNumber,
    _ingested_at,
    current_timestamp() AS _processed_at
FROM STREAM ${catalog}.bronze.transactions;

-- COMMAND ----------

-- DBTITLE 1,SCD Type 2
-- MAGIC %md
-- MAGIC ### SCD Type 2: silver.franchises
-- MAGIC
-- MAGIC `AUTO CDC INTO`와 SCD Type 2로 프랜차이즈 변경 이력을 유지합니다. 각 업데이트는 `__START_AT`/`__END_AT` 타임스탬프가 있는 새 행을 생성하여 시점 쿼리를 지원합니다.
-- MAGIC - 신규 개점 (`INSERT`), 이전/속성 변경 (`UPDATE`), 폐업 (`DELETE`) 추적

-- COMMAND ----------

-- DBTITLE 1,silver.franchises SQL
-- Silver: SCD Type 2 프랜차이즈 이력을 위한 대상 테이블 생성
CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.franchises
COMMENT "Franchise dimension with SCD Type 2 history tracking"
-- 성능 향상을 위해 Liquid Clustering 활성화
-- AUTO를 사용하여 액세스 패턴과 테이블 통계를 기반으로 Predictive Optimization이 데이터 레이아웃을 자동화하도록 설정
CLUSTER BY AUTO;

-- Apply CDC changes with SCD Type 2
CREATE FLOW franchises_cdc AS AUTO CDC INTO ${catalog}.silver.franchises
FROM STREAM ${catalog}.bronze.franchises
KEYS (franchiseID)
APPLY AS DELETE WHEN _operation = 'DELETE'
SEQUENCE BY _change_timestamp
COLUMNS * EXCEPT (_source_file, _ingested_at, _operation)
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- DBTITLE 1,Gold 계층
-- MAGIC %md
-- MAGIC ## Gold 계층: 비즈니스 집계
-- MAGIC
-- MAGIC 비즈니스 수준 집계를 위한 **구체화된 뷰(Materialized View)**. 상위 Silver 테이블 변경 시 자동 증분 갱신됩니다.

-- COMMAND ----------

-- DBTITLE 1,gold.daily_sales_summary SQL
-- Gold: 프랜차이즈별 일일 매출 집계
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.daily_sales_summary
COMMENT "Daily sales metrics aggregated by franchise"
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
WHERE f.__END_AT IS NULL  -- 현재 프랜차이즈 레코드만
GROUP BY t.transactionDate, t.franchiseID, f.name, f.country;

-- COMMAND ----------

-- DBTITLE 1,gold.franchise_performance SQL
-- Gold: 프랜차이즈 성과 순위
CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.franchise_performance
COMMENT "Franchise performance metrics with rankings"
AS SELECT
    f.franchiseID,
    f.name AS franchise_name,
    f.city,
    f.country,
    f.size AS franchise_size,
    COUNT(DISTINCT t.transactionID) AS total_transactions,
    COUNT(DISTINCT t.customerID) AS total_customers,
    SUM(t.totalPrice) AS total_revenue,
    AVG(t.totalPrice) AS avg_order_value,
    MIN(t.transactionDate) AS first_sale_date,
    MAX(t.transactionDate) AS last_sale_date,
    RANK() OVER (ORDER BY SUM(t.totalPrice) DESC) AS revenue_rank
FROM ${catalog}.silver.franchises f
LEFT JOIN ${catalog}.silver.transactions t ON f.franchiseID = t.franchiseID
WHERE f.__END_AT IS NULL  -- 현재 프랜차이즈 레코드만
GROUP BY f.franchiseID, f.name, f.city, f.country, f.size;

-- COMMAND ----------

-- DBTITLE 1,파이프라인 실행 및 증분 처리
-- MAGIC %md
-- MAGIC ## 파이프라인 실행 및 증분 처리
-- MAGIC
-- MAGIC **파이프라인 설정:**
-- MAGIC 1. Databricks UI에서 **Jobs & Pipelines** > **ETL pipeline**으로 이동
-- MAGIC 2. 파이프라인 이름 설정, 소스 코드 경로 지정
-- MAGIC 3. Default catalog/schema 확인, `catalog` 구성 파라미터 추가
-- MAGIC 4. **Run Pipeline** 클릭하여 실행
-- MAGIC
-- MAGIC **개발 팁:** Lakeflow Pipelines Editor에서 **Run Pipeline** 실행 전 **Serverless** 클러스터에 미리 연결하면 더 빠른 반복 작업 가능

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 증분 처리 시연
-- MAGIC
-- MAGIC 초기 파이프라인 실행이 완료된 후, 추가 데이터를 랜딩하여 다음을 시연할 수 있습니다:
-- MAGIC
-- MAGIC - **프랜차이즈 CDC 처리** - 신규 프랜차이즈, 이전된 프랜차이즈, 폐업한 프랜차이즈가 SCD Type 2로 추적됨
-- MAGIC - **데이터 품질 적용** - 잘못된 데이터가 있는 트랜잭션이 `EXPECT` 제약 조건에 의해 드롭됨
-- MAGIC
-- MAGIC `/Volumes/{your_catalog_name}/bakehouse_incoming/landing_zone/_staging`에 시뮬레이션된 수신 데이터가 있습니다:
-- MAGIC
-- MAGIC | 데이터 타입 | 레코드 | 목적 |
-- MAGIC |-----------|---------|---------|
-- MAGIC | 프랜차이즈 `INSERT` 연산 | 3 | 신규 프랜차이즈 위치 |
-- MAGIC | 프랜차이즈 `UPDATE` 연산 | 3 | 이전된 프랜차이즈 (새 주소) |
-- MAGIC | 프랜차이즈 `DELETE` 연산 | 2 | 폐업한 프랜차이즈 |
-- MAGIC | 유효한 트랜잭션 | 5 | 일반 매출 데이터 |
-- MAGIC | 잘못된 트랜잭션 | 5 | DQ 위반 (4개 드롭, 1개 경고) |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 추가 데이터 랜딩
-- MAGIC
-- MAGIC 증분 처리와 CDC를 시연하기 위해, 준비된 데이터 파일을 다운로드하고 Catalog UI를 사용하여 랜딩 존에 업로드합니다.
-- MAGIC
-- MAGIC **1단계: 프랜차이즈 CDC 데이터 추가**
-- MAGIC 1. 좌측 사이드바에서 **Catalog**를 엽니다
-- MAGIC 2. 다음으로 이동: 사용자 카탈로그 -> **bakehouse_incoming** -> **Volumes** -> **landing_zone** -> **_staging**
-- MAGIC 3. **franchise_changes.csv**를 클릭한 후 **Download Volume File**을 클릭합니다
-- MAGIC 4. 다음으로 이동: **landing_zone** -> **sales_franchises**
-- MAGIC 5. **Upload to Volume**을 클릭한 후 다운로드한 **franchise_changes.csv** 파일을 찾아 선택합니다
-- MAGIC
-- MAGIC **2단계: DQ 위반이 있는 트랜잭션 데이터 추가**
-- MAGIC 1. 다시 이동: **landing_zone** -> **_staging**
-- MAGIC 2. **additional_transactions.csv**를 클릭한 후 **Download Volume File**을 클릭합니다
-- MAGIC 3. 다음으로 이동: **landing_zone** -> **sales_transactions**
-- MAGIC 4. **Upload to Volume**을 클릭한 후 다운로드한 **additional_transactions.csv** 파일을 찾아 선택합니다

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 다음 단계: 파이프라인 갱신
-- MAGIC 두 파일 모두 랜딩 존에 업로드한 후:
-- MAGIC 1. Lakeflow Pipeline UI로 이동합니다
-- MAGIC 2. Run Pipeline을 클릭하여 파이프라인을 갱신합니다
-- MAGIC 3. 하단 패널에서 Pipeline graph 탭을 클릭합니다
-- MAGIC 4. 증분 처리를 확인: Output Records와 Upserted 카운트를 확인하여 새 파일만 수집되었는지 확인합니다
-- MAGIC 5. 두 번째 transactions 테이블 노드의 Data Quality를 검사합니다.
-- MAGIC  - 사각형 아이콘은 드롭된 레코드를 나타냅니다. 클릭하면 우측에 세부 정보 패널이 열리고 드롭 통계를 검토할 수 있습니다.
-- MAGIC  - 삼각형 아이콘은 경고된 레코드를 나타냅니다. 클릭하면 동일한 패널이 열립니다. Action 컬럼에 경고된 레코드에 대해 Allow가 표시됩니다. 이는 valid_payment_method 기대값에 해당합니다. 위의 테이블 정의에서 On Violations 설정을 비교하여 Drop과 기본값인 Warn의 차이를 확인하세요.
-- MAGIC 6. 여기로 돌아와 아래의 검증 쿼리를 실행하세요

-- COMMAND ----------

-- DBTITLE 1,예상 결과
-- MAGIC %md
-- MAGIC ### 예상 결과
-- MAGIC
-- MAGIC | 데이터 | 결과 |
-- MAGIC |------|------|
-- MAGIC | **프랜차이즈 CDC** | SCD Type 2 이력 업데이트 (`__START_AT`/`__END_AT` 확인) |
-- MAGIC | **신규 프랜차이즈** (9001-9003) | 현재 레코드로 추가 |
-- MAGIC | **이전된 프랜차이즈** (5, 8, 12) | 이전 버전 종료, 새 버전 추가 |
-- MAGIC | **폐업한 프랜차이즈** (3, 7) | 삭제 표시 (현재 레코드 없음) |
-- MAGIC | **유효한 트랜잭션** | 5개 신규 레코드 |
-- MAGIC | **잘못된 트랜잭션** | 4개 드롭, 1개 경고 유지 |

-- COMMAND ----------

-- DBTITLE 1,검증 쿼리 안내
-- MAGIC %md
-- MAGIC ## 검증 쿼리
-- MAGIC
-- MAGIC 파이프라인 완료 후 **SQL Editor**에서 실행하여 결과를 검증하세요.
-- MAGIC
-- MAGIC > **주의:** 이 쿼리들을 이 노트북에 붙여넣지 마세요 - 이후 파이프라인 실행이 실패합니다.

-- COMMAND ----------

-- DBTITLE 1,쿼리 1
-- MAGIC %md
-- MAGIC ### 쿼리 1: Bronze 계층 레코드 수 확인
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT 'bronze.transactions' AS table_name, COUNT(*) AS record_count 
-- MAGIC FROM bronze.transactions
-- MAGIC UNION ALL
-- MAGIC SELECT 'bronze.franchises', COUNT(*) 
-- MAGIC FROM bronze.franchises;
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,쿼리 2
-- MAGIC %md
-- MAGIC ### 쿼리 2: 데이터 품질 메트릭 확인
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT * 
-- MAGIC FROM event_log(TABLE(silver.transactions))
-- MAGIC WHERE event_type = 'flow_progress'
-- MAGIC ORDER BY timestamp DESC
-- MAGIC LIMIT 10;
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,쿼리 3
-- MAGIC %md
-- MAGIC ### 쿼리 3: SCD Type 2 프랜차이즈 이력 확인
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT 
-- MAGIC     franchiseID, name, city, country, __START_AT, __END_AT,
-- MAGIC     CASE WHEN __END_AT IS NULL THEN 'Current' ELSE 'Historical' END AS version_status
-- MAGIC FROM silver.franchises
-- MAGIC WHERE franchiseID IN (3, 5, 7, 8, 12, 9001, 9002, 9003)
-- MAGIC ORDER BY franchiseID, __START_AT;
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,쿼리 4
-- MAGIC %md
-- MAGIC ### 쿼리 4: 매출별 상위 프랜차이즈
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT franchise_name, city, country, total_transactions, total_revenue, revenue_rank
-- MAGIC FROM gold.franchise_performance
-- MAGIC ORDER BY revenue_rank
-- MAGIC LIMIT 10;
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,쿼리 5
-- MAGIC %md
-- MAGIC ### 쿼리 5: 일일 매출 추세
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT transactionDate, SUM(total_transactions) AS transactions, SUM(total_revenue) AS revenue, SUM(unique_customers) AS customers
-- MAGIC FROM gold.daily_sales_summary
-- MAGIC GROUP BY transactionDate
-- MAGIC ORDER BY transactionDate DESC
-- MAGIC LIMIT 30;
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,정리
-- MAGIC %md
-- MAGIC ## 정리 (선택 사항)
-- MAGIC
-- MAGIC 모든 데모 리소스 제거 시 SQL Editor에서 다음을 실행 (주석 해제 후 실행):
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- DROP SCHEMA IF EXISTS bakehouse_incoming CASCADE;
-- MAGIC -- DROP SCHEMA IF EXISTS bronze CASCADE;
-- MAGIC -- DROP SCHEMA IF EXISTS silver CASCADE;
-- MAGIC -- DROP SCHEMA IF EXISTS gold CASCADE;
-- MAGIC ```
-- MAGIC
-- MAGIC > **경고:** Lakeflow Pipeline 삭제 시 해당 파이프라인이 생성한 모든 스트리밍 테이블과 구체화된 뷰도 삭제됩니다 (약 5분 이내). 이 작업은 되돌릴 수 없습니다.

-- COMMAND ----------

-- DBTITLE 1,요약 및 핵심 요약
-- MAGIC %md
-- MAGIC ## 요약 및 핵심 요약
-- MAGIC
-- MAGIC **사용된 테이블:**
-- MAGIC
-- MAGIC | 계층 | 테이블 | 타입 | 목적 |
-- MAGIC |-------|-------|------|---------|
-- MAGIC | **Bronze** | `transactions` | Streaming Table | 원본 트랜잭션 수집 |
-- MAGIC | **Bronze** | `franchises` | Streaming Table | CDC 포함 원본 프랜차이즈 데이터 |
-- MAGIC | **Silver** | `transactions` | Streaming Table | DQ 제약 조건이 적용된 검증된 트랜잭션 |
-- MAGIC | **Silver** | `franchises` | SCD Type 2 | 변경 추적이 있는 프랜차이즈 이력 |
-- MAGIC | **Gold** | `daily_sales_summary` | Materialized View | 일일 매출 집계 |
-- MAGIC | **Gold** | `franchise_performance` | Materialized View | 프랜차이즈 성과 순위 |
-- MAGIC
-- MAGIC **핵심 요약:**
-- MAGIC 1. **Auto Loader를 활용한 Streaming Table** - `STREAM read_files()`로 증분 수집
-- MAGIC 2. **데이터 품질 기대값(Expectations)** - `EXPECT` + `DROP ROW`로 잘못된 레코드 필터링
-- MAGIC 3. **SCD Type 2** - `AUTO CDC INTO`로 전체 변경 이력 유지
-- MAGIC 4. **Materialized Views** - 비즈니스 집계를 위한 증분 배치 변환
-- MAGIC 5. **증분 처리** - 새 데이터 추가 후 갱신하여 추가 전용 수집과 CDC 시연
-- MAGIC
-- MAGIC > **명령형보다 선언형** | **기대값 = 데이터 검증** | **추가 전용에 Streaming Table** | **이력에 SCD Type 2** | **집계에 Materialized Views**