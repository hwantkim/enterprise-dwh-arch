-- Databricks notebook source
-- DBTITLE 1,제목
-- MAGIC %md
-- MAGIC # 데모 - Unity Catalog에서 FGAC 구현하기
-- MAGIC
-- MAGIC 이 데모는 Unity Catalog의 4가지 FGAC(Fine-Grained Access Control) 메커니즘 — Column Mask, Row Filter, Dynamic View, ABAC(Attribute-Based Access Control) — 을 Bakehouse 데이터셋에 적용하는 방법을 다룹니다.

-- COMMAND ----------

-- DBTITLE 1,학습 목표 및 데이터 개요
-- MAGIC %md
-- MAGIC ## 학습 목표 및 데이터 개요
-- MAGIC
-- MAGIC ### 학습 목표
-- MAGIC 1. **Column mask** 생성 및 적용 — 그룹 멤버십 기반 민감 데이터 마스킹
-- MAGIC 2. **Row filter** 생성 및 적용 — 사용자별 조회 가능 행 제한
-- MAGIC 3. **Dynamic view** 구성 — 행 필터링 + 열 마스킹을 단일 객체로 결합
-- MAGIC 4. **governed 태그** + **ABAC 정책** — 대규모 접근 제어 자동화
-- MAGIC 5. **owner bypass** 동작과 dynamic view의 차이점 이해
-- MAGIC
-- MAGIC ### 데이터 개요
-- MAGIC - **silver.customers**: PII 필드(`email_address`, `phone_number`) 포함 → 마스킹 대상
-- MAGIC - **silver.franchises**: `country` 열 포함 → 행 필터링 기준
-- MAGIC - FGAC 적용을 위해서는 테이블 소유권 필요 (강의실 설정에서 작업 복사본 제공)

-- COMMAND ----------

-- DBTITLE 1,Part 1: Column Mask
-- MAGIC %md
-- MAGIC ## Part 1: Column Mask
-- MAGIC
-- MAGIC Column mask는 그룹 멤버십에 따라 열 값을 변환하는 SQL 함수입니다. `data_admins` 그룹은 원본 값, 그 외 사용자는 마스킹된 값을 반환합니다.

-- COMMAND ----------

-- DBTITLE 1,현재 Catalog/Schema 확인
-- 현재 Catalog를 dbrx_demo로 설정합니다.  
USE CATALOG dbrx_demo;
SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- DBTITLE 1,메달리온 스키마 초기화
--
-- 스키마 삭제 후 재생성
--

DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

-- COMMAND ----------

-- DBTITLE 1,데모용 데이터 생성
--
--   FGAC 데모를 위해 silver 레이어에 bakehouse 테이블의 작업 복사본을 생성합니다.
--

CREATE OR REPLACE TABLE silver.customers
AS SELECT * FROM samples.bakehouse.sales_customers;

CREATE OR REPLACE TABLE silver.franchises
AS SELECT * FROM samples.bakehouse.sales_franchises;

SELECT 'silver.customers and silver.franchises created' AS status;

-- COMMAND ----------

-- DBTITLE 1,customers 데이터 확인
SELECT customerID, first_name, last_name, email_address, phone_number, country
FROM silver.customers
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,franchises 데이터 확인
SELECT franchiseID, name, city, country, size
FROM silver.franchises
ORDER BY country
LIMIT 15;

-- COMMAND ----------

-- DBTITLE 1,마스크 함수 생성
-- 마스크 함수 생성: data_admins는 원본, 그 외는 도메인만 표시
CREATE OR REPLACE FUNCTION silver.mask_email(email_address STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('data_admins') THEN email_address
  ELSE CONCAT('****@', SPLIT(email_address, '@')[1])
END;

-- COMMAND ----------

-- DBTITLE 1,마스크 적용
-- 마스크 적용
ALTER TABLE silver.customers ALTER COLUMN email_address SET MASK silver.mask_email;

-- COMMAND ----------

-- DBTITLE 1,마스크 등록 확인
-- 마스크 등록 확인
DESCRIBE TABLE EXTENDED silver.customers;

-- COMMAND ----------

-- DBTITLE 1,마스크 적용 후 조회
-- 마스크 적용 후 조회 (data_admins 그룹이 아니면 마스킹된 값 표시)
SELECT 
  customerID,
  first_name,
  email_address
FROM silver.customers
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,owner bypass 설명
-- MAGIC %md
-- MAGIC **owner bypass**: 테이블 소유자와 `data_admins` 멤버는 마스크를 우회하여 원본 값을 볼 수 있습니다.

-- COMMAND ----------

-- DBTITLE 1,Part 2: Row Filter
-- MAGIC %md
-- MAGIC ## Part 2: Row Filter
-- MAGIC
-- MAGIC Row filter는 사용자가 볼 수 있는 행에 대해 `TRUE`를 반환하는 SQL 함수입니다. Unity Catalog가 모든 쿼리에 자동 `WHERE` 절로 주입합니다.

-- COMMAND ----------

-- DBTITLE 1,프랜차이즈 국가 확인
-- 프랜차이즈 국가 확인
SELECT 
  country, count(*) as num_franchises
FROM silver.franchises
GROUP BY country;

-- COMMAND ----------

-- DBTITLE 1,행 필터 함수 생성
-- 행 필터 함수: data_admins는 전체, account users는 Australia/Japan만, 그 외는 FALSE
CREATE OR REPLACE FUNCTION silver.country_filter(country STRING)
RETURNS BOOLEAN
RETURN CASE
  WHEN is_account_group_member('data_admins') THEN TRUE
  WHEN is_account_group_member('account users') THEN country IN ('Australia', 'Japan')
  ELSE FALSE
END;

-- COMMAND ----------

-- DBTITLE 1,행 필터 적용
-- 필터 적용
ALTER TABLE silver.franchises SET ROW FILTER silver.country_filter ON (country);

-- COMMAND ----------

-- DBTITLE 1,필터 등록 확인
-- 필터 등록 확인
DESCRIBE EXTENDED silver.franchises;

-- COMMAND ----------

-- DBTITLE 1,필터 적용 후 조회
-- 필터 적용 후 조회 (account users 그룹이면 Australia/Japan만 표시)
SELECT 
  country, count(*) as num_franchises
FROM silver.franchises
GROUP BY country;

-- COMMAND ----------

-- DBTITLE 1,row filter owner bypass
-- MAGIC %md
-- MAGIC **owner bypass**: row filter도 테이블 소유자에게 우회됩니다.

-- COMMAND ----------

-- DBTITLE 1,Part 3: Dynamic View
-- MAGIC %md
-- MAGIC ## Part 3: Dynamic View
-- MAGIC
-- MAGIC Dynamic view는 `CASE` 표현식으로 행 필터링과 열 마스킹을 단일 뷰에 포함합니다. Column mask/row filter와 달리 **소유자를 포함한 모든 사용자**에게 적용되므로 검증이 가장 간단합니다.

-- COMMAND ----------

-- DBTITLE 1,동적 뷰 생성
-- 동적 뷰 생성: 행 필터링(Australia만) + 열 마스킹(email, phone)
CREATE OR REPLACE VIEW silver.v_customers_secure AS
SELECT
  customerID,
  first_name,
  last_name,
  CASE
    WHEN is_account_group_member('data_admins') THEN email_address
    ELSE CONCAT('****@', SPLIT(email_address, '@')[1])
  END AS email_address,
  CASE
    WHEN is_account_group_member('data_admins') THEN phone_number
    ELSE CONCAT('***-***-', RIGHT(phone_number, 4))
  END AS phone_number,
  city,
  country
FROM silver.customers
WHERE is_account_group_member('data_admins') OR country = 'Australia';

-- COMMAND ----------

-- DBTITLE 1,동적 뷰 조회
-- 동적 뷰 조회
SELECT * FROM silver.v_customers_secure LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Dynamic View와 Delta Sharing
-- MAGIC %md
-- MAGIC ### Dynamic View와 Delta Sharing
-- MAGIC
-- MAGIC **Delta Sharing**으로 데이터를 공유할 때 권장되는 FGAC 메커니즘입니다. Row filter와 column mask는 Delta Sharing 수신자에게 적용되지 않지만, dynamic view의 보안 로직은 수신자가 기본 테이블이 아닌 뷰를 쿼리하므로 그대로 적용됩니다.

-- COMMAND ----------

-- DBTITLE 1,Part 4: ABAC
-- MAGIC %md
-- MAGIC ## Part 4: ABAC (Attribute-Based Access Control)
-- MAGIC
-- MAGIC ABAC는 **governed 태그**와 **정책**으로 FGAC를 확장합니다. 개별 열에 마스크를 일일이 적용하는 대신, 태그 기반으로 자동화합니다:
-- MAGIC
-- MAGIC 1. 열에 governed 태그 지정 (예: `class.email_address`)
-- MAGIC 2. 태그 + 마스킹 함수를 참조하는 정책 생성
-- MAGIC 3. 정책이 범위 내 모든 테이블에서 해당 태그의 열에 자동 적용

-- COMMAND ----------

-- DBTITLE 1,수동 마스크 제거
-- 전제: 기존 수동 마스크 제거 (ABAC 정책과 동시 사용 불가)
ALTER TABLE silver.customers ALTER COLUMN email_address DROP MASK;

-- COMMAND ----------

-- DBTITLE 1,governed 태그 적용
-- Step 1: governed 태그 적용
ALTER TABLE silver.customers ALTER COLUMN email_address 
  SET TAGS ('class.email_address');

-- COMMAND ----------

-- DBTITLE 1,ABAC용 마스크 함수
-- ABAC용 마스크 함수 (is_account_group_member 없음 — 정책이 처리)
CREATE OR REPLACE FUNCTION silver.mask_email(email_address STRING)
RETURNS STRING
RETURN CONCAT('****@', SPLIT(email_address, '@')[1]);

-- COMMAND ----------

-- DBTITLE 1,ABAC 정책 생성
-- Step 2: ABAC 정책 생성 (스키마 범위, account users에 적용)
CREATE OR REPLACE POLICY pii_email_policy
ON SCHEMA silver
COMMENT '모든 사용자에 대해 class.email_address로 태그 지정된 이메일 열 마스킹 처리'
COLUMN MASK silver.mask_email
TO `account users`
FOR TABLES
MATCH COLUMNS hasTag('class.email_address') AS email_address
ON COLUMN email_address;

-- COMMAND ----------

-- DBTITLE 1,ABAC 정책 테스트
-- ABAC 정책 테스트 (account users 그룹이면 마스킹 적용)
SELECT 
  customerID,
  first_name,
  email_address
FROM silver.customers
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,현재 사용자 확인
-- 현재 사용자 확인
SELECT current_user();

-- COMMAND ----------

-- DBTITLE 1,특정 사용자 제외 정책
-- 특정 사용자 제외 (관리자 확인용)
CREATE OR REPLACE POLICY pii_email_policy
ON SCHEMA silver
COMMENT '모든 사용자에 대해 class.email_address로 태그 지정된 이메일 열 마스킹 처리'
COLUMN MASK silver.mask_email
TO `account users`
EXCEPT `hwantkim@hotmail.com` -- 이전 셀의 결과인 사용자 이름으로 대체
FOR TABLES
MATCH COLUMNS hasTag('class.email_address') AS email_address
ON COLUMN email_address;

-- COMMAND ----------

-- DBTITLE 1,예외 적용 후 조회
-- 예외 적용 후 조회 (본인이 제외 대상이면 원본 값 표시)
SELECT 
  customerID,
  first_name,
  email_address
FROM silver.customers
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,ABAC 요구 사항 및 제약 사항
-- MAGIC %md
-- MAGIC ### ABAC 요구 사항 및 제약 사항
-- MAGIC
-- MAGIC - **DBR 16.4 이상** 필요 (Serverless 포함)
-- MAGIC - 하나의 열에 수동 column mask와 ABAC 정책을 **동시에** 적용할 수 없음
-- MAGIC - 테이블당 사용자별로 한 번에 **하나의 row filter 정책**만 적용 가능
-- MAGIC - 정책은 하위로 상속: `CATALOG` 범위는 모든 스키마/테이블 포함

-- COMMAND ----------

-- DBTITLE 1,정리
-- MAGIC %md
-- MAGIC ## 정리
-- MAGIC
-- MAGIC 데모에서 생성한 모든 FGAC 객체를 제거하여 테이블을 원래 상태로 복원합니다.

-- COMMAND ----------

-- DBTITLE 1,ABAC 정책 및 태그 제거
-- ABAC 정책 및 governed 태그 제거
DROP POLICY pii_email_policy ON SCHEMA silver;
ALTER TABLE silver.customers ALTER COLUMN email_address UNSET TAGS ('class.email_address');

-- COMMAND ----------

-- DBTITLE 1,나머지 객체 제거
-- 남은 마스크, 필터, 뷰, 함수 제거
ALTER TABLE silver.customers ALTER COLUMN email_address DROP MASK;
ALTER TABLE silver.franchises DROP ROW FILTER;
DROP VIEW IF EXISTS silver.v_customers_secure;
DROP FUNCTION IF EXISTS silver.mask_email;
DROP FUNCTION IF EXISTS silver.country_filter;

-- COMMAND ----------

-- DBTITLE 1,핵심 요약
-- MAGIC %md
-- MAGIC ## 핵심 요약
-- MAGIC
-- MAGIC 1. **Column mask** — 그룹 멤버십에 따라 민감 열 값을 변환. `is_account_group_member()`로 마스킹 여부 결정. 소유자는 우회.
-- MAGIC
-- MAGIC 2. **Row filter** — 사용자별 조회 가능 행을 제한. `TRUE`를 반환하는 필터 함수가 자동 `WHERE` 절로 주입. 소유자는 우회.
-- MAGIC
-- MAGIC 3. **Dynamic view** — `CASE` 표현식으로 행 필터링 + 열 마스킹을 단일 뷰에 포함. **소유자를 포함한 모든 사용자**에게 적용. Delta Sharing에서도 권장.
-- MAGIC
-- MAGIC 4. **ABAC** — governed 태그와 정책을 결합하여 FGAC를 자동화. 한 번 태그를 지정하면 catalog 전체에 자동 적용. 테이블별 유지 관리 불필요.