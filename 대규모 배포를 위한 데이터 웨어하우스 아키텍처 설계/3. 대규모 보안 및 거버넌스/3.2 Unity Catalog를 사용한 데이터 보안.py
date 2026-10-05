# Databricks notebook source
# DBTITLE 1,요약 제목
# MAGIC %md
# MAGIC # 3.2 Unity Catalog를 사용한 데이터 보안
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. 규제 준수 정렬
# MAGIC %md
# MAGIC ## 1. 규제 준수 의무와의 정렬
# MAGIC
# MAGIC 컴플라이언스는 통제가 존재하고, 운영 중이며, 모니터링되고 있음을 감사자에게 입증하는 것입니다. Unity Catalog는 일반 규제 프레임워크에 필요한 기본 요소를 제공합니다.
# MAGIC
# MAGIC | 규제 | 핵심 요구 사항 | Unity Catalog 기능 |
# MAGIC |---|---|---|
# MAGIC | **GDPR / CCPA** | 접근권, 삭제권 | Lineage, 세분화된 권한, PII 컬럼 마스킹 |
# MAGIC | **HIPAA** | PHI 통제 | PHI 필드 Column Mask, 환자 격리 Row-Level Security |
# MAGIC | **SOX** | 재무 데이터 무결성, 접근 통제 | `system.access.audit` 감사 로그, 권한 계층을 통한 직무 분리 |
# MAGIC | **PCI-DSS** | 카드 소지자 데이터 보호 | 카드 번호 Column Mask, 암호화, 접근 로깅 |
# MAGIC
# MAGIC **Catalog 격리 전략(멀티 클라우드/멀티 리전 배포):**
# MAGIC
# MAGIC - Catalog를 특정 Workspace에 바인딩하여 규제 대상 데이터의 접근 환경 제한
# MAGIC - 데이터 분류 기준으로 Catalog 분리 (예: `pii_catalog`, `financial_catalog`, `general_catalog`)
# MAGIC - 그룹별 Catalog 접근 제한, `system.access.audit`로 격리 경계 준수 검증

# COMMAND ----------

# DBTITLE 1,2. 권한 상속과 그룹 모델
# MAGIC %md
# MAGIC ## 2. 표준 Grant와 권한 상속, 그룹 기반 권한 모델
# MAGIC
# MAGIC - Unity Catalog는 계층적 권한 모델(Metastore → Catalog → Schema → Table/View/Volume/Function)을 사용합니다. 상위 수준에서 부여된 권한은 하위 객체로 **아래 방향으로만** 자동 전파됩니다.
# MAGIC - 가능한 최상위 수준(Schema > 테이블)에서 부여하면 새 테이블이 생성돼도 권한 재부여가 필요 없습니다.
# MAGIC - 권한은 항상 **그룹**에 할당해야 하며(Account 수준에서 생성, 필요 시 중첩), 개별 사용자에게 할당하면 안 됩니다. IdP의 SCIM 프로비저닝으로 지속 동기화하는 것이 권장됩니다.
# MAGIC - 모범 사례: 사용자가 10명뿐이더라도 첫날부터 그룹 기반 권한으로 시작하세요.
# MAGIC
# MAGIC **대표적인 Schema 수준 권한 부여 예시(원본 SQL):**
# MAGIC
# MAGIC ```sql
# MAGIC -- Schema-level grants: engineers get full pipeline access
# MAGIC GRANT USE CATALOG ON CATALOG prod TO `data-engineers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod.bronze TO `data-engineers`;
# MAGIC GRANT CREATE TABLE ON SCHEMA prod.bronze TO `data-engineers`;
# MAGIC GRANT MODIFY ON SCHEMA prod.bronze TO `data-engineers`;
# MAGIC
# MAGIC -- Schema-level grants: analysts get read-only access to curated data
# MAGIC GRANT USE CATALOG ON CATALOG prod TO `analysts-finance`;
# MAGIC GRANT USE SCHEMA ON SCHEMA prod.gold_finance TO `analysts-finance`;
# MAGIC GRANT SELECT ON SCHEMA prod.gold_finance TO `analysts-finance`;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,3. Row Filter
# MAGIC %md
# MAGIC ## 3. FGAC 개요와 Row Filter
# MAGIC
# MAGIC Schema 수준 RBAC는 *누가 어떤 객체에* 접근하는지만 통제할 수 있습니다. 같은 테이블을 사용자별로 서로 다른 **행**으로 보여야 하거나 같은 컬럼의 서로 다른 **표현**이 필요하면 Fine-Grained Access Control(FGAC)을 사용합니다.
# MAGIC
# MAGIC - **Row Filter**: 사용자에게 열람 권한이 있는 행에만 `TRUE`를 반환하는 SQL 함수. 테이블의 모든 쿼리에 투명하게 적용되며, 엔진이 자동으로 주입하는 `WHERE` 절처럼 동작합니다.
# MAGIC
# MAGIC ```sql
# MAGIC -- Row filter function: region-based access
# MAGIC CREATE FUNCTION region_filter(iso_country_code STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN (
# MAGIC   is_account_group_member('data_admins')
# MAGIC   OR (is_account_group_member('analysts_apac') AND iso_country_code IN ('AU', 'JP', 'KR', 'IN'))
# MAGIC   OR (is_account_group_member('analysts_emea') AND iso_country_code IN ('GB', 'DE', 'FR'))
# MAGIC   OR (is_account_group_member('analysts_amer') AND iso_country_code IN ('US', 'CA', 'BR'))
# MAGIC );
# MAGIC
# MAGIC -- Apply the filter to a table
# MAGIC ALTER TABLE prod.gold.locations SET ROW FILTER region_filter ON (iso_country_code);
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4. Column Mask와 Dynamic View
# MAGIC %md
# MAGIC ## 4. Column Mask와 Dynamic View
# MAGIC
# MAGIC - **Column Mask**: 쿼리하는 사용자에 따라 컬럼 값을 변환하는 SQL 함수. 권한 없는 사용자는 편집/부분 마스킹된 값을 보지만 행 자체는 표시됩니다.
# MAGIC   - 마스킹 패턴: 전체 편집(`'***-**-****'`), 부분 공개(`CONCAT('***-***-', RIGHT(val, 4))`), 문자 치환(`mask(val)`), 해시/토큰화(`sha2(val, 256)` — 조인 보존), NULL 처리
# MAGIC - **Dynamic View**: `CASE` + 컨텍스트 함수로 View 정의 자체에 보안 로직 내장. 행 필터링과 컬럼 마스킹을 하나의 객체로 결합할 때, 또는 Delta Sharing 공유 시 권장됩니다.
# MAGIC
# MAGIC ```sql
# MAGIC -- Column mask function: partial SSN reveal
# MAGIC CREATE FUNCTION mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN (
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('billing_staff') THEN ssn
# MAGIC     ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))
# MAGIC   END
# MAGIC );
# MAGIC
# MAGIC -- Apply the mask to a column
# MAGIC ALTER TABLE prod.gold.patients ALTER COLUMN ssn SET MASK mask_ssn;
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC CREATE VIEW prod.gold.v_customer_secure AS
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     -- Column masking via CASE
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('data_admins') THEN email
# MAGIC         ELSE regexp_extract(email, '^.*@(.*)$', 1)
# MAGIC     END AS email,
# MAGIC     CASE 
# MAGIC         WHEN is_account_group_member('data_admins') THEN phone
# MAGIC         ELSE CONCAT('***-***-', RIGHT(phone, 4))
# MAGIC     END AS phone,
# MAGIC     city, country
# MAGIC FROM prod.gold.customers
# MAGIC -- Row filtering via WHERE
# MAGIC WHERE is_account_group_member('data_admins') OR country = 'United States';
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,5. FGAC 선택 가이드
# MAGIC %md
# MAGIC ## 5. FGAC 메커니즘 선택 가이드
# MAGIC
# MAGIC | 시나리오 | 권장 메커니즘 | 이유 |
# MAGIC |---|---|---|
# MAGIC | 사용자별로 서로 다른 행 노출 | **Row Filter** | 투명, 새 객체 불필요 |
# MAGIC | 민감 컬럼 마스킹 | **Column Mask** | 투명, 컬럼별 통제 |
# MAGIC | 행 + 컬럼 보안 결합, Delta Sharing 공유(`current_recipient()`), 보안 조인 | **Dynamic View** | 단일 객체, 유연한 SQL 로직 |
# MAGIC | 접근 방식과 무관한 강제 | **Row Filter + Column Mask** | 엔진 수준 적용, 우회 불가 |
# MAGIC
# MAGIC **감사 로그:** FGAC가 활성화되면 `system.access.audit`에 사용자가 제출한 **원본 쿼리**가 기록됩니다. 사용자는 필터링되지 않은 결과를 절대 볼 수 없으며, 완전한 감사 추적(audit trail)이 보장됩니다.

# COMMAND ----------

# DBTITLE 1,6. ABAC
# MAGIC %md
# MAGIC ## 6. ABAC: governed 태그 기반 정책으로 확장
# MAGIC
# MAGIC 수많은 테이블에 테이블별 FGAC를 적용하는 것은 비현실적입니다. ABAC는 **governed 태그**와 **정책**으로 태그 조건과 일치하는 모든 컬럼에 Row Filter/Column Mask를 자동 적용합니다 — "한 번 정의하면 태그가 존재하는 모든 곳에 적용".
# MAGIC
# MAGIC **작동 4단계:**
# MAGIC
# MAGIC 1. governed 태그 정의 (Catalog Explorer UI / REST API — 사전 정의된 허용 값 보유)
# MAGIC 2. 컬럼에 데이터 분류 태깅 (예: `class.us_ssn`, `region_filter`)
# MAGIC 3. 마스킹/필터 SQL UDF 생성
# MAGIC 4. ABAC 정책 생성 — `ON TABLE` / `ON SCHEMA` / `ON CATALOG` 범위, 아래 방향 자동 상속
# MAGIC
# MAGIC **Governed 태그 vs. 일반 태그:** ABAC 정책은 governed 태그에서만 작동(허용 값 통제, 태그에 대한 `ASSIGN` 권한 필요). 일반 태그는 탐색/구성용이며 접근 제어를 구동할 수 없습니다.
# MAGIC
# MAGIC **ABAC 제약:** Databricks Runtime 16.4+ 또는 serverless compute 필요. 수동 Row Filter(`ALTER TABLE ... SET ROW FILTER`)와 병립 불가. 테이블당 사용자당 하나의 Row Filter만 런타임에 강제 적용됩니다.
# MAGIC
# MAGIC ```sql
# MAGIC -- Tag the column
# MAGIC ALTER TABLE prod.gold.customers ALTER COLUMN ssn SET TAGS ('class.us_ssn');
# MAGIC
# MAGIC -- Create the masking function
# MAGIC CREATE FUNCTION mask_ssn_full(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN '***-**-****';
# MAGIC
# MAGIC -- Create the policy (applies to all tagged columns in the catalog)
# MAGIC CREATE POLICY mask_ssn_policy
# MAGIC ON CATALOG prod
# MAGIC COLUMN MASK mask_ssn_full
# MAGIC TO `restricted-users`
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS
# MAGIC   hasTag('class.us_ssn') AS ssn
# MAGIC ON COLUMN ssn;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,7. 전략 선택 기준
# MAGIC %md
# MAGIC ## 7. 접근 제어 전략 선택 기준
# MAGIC
# MAGIC | 구분 | RBAC (표준 Grant) | FGAC (테이블별) | ABAC (태그 기반 정책) |
# MAGIC |---|---|---|---|
# MAGIC | **세분화 수준** | 객체 수준 (Catalog, Schema, 테이블) | 행/컬럼 수준 | 행/컬럼 수준 |
# MAGIC | **관리 방식** | `GRANT` / `REVOKE` | 테이블별 `SET ROW FILTER` / `SET MASK` | governed 태그 + `CREATE POLICY` |
# MAGIC | **확장성** | 그룹 계층으로 잘 확장 | 대규모에서는 노동 집약적 | 한 번 태깅, 모든 곳에 적용 |
# MAGIC | **복잡도** | 낮음 | 중간 | 높음 (태그, 정책, 함수) |
# MAGIC | **적합한 경우** | 누가 어떤 객체에 접근하는지 통제 | 행/컬럼 제한이 필요한 민감 테이블 | 기업 전체 데이터 분류 및 보안 |
# MAGIC
# MAGIC **의사결정 흐름:** 그룹 전체가 같은 데이터를 봄 → RBAC / 행·컬럼 통제 필요 + 대상 테이블 소수 → FGAC / 수십~수백 개 이상 → ABAC

# COMMAND ----------

# DBTITLE 1,8. 성능과 체크리스트
# MAGIC %md
# MAGIC ## 8. FGAC 성능 고려 사항과 배포 전 체크리스트
# MAGIC
# MAGIC **성능:**
# MAGIC
# MAGIC - FGAC는 쿼리 시점에 실행됩니다. 필터 로직은 단순하게 유지(서브쿼리·복잡한 조인 회피)하고, Column Mask는 `CASE` + `is_account_group_member()` 같은 단순 조건부 로직을 사용하세요.
# MAGIC - 클러스터링 키를 필터 컬럼과 정렬(Liquid Clustering)하고, 반드시 프로덕션 규모에서 테스트하세요. `EXPLAIN`으로 주입된 필터 조건자를 확인할 수 있습니다.
# MAGIC - 팁: 매핑 테이블을 조인하는 Row Filter 대신, 그룹 멤버십에 매핑을 반영하고 `is_account_group_member()`를 직접 사용하세요.
# MAGIC
# MAGIC **배포 전 Unity Catalog 보안 체크리스트(핵심):**
# MAGIC
# MAGIC - Metastore admin·객체 소유권은 개인이 아닌 그룹에 할당, 환경별(dev/staging/prod) Catalog 분리, Workspace-Catalog 바인딩 구성
# MAGIC - 모든 권한은 Account 수준 그룹에, 상속 활용 최상위 부여(Schema > 테이블), 최소 권한 원칙
# MAGIC - 민감 데이터 테이블에 FGAC 적용, governed 태그 기반 ABAC 정책 검토, 자동화에는 최소 권한 Service Principal 사용, 권한 드리프트 탐지를 위한 정기 접근 검토

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC - **그룹 기반 권한으로 시작** — Schema/Catalog 수준에서 그룹에 권한 부여. 상속이 테이블별 권한을 없애고 온보딩을 간소화합니다.
# MAGIC - **행·컬럼 통제가 필요하면 FGAC** — Row Filter는 행 제한, Column Mask는 값 변환, Dynamic View는 둘을 결합하고 Delta Sharing을 지원합니다.
# MAGIC - **ABAC로 확장** — governed 태그와 정책이 보안 관리를 중앙화하고, 한 번의 태깅으로 Catalog 전체 일치 테이블에 적용됩니다.
# MAGIC - **FGAC는 강력하지만 비용이 따름** — 모든 쿼리에서 실행되므로 로직을 단순하게, 클러스터링 정렬, 현실적 볼륨 테스트가 필수입니다.
# MAGIC - **Go-live 전 체크리스트 검증** — 접근 통제·최소 권한 + 네트워크 격리·암호화·identity 통합(이전 섹션)·감사 로깅(다음 섹션)이 기업 배포의 최소 기준선입니다.