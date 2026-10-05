# Databricks notebook source
# DBTITLE 1,요약 제목
# MAGIC %md
# MAGIC # 2.3 대규모 환경을 위한 Unity Catalog 아키텍처 설계
# MAGIC

# COMMAND ----------

# DBTITLE 1,핵심 개념
# MAGIC %md
# MAGIC ## 1. 핵심 개념: Catalog vs. Metastore
# MAGIC
# MAGIC - **Metastore = 인프라**: 리전당 하나. 카탈로그, 스토리지 자격 증명, 외부 위치를 포함
# MAGIC - **Catalog = 조직 단위**: 3단계 네임스페이스(`catalog.schema.table`)의 첫 번째 수준으로, 핵심 거버넌스 의사결정이 이루어지는 곳
# MAGIC - **명명 규칙(naming convention) 중요**: 초기부터 표준 수립 (예: `prod_finance`, `dev_marketing`, `shared_reference`, 소문자+언더스코어 권장, 하이브리드 패턴 시 환경 접두사 포함)

# COMMAND ----------

# DBTITLE 1,접근 제어
# MAGIC %md
# MAGIC ## 2. 접근 제어: RBAC vs. FGAC
# MAGIC
# MAGIC **RBAC(Role-Based Access Control, 역할 기반 접근 제어)** - 기초:
# MAGIC - 권한은 그룹에 부여 (catalog/schema/개체 수준), 사용자는 그룹 멤버십으로 상속
# MAGIC - 스키마에 부여된 권한은 그 안의 모든 현재/향후 테이블에 적용
# MAGIC - 대부분의 접근 제어 요구를 충족하는 기본 계층
# MAGIC
# MAGIC **FGAC(Fine-Grained Access Control, 세분화된 접근 제어)** - 네 가지 메커니즘:
# MAGIC
# MAGIC | 메커니즘 | 용도 | 적합한 경우 |
# MAGIC |-----------|------|----------|
# MAGIC | **Row Filters** | 행 수준 제한 (`ALTER TABLE ... SET ROW FILTER`) | 소수 테이블의 단순 행 필터링 |
# MAGIC | **Column Masks** | 열 값 마스킹 (`ALTER TABLE ... ALTER COLUMN ... SET MASK`) | 특정 열의 PII 보호 |
# MAGIC | **Dynamic Views** | 뷰 SQL 안에 보안 로직 (`is_account_group_member()`, `current_user()`) | 다중 조인, 복잡한 로직, Delta Sharing |
# MAGIC | **ABAC Policies** | governed 태그 기반 자동 적용 | 대규모 배포의 '태그 한 번, 전체 보호' |
# MAGIC
# MAGIC **ABAC(Attribute-Based Access Control, 속성 기반 접근 제어) 워크플로**: 열에 governed 태그 지정 → UDF 생성 → 정책으로 태그-UDF 연결. 수동 행 필터/열 마스크와 ABAC 정책은 같은 테이블에 공존 불가.

# COMMAND ----------

# DBTITLE 1,카탈로그 구성 패턴
# MAGIC %md
# MAGIC ## 3. 카탈로그 구성 패턴
# MAGIC
# MAGIC | 패턴 | 예시 | 장점 | 단점 | 적합한 경우 |
# MAGIC |---------|------|------|------|----------|
# MAGIC | **환경 기반** | `prod`, `staging`, `dev` | 단순, CI/CD와 잘 맞음 | 환경마다 구조 중복 | 소규모~중간 규모 조직 |
# MAGIC | **도메인 기반** | `finance`, `marketing`, `operations` | 명확한 소유권, 자연스러운 권한 경계 | 도메인 간 분석에 다중 카탈로그 접근 필요 | 강한 도메인 경계가 있는 조직 |
# MAGIC | **하이브리드 (권장)** | `prod_finance`, `dev_shared` | 두 패턴의 장점 결합 | 관리 카탈로그 수 증가 | 대규모 기업 |
# MAGIC
# MAGIC - **항상 공유 카탈로그 포함**: 국가 코드, 환율, 달력 등 공통 참조 데이터는 모든 팀이 접근 가능한 `shared_reference` 같은 카탈로그에
# MAGIC - **이름 표준 강제**: `system.information_schema.tables` 등으로 정기 감사, CI/CD에서 Terraform/Asset Bundles로 사전 검증
# MAGIC - **재구성은 고비용**: 데이터가 사용 중이면 사실상 전체 데이터 자산 재구성 + 모든 노트북/대시보드/애플리케이션 업데이트 필요

# COMMAND ----------

# DBTITLE 1,안티패턴
# MAGIC %md
# MAGIC ## 4. 데이터 구성 안티패턴
# MAGIC
# MAGIC | 안티패턴 | 문제점 | 해결책 |
# MAGIC |-------------|---------|----------|
# MAGIC | 거대 카탈로그 하나에 전부 | 권한 복잡, 검색성 저하 | 도메인/하이브리드 카탈로그로 자연스러운 경계 |
# MAGIC | 팀당 카탈로그 (50팀=50카탈로그) | 카탈로그 급증, 팀 간 분석 어려움 | 도메인 카탈로그 내 스키마로 팀 분리 |
# MAGIC | 일관되지 않은 이름 (`sales_data` vs `SalesData`) | 혼란과 오류 | 초기 표준 수립 + 강제 |
# MAGIC | 과도하게 깊은 스키마 계층 | UC는 3단계(`catalog.schema.table`)만 지원 | 3단계 설계 그대로 사용 |
# MAGIC | 개별 테이블 단위 권한 | 확장 불가, 감사 어려움 | 스키마/카탈로그 수준 권한 + 그룹 멤버십 |

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 5. 핵심 요약
# MAGIC
# MAGIC - **Metastore는 인프라, Catalog는 조직** - 핵심 거버넌스 의사결정은 카탈로그 설계에서
# MAGIC - **RBAC가 기반** - 스키마 수준 그룹 권한 + 상속으로 관리 단순화 유지
# MAGIC - **FGAC는 4가지 메커니즘** - Row Filters / Column Masks / Dynamic Views / ABAC Policies, 규모·복잡도에 따라 선택
# MAGIC - **ABAC는 FGAC를 확장** - governed 태그로 대규모 자동화 (태그 한 번, 전체 보호)
# MAGIC - **대규모에서는 하이브리드 패턴이 최선** - 환경+도메인 결합(예: `prod_finance`)으로 격리와 명확성 동시 확보
# MAGIC - **카탈로그 구성은 나중에 바꾸기 어려움** - 설계 초기 투자 + 첫날부터 이름 규칙 강제
# MAGIC - **테이블 수준 권한 세분화 금지** - 스키마/카탈로그 수준 권한 사용