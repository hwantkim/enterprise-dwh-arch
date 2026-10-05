# Databricks notebook source
# DBTITLE 1,제목
# MAGIC %md
# MAGIC # 1.5 Lakehouse Federation 및 Foreign Catalogs
# MAGIC

# COMMAND ----------

# DBTITLE 1,두 가지 페더레이션 유형
# MAGIC %md
# MAGIC ## 1. 두 가지 페더레이션 유형
# MAGIC
# MAGIC | 특성 | Lakehouse Federation | 카탈로그 페더레이션 |
# MAGIC |---|---|---|
# MAGIC | **쿼리 경로** | JDBC를 통해 외부 DB로 쿼리 푸시다운. Databricks와 원격 컴퓨팅 모두에서 실행 | 객체 스토리지의 외부 테이블에 직접 액세스. Databricks 컴퓨팅에서만 실행 (더 비용 효율적) |
# MAGIC | **적합한 경우** | 임시 보고, 운영 DB에 대한 개념 증명 | 점진적 마이그레이션, 장기 하이브리드 모델 |
# MAGIC | **주요 이점** | 실시간 액세스, 최소 데이터 이동 | 직접 스토리지 액세스, 대용량 데이터에서 더 나은 성능 |
# MAGIC
# MAGIC **지원 소스 (Lakehouse Federation):** MySQL, PostgreSQL, SQL Server, Snowflake, BigQuery, Redshift, Oracle, Teradata, Salesforce Data 360, Azure Synapse, Databricks
# MAGIC
# MAGIC **지원 소스 (카탈로그 페더레이션):** Hive metastore, Salesforce Data 360, Snowflake

# COMMAND ----------

# DBTITLE 1,외부 연결 및 카탈로그 설정
# MAGIC %md
# MAGIC ## 2. 외부 연결 및 카탈로그 설정
# MAGIC
# MAGIC 외부 데이터를 쿼리하려면 3단계가 필요합니다:
# MAGIC
# MAGIC 1. **외부 연결 생성** - 페더레이션 서버를 Unity Catalog에 등록 (URL, 포트, 자격 증명)
# MAGIC 2. **외부 카탈로그 생성** - 외부 소스의 전체 카탈로그를 Unity Catalog에 등록. 스키마/테이블 자동 동기화
# MAGIC 3. **액세스 권한 부여** - 카탈로그, 스키마, 테이블 수준에서 권한 부여
# MAGIC
# MAGIC **PostgreSQL 예시:**
# MAGIC ```sql
# MAGIC CREATE CONNECTION postgresql_connection
# MAGIC   TYPE POSTGRESQL
# MAGIC   OPTIONS (host '...', port '5432', user secret(...), password secret(...));
# MAGIC
# MAGIC CREATE FOREIGN CATALOG postgresql_catalog
# MAGIC   USING CONNECTION postgresql_connection
# MAGIC   OPTIONS (database 'postgresdb');
# MAGIC
# MAGIC SELECT * FROM postgresql_catalog.a_schema.table1 LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **크로스 플랫폼 조인** also supported — Snowflake TPCH 데이터와 Databricks TPCH 데이터를 단일 쿼리로 조인 가능.

# COMMAND ----------

# DBTITLE 1,술어 푸시다운
# MAGIC %md
# MAGIC ## 3. 술어 푸시다운 (Predicate Pushdown)
# MAGIC
# MAGIC **핵심 최적화 기법:** `WHERE` 절을 소스 시스템으로 보내어 데이터 전송 *이전*에 필터링을 수행합니다.
# MAGIC
# MAGIC - **푸시다운 친화적 연산:** 단순 필터 (`WHERE date > '2024-01-01'`), 컬럼 선택, 기본 집계
# MAGIC - **푸시다운 불가능 연산:** 복잡한 UDF, 특정 JOIN 유형, 윈도우 함수 및 고급 분석
# MAGIC - **검증 방법:** 항상 `EXPLAIN`으로 쿼리 플랜을 확인하고 `PushedFilters` 확인
# MAGIC
# MAGIC > 소스에서 필터링하면 전송량이 100만 행에서 1,000행으로 줄어들어, 네트워크 트래픽이 1000배 감소합니다.

# COMMAND ----------

# DBTITLE 1,페더레이션 vs 복제
# MAGIC %md
# MAGIC ## 4. 페더레이션 vs. 복제: 의사결정 프레임워크
# MAGIC
# MAGIC | 조건 | 권장 접근 방식 |
# MAGIC |---|---|
# MAGIC | 드물게/임시 쿼리 + 소스가 부하 처리 가능 | **페더레이션** (원본에서 쿼리) |
# MAGIC | 드물게/임시 쿼리 + 소스가 부하 처리 불가 | **복제** (Lakehouse로 복사) |
# MAGIC | 자주/다수 사용자 + 데이터가 자주 변경 | **하이브리드** (복제 + CDC) |
# MAGIC | 자주/다수 사용자 + 데이터가 느리게 변경 | **복제** (Lakehouse로 복사) |
# MAGIC
# MAGIC ### 비용 비교
# MAGIC
# MAGIC | 접근 방식 | 비용 |
# MAGIC |---|---|
# MAGIC | **페더레이션** | 쿼리 컴퓨팅 + 네트워크 전송 (쿼리당) |
# MAGIC | **복제** | 스토리지 + 수집 컴퓨팅 + 쿼리 컴퓨팅 (더 빠른 쿼리, 더 낮은 쿼리당 비용) |

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 5. 핵심 요약
# MAGIC
# MAGIC - **두 가지 페더레이션 유형** — Lakehouse Federation은 JDBC로 쿼리를 소스로 푸시, 카탈로그 페더레이션은 객체 스토리지에 직접 액세스
# MAGIC - **Unity Catalog 거버넌스 통합** — 데이터 이동 없이 외부 데이터를 권한 모델, 계보 추적, 검색 아래로 가져옴
# MAGIC - **술어 푸시다운이 필수** — 최적화되지 않은 쿼리는 전체 데이터셋 스캔/전송, 최적화된 쿼리는 필터링된 결과만 전송. `EXPLAIN`으로 확인
# MAGIC - **의사결정 프레임워크** — 저용량 실시간 조회는 페더레이션, 고용량 분석은 복제, 대부분 하이브리드가 효과적
# MAGIC - **모니터링 및 적응** — 액세스 패턴은 변화함. 처음엔 간헐적 페더레이션이던 것이 점차 복제 필요 수준으로 증가할 수 있으므로 정기 검토 필요