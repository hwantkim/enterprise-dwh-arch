# Databricks notebook source
# MAGIC %md
# MAGIC # 데모 - DABs를 사용한 솔루션 배포
# MAGIC

# COMMAND ----------

# DBTITLE 1,학습 목표 요약
# MAGIC %md
# MAGIC ## 학습 목표
# MAGIC
# MAGIC 이 데모에서는 다음을 수행합니다:
# MAGIC
# MAGIC 1. YAML 구성 및 소스 노트북으로 Declarative Automation Bundle 프로젝트를 구성
# MAGIC 2. Lakeflow Pipeline, Lakeflow Job, SQL 알림을 번들 리소스로 정의
# MAGIC 3. Databricks CLI를 사용하여 번들을 검증, 배포, 실행 및 삭제
# MAGIC 4. 변수를 사용하여 환경별 구성을 번들에 전달
# MAGIC 5. Databricks 작업공간 UI에서 배포된 리소스를 확인

# COMMAND ----------

# DBTITLE 1,시나리오 및 배포 리소스
# MAGIC %md
# MAGIC ## 시나리오
# MAGIC
# MAGIC 1.3 데모에서 수동으로 생성한 Lakeflow Pipeline을 Declarative Automation Bundle로 묶어 배포합니다. 번들에는 파이프라인 외에도 Lakeflow Job과 SQL 알림이 포함됩니다.
# MAGIC
# MAGIC ### 번들이 배포하는 리소스
# MAGIC
# MAGIC | 리소스 | 이름 | 설명 |
# MAGIC |----------|------|-------------|
# MAGIC | **Lakeflow Pipeline** | `bakehouse-pipeline` | 메달리온 아키텍처 파이프라인 (Bronze, Silver, Gold) |
# MAGIC | **Lakeflow Job** | `bakehouse-pipeline-job` | 단일 파이프라인 태스크가 있는 수동 트리거 잡 |
# MAGIC | **SQL Alert** | `low-daily-revenue-alert` | 골드 계층의 일일 수익을 모니터링하고 임계값 미만일 때 트리거 |

# COMMAND ----------

# DBTITLE 1,번들 프로젝트 구조
# MAGIC %md
# MAGIC ## 번들 프로젝트 구조
# MAGIC
# MAGIC `4.3 DAB Demo/` 폴더에 완전한 번들 프로젝트가 포함되어 있습니다:
# MAGIC
# MAGIC ```
# MAGIC 4.3 DAB Demo/
# MAGIC   ├── databricks.yml          # 번들 구성: 파이프라인, 잡, 알림 정의, 변수, 타겟
# MAGIC   └── src/
# MAGIC       └── bakehouse_pipeline.sql  # 파이프라인 노트북 (Bronze, Silver, Gold 계층)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,databricks.yml 구성
# MAGIC %md
# MAGIC ## 번들 구성: databricks.yml
# MAGIC
# MAGIC `databricks.yml` 파일은 세 개의 리소스를 선언합니다: **파이프라인**, **잡**, **SQL 알림**. `${var.catalog}` 및 `${var.warehouse_id}` 변수는 배포 시점에 치환됩니다.
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: bakehouse-analytics
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: Unity Catalog catalog for pipeline output
# MAGIC   warehouse_id:
# MAGIC     description: SQL warehouse ID for alert execution
# MAGIC
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     bakehouse_pipeline:
# MAGIC       name: bakehouse-pipeline-${bundle.target}
# MAGIC       catalog: ${var.catalog}
# MAGIC       target: default
# MAGIC       libraries:
# MAGIC         - file:
# MAGIC             path: ./src/bakehouse_pipeline.sql
# MAGIC       configuration:
# MAGIC         catalog: ${var.catalog}
# MAGIC       channel: CURRENT
# MAGIC       continuous: false
# MAGIC       serverless: true
# MAGIC
# MAGIC   jobs:
# MAGIC     bakehouse_pipeline_job:
# MAGIC       name: bakehouse-pipeline-job-${bundle.target}
# MAGIC       tasks:
# MAGIC         - task_key: run_pipeline
# MAGIC           pipeline_task:
# MAGIC             pipeline_id: ${resources.pipelines.bakehouse_pipeline.id}
# MAGIC
# MAGIC   alerts:
# MAGIC     low_revenue_alert:
# MAGIC       display_name: low-daily-revenue-alert-${bundle.target}
# MAGIC       query_text: |
# MAGIC         SELECT transactionDate,
# MAGIC           SUM(total_revenue) AS daily_total_revenue
# MAGIC         FROM ${var.catalog}.gold.daily_sales_summary
# MAGIC         WHERE transactionDate = CURRENT_DATE() - INTERVAL 1 DAY
# MAGIC         GROUP BY transactionDate
# MAGIC         HAVING SUM(total_revenue) < 1000
# MAGIC       evaluation:
# MAGIC         comparison_operator: GREATER_THAN
# MAGIC         source:
# MAGIC           display: daily_total_revenue
# MAGIC           name: daily_total_revenue
# MAGIC         threshold:
# MAGIC           value:
# MAGIC             double_value: 0
# MAGIC       schedule:
# MAGIC         quartz_cron_schedule: "0 0 8 * * ?"
# MAGIC         timezone_id: UTC
# MAGIC         pause_status: UNPAUSED
# MAGIC       warehouse_id: ${var.warehouse_id}
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC ```
# MAGIC
# MAGIC ### 주요 구성 포인트
# MAGIC
# MAGIC - `${bundle.target}` 는 리소스 이름에 타겟 이름을 추가하여 환경 간 충돌 방지
# MAGIC - `${var.catalog}` 와 `${var.warehouse_id}` 는 배포 시점에 전달되어 YAML을 환경에 무관하게 유지
# MAGIC - `mode: development` 는 리소스에 `[dev ${user}]` 접두사를 추가하고 개발 모드 활성화
# MAGIC - `serverless: true` 는 Serverless 컴퓨트 사용 (클러스터 구성 불필요)
# MAGIC - 잡은 `${resources.pipelines.bakehouse_pipeline.id}` 로 파이프라인을 참조하며 DABs가 배포 후 자동 해석
# MAGIC - 알림은 `${var.warehouse_id}` 로 기존 SQL Warehouse를 참조하여 쿼리 실행

# COMMAND ----------

# DBTITLE 1,파이프라인 SQL 코드
# MAGIC %md
# MAGIC ## 파이프라인 코드: src/bakehouse_pipeline.sql
# MAGIC
# MAGIC 파이프라인 노트북은 1.3 데모의 동일한 Lakeflow Spark Declarative Pipelines SQL을 포함하며, Bronze, Silver, Gold 계층을 정의합니다. `${catalog}` 변수는 `databricks.yml`의 파이프라인 `configuration` 매핑에 의해 주입됩니다.
# MAGIC
# MAGIC ```sql
# MAGIC -- Bronze Layer: 원본 데이터 수집
# MAGIC CREATE OR REFRESH STREAMING TABLE ${catalog}.bronze.transactions
# MAGIC COMMENT "Raw transaction data ingested from landing zone CSV files"
# MAGIC AS SELECT 
# MAGIC     CAST(transactionID AS LONG) AS transactionID,
# MAGIC     CAST(customerID AS LONG) AS customerID,
# MAGIC     CAST(franchiseID AS LONG) AS franchiseID,
# MAGIC     CAST(dateTime AS TIMESTAMP) AS dateTime,
# MAGIC     product, CAST(quantity AS LONG) AS quantity,
# MAGIC     CAST(unitPrice AS LONG) AS unitPrice,
# MAGIC     CAST(totalPrice AS LONG) AS totalPrice,
# MAGIC     paymentMethod, CAST(cardNumber AS LONG) AS cardNumber,
# MAGIC     _metadata.file_path AS _source_file,
# MAGIC     current_timestamp() AS _ingested_at
# MAGIC FROM STREAM read_files(
# MAGIC     '/Volumes/${catalog}/bakehouse_incoming/landing_zone/sales_transactions/',
# MAGIC     format => 'csv', header => 'true', inferSchema => 'false'
# MAGIC );
# MAGIC
# MAGIC -- Silver Layer: 데이터 품질 및 CDC 처리 (제약 조건 생략)
# MAGIC CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.transactions
# MAGIC AS SELECT transactionID, customerID, franchiseID, dateTime,
# MAGIC     DATE(dateTime) AS transactionDate, product, quantity, unitPrice,
# MAGIC     totalPrice, paymentMethod, cardNumber, _ingested_at,
# MAGIC     current_timestamp() AS _processed_at
# MAGIC FROM STREAM ${catalog}.bronze.transactions;
# MAGIC
# MAGIC -- SCD Type 2 가맹점 차원
# MAGIC CREATE OR REFRESH STREAMING TABLE ${catalog}.silver.franchises
# MAGIC CLUSTER BY AUTO;
# MAGIC CREATE FLOW franchises_cdc AS AUTO CDC INTO ${catalog}.silver.franchises
# MAGIC FROM STREAM ${catalog}.bronze.franchises
# MAGIC KEYS (franchiseID) APPLY AS DELETE WHEN _operation = 'DELETE'
# MAGIC SEQUENCE BY _change_timestamp
# MAGIC COLUMNS * EXCEPT (_source_file, _ingested_at, _operation)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC
# MAGIC -- Gold Layer: 일일 매출 요약
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW ${catalog}.gold.daily_sales_summary
# MAGIC AS SELECT t.transactionDate, t.franchiseID, f.name AS franchise_name,
# MAGIC     f.country, COUNT(DISTINCT t.transactionID) AS total_transactions,
# MAGIC     COUNT(DISTINCT t.customerID) AS unique_customers,
# MAGIC     SUM(t.quantity) AS total_items_sold, SUM(t.totalPrice) AS total_revenue,
# MAGIC     AVG(t.totalPrice) AS avg_transaction_value
# MAGIC FROM ${catalog}.silver.transactions t
# MAGIC LEFT JOIN ${catalog}.silver.franchises f ON t.franchiseID = f.franchiseID
# MAGIC WHERE f.__END_AT IS NULL
# MAGIC GROUP BY t.transactionDate, t.franchiseID, f.name, f.country;
# MAGIC ```
# MAGIC
# MAGIC > 전체 코드는 원본 노트북의 `src/bakehouse_pipeline.sql` 파일을 참조하세요.

# COMMAND ----------

# DBTITLE 1,Databricks SQL 리소스 배포
# MAGIC %md
# MAGIC ## Databricks SQL 리소스 배포
# MAGIC
# MAGIC DABs는 파이프라인과 잡 외에도 **SQL Warehouse** 및 **SQL 알림** 등의 Databricks SQL 리소스를 배포할 수 있습니다.
# MAGIC
# MAGIC ### SQL Warehouse
# MAGIC
# MAGIC 번들 구성에서 직접 SQL Warehouse를 선언하여 여러 환경에서 일관되게 프로비저닝할 수 있습니다:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   sql_warehouses:
# MAGIC     analytics_warehouse:
# MAGIC       name: analytics-warehouse-${bundle.target}
# MAGIC       cluster_size: "2X-Small"
# MAGIC       enable_serverless_compute: true
# MAGIC       max_num_clusters: 1
# MAGIC       min_num_clusters: 1
# MAGIC       auto_stop_mins: 10
# MAGIC       warehouse_type: PRO
# MAGIC ```
# MAGIC
# MAGIC > **참고:** 이 데모에서는 Warehouse 생성 권한 제한으로 인해 `warehouse_id` 변수로 기존 Warehouse를 참조합니다.
# MAGIC
# MAGIC ### SQL 알림
# MAGIC
# MAGIC SQL 알림은 쿼리 결과를 임계값과 비교하여 모니터링하는 DBSQL 핵심 기능입니다. 알림은 일정에 따라 쿼리를 실행하고 조건 충족 시 알림을 트리거합니다.

# COMMAND ----------

# DBTITLE 1,컴퓨트 환경 요구사항
# MAGIC %md
# MAGIC ## 컴퓨트 환경 요구사항
# MAGIC
# MAGIC 이 노트북은 **Environment v5** 이상의 **Serverless** 컴퓨트 리소스에 연결되어야 합니다.
# MAGIC
# MAGIC - 노트북 툴바에서 **Connect**를 클릭하고 **Serverless** 컴퓨트 리소스 선택
# MAGIC - **Environment** 패널에서 **Base environment**가 **Standard v5** 이상인지 확인

# COMMAND ----------

# DBTITLE 1,CLI 명령 실행 방식
# MAGIC %md
# MAGIC ## 노트북에서 CLI 명령 실행
# MAGIC
# MAGIC 이 데모에서는 `%sh` 셀을 사용하여 노트북에서 직접 Databricks CLI 명령을 실행합니다. 데모 첫 부분에서 CLI를 설치하고 인증을 구성합니다.
# MAGIC
# MAGIC - 각 `%sh` 셀은 연결된 컴퓨트 리소스에서 셸 명령을 실행
# MAGIC - **전용(범용)** 및 **Serverless** 컴퓨트 모두에서 작동
# MAGIC - **Web Terminal**(`>_` 아이콘)에서도 동일한 명령 실행 가능 (대화형 탐색에 유용)

# COMMAND ----------

# DBTITLE 1,변수 전달 방식
# MAGIC %md
# MAGIC ## 변수 전달 방식
# MAGIC
# MAGIC 번들은 `${var.catalog}` 변수를 통해 **다중 환경 배포**를 지원합니다. 동일한 YAML이 파이프라인을 정의하지만, 카탈로그는 배포 위치(dev, staging, production)에 따라 달라집니다.
# MAGIC
# MAGIC | 방법 | 예시 | 사용 시기 |
# MAGIC |--------|---------|-------------|
# MAGIC | **CLI 인수** | `--var="catalog=my_catalog"` | 대화형 사용, 일회성 배포 |
# MAGIC | **환경 변수** | `export CATALOG=my_catalog` | CI/CD 파이프라인, 자동화된 워크플로우 |
# MAGIC | **타겟 기본값** | `databricks.yml` 변수에 `default` 설정 | 환경별 고정 값 |
# MAGIC
# MAGIC 이 데모에서는 `CATALOG`와 `WAREHOUSE_ID` 환경 변수를 `--var` 플래그로 CLI에 전달합니다.
# MAGIC
# MAGIC > **프로덕션 패턴:** CI/CD 파이프라인(GitHub Actions, Azure DevOps, GitLab CI)에서는 `CATALOG`와 `WAREHOUSE_ID`를 파이프라인 시크릿으로 설정한 후 동일한 방식으로 전달합니다. 번들 YAML에는 환경별 값이 포함되지 않습니다.

# COMMAND ----------

# DBTITLE 1,카탈로스 설정
# MAGIC %sql
# MAGIC -- 현재 Catalog를 dbrx_demo로 설정합니다.  
# MAGIC USE CATALOG dbrx_demo;

# COMMAND ----------

# DBTITLE 1,스키마 초기화
# MAGIC %sql
# MAGIC --
# MAGIC -- 스키마 삭제 후 재생성
# MAGIC --
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS bronze CASCADE;
# MAGIC DROP SCHEMA IF EXISTS silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS gold CASCADE;
# MAGIC
# MAGIC CREATE SCHEMA bronze;
# MAGIC CREATE SCHEMA silver;
# MAGIC CREATE SCHEMA gold;

# COMMAND ----------

# DBTITLE 1,강의실 설정
# Databricks CLI 인증, PATH, 카탈로그 환경 변수 설정
import os
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
os.environ["DATABRICKS_HOST"] = ctx.apiUrl().get()
os.environ["DATABRICKS_TOKEN"] = ctx.apiToken().get()
# CATALOG 환경 변수 설정 — %sh 셀에서 하드코딩 없이 $CATALOG 참조 가능
os.environ["CATALOG"] = spark.sql("SELECT current_catalog()").first()[0]
# SQL 알림 배포용 WAREHOUSE_ID 설정 (작업공간의 기존 Warehouse 사용)
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
warehouses = [wh for wh in w.warehouses.list()]
if warehouses:
    os.environ["WAREHOUSE_ID"] = warehouses[0].id
# 전용 및 Serverless 컴퓨트 모두에서 CLI를 찾을 수 있도록 ~/bin을 PATH에 추가
home_bin = os.path.join(os.path.expanduser("~"), "bin")
if home_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = home_bin + ":" + os.environ.get("PATH", "")

# COMMAND ----------

# MAGIC %sh
# MAGIC # Databricks CLI 설치 (전용 및 Serverless 컴퓨트 모두에서 작동)
# MAGIC # sudo 권한이 있으면 기존 시스템 래퍼 제거 (전용 컴퓨트만 해당)
# MAGIC sudo rm -f /root/bin/databricks /usr/local/bin/databricks 2>/dev/null || true
# MAGIC # CLI를 ~/bin/databricks에 설치
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.298.0/install.sh | sh
# MAGIC # 전용 컴퓨트에서는 시스템 경로로 이동하여 래퍼 교체
# MAGIC sudo mv ~/bin/databricks /usr/local/bin/databricks 2>/dev/null || true
# MAGIC # 설치 확인
# MAGIC databricks -v

# COMMAND ----------

# DBTITLE 1,변수 확인
# MAGIC %sh
# MAGIC echo "CATALOG is set to: ${CATALOG}"
# MAGIC echo "WAREHOUSE_ID is set to: ${WAREHOUSE_ID}"

# COMMAND ----------

# DBTITLE 1,4단계: 번들 검증
# MAGIC %md
# MAGIC ## 4단계: 번들 검증
# MAGIC
# MAGIC `bundle validate` 명령은 YAML 구문을 확인하고, 변수 참조를 해석하며, 모든 리소스 정의가 올바른지 검증합니다.
# MAGIC
# MAGIC **검증 항목:**
# MAGIC - 파이프라인 이름에 타겟 접미사 포함 여부
# MAGIC - `catalog`와 `warehouse_id` 변수 해석 확인
# MAGIC - 잡의 파이프라인 태스크가 올바른 파이프라인 ID 참조
# MAGIC - 알림이 Warehouse ID 참조 확인

# COMMAND ----------

# DBTITLE 1,bundle validate
# MAGIC %sh
# MAGIC cd "4.3 DAB Demo"
# MAGIC databricks bundle validate --var="catalog=${CATALOG}" --var="warehouse_id=${WAREHOUSE_ID}"

# COMMAND ----------

# DBTITLE 1,5단계: 번들 배포
# MAGIC %md
# MAGIC ## 5단계: 번들 배포
# MAGIC
# MAGIC `bundle deploy` 명령은 선언된 모든 리소스(파이프라인, 잡, 알림)를 생성 또는 업데이트합니다.
# MAGIC
# MAGIC 단일 명령으로 DABs가 세 개의 리소스를 생성합니다: Lakeflow Pipeline, 파이프라인 태스크가 있는 Lakeflow Job, 골드 계층 수익을 모니터링하는 SQL Alert입니다. CI/CD 파이프라인에서는 `main` 브랜치 머지 시 자동 실행됩니다.

# COMMAND ----------

# DBTITLE 1,bundle deploy
# MAGIC %sh
# MAGIC cd "4.3 DAB Demo"
# MAGIC databricks bundle deploy -t dev --var="catalog=${CATALOG}" --var="warehouse_id=${WAREHOUSE_ID}"

# COMMAND ----------

# DBTITLE 1,6단계: 배포된 리소스 확인
# MAGIC %md
# MAGIC ## 6단계: 배포된 리소스 확인
# MAGIC
# MAGIC Databricks 작업공간 UI에서 배포된 리소스를 확인합니다:
# MAGIC
# MAGIC ### 잡 확인
# MAGIC 1. **Jobs & Pipelines**에서 **`[dev <username>] bakehouse-pipeline-job-dev`** 잡 확인
# MAGIC 2. 단일 **`run_pipeline`** 태스크 확인
# MAGIC
# MAGIC ### 파이프라인 확인
# MAGIC 1. **Pipelines** 필터에서 **`[dev <username>] bakehouse-pipeline-dev`** 확인
# MAGIC 2. Bronze, Silver, Gold 계층이 있는 DAG 확인
# MAGIC
# MAGIC ### 알림 확인
# MAGIC 1. **Alerts**에서 **`[dev <username>] low-daily-revenue-alert-dev`** 확인
# MAGIC 2. 쿼리, 스케줄, Warehouse 할당 확인
# MAGIC
# MAGIC > **개발 모드 이름 지정:** `mode: development`로 배포하여 모든 리소스에 `[dev <username>]` 접두사가 자동 추가됩니다. 여러 개발자가 동일한 작업공간에 배포할 때 이름 충돌을 방지합니다.

# COMMAND ----------

# DBTITLE 1,7단계: 실행 및 삭제
# MAGIC %md
# MAGIC ## 7단계 (선택): 실행 및 삭제
# MAGIC
# MAGIC CLI 또는 작업공간 UI에서 직접 잡을 트리거할 수 있습니다.
# MAGIC
# MAGIC > **경고: `bundle destroy`는 모든 리소스를 삭제합니다.** `databricks bundle destroy`를 실행하면 잡, 파이프라인, 알림이 삭제됩니다. 파이프라인이 실행된 적이 있는 경우, 생성된 스트리밍 테이블과 구체화된 뷰도 약 5분 내에 제거됩니다. 이 작업은 되돌릴 수 없습니다.

# COMMAND ----------

# DBTITLE 1,bundle run & destroy
# MAGIC %sh
# MAGIC cd "4.3 DAB Demo"
# MAGIC # Run the job from the CLI
# MAGIC databricks bundle run bakehouse_pipeline_job --var="catalog=${CATALOG}" --var="warehouse_id=${WAREHOUSE_ID}"
# MAGIC
# MAGIC # When finished, destroy all deployed resources
# MAGIC databricks bundle destroy --auto-approve --var="catalog=${CATALOG}" --var="warehouse_id=${WAREHOUSE_ID}"

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC * **선언적 구성으로 재현 가능한 배포** — `databricks.yml`과 파이프라인 SQL 파일만으로 리소스 생성, 종속성, 수명 주기를 관리
# MAGIC * **DBSQL 리소스도 번들로 관리** — SQL 알림, Warehouse, 대시보드, 쿼리를 파이프라인/잡과 함께 YAML로 선언
# MAGIC * **변수로 다중 환경 배포** — `${var.catalog}`와 `${var.warehouse_id}` 변수가 배포 시점에 해석되어 코드 수정 없이 다른 환경에 배포 가능
# MAGIC * **개발 모드로 충돌 방지** — `[dev <username>]` 접두사로 모든 개발자가 동일한 작업공간에 안전하게 배포
# MAGIC * **CLI 중심 워크플로우** — `bundle validate` → `bundle deploy` → `bundle run` → `bundle destroy`가 전체 수명 주기 제공. 프로덕션에서는 CI/CD 파이프라인이 자동 실행
# MAGIC * **수동 클릭 불필요** — 1.3 데모에서 UI로 생성한 모든 것을 YAML로 선언하고 단일 명령으로 배포