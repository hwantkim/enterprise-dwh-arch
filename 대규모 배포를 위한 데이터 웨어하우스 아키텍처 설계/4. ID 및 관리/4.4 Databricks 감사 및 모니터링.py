# Databricks notebook source
# DBTITLE 1,요약 제목
# MAGIC %md
# MAGIC # 4.4 Databricks 감사 및 모니터링
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. 감사 및 모니터링의 3대 목적
# MAGIC %md
# MAGIC ## 1. 감사 및 모니터링의 3대 목적
# MAGIC
# MAGIC | 목적 | 핵심 질문 | 관련 도구 |
# MAGIC |------|-----------|----------|
# MAGIC | **보안 및 규정 준수** | 누가 민감한 데이터에 접근했는가? 권한이 변경되었는가? | `system.access.audit`, 상세 감사 로그 |
# MAGIC | **비용 관리** | 어떤 Warehouse가 과도하게 프로비저닝되었는가? | `system.billing.usage`, 예산 정책 |
# MAGIC | **운영 상태** | 파이프라인이 실패하고 있지 않은가? 데이터 품질이 저하되고 있지 않은가? | 파이프라인 이벤트 로그, Lakehouse Monitoring |

# COMMAND ----------

# DBTITLE 1,2. 모니터링 아키텍처
# MAGIC %md
# MAGIC ## 2. Databricks 모니터링 아키텍처 (3계층)
# MAGIC
# MAGIC ```
# MAGIC [이벤트 생성 주체] → [이벤트 수집 방식] → [이벤트 처리 방식]
# MAGIC ```
# MAGIC
# MAGIC * **생성 주체**: SQL Warehouses, Lakeflow Jobs & Pipelines, Notebook & Cluster, Unity Catalog, ID 및 접근
# MAGIC * **수집 방식**: 시스템 테이블(`system.access.audit`), 청구 테이블(`system.billing.usage`), 파이프라인 이벤트 로그(`event_log()`), 클러스터 로그 전송, Lakehouse Monitoring
# MAGIC * **처리 방식**: SQL Alerts, AI/BI Dashboards, 예산 정책, 외부 SIEM / APM

# COMMAND ----------

# DBTITLE 1,3. 시스템 테이블
# MAGIC %md
# MAGIC ## 3. 시스템 테이블: Databricks 감사의 기반
# MAGIC
# MAGIC 시스템 테이블은 `system` 카탈로그에 있는 관리형 Delta 테이블로, 표준 SQL로 쿼리할 수 있습니다. metastore 관리자 권한 또는 명시적 권한 부여가 필요합니다.
# MAGIC
# MAGIC | 시스템 테이블 | 용도 |
# MAGIC |-------------|---------|
# MAGIC | `system.access.audit` | 워크스페이스 및 계정 전체의 모든 감사 이벤트 |
# MAGIC | `system.billing.usage` | 제품, SKU, 리소스별 DBU 소비 |
# MAGIC | `system.billing.list_prices` | 시간 경과에 따른 SKU별 정가 |
# MAGIC | `system.compute.clusters` | Cluster 수명 주기 및 구성 변경 |
# MAGIC | `system.lakeflow.jobs` | Job 메타데이터(이름, 일정, 소유자) |
# MAGIC | `system.lakeflow.job_run_timeline` | Job 실행 기간 및 결과 |
# MAGIC
# MAGIC > **감사 테이블 접근**: `GRANT SELECT ON system.access.audit TO <group>`을 사용하여 워크스페이스 관리자 권한 없이 보안 팀이 감사 쿼리를 실행할 수 있도록 설정할 수 있습니다.

# COMMAND ----------

# DBTITLE 1,4. Serverless SQL Warehouse 감사 이벤트
# MAGIC %md
# MAGIC ## 4. Serverless SQL Warehouse 감사 이벤트
# MAGIC
# MAGIC `databrickssql` 서비스는 Warehouse, 쿼리, 대시보드, Alert에 대한 이벤트를 로깅합니다.
# MAGIC
# MAGIC | 카테고리 | 주요 이벤트 |
# MAGIC |----------|------------|
# MAGIC | **Warehouse 수명 주기** | `createWarehouse`, `editWarehouse`, `startWarehouse`/`stopWarehouse`, `deleteWarehouse`, `setWarehouseConfig`, `changeEndpointAcls` |
# MAGIC | **쿼리 활동** | `executeAdhocQuery`, `executeSavedQuery`, `commandSubmit`, `commandFinish`, `downloadQueryResult`, `listHistoryQueries` |
# MAGIC | **대시보드 및 Alert** | `createAlert`/`updateAlert`, `snapshotDashboard`, `createQuery`/`updateQuery`, `transferObjectOwnership` |
# MAGIC | **AI/BI Dashboards** | `getDashboard`, `getPublishedDashboard`, `executeQuery`, `publishDashboard`, `createSchedule`, `sendDashboardSnapshot` |

# COMMAND ----------

# DBTITLE 1,5. 상세 감사 로그
# MAGIC %md
# MAGIC ## 5. 상세 감사 로그 (Verbose Audit Logs)
# MAGIC
# MAGIC 기본적으로 감사 로그는 메타데이터(누가, 언제, 어느 Warehouse)만 캡처하고 SQL 텍스트는 캡처하지 않습니다. **상세 감사 로그**는 전체 SQL 명령 텍스트를 추가합니다.
# MAGIC
# MAGIC | 이벤트 | 서비스 | 캡처 내용 |
# MAGIC |-------|---------|-----------------|
# MAGIC | `notebook.runCommand` | notebook | 대화형 Notebook에서 셀 실행, `commandText` 포함 |
# MAGIC | `jobs.runCommand` | jobs | Job 실행 중 셀 실행, `commandText` 포함 |
# MAGIC | `databrickssql.commandSubmit` | databrickssql | Warehouse에 제출된 SQL 명령, `commandText` 및 `warehouseId` 포함 |
# MAGIC | `databrickssql.commandFinish` | databrickssql | SQL 명령 완료/취소, 상태 및 오류 메시지 포함 |
# MAGIC
# MAGIC > **주의**: 상세 감사 로그는 로그 볼륨을 크게 증가시키며, 쿼리 리터럴에 민감한 정보가 포함될 수 있습니다. **관리자 설정 > 고급 > Verbose Audit Logs**에서 활성화합니다. 규정 준수 요구사항의 트레이드오프로 간주해야 합니다.

# COMMAND ----------

# DBTITLE 1,6. 파이프라인 이벤트 로그 & 클러스터 로그 전송
# MAGIC %md
# MAGIC ## 6. 파이프라인 이벤트 로그 & 클러스터 로그 전송
# MAGIC
# MAGIC ### Lakeflow Pipeline 이벤트 로그
# MAGIC
# MAGIC `event_log()` 테이블 반환 함수로 접근하며, 감사 시스템 테이블에서 사용할 수 없는 파이프라인 특정 이벤트를 캡처합니다.
# MAGIC
# MAGIC | 이벤트 유형 | 캡처 내용 |
# MAGIC |-----------|-----------------|
# MAGIC | `flow_progress` | 처리된 레코드, 처리량, 데이터 품질 기대(Expectation) 결과 |
# MAGIC | `planning_information` | 쿼리 계획 및 최적화 결정 |
# MAGIC | `cluster_resources` | 파이프라인 업데이트 중 컴퓨팅 활용률 |
# MAGIC | `update_progress` | 전체 파이프라인 업데이트 상태 및 기간 |
# MAGIC
# MAGIC ### 클러스터 로그 전송
# MAGIC
# MAGIC * **대상**: 클래식 컴퓨팅(범용 Cluster, 클래식 SQL Warehouse)
# MAGIC * **전송 로그**: 드라이버 로그, 실행자 로그, 초기화 스크립트 로그, 환경 로그 → S3 또는 ADLS
# MAGIC * **Serverless**: 외부 스토리지로의 클러스터 로그 전송 미지원. 시스템 테이블과 파이프라인 이벤트 로그에 의존

# COMMAND ----------

# DBTITLE 1,7. Lakehouse Monitoring
# MAGIC %md
# MAGIC ## 7. Lakehouse Monitoring: 데이터 및 모델 품질
# MAGIC
# MAGIC 감사 로그가 **누가 무엇을 했는지** 추적한다면, Lakehouse Monitoring은 **데이터가 건강한지 여부**를 추적합니다. Unity Catalog 테이블에 모니터를 연결하여 자동 프로파일링, 드리프트 감지, 시간에 따른 품질 추적을 수행합니다.
# MAGIC
# MAGIC | 카테고리 | 기능 |
# MAGIC |----------|------|
# MAGIC | **데이터 품질 모니터링** | 스냅샷 분석, 시계열 분석, 사용자 정의 메트릭 |
# MAGIC | **ML 모델 품질 모니터링** | 추론 로깅, 드리프트 감지, 성능 추적 |
# MAGIC
# MAGIC > 데이터 웨어하우징 실무자에게 특히 중요한 것은 **골드 계층(Gold Layer) 테이블**에 모니터를 연결하는 것입니다. 예: `gold.daily_sales_summary`에 모니터를 설정하면 수익 수치가 0으로 떨어지거나 널 비율이 급증할 때 다운스트림 소비자에게 도달하기 전에 알림을 받을 수 있습니다.

# COMMAND ----------

# DBTITLE 1,8. SQL Alert 및 예산 정책
# MAGIC %md
# MAGIC ## 8. SQL Alert 및 예산 정책
# MAGIC
# MAGIC ### SQL Alert
# MAGIC
# MAGIC SQL Alert는 일정에 따라 SQL Warehouse에 대해 쿼리를 실행하고, 조건이 충족되면 알림을 트리거합니다.
# MAGIC
# MAGIC | Alert 패턴 | 예시 쿼리 | 트리거 |
# MAGIC |--------------|---------------|---------|
# MAGIC | **데이터 신선도** | `SELECT DATEDIFF(CURRENT_DATE, MAX(load_date)) FROM gold.daily_sales` | Value > 1 |
# MAGIC | **파이프라인 실패** | `SELECT COUNT(*) FROM event_log(TABLE(silver.transactions)) WHERE event_type = 'flow_progress' AND ...` | Value > 0 |
# MAGIC | **비용 급증** | `SELECT SUM(usage_quantity) FROM system.billing.usage WHERE usage_date = CURRENT_DATE AND ...` | Value > threshold |
# MAGIC | **권한 변경** | `SELECT COUNT(*) FROM system.access.audit WHERE action_name = 'changeEndpointAcls' AND event_date = CURRENT_DATE` | Value > 0 |
# MAGIC | **데이터 품질** | `SELECT COUNT(*) FROM gold.franchise_performance WHERE total_revenue < 0` | Value > 0 |
# MAGIC
# MAGIC 알림은 이메일, Slack(웹훅), PagerDuty 또는 사용자 정의 알림 대상으로 전송할 수 있습니다.
# MAGIC
# MAGIC ### 계정 수준 예산 정책
# MAGIC
# MAGIC * Serverless 리소스(Warehouse, Job, Pipeline)에 **예산 정책** 연결 가능
# MAGIC * `custom_tags` 컬럼을 통해 `system.billing.usage`에 나타남
# MAGIC * SQL Alert와 시스템 테이블 쿼리를 결합하면 능동적 비용 제어 메커니즘 구축 가능

# COMMAND ----------

# DBTITLE 1,9. 엔터프라이즈 통합
# MAGIC %md
# MAGIC ## 9. 엔터프라이즈 통합
# MAGIC
# MAGIC Databricks는 외부 SIEM 도구에 대한 단일 내장 커넥터를 제공하지 않습니다. 대신 **클라우드 스토리지로의 로그 전송**을 주요 통합 지점으로 사용합니다.
# MAGIC
# MAGIC ```
# MAGIC [Databricks Platform] → [Cloud Storage (S3/ADLS)] → [SIEM / APM 플랫폼]
# MAGIC ```
# MAGIC
# MAGIC ### 외부 로그 통합 방법
# MAGIC
# MAGIC | 방법 | 적합 사례 |
# MAGIC |------|----------|
# MAGIC | **S3/ADLS로 감사 로그 전송** | SIEM 수집(Splunk, Sentinel, CloudWatch) |
# MAGIC | **클러스터 로그 전송** | 심층 디버깅, 클래식 컴퓨팅 진단 |
# MAGIC | **Unity Catalog Volumes** | SQL로 쿼리 가능한 영구 로그 저장소 |
# MAGIC | **APM 에이전트 통합** | Databricks Apps를 위한 Datadog, New Relic |
# MAGIC | **시스템 테이블 내보내기** | 로그 전송 구성 없이 경량 통합 |

# COMMAND ----------

# DBTITLE 1,10. 엔터프라이즈 모니터링 모범 사례
# MAGIC %md
# MAGIC ## 10. 엔터프라이즈 모니터링 모범 사례
# MAGIC
# MAGIC | 카테고리 | 모범 사례 |
# MAGIC |----------|----------|
# MAGIC | **보안 및 규정 준수** | 상세 감사 로그 활성화, 감사 로그 전송 구성, 구조화된 JSON 로깅, 민감한 데이터 마스킹 |
# MAGIC | **비용 거버넌스** | 모든 Serverless 리소스에 예산 정책 연결, AI/BI 대시보드 구축, 일일 DBU 소비에 SQL Alert 생성, 사용자 정의 태그로 비용 귀속 |
# MAGIC | **운영 상태** | 골드 계층 테이블에 Lakehouse Monitor 연결, 파이프라인 이벤트 로그로 품질 추적, 데이터 신선도/널 비율 SQL Alert 설정, 로그에 사용자 ID 및 컨텍스트 포함 |
# MAGIC | **엔터프라이즈 통합** | SIEM으로 감사 로그 전달, APM 도구로 Databricks Apps 모니터링, CloudWatch/Azure Monitor 수집 구성, Unity Catalog Volumes에 영구 로그 작성 |