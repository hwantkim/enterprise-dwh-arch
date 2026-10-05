# Databricks notebook source
# MAGIC %md
# MAGIC # 대규모 환경에서의 데이터 공유
# MAGIC

# COMMAND ----------

# DBTITLE 1,공유 vs 복제
# MAGIC %md
# MAGIC ## 1. 공유 vs. 복제
# MAGIC
# MAGIC | | 데이터 공유 (실시간 액세스) | 데이터 복제 (물리적 복사) |
# MAGIC |---|---|---|
# MAGIC | **원칙** | 제공자 스토리지에서 읽기 — 데이터 이동 없음 | 소비자 환경에 물리적 복사본 생성 |
# MAGIC | **신선도** | 항상 최신 | 복제 주기에 따라 결정 |
# MAGIC | **비용** | 제공자가 스토리지 부담 | 소비자가 전체 스토리지 비용 부담 |
# MAGIC | **거버넌스** | 제공자 관리, 즉시 권한 회수 가능 | 소비자 독립 관리, 복제 후 권한 회수 불가 |
# MAGIC
# MAGIC - **공유 유리**: 읽기 전용, 최신 데이터, 거버넌스 우선
# MAGIC - **복제 유리**: 로컬 변환/조인, 지연 민감 워크로드, 데이터 주권, DR

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Sharing
# MAGIC
# MAGIC - **오픈 프로토콜** (Linux Foundation 기여) — 데이터 복사 없이 조직 간 실시간 액세스
# MAGIC - 제공자가 Share 생성 → 수신자에게 presigned URL 발급 → 클라우드 스토리지에서 직접 읽기
# MAGIC
# MAGIC | 모델 | 설명 | 사용 시기 |
# MAGIC |-------|------|-----------|
# MAGIC | **Databricks-to-Databricks (D2D)** | Databricks 계정 간 네이티브 공유 (카탈로그로 표시) | 양쪽 모두 Databricks |
# MAGIC | **Databricks-to-Open (D2O)** | 비-Databricks 클라이언트로 오픈 프로토콜 공유 | 수신자가 Databricks 아님 (라이선스 불필요) |
# MAGIC
# MAGIC **주요 이점**: 데이터 복사 없음, 벤더 중립적, 완전한 거버넌스, 감사 추적

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Marketplace 및 Clean Rooms
# MAGIC
# MAGIC | 솔루션 | 목적 | 특징 |
# MAGIC |--------|------|------|
# MAGIC | **Databricks Marketplace** | 데이터 프로덕트 교환/수익화 | 검색·게시·소비 플랫폼, 사용량 추적 |
# MAGIC | **Clean Rooms** | 프라이버시 보호 다자 분석 | 원본 데이터 노출 없이 집계 쿼리만 실행 |
# MAGIC
# MAGIC - **Delta Sharing**: 점대점 파트너 공유
# MAGIC - **Marketplace**: 광범위 데이터 프로덕트 게시
# MAGIC - **Clean Rooms**: 원본 데이터 비공개 다자 분석

# COMMAND ----------

# DBTITLE 1,Delta 클로닝
# MAGIC %md
# MAGIC ## 4. Delta 클로닝
# MAGIC
# MAGIC | 특징 | Deep Clone | Shallow Clone |
# MAGIC |------|-----------|---------------|
# MAGIC | **데이터** | 전체 물리적 복사 | 메타데이터만 (원본 Parquet 참조) |
# MAGIC | **비용** | 스토리지 2배 | 최소 (메타데이터만) |
# MAGIC | **독립성** | 완전 독립 | 소스 파일 의존 |
# MAGIC | **위험** | 소스와 동기화 불일치 | VACUUM 시 클론 손상 가능 |
# MAGIC | **사용 시기** | DR, 크로스 환경 프로모션 | 테스트, 실험, 샌드박스 |
# MAGIC
# MAGIC ```sql
# MAGIC -- Deep Clone
# MAGIC CREATE TABLE prod_catalog.finance.transactions_backup
# MAGIC DEEP CLONE staging_catalog.finance.transactions;
# MAGIC
# MAGIC -- Shallow Clone
# MAGIC CREATE TABLE dev_catalog.sandbox.transactions_test
# MAGIC SHALLOW CLONE prod_catalog.finance.transactions;
# MAGIC
# MAGIC -- 특정 버전 Shallow Clone
# MAGIC CREATE TABLE dev_catalog.sandbox.transactions_v42
# MAGIC SHALLOW CLONE prod_catalog.finance.transactions VERSION AS OF 42;
# MAGIC ```
# MAGIC
# MAGIC > ⚠️ Shallow Clone은 소스 Parquet 파일을 참조하므로, 소스에서 `VACUUM` 실행 시 클론이 손상될 수 있습니다.

# COMMAND ----------

# DBTITLE 1,데이터 복제 기법
# MAGIC %md
# MAGIC ## 5. 데이터 복제 기법
# MAGIC
# MAGIC | 기법 | 메커니즘 | 적합한 경우 |
# MAGIC |------|----------|-------------|
# MAGIC | **Deep Clone** | `DEEP CLONE` | 일회성 마이그레이션, DR |
# MAGIC | **CDF + Streaming** | Change Data Feed 행 수준 스트리밍 | 증분 근실시간 복제 |
# MAGIC | **CTAS / INSERT OVERWRITE** | SQL 덮어쓰기 | 소규모 배치 복제 |
# MAGIC | **Cloudflare R2** | 제로 이그레스 중간 스토리지 | 크로스 클라우드 공유 |
# MAGIC
# MAGIC ### Change Data Feed (CDF)
# MAGIC
# MAGIC CDF는 행 수준 변경(insert/update/delete)을 기록하여 전체 스냅샷 대신 변경분만 전송:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.table 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC
# MAGIC SELECT * FROM table_changes('prod_catalog.finance.transactions', 5, 10)
# MAGIC WHERE _change_type IN ('insert', 'update_postimage');
# MAGIC
# MAGIC SELECT * FROM table_changes('prod_catalog.finance.transactions', '2025-01-01T00:00:00')
# MAGIC WHERE _change_type != 'update_preimage';
# MAGIC ```
# MAGIC
# MAGIC CDF는 Delta Sharing을 통해 작동 — 전체 스캔 없이 증분 변경 소비 가능.

# COMMAND ----------

# DBTITLE 1,Cloudflare R2
# MAGIC %md
# MAGIC ## 6. Cloudflare R2 를 활용한 크로스 클라우드 복제
# MAGIC
# MAGIC - **S3 호환 객체 스토리지, 제로 이그레스 요금**
# MAGIC - Provider → R2 (External Table) → Recipient (View / MERGE)
# MAGIC - Unity Catalog에 R2 버킷을 External Location으로 등록
# MAGIC
# MAGIC | 항목 | 상세 |
# MAGIC |------|------|
# MAGIC | **프로토콜** | S3 호환 API |
# MAGIC | **Delta 지원** | Time Travel 포함 전체 읽기/쓰기 |
# MAGIC | **비용** | 제로 이그레스; 스토리지/운영 비용만 |
# MAGIC | **제한** | 별도 자격 증명 관리; 일부 기능 미검증 |
# MAGIC
# MAGIC **사용 시기**: AWS·Azure·GCP 멀티 클라우드, 이그레스 비용이 주요 비용인 경우

# COMMAND ----------

# DBTITLE 1,의사결정 프레임워크
# MAGIC %md
# MAGIC ## 7. 적절한 접근 방식 선택
# MAGIC
# MAGIC | 질문 | Yes | No |
# MAGIC |------|-----|-----|
# MAGIC | 같은 Metastore? | **Unity Catalog** 권한 | Delta Sharing 또는 복제 |
# MAGIC | 항상 최신 읽기 전용? | **Delta Sharing** | 복제 고려 |
# MAGIC | 로컬 변환/조인 필요? | **복제** (Deep Clone/CDF) | Delta Sharing 충분 |
# MAGIC | 이그레스가 주요 비용? | **Cloudflare R2** | 같은 리전 Delta Sharing |
# MAGIC | 수신자 비-Databricks? | **D2O** | **D2D** |
# MAGIC | DR 필요? | **Deep Clone** + **CDF** 동기화 | 실시간 공유 |
# MAGIC
# MAGIC > ❌ **불필요한 데이터 복제 방지**: Delta Sharing으로 시작하고, 실제 로컬 복사본이 필요한 경우에만 복제 추가. 두 복사본은 결국 분기됨.

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC - **복제보다 먼저 공유** — Delta Sharing을 기본으로 사용
# MAGIC - **Delta Sharing은 오픈 표준** (Linux Foundation) — 수신자에 Databricks 불필요 (D2D/D2O)
# MAGIC - **Marketplace & Clean Rooms** — 데이터 프로덕트 거래 / 프라이버시 보호 다자 분석
# MAGIC - **클로닝은 Delta Lake 고유 기능** — Shallow Clone: 제로 비용 테스트 환경 / Deep Clone: 완전 격리 DR
# MAGIC - **CDF로 효율적 증분 복제** — 전체 스냅샷 대신 변경분만 처리
# MAGIC - **Cloudflare R2** — 크로스 클라우드 제로 이그레스 중개자