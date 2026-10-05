# Databricks notebook source
# DBTITLE 1,요약 제목
# MAGIC %md
# MAGIC # 대규모 데이터 웨어하우스 수집
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. 클라우드 스토리지 설계
# MAGIC %md
# MAGIC ## 1. 클라우드 스토리지 설계
# MAGIC
# MAGIC 스토리지 아키텍처는 기반이 되며, 나중에 변경하기 매우 어렵습니다. 다음 6가지를 사전에 계획해야 합니다:
# MAGIC
# MAGIC | 고려 사항 | 핵심 포인트 |
# MAGIC |---|---|
# MAGIC | **버킷/컨테이너 전략** | 환경(dev/staging/prod) 및 데이터 민감도별 분리 |
# MAGIC | **경로 규칙** | 일관된 계층 구조로 검색성과 자동화 확보 |
# MAGIC | **리전 정렬** | 스토리지와 컴퓨팅 같은 리전 배치로 비용/지연 최소화 |
# MAGIC | **스토리지 클래스** | 활성 데이터는 Standard, 콜드 데이터는 Infrequent Access |
# MAGIC | **교차 계정 액세스** | IAM 역할 사용, 장기 액세스 키 금지 (하나의 대표 계정으로 다른 여러 계정의 자원에 안전하게 접근) |
# MAGIC | **암호화 및 키 관리** | CMK 기반 서버 측 암호화, 자동 키 순환 |
# MAGIC
# MAGIC ### 오픈 테이블 포맷 선택
# MAGIC
# MAGIC * **Delta Lake**: Databricks 네이티브 포맷, Liquid Clustering/Predictive Optimization 등의 기능 지원
# MAGIC * **Apache Iceberg**: 벤더 중립 포맷, 다양한 엔진 간 상호 운용성 제공
# MAGIC * **UniForm**: Delta 테이블을 Iceberg로 노출하여 중복 없이 외부 엔진 호환
# MAGIC * 순수 Databricks 워크로드 → Delta Lake 권장

# COMMAND ----------

# DBTITLE 1,2. 랜딩 존 아키텍처
# MAGIC %md
# MAGIC ## 2. 랜딩 존 아키텍처
# MAGIC
# MAGIC 랜딩 존은 외부 소스 시스템과 Bronze 레이어 사이의 **파일 스테이징 영역**입니다.
# MAGIC
# MAGIC ```
# MAGIC 외부 소스 → 랜딩 존(원시 파일) → Bronze(원시 Delta) → 검증/정제 → Silver(검증됨)
# MAGIC                                                               ↓ 실패 → 격리 → 알림/검토
# MAGIC ```
# MAGIC
# MAGIC **핵심 원칙:**
# MAGIC * Bronze는 원시 데이터를 있는 그대로 보존 (감사 가능성, 재생 목적)
# MAGIC * 데이터 품질 검증은 Bronze-to-Silver 전환에서 수행
# MAGIC * 소스 시스템과 수집 프로세스를 분리하여 독립적 운영
# MAGIC * 수집 성공 후 랜딩 존 데이터 자동 보관/삭제

# COMMAND ----------

# DBTITLE 1,3. 수집 패턴 선택
# MAGIC %md
# MAGIC ## 3. 배치 vs 스트리밍 수집 패턴
# MAGIC
# MAGIC | 구분 | 배치/증분 수집 | 스트리밍 수집 | 하이브리드 |
# MAGIC |---|---|---|---|
# MAGIC | **적합한 경우** | 야간 로드, 백필, 비용 민감 워크로드 | 실시간 대시보드, 사기 탐지 | 대부분의 엔터프라이즈 |
# MAGIC | **장점** | 간단한 오류 처리, 낮은 비용 | 낮은 지연, 정확히 한 번 처리 | Bronze는 스트리밍, Silver/Gold는 배치 |
# MAGIC | **도구** | COPY INTO, Auto Loader trigger-once | Auto Loader continuous, Lakeflow Connect | - |
# MAGIC
# MAGIC **의사 결정 기준**: "오래된 데이터의 비용은 무엇인가?"
# MAGIC * 낮은 비용 → **배치** (간단하고 저렴)
# MAGIC * 높은 비용 → **스트리밍** (복잡성 감수)
# MAGIC
# MAGIC > Databricks는 SQL 기반 수집에 **스트리밍 테이블**을 권장합니다. Auto Loader 기반의 확장 가능한 파일 검색, 스키마 진화, 정확히 한 번 처리를 제공합니다.

# COMMAND ----------

# DBTITLE 1,4. Lakeflow 제품
# MAGIC %md
# MAGIC ## 4. Lakeflow 제품군
# MAGIC
# MAGIC Lakeflow는 데이터 파이프라인의 수집, 변환, 오케스트레이션을 위한 통합 솔루션입니다.
# MAGIC
# MAGIC ### Lakeflow Connect
# MAGIC * 엔터프라이즈 SaaS(Salesforce, Workday, ServiceNow 등) 및 데이터베이스 커넥터
# MAGIC * CDC, 자동 스키마 진화, 내장 모니터링 제공
# MAGIC * 커스텀 커넥터 개발/유지보수 불필요
# MAGIC
# MAGIC ### Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC * 선언적 배치/스트리밍 ETL ("무엇을" 선언하면 "어떻게"는 프레임워크가 처리)
# MAGIC * 스트리밍 테이블, 구체화된 뷰, `EXPECT` 데이터 품질 제약 조건
# MAGIC * `AUTO CDC INTO`를 통한 SCD Type 1/2 차원 이력 추적
# MAGIC * Serverless 컴퓨팅으로 성능/비용 최적화
# MAGIC
# MAGIC ### Lakeflow Jobs
# MAGIC * 노트북, SQL, Python, SDP 파이프라인 통합 오케스트레이션
# MAGIC * 조건부 분기, 병렬 실행, 파일 도착 트리거
# MAGIC * 실패 태스크만 지능적 재시도, AI 기반 오류 진단
# MAGIC * Declarative Automation Bundles로 CI/CD 통합

# COMMAND ----------

# DBTITLE 1,5. 최신 최적화 기능
# MAGIC %md
# MAGIC ## 5. Liquid Clustering & Predictive Optimization
# MAGIC
# MAGIC ### Liquid Clustering
# MAGIC 정적 파티셔닝을 유연한 클러스터링 키로 대체하는 **동적 데이터 구성** 전략:
# MAGIC
# MAGIC * 쿼리 패턴 변경 시 데이터 재작성 불필요 (키만 업데이트)
# MAGIC * 고카디널리티 열 지원, 여러 액세스 패턴 동시 최적화
# MAGIC * 새 테이블은 Hive 스타일 파티셔닝보다 **Liquid Clustering 권장**
# MAGIC * `CLUSTER BY AUTO`로 Predictive Optimization이 키 자동 선택
# MAGIC
# MAGIC > ⚠️ 스트리밍 테이블은 생성 시 `CLUSTER BY`를 지정해야 함 (생성 후 `ALTER TABLE`으로 추가 불가)
# MAGIC
# MAGIC ### Predictive Optimization
# MAGIC Databricks가 테이블 활동 패턴을 기반으로 `OPTIMIZE` 및 `VACUUM`을 자동 실행:
# MAGIC
# MAGIC * 예약된 유지 관리 작업 불필요 → 운영 오버헤드 감소
# MAGIC * 작은 파일 누적으로 인한 성능 저하 자동 방지
# MAGIC * 워크로드 패턴 변화에 자동 적응
# MAGIC * 카탈로그 수준에서 활성화: `ALTER CATALOG my_catalog ENABLE PREDICTIVE OPTIMIZATION`

# COMMAND ----------

# DBTITLE 1,6. 핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC | 주제 | 핵심 메시지 |
# MAGIC |---|---|
# MAGIC | **스토리지** | 아키텍처는 기반, 첫 바이트 전에 신중히 계획 |
# MAGIC | **랜딩 존** | 소스와 수집 분리, Bronze는 원시 보존, Silver에서 검증 |
# MAGIC | **수집 패턴** | "오래된 데이터의 비용" 기준으로 배치/스트리밍/하이브리드 선택 |
# MAGIC | **Lakeflow Connect** | 관리형 커넥터로 엔터프라이즈 수집 간소화 |
# MAGIC | **Liquid Clustering** | 정적 파티셔닝 대체, 데이터 재작성 없이 쿼리 패턴에 적응 |
# MAGIC | **Predictive Optimization** | Databricks가 테이블 유지관리 자동화, 수동 OPTIMIZE 불필요 |