# Databricks notebook source
# DBTITLE 1,요약 제목
# MAGIC %md
# MAGIC # Databricks 계정 및 워크스페이스 개요
# MAGIC

# COMMAND ----------

# DBTITLE 1,계정-워크스페이스 계층 구조
# MAGIC %md
# MAGIC ## 1. 계정-워크스페이스 계층 구조
# MAGIC
# MAGIC Databricks 배포는 3계층 구조로 구성됩니다:
# MAGIC
# MAGIC | 계층 | 설명 |
# MAGIC |------|------|
# MAGIC | **계정** | 최상위 컨테이너 - 모든 워크스페이스, 사용자, 청구, 계정 수준 설정 포함 |
# MAGIC | **워크스페이스** | 계정 내의 개별 환경 - 격리된 컴퓨트, 노트북, 작업 제공 |
# MAGIC | **메타스토어** | Unity Catalog 기반의 데이터 거버넌스 계층 - 리전별로 하나씩 존재 |
# MAGIC
# MAGIC * 핵심 포인트: 사용자와 그룹은 **계정 수준에서 정의**된 후 개별 워크스페이스에 할당됩니다. ID 관리는 중앙 집중화되면서도 워크스페이스 액세스는 세분화됩니다.

# COMMAND ----------

# DBTITLE 1,ID 및 액세스 관리
# MAGIC %md
# MAGIC ## 2. ID 및 액세스 관리
# MAGIC
# MAGIC **2계층 ID 모델**: 계정 수준에서 ID 관리, 워크스페이스에 할당
# MAGIC
# MAGIC ### ID 유형
# MAGIC
# MAGIC | ID | 용도 |
# MAGIC |------|------|
# MAGIC | **사용자** | 개별 사용자 계정 (회사 이메일 매핑) |
# MAGIC | **그룹** | 사용자/서비스 주체 모음 - 대규모 권한 관리 간소화 |
# MAGIC | **서비스 주체** | 자동화/CI/CD용 머신 ID |
# MAGIC
# MAGIC ### ID 페더레이션 주요 메커니즘
# MAGIC
# MAGIC * **SSO**: IdP(Okta, Azure AD/Entra ID 등) 통해 인증, SAML 2.0/OIDC 지원
# MAGIC * **SCIM**(System for Cross-domain Identity Management) 프로비저닝: IdP → Databricks 계정으로 사용자/그룹 자동 동기화
# MAGIC * **Automatic Identity Management** (Public Preview): 중첩 그룹, just-in-time 프로비저닝 지원, SCIM과 공존 가능
# MAGIC * ID는 계정 수준에서 한 번 생성 후 여러 워크스페이스에 할당 가능

# COMMAND ----------

# DBTITLE 1,컨트롤 플레인 vs 데이터 플레인
# MAGIC %md
# MAGIC ## 3. 컨트롤 플레인 vs. 데이터 플레인
# MAGIC
# MAGIC | 구성 요소 | 관리 주체 | 포함 내용 |
# MAGIC |-----------|-----------|----------|
# MAGIC | **컨트롤 플레인** | Databricks | 웹 앱, API, 노트북 저장소, 작업 스케줄링, Unity Catalog 서비스, 인증 |
# MAGIC | **데이터 플레인** | 고객 클라우드 계정 | SQL Warehouse/클러스터, 클라우드 스토리지 데이터, VPC/네트워킹 |
# MAGIC
# MAGIC * 핵심: **고객의 데이터는 클라우드 계정을 떠나지 않음** - Databricks는 오케스트레이션만 수행
# MAGIC * Serverless SQL Warehouse는 Databricks 관리 컴퓨트 환경에서 실행 (데이터는 고객 스토리지에 유지)
# MAGIC * 워크스페이스가 두 플레인 간의 인터페이스 역할

# COMMAND ----------

# DBTITLE 1,워크스페이스-메타스토어 관계
# MAGIC %md
# MAGIC ## 4. 워크스페이스-메타스토어 관계
# MAGIC
# MAGIC * 각 워크스페이스는 **정확히 하나의** 메타스토어에 연결
# MAGIC * 동일한 메타스토어를 공유하는 워크스페이스 간 데이터 액세스 가능
# MAGIC * **카탈로그 바인딩** (선택): 워크스페이스별로 표시할 카탈로그 제어
# MAGIC   * 바인딩 없으면 → 모든 카탈로그가 모든 연결 워크스페이스에서 보임
# MAGIC   * 바인딩 사용 → 프로덕션/개발 환경 분리, 사업 부문별 데이터 격리 가능
# MAGIC * 바인딩되지 않은 카탈로그는 해당 워크스페이스에서 **보이지 않음** (검색/쿼리 불가)

# COMMAND ----------

# DBTITLE 1,메타스토어 구성
# MAGIC %md
# MAGIC ## 5. 메타스토어 구성
# MAGIC
# MAGIC ### 관리형 스토리지 위치 (계층적 상속)
# MAGIC
# MAGIC 1. **메타스토어 수준** → 모든 카탈로그의 기본값
# MAGIC 2. **카탈로그 수준** → 선택적 재정의
# MAGIC 3. **스키마 수준** → 추가 선택적 재정의
# MAGIC
# MAGIC ### Delta Sharing
# MAGIC
# MAGIC * 외부 Delta Sharing은 **메타스토어 수준**에서만 활성화 가능 (보안 통제)
# MAGIC * 개별 워크스페이스에서는 활성화 불가
# MAGIC
# MAGIC ### 메타스토어 관리자
# MAGIC
# MAGIC | 역할 | 범위 | 할당 위치 |
# MAGIC |------|------|----------|
# MAGIC | **Account Admin** | 전체 계정 관리 (워크스페이스, 청구, ID) | 계정 콘솔 (역할 기반) |
# MAGIC | **메타스토어 관리자** | Unity Catalog 거버넌스 (소유권 이전, 권한 관리) | 계정 콘솔 (주체 할당) |
# MAGIC
# MAGIC * 모범 사례: 개별 사용자 대신 **그룹**(예: `metastore-admins`)을 메타스토어 관리자로 할당

# COMMAND ----------

# DBTITLE 1,워크스페이스 ID 할당 및 권한
# MAGIC %md
# MAGIC ## 6. 워크스페이스 ID 할당 및 권한
# MAGIC
# MAGIC ### 워크스페이스 권한 수준
# MAGIC
# MAGIC | 권한 | 설명 |
# MAGIC |------|------|
# MAGIC | **Admin** | 워크스페이스 전체 관리 권한 |
# MAGIC | **User** | 기본 액세스만 - 추가 권한은 세분화된 권한으로 제어 |
# MAGIC
# MAGIC ### 두 권한 시스템 (독립적이며 상호 보완적)
# MAGIC
# MAGIC | 시스템 | 관리 대상 |
# MAGIC |--------|------------|
# MAGIC | **워크스페이스 수준 권한** | 노트북, 폴더, 작업, 파이프라인, SQL Warehouse, 대시보드 |
# MAGIC | **Unity Catalog 권한** | 카탈로그, 스키마, 테이블, 볼륨, 외부 위치, 함수, 모델, 공유 |
# MAGIC
# MAGIC * 사용자가 Unity Catalog 읽기 권한이 있어도 특정 워크스페이스에서 컴퓨트 생성 권한이 없을 수 있음 (그 반대도 가능)

# COMMAND ----------

# DBTITLE 1,계정 수준 vs 워크스페이스 수준 관리
# MAGIC %md
# MAGIC ## 7. 계정 수준 vs. 워크스페이스 수준 관리
# MAGIC
# MAGIC | 책임 | Account Admin | Workspace Admin |
# MAGIC |------|---------------|------------------|
# MAGIC | ID | 사용자/그룹 관리 | 워크스페이스에 사용자/그룹 할당 |
# MAGIC | 인프라 | 워크스페이스 생성/구성 | SQL Warehouse 구성 |
# MAGIC | 거버넌스 | Unity Catalog 메타스토어 관리 | 워크스페이스별 컴퓨트 정책 |
# MAGIC | 보안 | 계정 전체 보안 정책 | 워크스페이스 기능 활성화 |
# MAGIC | 청구 | 청구/사용량 보고 | -- |
# MAGIC | 컴퓨트 | -- | 작업/노트북 관리 |
# MAGIC
# MAGIC * 모범 사례: Account Admin 수를 최소화하고, 팀 리더에게 워크스페이스 관리 위임

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC 1. **계정은 관리 경계** - 청구, ID, 거버넌스는 계정 수준에 존재. 워크스페이스는 운영 경계.
# MAGIC 2. **컨트롤 플레인/데이터 플레인 분리는 기본** - 데이터는 고객의 클라우드 계정을 떠나지 않음.
# MAGIC 3. **메타스토어가 워크스페이스를 연결** - 어느 워크스페이스에서 로그인하든 일관된 데이터 액세스/권한 제공.
# MAGIC 4. **관리를 적절히 위임** - Account Admin은 ID/보안, Workspace Admin은 컴퓨트/작업 관리. 이 분리가 확장성을 제공.