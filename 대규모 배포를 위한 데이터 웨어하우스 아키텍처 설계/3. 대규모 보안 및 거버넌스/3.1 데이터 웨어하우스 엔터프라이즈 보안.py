# Databricks notebook source
# DBTITLE 1,요약_3.1 강의 - 데이터 웨어하우스 엔터프라이즈 보안
# MAGIC %md
# MAGIC # 3.1 강의 - 데이터 웨어하우스 엔터프라이즈 보안
# MAGIC

# COMMAND ----------

# DBTITLE 1,공유 책임 모델
# MAGIC %md
# MAGIC ## 공유 책임 모델
# MAGIC
# MAGIC 보안은 **클라우드 제공자**, **Databricks**, **고객** 세 계층 간의 파트너십입니다.
# MAGIC
# MAGIC | 계층 | 주요 책임 |
# MAGIC |--------|-------------|
# MAGIC | **클라우드 제공자** | 물리적 인프라 보안, 네트워크 인프라, 스토리지 서비스 암호화 |
# MAGIC | **Databricks** | 컨트롤 플레인 보안, 플랫폼 소프트웨어 보안, 보안 기본값 및 강화 가이드라인 |
# MAGIC | **고객** | 네트워크 구성, 액세스 관리, 데이터 분류 및 보호, 암호화 키 관리(CMK), 모니터링 및 사고 대응 |
# MAGIC
# MAGIC > 보안은 일회성 구성이 아닙니다. 정기적인 액세스 검토와 구성 모니터링이 필수적입니다.

# COMMAND ----------

# DBTITLE 1,인증, 인가, 암호화
# MAGIC %md
# MAGIC ## 인증, 인가, 암호화: 보안의 세 가지 핵심 축
# MAGIC
# MAGIC 세 가지 제어는 **상호 의존적**이며 하나의 시스템으로 작동해야 합니다.
# MAGIC
# MAGIC | 핵심 축 | 질문 | 주요 방법 |
# MAGIC |--------|------|-----------|
# MAGIC | **인증 (Authentication)** | 누구인가? | SSO (Okta, Azure AD, Google Workspace), Service Principals, OAuth M2M |
# MAGIC | **인가 (Authorization)** | 무엇을 할 수 있는가? | Unity Catalog 권한(GRANT/REVOKE), 워크스페이스 자격, 컴퓨트 정책 |
# MAGIC | **암호화 (Encryption)** | 데이터 보호 | 전송 중 TLS, 저장 시 클라우드 관리 키 또는 CMK, Databricks Secrets |
# MAGIC
# MAGIC > 강력한 인증과 약한 인가가 결합되면 신원이 확인된 사용자가 모든 것에 액세스할 수 있습니다. 세 가지 모두 강화해야 합니다.

# COMMAND ----------

# DBTITLE 1,주요 위험 및 완화 방법
# MAGIC %md
# MAGIC ## 주요 위험 및 완화 방법
# MAGIC
# MAGIC 보안 태세는 **침해 가정**을 전제로 하고 피해 범위를 제한하는 데 집중해야 합니다.
# MAGIC
# MAGIC | 위협 | 완화 방법 |
# MAGIC |--------|-------------|
# MAGIC | **데이터 유출** | 프라이빗 네트워킹, 이그레스 제어, DLP 모니터링 |
# MAGIC | **자격 증명 탈취** | 단기 유효 토큰, 시크릿 순환, OAuth M2M |
# MAGIC | **과도한 권한 액세스** | 최소 권한 원칙, 정기적 액세스 검토, 적시(JIT) 액세스 |
# MAGIC | **내부자 위협** | 감사 로깅, 데이터 분류, FGAC, 직무 분리 |
# MAGIC | **구성 드리프트** | Infrastructure as Code, 구성 모니터링, 컴플라이언스 대시보드 |

# COMMAND ----------

# DBTITLE 1,프라이빗 연결 아키텍처
# MAGIC %md
# MAGIC ## 프라이빗 연결 아키텍처
# MAGIC
# MAGIC | 옵션 | 복잡도 | 보안 수준 |
# MAGIC |--------|------------|----------------|
# MAGIC | **공용 (기본값)** | 낮음 | 기준 |
# MAGIC | **IP Access Lists** | 낮음 | 중간 |
# MAGIC | **Private Link (프런트엔드)** | 중간 | 높음 |
# MAGIC | **Private Link (전체)** | 높음 | 최상 |
# MAGIC
# MAGIC ### 클라우드별 구현
# MAGIC - **AWS**: AWS PrivateLink
# MAGIC - **Azure**: Azure Private Endpoints
# MAGIC - **GCP**: Private Service Connect
# MAGIC
# MAGIC > **프런트엔드 프라이빗**은 사용자의 UI/API 액세스를 보호하고, **백엔드 프라이빗**은 데이터 플레인-컨트롤 플레인 간 통신을 보호합니다. 가장 안전한 배포에는 모든 연결을 프라이빗으로 구성해야 합니다.

# COMMAND ----------

# DBTITLE 1,Customer-Managed Keys (CMK)
# MAGIC %md
# MAGIC ## Customer-Managed Keys (CMK)
# MAGIC
# MAGIC 기본적으로 Databricks는 클라우드 제공자 관리 키로 저장 데이터를 암호화합니다. **CMK**는 키 관리 권한을 고객에게 이전하여 컴플라이언스 요구 사항을 충족하고 키 폐기를 가능하게 합니다.
# MAGIC
# MAGIC ### CMK로 암호화할 수 있는 리소스
# MAGIC - 관리 스토리지 (워크스페이스 파일, Unity Catalog 볼륨)
# MAGIC - 노트북 및 작업 결과
# MAGIC - 클러스터 EBS 볼륨 (AWS)
# MAGIC - Unity Catalog 관리형 데이터
# MAGIC
# MAGIC ### 키 관리 모범 사례
# MAGIC - 정기적인 키 순환 (기존 암호화 데이터는 계속 액세스 가능)
# MAGIC - 키 액세스는 전담 보안 관리자로만 제한
# MAGIC - 키 복구 절차 문서화 및 테스트
# MAGIC
# MAGIC > **경고**: CMK 키 취소는 **되돌릴 수 없습니다**. 전체 Databricks 배포가 사용 불가능해지며, Databricks조차도 데이터를 복구할 수 없습니다. 프로덕션 적용 전 반드시 복구 절차를 테스트하십시오.

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC 1. **보안은 공유 책임** - Databricks는 보안 기본값과 도구를 제공하지만, 사용자가 네트워킹, 액세스 관리, 암호화, 모니터링을 적절히 구성해야 합니다.
# MAGIC 2. **세 가지 핵심 축은 하나의 시스템** - 인증, 인가, 암호화 중 하나라도 약하면 전체 보안이 무너집니다.
# MAGIC 3. **침해를 가정하고 심층 방어 계획** - 최소 권한, 감사 로깅, 네트워크 세분화로 피해를 제한합니다.
# MAGIC 4. **프라이빗 연결은 공격 표면 축소** - 규제 산업에서는 필수이며, 모든 조직에서 강력한 모범 사례입니다.
# MAGIC 5. **CMK는 최종 제어권** - 강력한 제어권을 제공하지만, 키 관리는 극도로 신중해야 합니다. 실수로 인한 취소는 전체 배포를 사용할 수 없게 만들 수 있습니다.