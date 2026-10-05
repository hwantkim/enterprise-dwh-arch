# Databricks notebook source
# DBTITLE 1,요약 타이틀
# MAGIC %md
# MAGIC # 4.1 Databricks Identities
# MAGIC

# COMMAND ----------

# DBTITLE 1,Identity 유형 개요
# MAGIC %md
# MAGIC ## 1. 세 가지 Identity 유형
# MAGIC
# MAGIC Databricks는 **계정 수준**에서 관리되는 세 가지 Identity를 지원합니다:
# MAGIC
# MAGIC | Identity 유형 | 식별자 | 용도 | UI 액세스 |
# MAGIC |---|---|---|---|
# MAGIC | **Users** | 이메일 주소 | 사람 사용자 (분석가, 엔지니어, 관리자) | 가능 |
# MAGIC | **Groups** | 그룹 이름 | 대량 권한 할당을 위한 사용자/그룹 컬렉션 | 해당 없음 |
# MAGIC | **Service Principals** | Application ID | 자동화, CI/CD, ETL 작업을 위한 머신 Identity | 불가 |
# MAGIC
# MAGIC **핵심 원칙:** 모든 Identity는 계정 수준에서 관리해야 합니다. 워크스페이스 수준 그룹은 Identity 분산과 권한 불일치를 초래하는 레거시 패턴입니다.

# COMMAND ----------

# DBTITLE 1,IdP Federation, SSO, SCIM
# MAGIC %md
# MAGIC ## 2. IdP Federation, SSO, SCIM
# MAGIC
# MAGIC ### SSO 프로토콜
# MAGIC
# MAGIC | 프로토콜 | 특징 |
# MAGIC |---|---|
# MAGIC | **SAML 2.0** | XML 기반, 기업 IdP에서 널리 지원 |
# MAGIC | **OIDC** | OAuth 2.0 기반, JWT 사용, 현대 클라우드 환경에 적합 |
# MAGIC
# MAGIC ### Unified Login
# MAGIC - 계정 수준에서 SSO를 한 번 구성하면 모든 워크스페이스에 적용
# MAGIC - 2023년 6월 이후 생성 계정은 기본 활성화, 비활성화 불가
# MAGIC - 계정관리자는 잠금 방지를 위해 SSO를 우회하여 최대 20명의 긴급 액세스 구성 가능
# MAGIC
# MAGIC ### 사용자 프로비저닝 방식
# MAGIC
# MAGIC | 방식 | 설명 |
# MAGIC |---|---|
# MAGIC | **JIT** | SSO 인증 시 자동으로 계정 생성. 소규모/초기 롤아웃에 적합 |
# MAGIC | **SCIM** | IdP와 Databricks 간 사용자/그룹 지속적 동기화. 대규모 배포 권장 |
# MAGIC | **Automatic Identity Management** | SCIM 대안. 중첩 그룹 및 Service Principals 직접 동기화 (Entra ID만 지원, Public Preview) |
# MAGIC
# MAGIC **참고:** SCIM은 직접 그룹 멤버십만 동기화. 중첩 그룹 해결에는 Automatic Identity Management 필요.

# COMMAND ----------

# DBTITLE 1,Service Principals 모범 사례
# MAGIC %md
# MAGIC ## 3. Service Principals 모범 사례
# MAGIC
# MAGIC Service Principals는 자동화, CI/CD, 프로덕션 워크로드를 위한 머신 Identity입니다.
# MAGIC
# MAGIC ### 사용 이유
# MAGIC - 인사 변경과 무관하게 유지됨 (사용자 계정과 분리)
# MAGIC - 각 자동화 프로세스별 세밀한 감사 가능
# MAGIC - 최소 권한 원칙 적용 가능
# MAGIC
# MAGIC ### 핵심 사례
# MAGIC
# MAGIC | 사례 | 설명 |
# MAGIC |---|---|
# MAGIC | 애플리케이션당 하나의 SP | ETL, CI/CD, 리포팅, ML Serving마다 별도 SP |
# MAGIC | 최소 권한 | 필요한 권한만 부여 |
# MAGIC | 시크릿 관리 | Azure Key Vault, AWS Secrets Manager, Databricks Secrets 사용 |
# MAGIC | 시크릿 커밋 금지 | 코드 저장소/노트북/구성 파일에 시크릿 포함 금지 |
# MAGIC | 정기적 로테이션 | 최소 분기별 시크릿 로테이션 |
# MAGIC | OAuth 권장 | 장기 수명 PATs 대신 OAuth M2M 토큰 사용 |

# COMMAND ----------

# DBTITLE 1,토큰 및 토큰 관리
# MAGIC %md
# MAGIC ## 4. 토큰 및 토큰 관리
# MAGIC
# MAGIC ### PATs vs. OAuth 토큰
# MAGIC
# MAGIC | 속성 | PATs(Personal Access Tokens) | OAuth 토큰 |
# MAGIC |---|---|---|
# MAGIC | **수명** | 장기 (기본 최대 730일) | 단기 (자동 갱신) |
# MAGIC | **관리** | 수동 생성/모니터링/폐지 | 자동 수명 주기 |
# MAGIC | **보안 위험** | 높음 (유출/공유/잊어버림) | 낮음 (자동 만료) |
# MAGIC | **적합한 용도** | 개발, 프로토타이핑 | 프로덕션 자동화, CI/CD |
# MAGIC
# MAGIC ### 대규모 토큰 관리 전략
# MAGIC - `maxTokenLifetimeDays`를 90일 미만으로 설정
# MAGIC - 세밀한 권한으로 PAT 생성을 특정 사용자/그룹으로 제한
# MAGIC - 계정 콘솔의 **Token report**로 모든 워크스페이스 PATs 모니터링
# MAGIC - 90일 미사용 PATs 자동 폐지
# MAGIC - CI/CD 자동화는 OAuth로 마이그레이션
# MAGIC
# MAGIC **안티패턴 — Token Sprawl:** Git/Slack/노트북에 하드코딩된 토큰은 보안 사고의 주요 원인. 대규모에서는 사용자 PATs를 비활성화하고 OAuth 기반 Service Principals를 사용 권장. Token Sprawl은 기업이나 조직 내에서 API 키, 액세스 토큰, 비밀번호 등 기계 및 서비스 간 인증 정보가 통제 범위를 벗어나 무분별하게 복제되고 확산(Sprawl)되는 현상을 의미

# COMMAND ----------

# DBTITLE 1,그룹 구조 설계 원칙
# MAGIC %md
# MAGIC ## 5. 그룹 구조 설계 원칙
# MAGIC
# MAGIC ### 계층형 그룹 설계 4원칙
# MAGIC
# MAGIC 1. **조직 역할에 정렬** — 비즈니스 기능에 매핑되는 그룹 생성 (예: `data-engineers`, `analytics-team`)
# MAGIC 2. **중첩 그룹으로 상속 구현** — `data-platform-all`이 하위 그룹들을 포함. 단, SCIM은 직접 멤버십만 동기화 (중첩 해결에는 Automatic Identity Management 필요)
# MAGIC 3. **워크스페이스 액세스와 데이터 액세스 분리** — 워크스페이스 멤버십용 그룹과 Unity Catalog 권한용 그룹을 분리
# MAGIC 4. **변화에 대비한 설계** — 권한 손상 없이 재구성 가능한 그룹 구조 설계
# MAGIC
# MAGIC ### 그룹 명명 규칙 (권장)
# MAGIC
# MAGIC | 접두사 | 용도 |
# MAGIC |---|---|
# MAGIC | `ws-` | 워크스페이스 액세스 |
# MAGIC | `uc-` | Unity Catalog 권한 |
# MAGIC | `sp-` | Service Principals |
# MAGIC
# MAGIC 일관된 명명 규칙을 초기에 설정하면 감사가 간소화되고 각 그룹의 제어 대상이 명확해집니다.

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 6. 핵심 요약
# MAGIC
# MAGIC - **세 가지 Identity 유형** — Users(사람), Groups(확장 가능한 권한), Service Principals(자동화). 프로덕션 Job에 개인 계정 사용 금지
# MAGIC - **SCIM 또는 Automatic Identity Management 필수** — 대규모 환경에서 수동 프로비저닝은 오류 발생 가능. 퇴사자가 수동 해제될 때까지 액세스 유지 위험
# MAGIC - **Unified Login으로 계정 수준 SSO** — 한 번 구성으로 모든 워크스페이스 적용. 잠금 방지를 위해 최대 20명 긴급 액세스 구성
# MAGIC - **애플리케이션당 하나의 Service Principal** — 공유 금지. 세밀한 감사와 최소 권한 제어
# MAGIC - **PATs보다 OAuth 권장** — 프로덕션 자동화에 OAuth 토큰 사용. PAT 생성 권한 제한, 토큰 수명 90일 미만 설정
# MAGIC - **그룹 계층 조기 설계** — 조직 역할에 정렬, 중첩으로 상속, 변화에 대비. 중첩 그룹 지원 시 Entra ID와 함께 Automatic Identity Management 필요