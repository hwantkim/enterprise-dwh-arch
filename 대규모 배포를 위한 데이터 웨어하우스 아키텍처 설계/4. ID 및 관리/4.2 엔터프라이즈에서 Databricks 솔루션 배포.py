# Databricks notebook source
# DBTITLE 1,요약 노트북 제목
# MAGIC %md
# MAGIC # 엔터프라이즈에서 Databricks 솔루션 배포
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. 배포 과제와 핵심 원칙
# MAGIC %md
# MAGIC ## 1. 배포 과제와 핵심 원칙
# MAGIC
# MAGIC 데이터 플랫폼 배포는 애플리케이션 배포보다 복잡합니다. 인프라, 코드, 데이터를 함께 관리해야 하며, 단일 Lakeflow Job만 해도 노트북 코드, 클러스터 구성, 스케줄, 권한, 카탈로그 참조의 조합이기 때문입니다.
# MAGIC
# MAGIC **핵심 원칙:**
# MAGIC
# MAGIC 1. **모든 것을 버전 관리** — 코드, 구성, 인프라 정의를 Git에 저장
# MAGIC 2. **모든 것을 자동화** — 프로덕션에서 수동 작업 금지, 모든 변경은 파이프라인을 통해
# MAGIC 3. **배포 전 테스트** — 구성 검증, 통합 테스트, 권한 확인
# MAGIC 4. **환경 승격** — 동일한 코드, 환경별 다른 구성으로 개발 → 스테이징 → 프로덕션

# COMMAND ----------

# DBTITLE 1,2. 배포 방식 비교
# MAGIC %md
# MAGIC ## 2. 배포 방식 비교
# MAGIC
# MAGIC | 방식 | 포함 사항 | 적합한 경우 |
# MAGIC |------|-----------|------------|
# MAGIC | **Git Folders** | 노트북 및 파일(버전 관리) | 개발 워크플로우, 코드 리뷰 |
# MAGIC | **CI/CD + Databricks CLI** | API 기반 리소스 관리 | 맞춤형 자동화 파이프라인 |
# MAGIC | **Declarative Automation Bundles (DABs)** | 모든 리소스에 대한 선언적 YAML | 전체 수명주기 관리 (**권장**) |
# MAGIC | **IaC 도구 (Terraform, StackQL 등)** | 계정 수준 작업, 클라우드 IaC | 다른 클라우드 인프라와 함께 관리 시 |

# COMMAND ----------

# DBTITLE 1,3. Git Folders
# MAGIC %md
# MAGIC ## 3. Git Folders
# MAGIC
# MAGIC Git Folders는 리포지토리를 Databricks 워크스페이스에 직접 클론하여 노트북과 코드 파일의 버전 관리를 제공합니다. GitHub, GitLab, Azure DevOps, Bitbucket, AWS CodeCommit 등 주요 Git 제공자와 통합됩니다.
# MAGIC
# MAGIC **주요 기능:** Databricks UI에서 pull, commit, push, 브랜치 생성, 시각적 diff 비교 지원
# MAGIC
# MAGIC **사용자 폴더 vs. 프로덕션 폴더:**
# MAGIC
# MAGIC | 구분 | 사용자 Git Folders | 프로덕션 Git Folders |
# MAGIC |------|-------------------|---------------------|
# MAGIC | 위치 | `/Workspace/Users/<email>/` | 공유 프로젝트 폴더 |
# MAGIC | 목적 | 개인 개발 및 실험 | 자동화된 프로덕션 배포 |
# MAGIC | 업데이트 주체 | 개발자 (수동) | CI/CD 파이프라인 (API 기반) |
# MAGIC | 브랜치 | 피처 브랜치 | 배포 브랜치 (`main`) |
# MAGIC | 권한 | 소유자 완전 제어 | `Can Run`; 서비스 주체만 편집 |
# MAGIC
# MAGIC **제한 사항:** 노트북과 파일만 관리. 잡, 클러스터, 워어하우스, 권한은 미지원. 전체 배포 자동화에는 DABs가 필요.

# COMMAND ----------

# DBTITLE 1,4. DABs
# MAGIC %md
# MAGIC ## 4. Declarative Automation Bundles (DABs)
# MAGIC
# MAGIC DABs는 Databricks 리소스를 위한 **Infrastructure as Code**를 제공합니다. 모든 리소스를 YAML 파일로 정의하여 소스 코드와 함께 관리합니다.
# MAGIC
# MAGIC **DABs로 관리할 수 있는 리소스:**
# MAGIC
# MAGIC * **컴퓨트 및 실행:** Lakeflow Jobs, Lakeflow Pipelines, SQL Warehouses, 범용 클러스터
# MAGIC * **데이터 및 거버넌스:** 스키마 (Unity Catalog), Volumes, 등록된 모델, 품질 모니터
# MAGIC * **분석 및 AI:** AI/BI Dashboards, MLflow Experiments, 모델 서빙 엔드포인트, Databricks Apps
# MAGIC * **보안 및 구성:** 권한, Grants (Unity Catalog), 시크릿 스코프, 알림
# MAGIC
# MAGIC **주요 CLI 명령어:**
# MAGIC
# MAGIC | 명령어 | 용도 |
# MAGIC |---------|------|
# MAGIC | `databricks bundle init` | 템플릿에서 새 번들 생성 |
# MAGIC | `databricks bundle validate` | 구성 문법 및 참조 확인 |
# MAGIC | `databricks bundle deploy --target <env>` | 워크스페이스에 리소스 생성 또는 업데이트 |
# MAGIC | `databricks bundle run <job> --target <env>` | 배포된 잡 실행 |
# MAGIC | `databricks bundle destroy --target <env>` | 배포된 모든 리소스 제거 |

# COMMAND ----------

# DBTITLE 1,5. CI/CD 파이프라인
# MAGIC %md
# MAGIC ## 5. CI/CD 파이프라인
# MAGIC
# MAGIC 번들이 정의되고 코드가 Git에 저장되면, CI/CD를 통해 배포를 자동화합니다. CI/CD 파이프라인은 코드 변경사항이 병합될 때 번들을 자동으로 검증, 테스트, 배포합니다.
# MAGIC
# MAGIC **인증:** 서비스 주체에 대한 **OAuth M2M** 자격 증명 사용. `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, `DATABRICKS_HOST`를 CI/CD 플랫폼의 암호화된 시크릿으로 설정. 자격 증명은 절대 리포지토리에 커밋하지 않음.
# MAGIC
# MAGIC **지원 플랫폼:**
# MAGIC
# MAGIC | 플랫폼 | 구성 파일 | 시크릿 저장소 |
# MAGIC |--------|-----------|---------------|
# MAGIC | GitHub Actions | `.github/workflows/*.yml` | GitHub Secrets |
# MAGIC | GitLab CI | `.gitlab-ci.yml` | CI/CD Variables |
# MAGIC | Azure DevOps | `azure-pipelines.yml` | Service Connections / Variable Groups |
# MAGIC
# MAGIC **프로덕션 배포 워크플로우 (GitHub Actions 예시):**
# MAGIC
# MAGIC 1. PR에서 `bundle validate` 실행 (검증)
# MAGIC 2. `main` 브랜치 병합 시 `bundle deploy --target staging` 실행 (스테이징 배포)
# MAGIC 3. 수동 승인 후 `bundle deploy --target prod` 실행 (프로덕션 배포)

# COMMAND ----------

# DBTITLE 1,6. 환경 승격 및 구성 차이
# MAGIC %md
# MAGIC ## 6. 환경 승격 및 구성 차이
# MAGIC
# MAGIC 동일한 번들 코드가 각 환경에 배포되며, 타깃별 변수만 변경됩니다:
# MAGIC
# MAGIC | 구성 | 개발 | 스테이징 | 프로덕션 |
# MAGIC |------|------|----------|----------|
# MAGIC | 카탈로그 | `dev_bakehouse` | `staging_bakehouse` | `prod_bakehouse` |
# MAGIC | 워어하우스 크기 | X-Small | Small | Medium 또는 Large |
# MAGIC | 잡 스케줄 | 수동 / 일시 중지 | 일시 중지 또는 매시간 | 프로덕션 cron |
# MAGIC | 클러스터 정책 | 개발자 정책 | 표준 정책 | 프로덕션 정책 |
# MAGIC | `run_as` | 현재 사용자 | 서비스 주체 | 서비스 주체 |
# MAGIC | 권한 | 개발자 전용 | QA 팀 | 프로덕션 그룹 |
# MAGIC
# MAGIC **프로덕션 모범 사례:** `run_as`를 서비스 주체로 설정하여 직원 이직과 관계없이 잡이 계속 실행되도록 해야 합니다.

# COMMAND ----------

# DBTITLE 1,핵심 요약
# MAGIC %md
# MAGIC ## 핵심 요약
# MAGIC
# MAGIC * **데이터 플랫폼 배포는 애플리케이션 배포보다 복잡** — 코드, 인프라, 데이터를 함께 관리
# MAGIC * **개발은 Git Folders, 배포는 DABs** — Git Folders는 버전 관리/브랜칭/코드 리뷰, DABs는 잡/파이프라인/워어하우스/권한의 전체 수명주기 관리
# MAGIC * **DABs는 Databricks의 권장 배포 방식** — 단일 `databricks.yml`로 모든 리소스를 환경별 타깃과 함께 정의
# MAGIC * **CI/CD가 승격 파이프라인 자동화** — GitHub Actions/GitLab CI/Azure DevOps가 병합 시 `bundle validate`와 `bundle deploy`를 트리거
# MAGIC * **동일한 코드, 환경별 다른 구성** — 카탈로그, 컴퓨트 크기, 스케줄, 권한은 DABs 환경 타깃으로 처리. 프로덕션에서 수동 UI 클릭 없음