# Databricks notebook source
# DBTITLE 1,요약: 과정 개요 (제목)
# MAGIC %md
# MAGIC # 대규모 배포를 위한 데이터 웨어하우스 아키텍처 설계
# MAGIC

# COMMAND ----------

# DBTITLE 1,과정 개요
# MAGIC %md
# MAGIC ## 과정 개요
# MAGIC
# MAGIC 이 과정은 **대규모 데이터 웨어하우징 배포**를 위한 **성능 최적화, 비용 제어, 보안**을 다루는 심화 과정입니다.
# MAGIC
# MAGIC - **대상 환경**: 여러 사업 부문에 걸쳐 수백~수천 명의 사용자에게 서비스를 제공하는 Databricks 환경
# MAGIC - **핵심 목표**: 높은 성능, 비용 효율성, 보안 표준 준수를 유지하면서 데이터 웨어하우징 운영을 효율적으로 확장
# MAGIC - **과정 흐름**: 수집 & 저장 → 워크스페이스 → 보안 → ID (총 4개 섹션)

# COMMAND ----------

# DBTITLE 1,섹션 구성
# MAGIC %md
# MAGIC ## 섹션 구성
# MAGIC
# MAGIC | 섹션 | 주제 | 다루는 내용 | 구성 |
# MAGIC |------|------|-------------|------|
# MAGIC | **1. 수집 & 저장** | 데이터 아키텍처 | "스케일"의 의미, Lakehouse 기초, 메달리온 아키텍처, 클라우드 스토리지 패턴, Lakeflow 파이프라인, CDC, 스키마 진화, Lakehouse Federation, 외부 카탈로그 | 강의 3 + 데모 1 |
# MAGIC | **2. 워크스페이스** | 다중 워크스페이스 전략 | 계정 계층 구조, 컨트롤 플레인 vs 데이터 플레인, 워크스페이스 할당 패턴, 격리 전략, Metastore 설계, 크로스 카탈로그 액세스, Delta Sharing, 마켓플레이스 | 강의 4 |
# MAGIC | **3. 보안** | 보안 및 거버넌스 | 보안 모델, 네트워크 보안, CMK, 워크스페이스 강화, RBAC/ABAC, 세분화된 액세스 제어(행 필터, 열 마스킹), 컴플라이언스 | 강의 2 + 데모 1  |
# MAGIC | **4. ID** | ID 및 관리 | 사용자/그룹/서비스 프린시펄, IdP 페더레이션, SCIM, 토큰 관리, DABs 및 GitOps, 감사 로그, 성능 모니터링, 비용 귀속 | 강의 3 + 데모 1 |

# COMMAND ----------

# DBTITLE 1,섹션 1: 효율적인 데이터 수집 및 저장
# MAGIC %md
# MAGIC ## 섹션 1: 효율적인 데이터 수집 및 저장
# MAGIC
# MAGIC | 노트북 | 유형 | 설명 |
# MAGIC |----------|------|-------------|
# MAGIC | [1.1 대규모 데이터 아키텍처 소개]($./1. 효율적인 데이터 수집 및 저장/1.1 대규모 데이터 아키텍처 소개) | 강의 | "스케일"의 의미, 증폭 효과, Lakehouse 기초, 메달리온 아키텍처 |
# MAGIC | [1.2 대규모 데이터 웨어하우스 수집]($./1. 효율적인 데이터 수집 및 저장/1.2 대규모 데이터 웨어하우스 수집) | 강의 | 클라우드 스토리지 패턴, 수집 모범 사례, Lakeflow 파이프라인 |
# MAGIC | [1.3 데모 - Lakeflow SDP를 사용한 대규모 수집 및 변환]($./1. 효율적인 데이터 수집 및 저장/1.3 데모 - Lakeflow SDP를 사용한 대규모 수집 및 변환) | 데모 | Lakeflow 파이프라인 구성, CDC, 스키마 진화, 메달리온 아키텍처 |
# MAGIC | [1.5 Lakehouse Federation 및 Foreign Catalogs]($./1. 효율적인 데이터 수집 및 저장/1.5 Lakehouse Federation 및 Foreign Catalogs) | 강의 | 페더레이션 데이터 소스, 외부 카탈로그, 대규모 Lakehouse Federation |
# MAGIC
# MAGIC **학습 목표:**
# MAGIC - 데이터 저장 및 수집 모범 사례 이해 (S3, ADLS 온보딩, 스테이징, 메달리온 아키텍처)
# MAGIC - 페더레이션 데이터 소스를 온보딩하고 효율적으로 쿼리하는 방법 학습
# MAGIC - 페더레이션 데이터 소스 확장 모범 사례 이해
# MAGIC - 엔터프라이즈 데이터 수집을 위한 Lakeflow 파이프라인 구성
# MAGIC
# MAGIC <hr/>

# COMMAND ----------

# DBTITLE 1,섹션 2: 다중 워크스페이스 전략
# MAGIC %md
# MAGIC ## 섹션 2: 다중 워크스페이스 전략
# MAGIC
# MAGIC | 노트북 | 유형 | 설명 |
# MAGIC |----------|------|-------------|
# MAGIC | [2.1 Databricks 계정 및 워크스페이스 개요]($./2. 다중 워크스페이스 전략/2.1 Databricks 계정 및 워크스페이스 개요) | 강의 | 계정 계층 구조, 워크스페이스 기초, 컨트롤 플레인 vs 데이터 플레인 |
# MAGIC | [2.2 다중 워크스페이스 아키텍처 설계]($./2. 다중 워크스페이스 전략/2.2 다중 워크스페이스 아키텍처 설계) | 강의 | 워크스페이스 할당 패턴, 격리 전략, 환경 |
# MAGIC | [2.3 대규모 환경을 위한 Unity Catalog 아키텍처 설계]($./2. 다중 워크스페이스 전략/2.3 대규모 환경을 위한 Unity Catalog 아키텍처 설계) | 강의 | Metastore 설계, 카탈로그 전략, 크로스 워크스페이스 데이터 액세스 |
# MAGIC | [2.4 대규모 환경에서의 데이터 공유]($./2. 다중 워크스페이스 전략/2.4 대규모 환경에서의 데이터 공유) | 강의 | Delta Sharing, Databricks-to-Databricks 공유, 마켓플레이스 |
# MAGIC
# MAGIC **학습 목표:**
# MAGIC - 엔터프라이즈 데이터 웨어하우징 배포를 위한 워크스페이스 할당 모범 사례
# MAGIC - 다중 워크스페이스 사용 시기 및 필요한 워크스페이스 수
# MAGIC - 사업 부문 간 크로스 카탈로그 데이터 액세스 전략 이해
# MAGIC - Unity Catalog를 사용한 안전한 크로스 카탈로그 액세스 학습
# MAGIC - 피해야 할 안티 패턴 식별 (과도한 격리, 직접 데이터 복제, 혼합 환경)
# MAGIC
# MAGIC <hr/>

# COMMAND ----------

# DBTITLE 1,섹션 3: 대규모 보안 및 거버넌스
# MAGIC %md
# MAGIC ## 섹션 3: 대규모 보안 및 거버넌스
# MAGIC
# MAGIC | 노트북 | 유형 | 설명 |
# MAGIC |----------|------|-------------|
# MAGIC | [3.1 데이터 웨어하우스 엔터프라이즈 보안]($./3. 대규모 보안 및 거버넌스/3.1 데이터 웨어하우스 엔터프라이즈 보안) | 강의 | 보안 모델, 네트워크 보안, 암호화, 워크스페이스 강화 |
# MAGIC | [3.2 Unity Catalog를 사용한 데이터 보안]($./3. 대규모 보안 및 거버넌스/3.2 Unity Catalog를 사용한 데이터 보안) | 강의 | RBAC/ABAC, 세분화된 액세스 제어, 열 마스킹, 행 필터 |
# MAGIC | [3.3 데모 - Unity Catalog에서 FGAC 구현하기]($./3. 대규모 보안 및 거버넌스/3.3 데모 - Unity Catalog에서 FGAC 구현하기) | 데모 | 행 필터, 열 마스크, ABAC |
# MAGIC
# MAGIC **학습 목표:**
# MAGIC - 워크스페이스 보안 체크리스트 나열 (CMK, 데이터 유출, 프라이빗 연결)
# MAGIC - 데이터 검색 및 자산 관리를 위한 Unity Catalog 사용 필요성 이해
# MAGIC - 멀티 클라우드 배포를 위한 카탈로그 격리 전략 평가
# MAGIC - 열 수준 마스킹 및 행 수준 보안 구현
# MAGIC - RBAC와 ABAC 접근 방식 비교
# MAGIC
# MAGIC <hr/>

# COMMAND ----------

# DBTITLE 1,섹션 4: ID 및 관리
# MAGIC %md
# MAGIC ## 섹션 4: ID 및 관리
# MAGIC
# MAGIC | 노트북 | 유형 | 설명 |
# MAGIC |----------|------|-------------|
# MAGIC | [4.1 Databricks Identities]($./4. ID 및 관리/4.1 Databricks Identities) | 강의 | 사용자, 그룹, 서비스 프린시펄, IdP 페더레이션, SCIM, 토큰 관리, 그룹 설계 |
# MAGIC | [4.2 엔터프라이즈에서 Databricks 솔루션 배포]($./4. ID 및 관리/4.2 엔터프라이즈에서 Databricks 솔루션 배포) | 강의 | DevOps 패턴, Git Folders, Declarative Automation Bundles (DABs) |
# MAGIC | [4.3 데모 - DABs를 사용한 솔루션 배포]($./4. ID 및 관리/4.3 데모 - DABs를 사용한 솔루션 배포) | 데모 | 번들 구성, 다중 환경 타깃, CI/CD 통합 |
# MAGIC | [4.4 Databricks 감사 및 모니터링]($./4. ID 및 관리/4.4 Databricks 감사 및 모니터링) | 강의 | 감사 로그, 웨어하우스 성능, 비용 귀속, Lakehouse Monitoring |
# MAGIC
# MAGIC **학습 목표:**
# MAGIC - 대규모 배포를 위한 SSO, IdP 통합 및 SCIM 이해
# MAGIC - 사용자 및 그룹 관리 모범 사례 파악
# MAGIC - Declarative Automation Bundles (DABs) 및 GitOps를 사용한 솔루션 배포
# MAGIC - 감사 로그 분석을 위한 시스템 테이블 쿼리
# MAGIC - 성능 및 비용 귀속을 위한 모니터링 대시보드 구축
# MAGIC
# MAGIC <hr/>

# COMMAND ----------

# DBTITLE 1,대상 청중 및 사전 요구 사항
# MAGIC %md
# MAGIC ## 대상 청중 및 사전 요구 사항
# MAGIC
# MAGIC **대상 청중**
# MAGIC - 대규모 Databricks 환경 관리를 담당하는 **플랫폼 관리자**
# MAGIC - 대규모 데이터 웨어하우징 배포를 설계하는 **데이터 웨어하우스 아키텍트**
# MAGIC - 엔터프라이즈 수집·변환 파이프라인을 구축하는 **데이터 엔지니어**
# MAGIC - 거버넌스·컴플라이언스 제어를 구현하는 **보안 엔지니어**
# MAGIC
# MAGIC **사전 요구 사항**
# MAGIC - Databricks serverless SQL 웨어하우스에 대한 실무 지식
# MAGIC - Unity Catalog 기초
# MAGIC - 기본 클라우드 인프라 개념 (AWS, Azure 또는 GCP)