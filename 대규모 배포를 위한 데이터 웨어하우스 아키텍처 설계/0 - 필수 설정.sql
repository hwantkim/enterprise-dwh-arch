-- Databricks notebook source
-- DBTITLE 1,요약: 필수 설정
-- MAGIC %md
-- MAGIC # 필수 설정
-- MAGIC
-- MAGIC 이 노트북은 과정의 모든 데모와 실습을 위한 기본 환경을 준비합니다. 카탈로그/스키마(bronze, silver, gold) 생성, 랜딩 존 볼륨 생성, 샘플 데이터 CSV 내보내기, 증분 처리용 스테이징 데이터 생성을 수행합니다. **Serverless** 컴퓨팅이 필요하며, 상단의 **Run All**로 실행하세요 (1~2분 소요).

-- COMMAND ----------

-- DBTITLE 1,Catalog Setup
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_name", "dbrx_demo", "카탈로그 이름")
-- MAGIC
-- MAGIC my_catalog = dbutils.widgets.get("catalog_name")
-- MAGIC
-- MAGIC print(f"CATALOG    = {my_catalog}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE CATALOG IF NOT EXISTS {my_catalog};
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 1,스키마 및 볼륨 생성
-- 스키마 및 랜딩 존 볼륨 생성

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name || '.bakehouse_incoming');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name || '.bronze');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name || '.silver');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog_name || '.gold');

CREATE VOLUME IF NOT EXISTS IDENTIFIER(:catalog_name || '.bakehouse_incoming.landing_zone');

SELECT 'Schemas and volume created successfully' AS status;

-- COMMAND ----------

-- DBTITLE 1,샘플 데이터 내보내기
-- MAGIC %python
-- MAGIC
-- MAGIC # 랜딩 존에 Bakehouse 데이터 내보내기
-- MAGIC # samples.bakehouse의 데이터를 CSV 파일로 내보내어 외부 시스템의 원본 데이터를 시뮬레이션합니다.
-- MAGIC
-- MAGIC from pyspark.sql.functions import lit, current_timestamp
-- MAGIC
-- MAGIC catalog = my_catalog
-- MAGIC landing_zone = f"/Volumes/{catalog}/bakehouse_incoming/landing_zone"
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("Exporting initial data to landing zone...")
-- MAGIC print("=" * 60)
-- MAGIC
-- MAGIC # 트랜잭션 내보내기
-- MAGIC print("\nExporting sales_transactions...")
-- MAGIC txn_df = spark.table("samples.bakehouse.sales_transactions")
-- MAGIC txn_df.write.mode("overwrite").option("header", "true").csv(f"{landing_zone}/sales_transactions")
-- MAGIC print(f"  Exported {txn_df.count():,} transactions")
-- MAGIC
-- MAGIC # 프랜차이즈 내보내기 (스키마 호환성을 위한 CDC 컬럼 포함)
-- MAGIC print("\nExporting sales_franchises...")
-- MAGIC franchise_df = (
-- MAGIC     spark.table("samples.bakehouse.sales_franchises")
-- MAGIC     .withColumn("_operation", lit("INSERT"))
-- MAGIC     .withColumn("_change_timestamp", lit("2024-01-01 00:00:00"))
-- MAGIC )
-- MAGIC franchise_df.write.mode("overwrite").option("header", "true").csv(f"{landing_zone}/sales_franchises")
-- MAGIC print(f"  Exported {franchise_df.count():,} franchises")
-- MAGIC
-- MAGIC print("\n" + "=" * 60)
-- MAGIC print("Initial data export complete!")
-- MAGIC print("=" * 60)

-- COMMAND ----------

-- DBTITLE 1,증분 데이터 스테이징
-- MAGIC %md
-- MAGIC ## 증분 데이터 스테이징
-- MAGIC
-- MAGIC 증분 처리 데모를 위한 스테이징 데이터 파일(CDC 레코드, DQ 테스트 데이터)을 생성합니다. 실습 중 Catalog UI를 통해 수동으로 랜딩 존으로 이동합니다.

-- COMMAND ----------

-- DBTITLE 1,Franchise CDC 데이터 스테이징
-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC # 프랜차이즈 CDC 데이터 스테이징 (SCD Type 2 데모용)
-- MAGIC
-- MAGIC # 다음을 나타내는 CDC 레코드로 단일 CSV 파일을 생성합니다:
-- MAGIC # - 신규 프랜차이즈 (INSERT)
-- MAGIC # - 이전된 프랜차이즈 (UPDATE)  
-- MAGIC # - 폐업한 프랜차이즈 (DELETE)
-- MAGIC
-- MAGIC import os
-- MAGIC
-- MAGIC catalog = my_catalog
-- MAGIC staging_path = f"/Volumes/{catalog}/bakehouse_incoming/landing_zone/_staging"
-- MAGIC
-- MAGIC # 스테이징 디렉토리 생성
-- MAGIC os.makedirs(staging_path, exist_ok=True)
-- MAGIC
-- MAGIC # Spark 없이 직접 CSV 작성
-- MAGIC csv_content = """franchiseID,name,city,district,zipcode,country,size,longitude,latitude,supplierID,_operation,_change_timestamp
-- MAGIC 9001,Melbourne Central Bakehouse,Melbourne,CBD,3000,Australia,Large,144.9631,-37.8136,101,INSERT,2024-12-15 09:00:00
-- MAGIC 9002,Sydney Harbor Breads,Sydney,The Rocks,2000,Australia,Medium,151.2093,-33.8688,102,INSERT,2024-12-15 09:30:00
-- MAGIC 9003,Brisbane River Bakery,Brisbane,South Bank,4101,Australia,Small,153.0251,-27.4698,103,INSERT,2024-12-15 10:00:00
-- MAGIC 5,Downtown Bakehouse,San Francisco,Financial District,94111,USA,Large,-122.3964,37.7949,50,UPDATE,2024-12-15 11:00:00
-- MAGIC 8,Riverside Breads,London,Canary Wharf,E14 5AB,UK,Large,-0.0235,51.5054,80,UPDATE,2024-12-15 11:30:00
-- MAGIC 12,Hilltop Pastries,Paris,Montmartre,75018,France,Medium,2.3387,48.8867,120,UPDATE,2024-12-15 12:00:00
-- MAGIC 3,,,,,,,,,DELETE,2024-12-15 13:00:00
-- MAGIC 7,,,,,,,,,DELETE,2024-12-15 13:30:00"""
-- MAGIC
-- MAGIC file_path = f"{staging_path}/franchise_changes.csv"
-- MAGIC with open(file_path, 'w') as f:
-- MAGIC     f.write(csv_content)
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("Franchise CDC data staged")
-- MAGIC print("=" * 60)
-- MAGIC print(f"\nFile: {file_path}")
-- MAGIC print("\nRecords:")
-- MAGIC print("  - 3 new franchises (INSERT)")
-- MAGIC print("  - 3 relocated franchises (UPDATE)")
-- MAGIC print("  - 2 closed franchises (DELETE)")

-- COMMAND ----------

-- DBTITLE 1,DQ 테스트 트랜잭션 데이터 스테이징
-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC # DQ 위반이 포함된 트랜잭션 데이터 스테이징
-- MAGIC
-- MAGIC # 일부 데이터 품질 위반이 포함된 트랜잭션 레코드로 
-- MAGIC # 단일 CSV 파일을 생성하여 EXPECT 제약 조건을 시연합니다.
-- MAGIC
-- MAGIC import os
-- MAGIC
-- MAGIC catalog = my_catalog
-- MAGIC staging_path = f"/Volumes/{catalog}/bakehouse_incoming/landing_zone/_staging"
-- MAGIC
-- MAGIC # Spark 없이 직접 CSV 작성
-- MAGIC csv_content = """transactionID,customerID,franchiseID,dateTime,product,quantity,unitPrice,totalPrice,paymentMethod,cardNumber
-- MAGIC 900001,1001,5,2024-12-15 10:30:00,Croissant,3,4,12,Credit Card,4111111111111111
-- MAGIC 900002,1002,5,2024-12-15 11:00:00,Baguette,2,5,10,Cash,
-- MAGIC 900003,1003,8,2024-12-15 11:30:00,Sourdough,1,7,7,Debit Card,5500000000000004
-- MAGIC 900004,1004,8,2024-12-15 12:00:00,Muffin,6,3,18,Mobile Payment,
-- MAGIC 900005,1005,12,2024-12-15 12:30:00,Danish,4,4,16,Credit Card,4000000000000002
-- MAGIC 900006,,5,2024-12-15 15:00:00,Croissant,2,4,8,Credit Card,4111111111111111
-- MAGIC 900007,1006,,2024-12-15 15:30:00,Baguette,1,5,5,Cash,
-- MAGIC 900008,1007,5,2024-12-15 16:00:00,Muffin,-2,3,-6,Debit Card,5500000000000004
-- MAGIC 900009,1008,8,2024-12-15 16:30:00,Danish,3,4,0,Credit Card,4000000000000002
-- MAGIC 900010,1009,12,2024-12-15 17:00:00,Sourdough,2,7,14,Bitcoin,"""
-- MAGIC
-- MAGIC file_path = f"{staging_path}/additional_transactions.csv"
-- MAGIC with open(file_path, 'w') as f:
-- MAGIC     f.write(csv_content)
-- MAGIC
-- MAGIC print("=" * 60)
-- MAGIC print("Transaction DQ test data staged")
-- MAGIC print("=" * 60)
-- MAGIC print(f"\nFile: {file_path}")
-- MAGIC print("\nRecords:")
-- MAGIC print("  - 5 valid transactions")
-- MAGIC print("  - 4 with DQ violations (will be dropped)")
-- MAGIC print("  - 1 with invalid payment method (warning only)")

-- COMMAND ----------

-- DBTITLE 1,스테이징 데이터 요약
-- MAGIC %md
-- MAGIC **_staging 폴더 안내:** `franchise_changes.csv`(SCD Type 2용 CDC)와 `additional_transactions.csv`(DQ 위반 트랜잭션)가 포함되어 있습니다. 데모 중 안내에 따라 사용하세요.

-- COMMAND ----------

-- DBTITLE 1,설정 완료
-- MAGIC %md
-- MAGIC ## 설정 완료
-- MAGIC
-- MAGIC 과정 시작을 위한 환경이 준비되었습니다.

-- COMMAND ----------

-- DBTITLE 1,설정 완료 출력
-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC catalog = my_catalog
-- MAGIC landing_zone = f"/Volumes/{catalog}/bakehouse_incoming/landing_zone"
-- MAGIC
-- MAGIC print("=" * 70)
-- MAGIC print("SETUP COMPLETE")
-- MAGIC print("=" * 70)
-- MAGIC
-- MAGIC print(f"\nCatalog: {catalog}")
-- MAGIC
-- MAGIC print("\nSchemas created:")
-- MAGIC print(f"  - {catalog}.bakehouse_incoming")
-- MAGIC print(f"  - {catalog}.bronze")
-- MAGIC print(f"  - {catalog}.silver")
-- MAGIC print(f"  - {catalog}.gold")
-- MAGIC
-- MAGIC print(f"\nLanding zone volume:")
-- MAGIC print(f"  {landing_zone}/")
-- MAGIC
-- MAGIC print("\nInitial data files:")
-- MAGIC print(f"  {landing_zone}/sales_transactions/")
-- MAGIC print(f"  {landing_zone}/sales_franchises/")
-- MAGIC
-- MAGIC print("\nStaged incremental data (for demos):")
-- MAGIC print(f"  {landing_zone}/_staging/franchise_cdc/")
-- MAGIC print(f"  {landing_zone}/_staging/transactions_dq/")
-- MAGIC
-- MAGIC print("\n" + "=" * 70)
-- MAGIC print("You are ready to begin the course!")
-- MAGIC print("=" * 70)

-- COMMAND ----------
