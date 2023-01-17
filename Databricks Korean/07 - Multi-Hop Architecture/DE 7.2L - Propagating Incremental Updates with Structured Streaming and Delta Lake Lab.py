# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 구조화 스트리밍 및 델타 레이크를 통한 증분 업데이트 전파
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
# MAGIC * 구조화된 스트리밍 및 자동 로더에 대한 지식을 적용하여 간단한 멀티홉 아키텍처 구현
# MAGIC 
# MAGIC # Propagating Incremental Updates with Structured Streaming and Delta Lake
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Apply your knowledge of structured streaming and Auto Loader to implement a simple multi-hop architecture

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Setup
# MAGIC 다음 스크립트를 실행하여 필요한 변수를 설정하고 이 노트북의 이전 실행을 지웁니다. 이 셀을 다시 실행하면 랩을 다시 시작할 수 있습니다.
# MAGIC 
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.2L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 데이터 수집
# MAGIC 
# MAGIC 이 랩에서는 */databricks-datasets/retail-org/customers/* 에 있는 DBFS의 고객 관련 CSV 데이터 모음을 사용합니다.
# MAGIC 
# MAGIC 스키마 추론을 사용하여 자동 로더를 사용하여 이 데이터를 읽습니다(스키마 정보를 저장하려면 **`customers_checkpoint_path`** 를 사용하십시오). 원시 데이터를 **`bronze`** 라는 델타 테이블로 스트리밍합니다.
# MAGIC 
# MAGIC ## Ingest data
# MAGIC 
# MAGIC This lab uses a collection of customer-related CSV data from DBFS found in */databricks-datasets/retail-org/customers/*.
# MAGIC 
# MAGIC Read this data using Auto Loader using its schema inference (use **`customers_checkpoint_path`** to store the schema info). Stream the raw data to a Delta table called **`bronze`**.

# COMMAND ----------

# TODO
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

query = (spark
  .readStream
  <FILL-IN>
  .load("/databricks-datasets/retail-org/customers/")
  .writeStream
  <FILL-IN>
  .table("bronze")
)

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래 셀을 실행하여 작업을 확인하십시오.
# MAGIC 
# MAGIC Run the cell below to check your work.

# COMMAND ----------

assert spark.table("bronze"), "Table named `bronze` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'bronze'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("bronze").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL을 사용하여 변환을 수행할 수 있도록 브론즈 테이블에 스트리밍 임시 뷰를 생성해 보겠습니다.
# MAGIC 
# MAGIC Let's create a streaming temporary view into the bronze table, so that we can perform transforms using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데이터 정리 및 향상
# MAGIC 
# MAGIC CTAS 구문을 사용하여 다음 작업을 수행하는 **`bronze_enhanced_temp`** 라는 새 스트리밍 보기를 정의합니다:
# MAGIC * null **`postcode`** (0으로 설정)의 레코드를 건너뜁니다
# MAGIC * 현재 타임스탬프가 포함된 **`receipt_time`** 라는 열을 삽입합니다
# MAGIC * 입력 파일 이름이 포함된 **`source_file`** 이라는 열을 삽입합니다
# MAGIC 
# MAGIC ## Clean and enhance data
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`bronze_enhanced_temp`** that does the following:
# MAGIC * Skips records with a null **`postcode`** (set to zero)
# MAGIC * Inserts a column called **`receipt_time`** containing a current timestamp
# MAGIC * Inserts a column called **`source_file`** containing the input filename

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC SELECT
# MAGIC   <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래 셀을 실행하여 작업을 확인하십시오.
# MAGIC 
# MAGIC Run the cell below to check your work.

# COMMAND ----------

assert spark.table("bronze_enhanced_temp"), "Table named `bronze_enhanced_temp` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'bronze_enhanced_temp'").first()["isTemporary"] == True, "Table is not temporary"
assert spark.table("bronze_enhanced_temp").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string'), ('receipt_time', 'timestamp'), ('source_file', 'string')], "Incorrect Schema"
assert spark.table("bronze_enhanced_temp").isStreaming, "Not a streaming table"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Silver table
# MAGIC 
# MAGIC **`bronze_enhanced_temp`** 의 데이터를 **`silver`** 라는 테이블로 스트리밍합니다.
# MAGIC 
# MAGIC ## Silver table
# MAGIC 
# MAGIC Stream the data from **`bronze_enhanced_temp`** to a table called **`silver`**.

# COMMAND ----------

# TODO
silver_checkpoint_path = f"{DA.paths.checkpoints}/silver"

query = (spark.table("bronze_enhanced_temp")
  <FILL-IN>
  .table("silver"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래 셀을 실행하여 작업을 확인하십시오.
# MAGIC 
# MAGIC Run the cell below to check your work.

# COMMAND ----------

assert spark.table("silver"), "Table named `silver` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'silver'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("silver").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string'), ('receipt_time', 'timestamp'), ('source_file', 'string')], "Incorrect Schema"
assert spark.table("silver").filter("postcode <= 0").count() == 0, "Null postcodes present"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC SQL을 사용하여 비즈니스 레벨을 수행할 수 있도록 실버 테이블에 스트리밍 임시 뷰를 생성해 보겠습니다.
# MAGIC 
# MAGIC Let's create a streaming temporary view into the silver table, so that we can perform business-level using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Gold tables
# MAGIC 
# MAGIC CTAS 구문을 사용하여 주별로 고객 수를 계산하는 **`customer_count_temp`** 라는 새로운 스트리밍 보기를 정의합니다.
# MAGIC 
# MAGIC ## Gold tables
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_temp`** that counts customers per state.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_temp AS
# MAGIC SELECT 
# MAGIC <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 아래 셀을 실행하여 작업을 확인하십시오.
# MAGIC 
# MAGIC Run the cell below to check your work.

# COMMAND ----------

assert spark.table("customer_count_temp"), "Table named `customer_count_temp` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'customer_count_temp'").first()["isTemporary"] == True, "Table is not temporary"
assert spark.table("customer_count_temp").dtypes ==  [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 마지막으로 **`customer_count_temp`** 보기의 데이터를 **`gold_customer_count_by_state`** 라는 델타 테이블로 스트리밍합니다.
# MAGIC 
# MAGIC Finally, stream the data from the **`customer_count_temp`** view to a Delta table called **`gold_customer_count_by_state`**.

# COMMAND ----------

# TODO
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_counts"

query = (spark
  .table("customer_count_temp")
  .writeStream
  <FILL-IN>
  .table("gold_customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 아래 셀을 실행하여 작업을 확인하십시오.
# MAGIC 
# MAGIC Run the cell below to check your work.

# COMMAND ----------

assert spark.table("gold_customer_count_by_state"), "Table named `gold_customer_count_by_state` does not exist"
assert spark.sql(f"show tables").filter(f"tableName == 'gold_customer_count_by_state'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("gold_customer_count_by_state").dtypes ==  [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"
assert spark.table("gold_customer_count_by_state").count() == 51, "Incorrect number of rows" 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 결과 쿼리
# MAGIC 
# MAGIC **`gold_customer_count_by_state`** 테이블을 쿼리합니다(이것은 스트리밍 쿼리가 아닙니다). 결과를 막대 그래프로 표시하고 지도 그림도 사용합니다.
# MAGIC 
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`gold_customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_count_by_state

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC 다음 셀을 실행하여 데이터베이스 및 이 실습과 관련된 모든 데이터를 제거합니다.
# MAGIC 
# MAGIC Run the following cell to remove the database and all data associated with this lab.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 실습을 완료하면 다음과 같은 편안함을 느낄 수 있습니다:
# MAGIC * PySpark를 사용하여 증분 데이터 수집을 위한 자동 로더 구성
# MAGIC * Spark SQL을 사용하여 스트리밍 데이터 집계
# MAGIC * 델타 테이블로 데이터 스트리밍
# MAGIC 
# MAGIC By completing this lab, you should now feel comfortable:
# MAGIC * Using PySpark to configure Auto Loader for incremental data ingestion
# MAGIC * Using Spark SQL to aggregate streaming data
# MAGIC * Streaming data to a Delta table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>