# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Spark SQL과 함께 자동 로더 및 구조화 스트리밍 사용
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
# MAGIC * 자동 로더를 사용하여 데이터 수집
# MAGIC * 스트리밍 데이터 집계
# MAGIC * 데이터를 델타 테이블로 스트리밍
# MAGIC 
# MAGIC # Using Auto Loader and Structured Streaming with Spark SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Ingest data using Auto Loader
# MAGIC * Aggregate streaming data
# MAGIC * Stream data to a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Setup
# MAGIC 다음 스크립트를 실행하여 필요한 변수를 설정하고 이 노트북의 이전 실행을 지웁니다. 이 셀을 다시 실행하면 랩을 다시 시작할 수 있습니다.
# MAGIC 
# MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.3L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 스트리밍 읽기 구성
# MAGIC 
# MAGIC 이 랩에서는 */databricks-datasets/retail-org/customers/*에 있는 DBFS의 고객 관련 CSV 데이터 모음을 사용합니다.
# MAGIC 
# MAGIC 스키마 추론을 사용하여 <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a>를 사용하여 이 데이터를 읽습니다(스키마 정보를 저장하려면 **`customers_checkpoint_path`** 를 사용하십시오). **`customers_raw_temp`** 라는 스트리밍 임시 보기를 만듭니다.
# MAGIC 
# MAGIC ## Configure Streaming Read
# MAGIC 
# MAGIC This lab uses a collection of customer-related CSV data from DBFS found in */databricks-datasets/retail-org/customers/*.
# MAGIC 
# MAGIC Read this data using <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> using its schema inference (use **`customers_checkpoint_path`** to store the schema info). Create a streaming temporary view called **`customers_raw_temp`**.

# COMMAND ----------

# TODO
customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

(spark
  .readStream
  <FILL-IN>
  .load("/databricks-datasets/retail-org/customers/")
  .createOrReplaceTempView("customers_raw_temp"))

# COMMAND ----------

from pyspark.sql import Row
assert Row(tableName="customers_raw_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customers_raw_temp").dtypes ==  [('customer_id', 'string'),
 ('tax_id', 'string'),
 ('tax_code', 'string'),
 ('customer_name', 'string'),
 ('state', 'string'),
 ('city', 'string'),
 ('postcode', 'string'),
 ('street', 'string'),
 ('number', 'string'),
 ('unit', 'string'),
 ('region', 'string'),
 ('district', 'string'),
 ('lon', 'string'),
 ('lat', 'string'),
 ('ship_to_address', 'string'),
 ('valid_from', 'string'),
 ('valid_to', 'string'),
 ('units_purchased', 'string'),
 ('loyalty_segment', 'string'),
 ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 스트리밍 집계 정의
# MAGIC 
# MAGIC CTAS 구문을 사용하여 **`customer_count`** 라는 필드에서 **`state`** 당 고객 수를 계산하는 **`customer_count_by_state_temp`** 라는 새로운 스트리밍 보기를 정의합니다.
# MAGIC 
# MAGIC ## Define a streaming aggregation
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_by_state_temp`** that counts the number of customers per **`state`**, in a field called **`customer_count`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC SELECT
# MAGIC   <FILL-IN>

# COMMAND ----------

assert Row(tableName="customer_count_by_state_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state_temp").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 집계된 데이터를 델타 테이블에 기록
# MAGIC 
# MAGIC **`customer_count_by_state_temp`** 보기에서 **`customer_count_by_state`** 라는 델타 테이블로 데이터를 스트리밍합니다.
# MAGIC 
# MAGIC ## Write aggregated data to a Delta table
# MAGIC 
# MAGIC Stream data from the **`customer_count_by_state_temp`** view to a Delta table called **`customer_count_by_state`**.

# COMMAND ----------

# TODO
customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_count"

query = (spark
  <FILL-IN>

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

assert Row(tableName="customer_count_by_state", isTemporary=False) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 결과 쿼리
# MAGIC 
# MAGIC **`customer_count_by_state`** 테이블을 쿼리합니다(이것은 스트리밍 쿼리가 아닙니다). 결과를 막대 그래프로 표시하고 지도 그림도 사용합니다.
# MAGIC 
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO

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