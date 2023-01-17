-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # 데이터 랩 추출 및 로드
-- MAGIC 
-- MAGIC 이 실습에서는 JSON 파일에서 원시 데이터를 추출하여 델타 테이블로 로드합니다.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
-- MAGIC - JSON 파일에서 데이터 추출을 위한 외부 테이블 생성
-- MAGIC - 제공된 스키마를 사용하여 빈 델타 테이블 생성
-- MAGIC - 기존 테이블의 레코드를 델타 테이블에 삽입
-- MAGIC - CTAS 문을 사용하여 파일에서 델타 테이블 만들기
-- MAGIC 
-- MAGIC # Extract and Load Data Lab
-- MAGIC 
-- MAGIC In this lab, you will extract and load raw data from JSON files into a Delta table.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create an external table to extract data from JSON files
-- MAGIC - Create an empty Delta table with a provided schema
-- MAGIC - Insert records from an existing table into a Delta table
-- MAGIC - Use a CTAS statement to create a Delta table from files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 다음 셀을 실행하여 이 과정에 대한 변수 및 데이터 세트를 구성합니다.
-- MAGIC 
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.5L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 데이터 개요
-- MAGIC 
-- MAGIC 우리는 JSON 파일로 작성된 원시 Kafka 데이터 샘플로 작업할 것이다.
-- MAGIC 
-- MAGIC 각 파일은 5초 간격 동안 소비된 모든 레코드를 포함하며, 전체 Kafka 스키마와 함께 다중 레코드 JSON 파일로 저장됩니다.
-- MAGIC 
-- MAGIC ## Overview of the Data
-- MAGIC 
-- MAGIC We will work with a sample of raw Kafka data written as JSON files. 
-- MAGIC 
-- MAGIC Each file contains all records consumed during a 5-second interval, stored with the full Kafka schema as a multiple-record JSON file. 
-- MAGIC 
-- MAGIC The schema for the table:
-- MAGIC 
-- MAGIC | field  | type | description |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | timestamp | LONG    | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## JSON 파일에서 원시 이벤트 추출
-- MAGIC 이 데이터를 Delta에 올바르게 로드하려면 먼저 올바른 스키마를 사용하여 JSON 데이터를 추출해야 합니다.
-- MAGIC 
-- MAGIC 아래 제공된 파일 경로에 있는 JSON 파일에 대해 외부 테이블을 만듭니다. 이 테이블의 이름을 **`events_json`** 로 지정하고 위의 스키마를 선언하십시오.
-- MAGIC  
-- MAGIC ## Extract Raw Events From JSON Files
-- MAGIC To load this data into Delta properly, we first need to extract the JSON data using the correct schema.
-- MAGIC 
-- MAGIC Create an external table against JSON files located at the filepath provided below. Name this table **`events_json`** and declare the schema above.

-- COMMAND ----------

-- TODO
<FILL_IN> ${da.paths.datasets}/raw/events-kafka/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **참고**: Python을 사용하여 랩 전체에서 가끔 검사를 실행합니다. 다음 셀은 지침을 따르지 않은 경우 변경해야 할 내용에 대한 메시지와 함께 오류를 반환합니다. 셀 실행에서 출력되지 않음은 이 단계를 완료했음을 의미합니다. 
-- MAGIC 
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return an error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC 
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 델타 테이블에 원시 이벤트 삽입
-- MAGIC 동일한 스키마를 사용하여 **`events_raw`** 라는 빈 관리 델타 테이블을 생성합니다.
-- MAGIC 
-- MAGIC ## Insert Raw Events Into Delta Table
-- MAGIC Create an empty managed Delta table named **`events_raw`** using the same schema.

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 테이블이 올바르게 작성되었는지 확인하십시오.
-- MAGIC 
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw"), "Table named `events_raw` does not exist"
-- MAGIC assert spark.table("events_raw").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_raw").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC assert spark.table("events_raw").count() == 0, "The table should have 0 records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 추출된 데이터와 델타 테이블이 준비되면 **`events_json`** 테이블의 JSON 레코드를 새 **`events_raw`** 델타 테이블에 삽입합니다.
-- MAGIC 
-- MAGIC Once the extracted data and Delta table are ready, insert the JSON records from the **`events_json`** table into the new **`events_raw`** Delta table.

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 테이블 내용을 수동으로 검토하여 데이터가 예상대로 작성되었는지 확인합니다.
-- MAGIC 
-- MAGIC Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

-- TODO
<FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 데이터가 올바르게 로드되었는지 확인하십시오.
-- MAGIC 
-- MAGIC Run the cell below to confirm the data has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw").count() == 2252, "The table should have 2252 records"
-- MAGIC assert set(row['timestamp'] for row in spark.table("events_raw").select("timestamp").limit(5).collect()) == {1593880885085, 1593880892303, 1593880889174, 1593880886106, 1593880889725}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 쿼리에서 델타 테이블 생성
-- MAGIC 새 이벤트 데이터 외에도 본 과정의 뒷부분에서 사용할 제품 세부 정보를 제공하는 작은 조회 테이블도 로드해 보겠습니다.
-- MAGIC CTAS 문을 사용하여 아래 제공된 parquet 디렉토리에서 데이터를 추출하는 **`item_lookup`** 이라는 관리되는 델타 테이블을 만듭니다.
-- MAGIC 
-- MAGIC ## Create Delta Table from a Query
-- MAGIC In addition to new events data, let's also load a small lookup table that provides product details that we'll use later in the course.
-- MAGIC Use a CTAS statement to create a managed Delta table named **`item_lookup`** that extracts data from the parquet directory provided below.

-- COMMAND ----------

-- TODO
<FILL_IN> ${da.paths.datasets}/raw/item-lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 Look 테이블을 실행하여 조회 테이블을 올바르게 로드되었습니다.
-- MAGIC 
-- MAGIC Run the cell below to confirm the lookup table has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("item_lookup").count() == 12, "The table should have 12 records"
-- MAGIC assert set(row['item_id'] for row in spark.table("item_lookup").select("item_id").limit(5).collect()) == {'M_PREM_F', 'M_PREM_K', 'M_PREM_Q', 'M_PREM_T', 'M_STAN_F'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다.
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>