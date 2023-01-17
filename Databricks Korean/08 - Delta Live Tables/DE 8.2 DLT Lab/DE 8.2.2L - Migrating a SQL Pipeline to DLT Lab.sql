-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # 실습: SQL 파이프라인을 델타 라이브 테이블로 마이그레이션
-- MAGIC 
-- MAGIC 이 노트북은 SQL을 사용하여 DLT 파이프라인을 구현하기 위해 작성됩니다. 
-- MAGIC 
-- MAGIC 이는 **대화식으로 실행되는 것이 아니라** 변경을 완료한 후 파이프라인으로 배포됩니다.
-- MAGIC 
-- MAGIC 이 노트북을 작성하는 데 도움이 되려면 <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql" target="_blank">를 참조하십시오DLT 구문 설명서</a>.
-- MAGIC 
-- MAGIC # Lab: Migrating a SQL Pipeline to Delta Live Tables
-- MAGIC 
-- MAGIC This notebook will be completed by you to implement a DLT pipeline using SQL. 
-- MAGIC 
-- MAGIC It is **not intended** to be executed interactively, but rather to be deployed as a pipeline once you have completed your changes.
-- MAGIC 
-- MAGIC To aid in completion of this Notebook, please refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql" target="_blank">DLT syntax documentation</a>.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Bronze Table 선언
-- MAGIC 
-- MAGIC 시뮬레이션된 클라우드 소스에서 자동 로더를 사용하여 JSON 데이터를 증분 수집하는 Bronze 테이블을 선언합니다. 원본 위치가 이미 인수로 제공되었습니다. 이 값을 사용하는 방법은 아래 셀에 나와 있습니다.
-- MAGIC 
-- MAGIC 이전에 수행한 것처럼 두 개의 열을 추가로 포함합니다:
-- MAGIC * **`current_timestamp()`**  에서 반환한 타임스탬프를 기록하는 **`receipt_time`**
-- MAGIC * **`input_file_name()`** 에서 가져온 **`source_file`** 
-- MAGIC 
-- MAGIC ## Declare Bronze Table
-- MAGIC 
-- MAGIC Declare a bronze table that ingests JSON data incrementally (using Auto Loader) from the simulated cloud source. The source location is already supplied as an argument; using this value is illustrated in the cell below.
-- MAGIC 
-- MAGIC As we did previously, include two additional columns:
-- MAGIC * **`receipt_time`** that records a timestamp as returned by **`current_timestamp()`** 
-- MAGIC * **`source_file`** that is obtained by **`input_file_name()`**

-- COMMAND ----------

-- TODO
CREATE <FILL-IN>
AS SELECT <FILL-IN>
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### PII 파일
-- MAGIC 
-- MAGIC 유사한 CTAS 구문을 사용하여 */mnt/training/healthcare/patient* 에 있는 CSV 데이터에 라이브 **table** 을 생성합니다.
-- MAGIC 
-- MAGIC 이 소스에 대해 자동 로더를 올바르게 구성하려면 다음과 같은 추가 매개 변수를 지정해야 합니다:
-- MAGIC 
-- MAGIC ### PII File
-- MAGIC 
-- MAGIC Using a similar CTAS syntax, create a live **table** into the CSV data found at */mnt/training/healthcare/patient*.
-- MAGIC 
-- MAGIC To properly configure Auto Loader for this source, you will need to specify the following additional parameters:
-- MAGIC 
-- MAGIC | option | value |
-- MAGIC | --- | --- |
-- MAGIC | **`header`** | **`true`** |
-- MAGIC | **`cloudFiles.inferColumnTypes`** | **`true`** |
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> CSV용 자동 로더 구성은 <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-csv.html" target="_blank">여기</a> 에서 확인할 수 있습니다.
-- MAGIC   
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Auto Loader configurations for CSV can be found <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-csv.html" target="_blank">here</a>.

-- COMMAND ----------

-- TODO
CREATE <FILL-IN> pii
AS SELECT *
  FROM cloud_files("/mnt/training/healthcare/patient", "csv", map(<FILL-IN>))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Silver Tables 선언
-- MAGIC 
-- MAGIC 당사의 실버 테이블인 **`recordings_parsed`** 는 다음 필드로 구성됩니다:
-- MAGIC 
-- MAGIC ## Declare Silver Tables
-- MAGIC 
-- MAGIC Our silver table, **`recordings_parsed`**, will consist of the following fields:
-- MAGIC 
-- MAGIC | Field | Type |
-- MAGIC | --- | --- |
-- MAGIC | **`device_id`** | **`INTEGER`** |
-- MAGIC | **`mrn`** | **`LONG`** |
-- MAGIC | **`heartrate`** | **`DOUBLE`** |
-- MAGIC | **`time`** | **`TIMESTAMP`** (example provided below) |
-- MAGIC | **`name`** | **`STRING`** |
-- MAGIC 
-- MAGIC 이 쿼리는 또한 이름을 얻기 위해 공통 **`mrn`** 필드의 **`pii`**  테이블과 내부 조인을 통해 데이터를 풍부하게 해야 한다.
-- MAGIC 
-- MAGIC 유효하지 않은 **`heartrate`** (즉, 0보다 크지 않음)의 레코드를 삭제하는 제약 조건을 적용하여 품질 관리를 구현합니다.
-- MAGIC 
-- MAGIC This query should also enrich the data through an inner join with the **`pii`** table on the common **`mrn`** field to obtain the name.
-- MAGIC 
-- MAGIC Implement quality control by applying a constraint to drop records with an invalid **`heartrate`** (that is, not greater than zero).

-- COMMAND ----------

-- TODO
CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
  (<FILL-IN add a constraint to drop records when heartrate ! > 0>)
AS SELECT 
  CAST(<FILL-IN>) device_id, 
  <FILL-IN mrn>, 
  <FILL-IN heartrate>, 
  CAST(FROM_UNIXTIME(DOUBLE(time), 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time 
  FROM STREAM(live.recordings_bronze)
  <FILL-IN specify source and perform inner join with pii on mrn>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Table
-- MAGIC 
-- MAGIC **`mrn`**, **`name`**, **`date`** 를 기준으로 **`recordings_enriched`** 를 집계하여 다음 열을 제공하는 **`daily_patient_avg`** 라는 골드 테이블을 만듭니다:
-- MAGIC 
-- MAGIC ## Gold Table
-- MAGIC 
-- MAGIC Create a gold table, **`daily_patient_avg`**, that aggregates **`recordings_enriched`** by **`mrn`**, **`name`**, and **`date`** and delivers the following columns:
-- MAGIC 
-- MAGIC | Column name | Value |
-- MAGIC | --- | --- |
-- MAGIC | **`mrn`** | **`mrn`** from source |
-- MAGIC | **`name`** | **`name`** from source |
-- MAGIC | **`avg_heartrate`** | Average **`heartrate`** from the grouping |
-- MAGIC | **`date`** | Date extracted from **`time`** |

-- COMMAND ----------

-- TODO
CREATE <FILL-IN> daily_patient_avg
  COMMENT <FILL-IN insert comment here>
AS SELECT <FILL-IN>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>