# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lakehouse의 엔드 투 엔드 ETL
# MAGIC ## 최종 단계
# MAGIC 
# MAGIC 이 랩의 첫 번째 노트북인 [DE 12.2.1L - 지침 및 구성]($/DE 12.2.1L - 지침 및 구성)을 살펴봅니다
# MAGIC 
# MAGIC 모든 것이 올바르게 설정된 경우 다음을 수행해야 합니다:
# MAGIC * **Continuous** 모드로 실행되는 DLT 파이프라인
# MAGIC * 2분마다 해당 파이프라인에 새 데이터를 공급하는 작업
# MAGIC * 해당 파이프라인의 출력을 분석하는 일련의 Datbricks SQL 쿼리
# MAGIC 
# MAGIC # End-to-End ETL in the Lakehouse
# MAGIC ## Final Steps
# MAGIC 
# MAGIC We are picking up from the first notebook in this lab, [DE 12.2.1L - Instructions and Configuration]($./DE 12.2.1L - Instructions and Configuration)
# MAGIC 
# MAGIC If everything is setup correctly, you should have:
# MAGIC * A DLT Pipeline running in **Continuous** mode
# MAGIC * A job that is feeding that pipline new data every 2 minutes
# MAGIC * A series of Databricks SQL Queries analysing the outputs of that pipeline

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC ## 손상된 데이터를 복구하기 위한 쿼리 실행
# MAGIC 
# MAGIC **`recordings_enriched`** 테이블을 정의한 코드를 검토하여 품질 검사에 적용되는 필터를 식별합니다.
# MAGIC 
# MAGIC 아래 셀에 이 품질 검사에서 거부된 **`recordings_bronze`** 테이블의 모든 레코드를 반환하는 쿼리를 작성합니다.
# MAGIC 
# MAGIC ## Execute a Query to Repair Broken Data
# MAGIC 
# MAGIC Review the code that defined the **`recordings_enriched`** table to identify the filter applied for the quality check.
# MAGIC 
# MAGIC In the cell below, write a query that returns all the records from the **`recordings_bronze`** table that were refused by this quality check.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 데모의 목적을 위해, 데이터와 시스템에 대한 철저한 수동 검토가 그렇지 않을 경우 유효한 심박수 기록이 음수 값으로 반환된다는 것을 입증했다고 가정한다.
# MAGIC 
# MAGIC 다음 쿼리를 실행하여 음수 기호가 제거된 동일한 행을 검사합니다.
# MAGIC 
# MAGIC For the purposes of our demo, let's assume that thorough manual review of our data and systems has demonstrated that occasionally otherwise valid heartrate recordings are returned as negative values.
# MAGIC 
# MAGIC Run the following query to examine these same rows with the negative sign removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT abs(heartrate), * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC 데이터 세트를 완료하기 위해 이러한 고정 레코드를 은색 **`recordings_enriched`** 테이블에 삽입하고자 한다.
# MAGIC 
# MAGIC 아래 셀을 사용하여 DLT 파이프라인에서 사용한 쿼리를 업데이트하여 이 복구를 실행합니다.
# MAGIC 
# MAGIC **참고**: 품질 검사로 인해 이전에 거부된 레코드만 처리하도록 코드를 업데이트하십시오.
# MAGIC 
# MAGIC To complete our dataset, we wish to insert these fixed records into the silver **`recordings_enriched`** table.
# MAGIC 
# MAGIC Use the cell below to update the query used in the DLT pipeline to execute this repair.
# MAGIC 
# MAGIC **NOTE**: Make sure you update the code to only process those records that were previously rejected due to the quality check.

# COMMAND ----------

# TODO
# CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
#   (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
# AS SELECT 
#   CAST(a.device_id AS INTEGER) device_id, 
#   CAST(a.mrn AS LONG) mrn, 
#   CAST(a.heartrate AS DOUBLE) heartrate, 
#   CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
#   b.name
#   FROM STREAM(live.recordings_bronze) a
#   INNER JOIN STREAM(live.pii) b
#   ON a.mrn = b.mrn

# COMMAND ----------

# MAGIC %md
# MAGIC 아래 셀을 사용하여 수동 또는 프로그래밍 방식으로 이 업데이트가 성공적으로 완료되었는지 확인하십시오.
# MAGIC 
# MAGIC (이제 **`recordings_bronze`** 의 총 레코드 수는 **`recordings_enriched`** 의 총 레코드 수와 같아야 합니다.).
# MAGIC 
# MAGIC Use the cell below to manually or programmatically confirm that this update has been successful.
# MAGIC 
# MAGIC (The total number of records in the **`recordings_bronze`** should now be equal to the total records in **`recordings_enriched`**).

# COMMAND ----------

# TODO
<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 프로덕션 데이터 사용 권한 고려
# MAGIC 
# MAGIC 데이터의 수동 복구는 성공적이었지만 이러한 데이터 세트의 소유자로서 기본적으로 코드를 실행하는 모든 위치에서 이러한 데이터를 수정하거나 삭제할 수 있는 권한이 있습니다.
# MAGIC 
# MAGIC 즉, 현재 사용자의 권한으로 실수로 SQL 쿼리가 실행되거나 다른 사용자에게 유사한 권한이 부여된 경우 현재 사용 권한으로 프로덕션 테이블을 영구적으로 변경하거나 삭제할 수 있습니다.
# MAGIC 
# MAGIC 본 연구소에서는 개발에서 프로덕션으로 코드를 이동함에 따라 데이터에 대한 전체 권한을 갖기를 원했지만, 실수를 방지하기 위해 작업 및 DLT 파이프라인을 예약할 때는 <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank"> 서비스 원리를 활용하는 것이 더 안전합니다 데이터 수정.
# MAGIC 
# MAGIC ## Consider Production Data Permissions
# MAGIC 
# MAGIC Note that while our manual repair of the data was successful, as the owner of these datasets, by default we have permissions to modify or delete these data from any location we're executing code.
# MAGIC 
# MAGIC To put this another way: our current permissions would allow us to change or drop our production tables permanently if an errant SQL query is accidentally executed with the current user's permissions (or if other users are granted similar permissions).
# MAGIC 
# MAGIC While for the purposes of this lab, we desired to have full permissions on our data, as we move code from development to production, it is safer to leverage <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank">service principals</a> when scheduling Jobs and DLT Pipelines to avoid accidental data modifications.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 운영 인프라 종료
# MAGIC 
# MAGIC Databricks Jobs, DLT Pipeline 및 예약된 DBSQL 쿼리 및 대시보드는 모두 프로덕션 코드의 지속적인 실행을 제공하도록 설계되었습니다. 이 엔드 투 엔드 데모에서는 연속적인 데이터 처리를 위해 작업 및 파이프라인을 구성하라는 지시를 받았습니다. 이러한 워크로드가 계속 실행되지 않도록 하려면 데이터브릭스 작업을 일시 중지하고 DLT 파이프라인을 **중지*해야 합니다. 이러한 자산을 삭제하면 프로덕션 인프라도 종료됩니다.
# MAGIC 
# MAGIC **참고**: 이전 교육에서 DBSQL 자산 스케줄링에 대한 모든 지침은 사용자에게 업데이트 일정을 내일 종료하도록 설정하도록 지시했습니다. 이전으로 돌아가서 이러한 업데이트를 취소하여 DBSQL 엔드포인트가 해당 시간까지 유지되지 않도록 할 수도 있습니다.
# MAGIC 
# MAGIC ## Shut Down Production Infrastructure
# MAGIC 
# MAGIC Note that Databricks Jobs, DLT Pipelines, and scheduled DBSQL queries and dashboards are all designed to provide sustained execution of production code. In this end-to-end demo, you were instructed to configure a Job and Pipeline for continuous data processing. To prevent these workloads from continuing to execute, you should **Pause** your Databricks Job and **Stop** your DLT pipeline. Deleting these assets will also ensure that production infrastructure is terminated.
# MAGIC 
# MAGIC **NOTE**: All instructions for DBSQL asset scheduling in previous lessons instructed users to set the update schedule to end tomorrow. You may choose to go back and also cancel these updates to prevent DBSQL endpoints from staying on until that time.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>