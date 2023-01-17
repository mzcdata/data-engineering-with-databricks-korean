# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-9.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # DLT 파이프라인 결과 탐색
# MAGIC 
# MAGIC 다음 셀을 실행하여 저장 위치의 출력을 열거합니다:
# MAGIC 
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC Run the following cell to enumerate the output of your storage location:

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC **system** 디렉토리는 파이프라인과 관련된 이벤트를 캡처합니다.
# MAGIC 
# MAGIC The **system** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이러한 이벤트 로그는 델타 테이블로 저장됩니다. 
# MAGIC 
# MAGIC 테이블을 조회해 봅시다.
# MAGIC 
# MAGIC These event logs are stored as a Delta table. 
# MAGIC 
# MAGIC Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.working_dir}/storage/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC *tables* 디렉토리의 내용을 살펴보겠습니다.
# MAGIC 
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/storage/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 골드 테이블을 조회해 봅시다.
# MAGIC 
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()
