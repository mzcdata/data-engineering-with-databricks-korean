# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # DLT 파이프라인 결과 탐색
# MAGIC 
# MAGIC 이 노트북에서는 DLT 파이프라인의 실행 결과를 살펴봅니다.
# MAGIC 
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC This notebook explores the execution results of a DLT pipeline.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.3

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC **`system`** 디렉토리는 파이프라인과 관련된 이벤트를 캡처합니다.
# MAGIC 
# MAGIC The **`system`** directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 이러한 이벤트 로그는 델타 테이블로 저장됩니다. 테이블을 조회해 봅시다.
# MAGIC 
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC *tables* 디렉토리의 내용을 살펴보겠습니다.
# MAGIC 
# MAGIC Let's view the contents of the *tables* directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC gold table을 조회해 봅시다.
# MAGIC 
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.sales_order_in_la

# COMMAND ----------

# MAGIC %md
# MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다.
# MAGIC  
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()
