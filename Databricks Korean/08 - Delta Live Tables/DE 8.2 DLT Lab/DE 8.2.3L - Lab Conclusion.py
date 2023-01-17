# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 실습: 결론
# MAGIC 다음 셀을 실행하여 실험실 환경을 구성합니다:
# MAGIC 
# MAGIC # Lab: Conclusion
# MAGIC Running the following cell to configure the lab environment:

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.3L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 결과 표시
# MAGIC 
# MAGIC 파이프라인이 성공적으로 실행되면 골드 테이블의 내용을 표시합니다.
# MAGIC 
# MAGIC **참고**: **Target**에 값을 지정했기 때문에 테이블이 지정된 데이터베이스에 게시됩니다. **Target** 사양이 없으면 DBFS의 기본 위치(*스토리지 위치** 기준)를 기준으로 테이블을 쿼리해야 합니다.
# MAGIC 
# MAGIC ## Display Results
# MAGIC 
# MAGIC Assuming your pipeline runs successfully, display the contents of the gold table.
# MAGIC 
# MAGIC **NOTE**: Because we specified a value for **Target**, tables are published to the specified database. Without a **Target** specification, we would need to query the table based on its underlying location in DBFS (relative to the **Storage Location**).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀로 다른 파일 도착을 트리거합니다. 
# MAGIC 
# MAGIC 원하는 경우 몇 번 더 실행해 보십시오. 
# MAGIC 
# MAGIC 그런 다음 파이프라인을 다시 실행하고 결과를 봅니다. 
# MAGIC 
# MAGIC 위의 셀을 자유롭게 다시 실행하여 **`daily_patient_avg`** 테이블의 업데이트된 보기를 확인하십시오.
# MAGIC 
# MAGIC Trigger another file arrival with the following cell. 
# MAGIC 
# MAGIC Feel free to run it a couple more times if desired. 
# MAGIC 
# MAGIC Following this, run the pipeline again and view the results. 
# MAGIC 
# MAGIC Feel free to re-run the cell above to gain an updated view of the **`daily_patient_avg`** table.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC DLT UI에서 파이프라인을 삭제하고 다음 셀을 실행하여 랩 설정 및 실행의 일부로 작성된 파일 및 테이블을 정리하십시오.
# MAGIC 
# MAGIC Ensure that you delete your pipeline from the DLT UI, and run the following cell to clean up the files and tables that were created as part of the lab setup and execution.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Summary
# MAGIC 이 실습에서는 기존 데이터 파이프라인을 Delta Live Tables SQL 파이프라인으로 변환하는 방법을 배웠고 DLT UI를 사용하여 해당 파이프라인을 배포했습니다.
# MAGIC 
# MAGIC In this lab, you learned to convert an existing data pipeline to a Delta Live Tables SQL pipeline, and deployed that pipeline using the DLT UI.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/data-engineering/delta-live-tables/index.html" target="_blank">Delta Live Tables Documentation</a>
# MAGIC * <a href="https://youtu.be/6Q8qPZ7c1O0" target="_blank">Delta Live Tables Demo</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>