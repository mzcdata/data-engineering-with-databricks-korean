-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Databricks에서의 데이터베이스 및 테이블(Databases and Tables on Databricks)
-- MAGIC
-- MAGIC 이 데모에서는 데이터베이스와 테이블을 만들고 탐색합니다. <br>
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC * Spark SQL DDL을 사용하여 데이터베이스 및 테이블 정의
-- MAGIC * **`LOCATION`** 키워드가 기본 저장소 디렉터리에 미치는 영향 설명
-- MAGIC
-- MAGIC
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Databases and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Lesson Setup
-- MAGIC
-- MAGIC 다음 스크립트는 이 데모의 이전 실행을 지우고 SQL 쿼리에 사용할 일부 Hive 변수를 구성합니다. <br>
-- MAGIC

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## 하이브 변수 사용(Using Hive Variables)
-- MAGIC
-- MAGIC Spark SQL에서 일반적으로 권장되는 패턴은 아니지만 이 노트북은 일부 Hive 변수를 사용하여 현재 사용자의 계정 이메일에서 파생된 문자열 값으로 대체합니다.<br>
-- MAGIC
-- MAGIC 다음 셀은 이 패턴을 보여줍니다.<br>

-- COMMAND ----------

SELECT "${da.db_name}" AS db_name,
       "${da.paths.working_dir}" AS working_dir

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 공유 작업영역에서 작업할 수 있으므로, 이 과정은 데이터베이스가 다른 사용자와 충돌하지 않도록 사용자 이름에서 파생된 변수를 사용합니다. <br/>
-- MAGIC Hive 변수의 이러한 사용은 개발을 위한 좋은 관행이라기보다는 우리의 수업 환경을 위한 해킹이라고 생각한다. <br>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## 데이터베이스(Databases)
-- MAGIC
-- MAGIC 먼저 두 개의 데이터베이스를 작성합니다:
-- MAGIC - **`LOCATION`** 이 지정되지 않은 항목
-- MAGIC - **`LOCATION`** 이 지정된 하나
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;
CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 첫 번째 데이터베이스의 위치는 **`dbfs:/user/hive/warehouse/`** 아래의 기본 위치에 있으며 데이터베이스 디렉토리는 확장자가 **`.db`** 인 데이터베이스의 이름입니다 <br>
-- MAGIC

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 두 번째 데이터베이스의 위치는 **`LOCATION`** 키워드 뒤에 지정된 디렉토리에 있습니다. <br>

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED ${da.db_name}_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 데이터베이스에 기본 위치의 테이블을 만들고 데이터를 삽입합니다. <br>
-- MAGIC
-- MAGIC 스키마를 유추할 데이터가 없으므로 스키마를 제공해야 합니다.<br>
-- MAGIC

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_default_location 
VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 확장 테이블 DESCRIBE 명령어를 통해 위치를 찾을 수 있습니다(결과에서 아래로 스크롤해야 함). <br>

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 기본적으로 지정된 위치가 없는 데이터베이스의 관리되는 테이블은 **`dbfs:/user/hive/warehouse/<database_name>.db/`** 디렉토리에 작성됩니다. <br>
-- MAGIC
-- MAGIC 예상대로 델타 테이블의 데이터와 메타데이터가 해당 위치에 저장되어 있는 것을 확인할 수 있습니다.<br>

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC hive_root =  f"dbfs:/user/hive/warehouse"
-- MAGIC db_name =    f"{DA.db_name}_default_location.db"
-- MAGIC table_name = f"managed_table_in_db_with_default_location"
-- MAGIC
-- MAGIC tbl_location = f"{hive_root}/{db_name}/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 드랍 <br>

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 테이블의 디렉토리와 로그 및 데이터 파일이 삭제됩니다. 데이터베이스 디렉토리만 남아 있습니다. <br>

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC db_location = f"{hive_root}/{db_name}"
-- MAGIC print(db_location)
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 이제 데이터베이스에 사용자 지정 위치가 있는 테이블을 만들고 데이터를 삽입합니다. <br>
-- MAGIC
-- MAGIC 스키마를 유추할 데이터가 없으므로 스키마를 제공해야 합니다.<br>

-- COMMAND ----------

USE ${da.db_name}_custom_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_custom_location (width INT, length INT, height INT);
INSERT INTO managed_table_in_db_with_custom_location VALUES (3, 2, 1);
SELECT * FROM managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 위치를 찾기 위해 설명을 DESCRIBE 명령어를 실행해보겠습니다. <br>

-- COMMAND ----------

DESCRIBE EXTENDED managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 데이터베이스 작성 중에 **`LOCATION`** 키워드로 지정된 경로에 관리되는 테이블이 생성됩니다. 따라서 테이블의 데이터 및 메타데이터는 여기의 디렉토리에 유지됩니다. <br>
-- MAGIC

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC table_name = f"managed_table_in_db_with_custom_location"
-- MAGIC tbl_location =   f"{DA.paths.working_dir}/_custom_location.db/{table_name}"
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 삭제 <br> 

-- COMMAND ----------

DROP TABLE managed_table_in_db_with_custom_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 테이블의 폴더와 로그 파일 및 데이터 파일이 삭제됩니다.<br>
-- MAGIC   
-- MAGIC 데이터베이스 위치만 남음<br>

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC db_location =   f"{DA.paths.working_dir}/_custom_location.db"
-- MAGIC print(db_location)
-- MAGIC
-- MAGIC dbutils.fs.ls(db_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Tables
-- MAGIC 샘플 데이터로 외부(unmanaged) 테이블을 만들 것입니다. <br>
-- MAGIC
-- MAGIC 우리가 사용할 데이터는 CSV 형식입니다. 우리는 우리가 선택한 디렉토리에 LOCATION이 제공된 Delta 테이블을 만들고 싶습니다.<br>

-- COMMAND ----------

USE ${da.db_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  이 과정의 작업 디렉토리에서 테이블의 데이터 위치를 기록해 두겠습니다. <br>

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 지금 테이블을 삭제합니다 <br> 

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 정의는 더 이상 metastore에 존재하지 않지만 기본 데이터는 그대로 유지됩니다. <br>

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC ## Clean up
-- MAGIC Drop both databases.

-- COMMAND ----------

DROP DATABASE ${da.db_name}_default_location CASCADE;
DROP DATABASE ${da.db_name}_custom_location CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다. <br>

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
