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
-- MAGIC # Databases, Tables, and Views Lab
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
-- MAGIC - 다음을 포함한 다양한 관계 엔티티 간의 상호 작용을 만들고 탐색합니다:
-- MAGIC - 데이터베이스
-- MAGIC - 테이블(managed and external)
-- MAGIC - 뷰(views, temp views, and global temp views)
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create and explore interactions between various relational entities, including:
-- MAGIC   - Databases
-- MAGIC   - Tables (managed and external)
-- MAGIC   - Views (views, temp views, and global temp views)
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
-- MAGIC ### Getting Started
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 이 과정에 대한 변수 및 데이터 세트를 구성합니다. <br>
-- MAGIC Run the following cell to configure variables and datasets for this lesson.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.3L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 데이터 개요(Overview of the Data)
-- MAGIC 
-- MAGIC 데이터에는 화씨 또는 섭씨로 기록된 평균 기온을 포함하여 선택한 기상 관측소의 여러 항목이 포함됩니다. 테이블의 스키마:<br>
-- MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celsius. The schema for the table:
-- MAGIC 
-- MAGIC |ColumnName  | DataType| Description|
-- MAGIC |------------|---------|------------|
-- MAGIC |NAME        |string   | Station name |
-- MAGIC |STATION     |string   | Unique ID |
-- MAGIC |LATITUDE    |float    | Latitude |
-- MAGIC |LONGITUDE   |float    | Longitude |
-- MAGIC |ELEVATION   |float    | Elevation |
-- MAGIC |DATE        |date     | YYYY-MM-DD |
-- MAGIC |UNIT        |string   | Temperature units |
-- MAGIC |TAVG        |float    | Average temperature |
-- MAGIC 
-- MAGIC 이 데이터는 Parquet 형식으로 저장됩니다. 아래 쿼리를 사용하여 데이터를 미리 봅니다.<br>
-- MAGIC This data is stored in the Parquet format; preview the data with the query below.

-- COMMAND ----------

SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 데이터베이스 만들기(Create a Database)
-- MAGIC 
-- MAGIC 설치 스크립트에 정의된 **`da.db_name`** 변수를 사용하여 기본 위치에 데이터베이스를 생성합니다.<br>
-- MAGIC Create a database in the default location using the **`da.db_name`** variable defined in setup script.

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 1, "Database not present"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Change to Your New Database
-- MAGIC 
-- MAGIC 새로 작성한 데이터베이스를 **`USE`** 합니다.<br>
-- MAGIC **`USE`** your newly created database.

-- COMMAND ----------

-- TODO

<FILL-IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW CURRENT DATABASE").first()["namespace"] == DA.db_name, "Not using the correct database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Managed Table 만들기(Create a Managed Table)
-- MAGIC 
-- MAGIC CTAS 문을 사용하여 **`weather_managed`** 라는 이름의 관리 테이블을 생성합니다.<br>
-- MAGIC Use a CTAS statement to create a managed table named **`weather_managed`**.

-- COMMAND ----------

-- TODO

<FILL-IN>
SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 외부 테이블 만들기(Create an External Table)
-- MAGIC 
-- MAGIC 외부 테이블은 위치 지정을 통해 관리되는 테이블과 다르다는 것을 기억하십시오. 아래 **`weather_external`** 라는 외부 테이블을 생성합니다. <br>
-- MAGIC Recall that an external table differs from a managed table through specification of a location. Create an external table called **`weather_external`** below.

-- COMMAND ----------

-- TODO

<FILL-IN>
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_external"), "Table named `weather_external` does not exist"
-- MAGIC assert spark.table("weather_external").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 테이블 세부 정보 검사(Examine Table Details)
-- MAGIC 
-- MAGIC SQL 명령 **`DESCRIBE EXTENDED table_name`** 을 사용하여 두 개의 날씨 테이블을 검사합니다.<br>
-- MAGIC Use the SQL command **`DESCRIBE EXTENDED table_name`** to examine the two weather tables.

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE EXTENDED weather_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 다음 helper code를 실행하여 테이블 위치를 추출하고 비교합니다. <br>
-- MAGIC Run the following helper code to extract and compare the table locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC 
-- MAGIC print(f"""The weather_managed table is saved at: 
-- MAGIC 
-- MAGIC     {managedTablePath}
-- MAGIC 
-- MAGIC The weather_external table is saved at:
-- MAGIC 
-- MAGIC     {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이러한 디렉토리의 내용을 나열하여 데이터가 두 위치에 모두 있는지 확인합니다.<br>
-- MAGIC List the contents of these directories to confirm that data exists in both locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(managedTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### 데이터베이스 및 모든 테이블 삭제 후 디렉토리 내용 확인(Check Directory Contents after Dropping Database and All Tables)
-- MAGIC 
-- MAGIC CASCADE 키워드를 사용하면 이 작업을 수행할 수 있습니다. <br>
-- MAGIC The **`CASCADE`** keyword will accomplish this.

-- COMMAND ----------

-- TODO

<FILL_IN> ${da.db_name}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 0, "Database present"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 데이터베이스가 삭제되면 파일도 삭제됩니다.<br>
-- MAGIC With the database dropped, the files will have been deleted as well.
-- MAGIC 
-- MAGIC 다음 셀의 주석을 제거하고 실행하면 **`FileNotFoundException`** 이 표시됩니다.<br>
-- MAGIC Uncomment and run the following cell, which will throw a **`FileNotFoundException`** as your confirmation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # files = dbutils.fs.ls(managedTablePath)
-- MAGIC # display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(DA.paths.working_dir)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **managed tables과 external tables의 주요 차이점을 강조합니다.** 기본적으로 managed table과 연결된 파일은 작업영역에 연결된 루트 DBFS 저장소의 이 위치에 저장되며 테이블이 삭제될 때 삭제됩니다. <br>
-- MAGIC **This highlights the main differences between managed and external tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
-- MAGIC 
-- MAGIC 외부 테이블용 파일은 테이블 작성 시 제공된 위치에 유지되므로 사용자가 실수로 기본 파일을 삭제하는 것을 방지합니다. **External tables을 다른 데이터베이스로 쉽게 마이그레이션하거나 이름을 변경할 수 있지만, managed tables을 사용하는 이러한 작업은 모든 기본 파일을 다시 작성해야 합니다.**<br>
-- MAGIC Files for external tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 지정된 경로를 사용하여 데이터베이스 생성(Create a Database with a Specified Path)
-- MAGIC 
-- MAGIC 마지막 단계에서 데이터베이스를 삭제했다고 가정하면 동일한 **`database`** 이름을 사용할 수 있습니다. <br>
-- MAGIC Assuming you dropped your database in the last step, you can use the same **`database`** name.

-- COMMAND ----------

CREATE DATABASE ${da.db_name} LOCATION '${da.paths.working_dir}/${da.db_name}';
USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이 새 데이터베이스에서 **`weather_managed`** 테이블을 다시 만들고 이 테이블의 위치를 인쇄하십시오. <br>
-- MAGIC Recreate your **`weather_managed`** table in this new database and print out the location of this table.

-- COMMAND ----------

-- TODO

<FILL_IN>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC DBFS 루트에 생성된 **`working_dir`** 디렉토리를 사용하는 동안 모든 개체 저장소를 데이터베이스 디렉토리로 사용할 수 있습니다. **사용자 그룹에 대한 데이터베이스 디렉토리를 정의하면 실수로 데이터가 유출될 가능성을 크게 줄일 수 있습니다.**<br>
-- MAGIC While here we're using the **`working_dir`** directory created on the DBFS root, _any_ object store can be used as the database directory. **Defining database directories for groups of users can greatly reduce the chances of accidental data exfiltration**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 뷰 및 범위(Views and their Scoping)
-- MAGIC 
-- MAGIC 제공된 **'AS'** 조항을 사용하여 다음을 등록합니다:
-- MAGIC - 이름이 **`celsius`** 인 view
-- MAGIC - **`celsius_temp`** 라는 이름의 temporary view
-- MAGIC - **`celsius_global`** 라는 이름의 global temp view
-- MAGIC 
-- MAGIC Using the provided **`AS`** clause, register:
-- MAGIC - a view named **`celsius`**
-- MAGIC - a temporary view named **`celsius_temp`**
-- MAGIC - a global temp view named **`celsius_global`**

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius"), "Table named `celsius` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius'").first()["isTemporary"] == False, "Table is temporary"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이제 임시 보기를 만듭니다.<br>
-- MAGIC Now create a temporary view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius_temp"), "Table named `celsius_temp` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius_temp'").first()["isTemporary"] == True, "Table is not temporary"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이제 전역 온도 보기를 등록합니다.<br>
-- MAGIC Now register a global temp view.

-- COMMAND ----------

-- TODO

<FILL-IN>
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오.<br>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("global_temp.celsius_global"), "Global temporary view named `celsius_global` does not exist"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 카탈로그에서 나열할 때 보기가 표 옆에 표시됩니다.<br>
-- MAGIC Views will be displayed alongside tables when listing from the catalog.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 다음 사항에 유의하십시오:
-- MAGIC - view가 현재 데이터베이스와 연결되어 있습니다. 이 view는 이 데이터베이스에 액세스할 수 있는 모든 사용자가 사용할 수 있으며 세션 간에도 유지됩니다.
-- MAGIC - temp view가 데이터베이스와 연결되어 있지 않습니다. temp view는 일시적이며 현재 SparkSession에서만 액세스할 수 있습니다.
-- MAGIC - global temp view가 카탈로그에 표시되지 않습니다. **Global temp views는 항상 **`global_temp`** 데이터베이스에 등록됩니다. **`global_temp`** 데이터베이스는 일시적이지만 클러스터의 수명과 연결되어 있지만, 데이터베이스가 생성된 동일한 클러스터에 연결된 노트북에서만 액세스할 수 있습니다.
-- MAGIC 
-- MAGIC Note the following:
-- MAGIC - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
-- MAGIC - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
-- MAGIC - The global temp view does not appear in our catalog. **Global temp views will always register to the **`global_temp`** database**. The **`global_temp`** database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

-- COMMAND ----------

SELECT * FROM global_temp.celsius_global

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이러한 보기를 정의할 때 작업이 트리거되지 않은 동안 작업은 보기에 대해 쿼리가 실행될 때마다 트리거됩니다.<br>
-- MAGIC While no job was triggered when defining these views, a job is triggered _each time_ a query is executed against the view.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Clean Up
-- MAGIC 
-- MAGIC 데이터베이스와 모든 테이블을 삭제하여 작업영역을 정리합니다.<br>
-- MAGIC Drop the database and all tables to clean up your workspace.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Synopsis
-- MAGIC 
-- MAGIC 이 실습에서는 다음을 수행합니다:
-- MAGIC - 데이터베이스 작성 및 삭제
-- MAGIC - 관리 및 외부 테이블의 동작 조사
-- MAGIC - 보기 범위 지정에 대해 알아보기
-- MAGIC 
-- MAGIC In this lab we:
-- MAGIC - Created and deleted databases
-- MAGIC - Explored behavior of managed and external tables
-- MAGIC - Learned about the scoping of views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다. <br>
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
