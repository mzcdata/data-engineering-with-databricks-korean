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
-- MAGIC # Databricks의 뷰 및 CTE(Views and CTEs on Databricks)
-- MAGIC 이 데모에서는 뷰와 공통 테이블 표현식(CTE - Common Table Expressions)을 만들고 탐색합니다. <br/>
-- MAGIC In this demonstration, you will create and explore views and common table expressions (CTEs).
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다 : 
-- MAGIC * Spark SQL DDL을 사용하여 뷰 정의
-- MAGIC * 공통 테이블 식을 사용하는 쿼리 실행
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use Spark SQL DDL to define views
-- MAGIC * Run queries that use common table expressions
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html" target="_blank">Create View - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-qry-select-cte.html" target="_blank">Common Table Expressions - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Classroom Setup
-- MAGIC 다음 스크립트는 이 데모의 이전 실행을 지우고 SQL 쿼리에 사용할 일부 Hive 변수를 구성합니다.<br/>
-- MAGIC The following script clears out previous runs of this demo and configures some Hive variables that will be used in our SQL queries.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-3.2A

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 먼저 데모에 사용할 수 있는 데이터 표를 작성합니다. <br/>
-- MAGIC We start by creating a table of data we can use for the demonstration.

-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.working_dir}/flight_delays',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 테이블(및 뷰) 목록을 표시하려면 아래에 설명된 **`SHOW TABLES`** 명령을 사용합니다. <br/>
-- MAGIC To show a list of tables (and views), we use the **`SHOW TABLES`** command also demonstrated below.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Views, Temp Views & Global Temp Views
-- MAGIC 
-- MAGIC 이 데모를 설정하기 위해 먼저 각 유형의 뷰 중 하나를 작성합니다.<br>
-- MAGIC To set this demonstration up, we are going to first create one of each type of view.
-- MAGIC 
-- MAGIC 다음 노트에서는 각 사용자가 어떻게 행동하는지에 대한 차이점을 살펴보겠습니다.<br>
-- MAGIC Then in the next notebook, we will explore the differences between how each one behaves.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### Views
-- MAGIC 
-- MAGIC origin이 "ABQ"이고 destination이 "LAX"인 데이터만 포함하는 뷰를 작성해 보겠습니다.<br>
-- MAGIC Let's create a view that contains only the data where the origin is "ABQ" and the destination is "LAX".

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC **`view_delays_abq_lax`** 뷰가 아래 목록에 추가되었습니다: <br>
-- MAGIC Note that the **`view_delays_abq_lax`** view has been added to the list below:

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC ### Temporary Views
-- MAGIC 
-- MAGIC 다음은 임시 보기를 만들겠습니다. <br>
-- MAGIC Next we'll create a temporary view. 
-- MAGIC 
-- MAGIC 구문은 매우 유사하지만 명령에 **`TEMPORARY`** 을 추가합니다.<br>
-- MAGIC The syntax is very similar but adds **`TEMPORARY`** to the command.

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_delays_gt_120
AS SELECT * FROM external_table WHERE delay > 120 ORDER BY delay ASC;

SELECT * FROM temp_view_delays_gt_120;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 이제 테이블을 다시 보여드리면 하나의 테이블과 두 개의 뷰가 모두 표시됩니다. <br>
-- MAGIC Now if we show our tables again, we will see the one table and both views.
-- MAGIC 
-- MAGIC **`isTemporary`** 열의 값을 기록합니다. <br>
-- MAGIC Make note of the values in the **`isTemporary`** column.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ### Global Temp Views
-- MAGIC 
-- MAGIC 마지막으로 글로벌 temp 뷰를 생성합니다. <br>
-- MAGIC Lastly, we'll create a global temp view. 
-- MAGIC 
-- MAGIC 여기서는 **`GLOBAL`** 을 명령에 추가하기만 하면 됩니다. <br>
-- MAGIC Here we simply add **`GLOBAL`** to the command. 
-- MAGIC 
-- MAGIC 또한 후속 **`SELECT`** 문에서 **`global_temp`** 데이터베이스 한정자를 확인하십시오.
-- MAGIC Also note the **`global_temp`** database qualifer in the subsequent **`SELECT`** statement.

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 다음으로 넘어가기 전에 데이터베이스의 테이블과 뷰를 마지막으로 검토하십시오 <br>
-- MAGIC Before we move on, review one last time the database's tables and views...

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 그리고 **`global_temp`** 데이터베이스의 테이블과 뷰: <br>
-- MAGIC ...and the tables and views in the **`global_temp`** database:

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 다음으로 테이블과 뷰가 여러 세션에 걸쳐 유지되는 방식과 임시 뷰가 유지되지 않는 방식을 보여드리겠습니다.<br>
-- MAGIC Next we are going to demonstrate how tables and views are persisted across multiple sessions and how temp views are not.
-- MAGIC 
-- MAGIC 이렇게 하려면 다음 노트북인 [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont)에서 보기 및 CTE를 열고 수업을 계속 진행하기만 하면 됩니다.<br>
-- MAGIC To do this simply open the next notebook, [DE 3.2B - Views and CTEs on Databricks, Cont]($./DE 3.2B - Views and CTEs on Databricks, Cont), and continue with the lesson.
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> 참고: 새 세션을 생성할 수 있는 시나리오는 다음과 같습니다:<br>
-- MAGIC * 클러스터 재시작
-- MAGIC * 클러스터 분리 및 재연결
-- MAGIC * Python 패키지를 설치하여 Python 인터프리터를 다시 시작
-- MAGIC * 또는 단순히 새 노트북 열기
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> Note: There are several scenarios in which a new session may be created:
-- MAGIC * Restarting a cluster
-- MAGIC * Detaching and reataching to a cluster
-- MAGIC * Installing a python package which in turn restarts the Python interpreter
-- MAGIC * Or simply opening a new notebook

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>