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
-- MAGIC 
-- MAGIC # Delta Lake를 사용한 테이블 조작(Manipulating Tables with Delta Lake)
-- MAGIC 
-- MAGIC 이 노트북은 델타 레이크의 기본 기능 중 일부를 직접 확인할 수 있습니다. <br/>
-- MAGIC This notebook provides a hands-on review of some of the basic functionality of Delta Lake.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
-- MAGIC - 다음을 포함하여 델타 레이크 테이블을 만들고 조작하기 위한 표준 작업을 실행합니다:
-- MAGIC   - **`CREATE TABLE`**
-- MAGIC   - **`INSERT INTO`**
-- MAGIC   - **`SELECT FROM`**
-- MAGIC   - **`UPDATE`**
-- MAGIC   - **`DELETE`**
-- MAGIC   - **`MERGE`**
-- MAGIC   - **`DROP TABLE`**
-- MAGIC   
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Execute standard operations to create and manipulate Delta Lake tables, including:
-- MAGIC   - **`CREATE TABLE`**
-- MAGIC   - **`INSERT INTO`**
-- MAGIC   - **`SELECT FROM`**
-- MAGIC   - **`UPDATE`**
-- MAGIC   - **`DELETE`**
-- MAGIC   - **`MERGE`**
-- MAGIC   - **`DROP TABLE`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC 
-- MAGIC 다음 스크립트를 실행하여 필요한 변수를 설정하고 이 노트북의 이전 실행을 지웁니다. 이 셀을 다시 실행하면 랩을 다시 시작할 수 있습니다. <br/>
-- MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.2L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 테이블 생성(Create a Table)
-- MAGIC 
-- MAGIC 이 노트에서, 우리는 우리의 bean collection을 추적하기 위한 표를 만들 것입니다. <br/>
-- MAGIC In this notebook, we'll be creating a table to track our bean collection.
-- MAGIC 
-- MAGIC 아래 셀을 사용하여 **`beans`** 라는 델타 레이크 테이블을 만듭니다. <br>
-- MAGIC Use the cell below to create a managed Delta Lake table named **`beans`**.
-- MAGIC 
-- MAGIC 다음 스키마를 제공하십시오: <br/>
-- MAGIC Provide the following schema:
-- MAGIC 
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 참고: Python을 사용하여 랩 전체에서 가끔 검사를 실행합니다. 다음 셀은 지침을 따르지 않은 경우 변경해야 할 내용에 대한 메시지와 함께 오류로 반환됩니다. 셀 실행에서 출력되지 않음은 이 단계를 완료했음을 의미합니다. <br/>
-- MAGIC **NOTE**: We'll use Python to run checks occasionally throughout the lab. The following cell will return as error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 데이터 삽입(Insert Data)
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 표에 세 개의 행을 삽입합니다. <br/>
-- MAGIC Run the following cell to insert three rows into the table.

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 테이블 내용을 수동으로 검토하여 데이터가 예상대로 작성되었는지 확인합니다. <br/>
-- MAGIC Manually review the table contents to ensure data was written as expected.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 아래에 제공된 추가 레코드를 삽입합니다. 이 작업을 단일 트랜잭션으로 실행해야 합니다. <br/>
-- MAGIC Insert the additional records provided below. Make sure you execute this as a single transaction.

-- COMMAND ----------

-- TODO
<FILL-IN>
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 데이터가 올바른 상태인지 확인하십시오. <br/>
-- MAGIC Run the cell below to confirm the data is in the proper state.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 레코드 업데이트(Update Records)
-- MAGIC 
-- MAGIC 친구가 당신의 beans 재고를 검토하고 있습니다. 많은 논쟁 후에, 여러분은 jelly beans이 맛있다는 것에 동의합니다. <br/>
-- MAGIC A friend is reviewing your inventory of beans. After much debate, you agree that jelly beans are delicious.
-- MAGIC 
-- MAGIC 이 레코드를 업데이트하려면 다음 셀을 실행하십시오. <br/>
-- MAGIC Run the following cell to update this record.

-- COMMAND ----------

UPDATE beans
SET delicious = true
WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 당신은 실수로 pinto beans의 무게를 잘못 입력했다는 것을 깨닫는다. <br/>
-- MAGIC You realize that you've accidentally entered the weight of your pinto beans incorrectly.
-- MAGIC 
-- MAGIC 이 레코드의 **`grams`** 열을 올바른 무게인 1500으로 업데이트하십시오. <br/>
-- MAGIC Update the **`grams`** column for this record to the correct weight of 1500.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 이 작업이 올바르게 완료되었는지 확인하십시오. <br/>
-- MAGIC Run the cell below to confirm this has completed properly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='pinto'").count() == 1, "There should only be 1 entry for pinto beans"
-- MAGIC row = spark.table("beans").filter("name='pinto'").first()
-- MAGIC assert row["color"] == "brown", "The pinto bean should be labeled as the color brown"
-- MAGIC assert row["grams"] == 1500, "Make sure you correctly specified the `grams` as 1500"
-- MAGIC assert row["delicious"] == True, "The pinto bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 레코드 삭제(Delete Records)
-- MAGIC 
-- MAGIC 당신은 맛있는 콩만 추적하기로 결정했습니다. <br/>
-- MAGIC You've decided that you only want to keep track of delicious beans.
-- MAGIC 
-- MAGIC 쿼리를 실행하여 맛없는 모든 콩을 삭제합니다. <br/>
-- MAGIC Execute a query to drop all beans that are not delicious.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 이 작업이 성공했는지 확인합니다. <br/>
-- MAGIC Run the following cell to confirm this operation was successful.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Merge를 사용하여 레코드 Upsert(Using Merge to Upsert Records)
-- MAGIC 
-- MAGIC 당신의 친구는 당신에게 새 beans을 줍니다. 아래 셀은 이를 임시 보기로 등록합니다. <br/>
-- MAGIC Your friend gives you some new beans. The cell below registers these as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 아래 셀에서 위의 보기를 사용하여 merge 문을 작성하여 **`beans`** 테이블에 새 레코드를 하나의 트랜잭션으로 업데이트하고 삽입합니다. <br>
-- MAGIC In the cell below, use the above view to write a merge statement to update and insert new records to your **`beans`** table as one transaction.
-- MAGIC 논리를 확인하십시오:
-- MAGIC - 이름과 색상별로 콩을 일치시킵니다
-- MAGIC - 기존 무게에 새 무게를 추가하여 기존 콩을 업데이트
-- MAGIC - 콩이 맛있는 경우에만 새 콩을 삽입합니다
-- MAGIC 
-- MAGIC Make sure your logic:
-- MAGIC - Matches beans by name **and** color
-- MAGIC - Updates existing beans by adding the new weight to the existing weight
-- MAGIC - Inserts new beans only if they are delicious

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 작업을 확인하십시오. <br/>
-- MAGIC Run the cell below to check your work.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC version = spark.sql("DESCRIBE HISTORY beans").selectExpr("max(version)").first()[0]
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").filter(f"version={version}")
-- MAGIC assert last_tx.select("operation").first()[0] == "MERGE", "Transaction should be completed as a merge"
-- MAGIC metrics = last_tx.select("operationMetrics").first()[0]
-- MAGIC assert metrics["numOutputRows"] == "3", "Make sure you only insert delicious beans"
-- MAGIC assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
-- MAGIC assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
-- MAGIC assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 테이블 삭제(Dropping Tables)
-- MAGIC 
-- MAGIC 관리되는 Delta Lake 테이블로 작업할 때 테이블을 삭제하면 테이블 및 모든 기본 데이터 파일에 대한 액세스가 영구적으로 삭제됩니다.<br/>
-- MAGIC When working with managed Delta Lake tables, dropping a table results in permanently deleting access to the table and all underlying data files.
-- MAGIC 
-- MAGIC 참고: 나중에 외부 테이블에 대해 알아보겠습니다. 외부 테이블은 파일 모음으로 Delta Lake 테이블에 접근하고 다른 지속성을 보장합니다.<br/>
-- MAGIC **NOTE**: Later in the course, we'll learn about external tables, which approach Delta Lake tables as a collection of files and have different persistence guarantees.
-- MAGIC 
-- MAGIC 아래 셀에 **`beans`** 테이블을 삭제하는 쿼리를 작성합니다.<br/>
-- MAGIC In the cell below, write a query to drop the **`beans`** table.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 테이블이 더 이상 존재하지 않는 지 확인하세요. <br/>
-- MAGIC Run the cell below to assert that your table no longer exists.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Wrapping Up
-- MAGIC 
-- MAGIC 이 실습을 완료하면 다음 작업을 할 수 있습니다:
-- MAGIC * 표준 Delta Lake 테이블 생성 및 데이터 조작 명령 완료
-- MAGIC 
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다. <br/>
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