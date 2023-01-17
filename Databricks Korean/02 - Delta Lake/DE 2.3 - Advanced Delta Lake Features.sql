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
-- MAGIC # 고급 델타 레이크 기능(Advanced Delta Lake Features)
-- MAGIC 
-- MAGIC 이제 Delta Lake로 기본 데이터 작업을 수행할 수 있으므로 Delta Lake 고유의 몇 가지 기능을 사용할 수 있습니다. <br/>
-- MAGIC Now that you feel comfortable performing basic data tasks with Delta Lake, we can discuss a few features unique to Delta Lake.
-- MAGIC 
-- MAGIC 여기서 사용되는 키워드 중 일부는 표준 ANSI SQL의 일부는 아니지만 모든 델타 레이크 작업은 SQL을 사용하여 Datbricks에서 실행할 수 있습니다 <br/>
-- MAGIC Note that while some of the keywords used here aren't part of standard ANSI SQL, all Delta Lake operations can be run on Databricks using SQL
-- MAGIC 
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC * **`OPTIMIZE`** 를 사용하여 작은 파일로 압축
-- MAGIC * **`ZORDER`** 를 사용하여 테이블 인덱싱
-- MAGIC * Delta Lake 파일의 디렉터리 구조를 설명합니다
-- MAGIC * 테이블 트랜잭션 기록 검토
-- MAGIC * 이전 테이블 버전으로 쿼리 및 롤백
-- MAGIC * **`VACUUM`** 으로 오래된 데이터 파일 정리
-- MAGIC 
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use **`OPTIMIZE`** to compact small files
-- MAGIC * Use **`ZORDER`** to index tables
-- MAGIC * Describe the directory structure of Delta Lake files
-- MAGIC * Review a history of table transactions
-- MAGIC * Query and roll back to previous table version
-- MAGIC * Clean up stale data files with **`VACUUM`**
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC 가장 먼저 할 일은 설치 스크립트를 실행하는 것입니다. 사용자 이름, 사용자 홈 및 각 사용자에 대한 범위가 지정된 데이터베이스를 정의합니다. <br/>
-- MAGIC The first thing we're going to do is run a setup script. It will define a username, userhome, and database that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## History가 포함된 델타 테이블 생성(Creating a Delta Table with History)
-- MAGIC 
-- MAGIC 아래 셀은 이전 레슨의 모든 트랜잭션을 단일 셀로 응축합니다. (**`DROP TABLE`** 제외!) <br/>
-- MAGIC The cell below condenses all the transactions from the previous lesson into a single cell. (Except for the **`DROP TABLE`**!)
-- MAGIC 
-- MAGIC 이 쿼리가 실행되기를 기다리는 동안 실행 중인 총 트랜잭션 수를 식별할 수 있는지 확인하십시오. <br/>
-- MAGIC As you're waiting for this query to run, see if you can identify the total number of transactions being executed.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 테이블 세부 정보 검사(Examine Table Details)
-- MAGIC 
-- MAGIC 데이터브릭은 기본적으로 하이브 전이를 사용하여 데이터베이스, 테이블 및 보기를 등록합니다. <br/>
-- MAGIC Databricks uses a Hive metastore by default to register databases, tables, and views.
-- MAGIC 
-- MAGIC **`DESCRIBE EXTENDED`** 를 사용하면 테이블에 대한 중요한 메타데이터를 볼 수 있습니다.<br/>
-- MAGIC Using **`DESCRIBE EXTENDED`** allows us to see important metadata about our table.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`DESCRIBE DETAIL`** 은 테이블 메타데이터를 탐색할 수 있는 또 다른 명령입니다. <br/>
-- MAGIC **`DESCRIBE DETAIL`** is another command that allows us to explore table metadata.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`Location`** 필드를 기록합니다. <br/>
-- MAGIC Note the **`Location`** field.
-- MAGIC 
-- MAGIC 지금까지 테이블을 데이터베이스 내의 관계형 엔터티로만 생각해 왔지만, 실제로 Delta Lake 테이블은 클라우드 객체 스토리지에 저장된 파일 모음에 의해 백업됩니다. <br/>
-- MAGIC While we've so far been thinking about our table as just a relational entity within a database, a Delta Lake table is actually backed by a collection of files stored in cloud object storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 델타 레이크 파일 탐색(Explore Delta Lake Files)
-- MAGIC 
-- MAGIC 데이터브릭 유틸리티 기능을 사용하여 델타 레이크 테이블을 백업하는 파일을 볼 수 있습니다.<br/>
-- MAGIC We can see the files backing our Delta Lake table by using a Databricks Utilities function.
-- MAGIC 
-- MAGIC **참고**: Delta Lake와 함께 작동하기 위해 이러한 파일에 대한 모든 것을 아는 것이 지금 당장 중요한 것은 아니지만, 기술이 어떻게 구현되는지에 대한 이해를 높이는 데 도움이 될 것입니다. <br/>
-- MAGIC **NOTE**: It's not important right now to know everything about these files to work with Delta Lake, but it will help you gain a greater appreciation for how the technology is implemented.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 당사의 디렉토리에는 수많은 파르케 데이터 파일과 **`_delta_log`** 라는 이름의 디렉토리가 포함되어 있습니다. <br/>
-- MAGIC Note that our directory contains a number of Parquet data files and a directory named **`_delta_log`**.
-- MAGIC 
-- MAGIC 델타 레이크 테이블의 레코드는 Parquet 파일에 데이터로 저장된다. <br/>
-- MAGIC Records in Delta Lake tables are stored as data in Parquet files.
-- MAGIC 
-- MAGIC 델타 레이크 테이블에 대한 트랜잭션은 **`_delta_log`** 에 기록됩니다. <br/>
-- MAGIC Transactions to Delta Lake tables are recorded in the **`_delta_log`**.
-- MAGIC 
-- MAGIC 우리는 **`_delta_log`** 안을 들여다보면 더 많은 것을 볼 수 있다. <br/>
-- MAGIC We can peek inside the **`_delta_log`** to see more.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 각 트랜잭션은 새 JSON 파일을 델타 레이크 트랜잭션 로그에 기록합니다. 여기서 이 테이블에 대해 총 8개의 트랜잭션이 있음을 확인할 수 있습니다(Delta Lake는 0 인덱스). <br/>
-- MAGIC Each transaction results in a new JSON file being written to the Delta Lake transaction log. Here, we can see that there are 8 total transactions against this table (Delta Lake is 0 indexed).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 데이터 파일에 대한 추론(Reasoning about Data Files)
-- MAGIC 
-- MAGIC 우리는 아주 작은 테이블을 위한 많은 데이터 파일을 보았습니다. <br/>
-- MAGIC We just saw a lot of data files for what is obviously a very small table.
-- MAGIC 
-- MAGIC **`DESCRIBE DETAIL`** 을 사용하면 파일 수를 포함하여 델타 테이블에 대한 기타 세부 정보를 볼 수 있습니다. <br/> 
-- MAGIC **`DESCRIBE DETAIL`** allows us to see some other details about our Delta table, including the number of files.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 여기서는 현재 테이블에 현재 버전의 데이터 파일 4개가 포함되어 있음을 확인할 수 있습니다. 그럼 우리 테이블 디렉토리에 있는 다른 Parquet 파일들은 무엇을 하는걸까요? <br/>
-- MAGIC Here we see that our table currently contains 4 data files in its present version. So what are all those other Parquet files doing in our table directory? 
-- MAGIC 
-- MAGIC 델타 레이크는 변경된 데이터가 포함된 파일을 덮어쓰거나 즉시 삭제하는 대신 트랜잭션 로그를 사용하여 현재 버전의 테이블에서 파일이 유효한지 여부를 나타냅니다. <br/>
-- MAGIC Rather than overwriting or immediately deleting files containing changed data, Delta Lake uses the transaction log to indicate whether or not files are valid in a current version of the table.
-- MAGIC 
-- MAGIC 여기서는 레코드가 삽입, 업데이트 및 삭제된 위의 **`MERGE`** 문에 해당하는 트랜잭션 로그를 살펴보겠습니다. <br/>
-- MAGIC Here, we'll look at the transaction log corresponding the **`MERGE`** statement above, where records were inserted, updated, and deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`add`** 열에는 테이블에 기록된 모든 새 파일 목록이 포함되어 있습니다. **`remove`** 열에는 더 이상 테이블에 포함되지 않아야 하는 파일이 표시됩니다. <br/>
-- MAGIC The **`add`** column contains a list of all the new files written to our table; the **`remove`** column indicates those files that no longer should be included in our table.
-- MAGIC 
-- MAGIC 델타 레이크 테이블을 쿼리할 때 쿼리 엔진은 트랜잭션 로그를 사용하여 현재 버전에서 유효한 모든 파일을 확인하고 다른 모든 데이터 파일은 무시합니다. <br/>
-- MAGIC When we query a Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current version, and ignores all other data files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 파일 압축 및 인덱싱(Compacting Small Files and Indexing)
-- MAGIC 
-- MAGIC 작은 파일은 다양한 이유로 발생할 수 있습니다. 이 경우 하나 또는 여러 개의 레코드만 삽입되는 여러 작업을 수행했습니다.<br/>
-- MAGIC Small files can occur for a variety of reasons; in our case, we performed a number of operations where only one or several records were inserted.
-- MAGIC 
-- MAGIC **`OPTIMIZE`**(최적화) 명령을 사용하여 파일이 최적의 크기(테이블 크기에 따라 크기 조정)로 결합됩니다.<br/>
-- MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
-- MAGIC 
-- MAGIC **`OPTIMIZE`** 는 레코드를 결합하고 결과를 다시 작성하여 기존 데이터 파일을 대체합니다.<br/>
-- MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
-- MAGIC 
-- MAGIC **`OPTIMIZE`**(최적화)를 실행할 때 사용자는 **`ZORDER`** 인덱싱을 위한 하나 또는 여러 필드를 선택적으로 지정할 수 있습니다. Z 차수의 특정 산술은 중요하지 않지만, 데이터 파일 내에서 유사한 값을 가진 데이터를 공동으로 배치하여 제공된 필드에서 필터링할 때 데이터 검색 속도를 향상시킨다. <br/>
-- MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 우리의 데이터가 얼마나 작은지를 고려할 때, **`ZORDER`** 는 어떠한 이점도 제공하지 않지만, 우리는 이 작업에서 발생하는 모든 메트릭을 볼 수 있다. <br/>
-- MAGIC Given how small our data is, **`ZORDER`** does not provide any benefit, but we can see all of the metrics that result from this operation.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 델타 레이크 트랜잭션 검토(Reviewing Delta Lake Transactions)
-- MAGIC 
-- MAGIC 델타 레이크 테이블의 모든 변경 사항이 트랜잭션 로그에 저장되므로 <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a>를 쉽게 검토할 수 있습니다. <br/>
-- MAGIC Because all changes to the Delta Lake table are stored in the transaction log, we can easily review the <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a>.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 예상대로 **`OPTIMIZE`** 는 우리 테이블의 또 다른 버전을 만들었는데, 이는 버전 8이 우리의 가장 최신 버전이라는 것을 의미한다. <br/>
-- MAGIC As expected, **`OPTIMIZE`** created another version of our table, meaning that version 8 is our most current version.
-- MAGIC 
-- MAGIC 트랜잭션 로그에 제거된 것으로 표시된 추가 데이터 파일을 모두 기억하십니까? 이를 통해 이전 버전의 테이블을 쿼리할 수 있습니다. <br/>
-- MAGIC Remember all of those extra data files that had been marked as removed in our transaction log? These provide us with the ability to query previous versions of our table.
-- MAGIC 
-- MAGIC 이러한 time travel 쿼리는 정수 버전 또는 타임스탬프를 지정하여 수행할 수 있습니다.<br/>
-- MAGIC These time travel queries can be performed by specifying either the integer version or a timestamp.
-- MAGIC 
-- MAGIC **참고**: 대부분의 경우 타임스탬프를 사용하여 원하는 시간에 데이터를 재생성합니다. 데모에서는 버전을 사용할 것입니다. 이는 결정적이기 때문입니다(앞으로 언제든지 이 데모를 실행할 수 있습니다). <br/>
-- MAGIC **NOTE**: In most cases, you'll use a timestamp to recreate data at a time of interest. For our demo we'll use version, as this is deterministic (whereas you may be running this demo at any time in the future).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC time travel과 관련하여 주목해야 할 중요한 것은 현재 버전에 대해 트랜잭션을 실행 취소하여 이전 상태의 테이블을 다시 만드는 것이 아니라 지정된 버전에서 유효하다고 표시된 모든 데이터 파일을 쿼리하는 것입니다. <br/>
-- MAGIC What's important to note about time travel is that we're not recreating a previous state of the table by undoing transactions against our current version; rather, we're just querying all those data files that were indicated as valid as of the specified version.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 롤백 버전(Rollback Versions)
-- MAGIC 
-- MAGIC 테이블에서 일부 레코드를 수동으로 삭제하기 위해 쿼리를 입력하다가 실수로 다음과 같은 상태로 이 쿼리를 실행했다고 가정합니다. <br/>
-- MAGIC Suppose you're typing up query to manually delete some records from a table and you accidentally execute this query in the following state.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 삭제의 영향을 받는 행 수에 대해 -1이 표시되면 전체 데이터 디렉토리가 제거되었음을 의미합니다. <br/>
-- MAGIC Note that when we see a **`-1`** for number of rows affected by a delete, this means an entire directory of data has been removed.
-- MAGIC 
-- MAGIC 아래 셀에서 확인해 보겠습니다. <br/>
-- MAGIC Let's confirm this below.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블의 모든 레코드를 삭제하는 것은 바람직한 결과가 아닐 수 있습니다. 다행히도, 우리는 이 약속을 간단히 롤백할 수 있습니다. <br/>
-- MAGIC Deleting all the records in your table is probably not a desired outcome. Luckily, we can simply rollback this commit.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">명령어</a>는 트랜잭션으로 기록됩니다. 테이블의 모든 레코드를 실수로 삭제했다는 사실을 완전히 숨길 수는 없지만 작업을 취소하고 테이블을 원하는 상태로 되돌릴 수는 있습니다. <br/>
-- MAGIC Note that a **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">command</a> is recorded as a transaction; you won't be able to completely hide the fact that you accidentally deleted all the records in the table, but you will be able to undo the operation and bring your table back to a desired state.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 오래된 파일 정리(Cleaning Up Stale Files)
-- MAGIC 
-- MAGIC 데이터브릭은 델타 레이크 테이블의 오래된 파일을 자동으로 정리합니다. <br/>
-- MAGIC Databricks will automatically clean up stale files in Delta Lake tables.
-- MAGIC 
-- MAGIC Delta Lake 버전 및 시간 이동은 최신 버전을 쿼리하고 쿼리를 롤백하는 데 유용하지만, 대규모 프로덕션 테이블의 모든 버전에 대한 데이터 파일을 무한정 유지하는 것은 매우 비용이 많이 듭니다(PII가 있는 경우 컴플라이언스 문제로 이어질 수 있음).<br/>
-- MAGIC Personal Identifiable Information(PII)은 개인 식별 정보를 의미하며 기업이 개인을 식별하거나 조회하기 위해 사용하는 정보입니다. PII는 여권 정보와 같이 개인을 특정할 수 있는 직접적인 식별정보와 인종, 생년월일 등 여러 정보를 조합하여 개인을 식별해낼 수 있는 준 식별정보가 있습니다. <br>
-- MAGIC While Delta Lake versioning and time travel are great for querying recent versions and rolling back queries, keeping the data files for all versions of large production tables around indefinitely is very expensive (and can lead to compliance issues if PII is present).
-- MAGIC 
-- MAGIC 오래된 데이터 파일을 수동으로 제거하려는 경우 **`VACUUM`** 작업으로 이 작업을 수행할 수 있습니다. <br/>
-- MAGIC If you wish to manually purge old data files, this can be performed with the **`VACUUM`** operation.
-- MAGIC 
-- MAGIC 다음 셀의 주석을 제거하고 **`0 시간`** 동안 유지하여 현재 버전만 유지합니다: <br/>
-- MAGIC Uncomment the following cell and execute it with a retention of **`0 HOURS`** to keep only the current version:

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 기본적으로 **`VACUUM`** 은 7일 미만의 파일을 삭제하지 못하도록 합니다. 델타 테이블에서 **`VACUUM`** 을 실행하면 지정된 데이터 보존 기간보다 오래된 버전으로 time travel 기능이 손실됩니다. 데모에서는 데이터브릭이 **`0 시간`** 보존을 지정하는 코드를 실행하는 것을 볼 수 있습니다. 이는 단순히 기능을 시연하기 위한 것일 뿐 일반적으로 프로덕션에서 수행되지는 않습니다. <br/>
-- MAGIC By default, **`VACUUM`** will prevent you from deleting files less than 7 days old, just to ensure that no long-running operations are still referencing any of the files to be deleted. If you run **`VACUUM`** on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.  In our demos, you may see Databricks executing code that specifies a retention of **`0 HOURS`**. This is simply to demonstrate the feature and is not typically done in production.  
-- MAGIC 
-- MAGIC 다음 셀에서, 우리는:
-- MAGIC 1. 데이터 파일의 조기 삭제를 방지하기 위한 검사 실행 중지
-- MAGIC 1. **`VACUUM`** 명령 로깅이 활성화되었는지 확인
-- MAGIC 1. 삭제할 모든 레코드를 출력하려면 **`DRY RUN`** 버전의 Vacuum을 사용
-- MAGIC 
-- MAGIC In the following cell, we:
-- MAGIC 1. Turn off a check to prevent premature deletion of data files
-- MAGIC 1. Make sure that logging of **`VACUUM`** commands is enabled
-- MAGIC 1. Use the **`DRY RUN`** version of vacuum to print out all records to be deleted

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`VACUUM`** 을 실행하고 위의 10개 파일을 삭제하면 이러한 파일을 구현해야 하는 테이블 버전에 대한 액세스가 영구적으로 제거됩니다. <br/>
-- MAGIC By running **`VACUUM`** and deleting the 10 files above, we will permanently remove access to versions of the table that require these files to materialize.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블 디렉토리를 확인하여 파일이 성공적으로 삭제되었음을 표시합니다. <br/>
-- MAGIC Check the table directory to show that files have been successfully deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md
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