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
-- MAGIC # 델타 레이크 버전 관리, 최적화 및 
-- MAGIC # Vacuuming(Delta Lake Versioning, Optimization, and Vacuuming)
-- MAGIC 
-- MAGIC 이 노트북은 Delta Lake가 데이터 레이크 하우스에 제공하는 보다 난해한 기능 중 일부를 직접 확인할 수 있습니다.<br/>
-- MAGIC This notebook provides a hands-on review of some of the more esoteric features Delta Lake brings to the data lakehouse.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 실습을 완료하면, 아래 작업을 수행할 수 있습니다. 
-- MAGIC - 테이블 내역 검토
-- MAGIC - 이전 테이블 버전을 쿼리하고 테이블을 특정 버전으로 롤백
-- MAGIC - 파일 압축 및 Z-order 인덱싱 수행
-- MAGIC - 영구 삭제로 표시된 파일을 미리 보고 삭제 commit
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Review table history
-- MAGIC - Query previous table versions and rollback a table to a specific version
-- MAGIC - Perform file compaction and Z-order indexing
-- MAGIC - Preview files marked for permanent deletion and commit these deletes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC 다음 스크립트를 실행하여 필요한 변수를 설정하고 이 노트북의 이전 실행을 지웁니다. 이 셀을 다시 실행하면 랩을 다시 시작할 수 있습니다.<br/>
-- MAGIC Run the following script to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.4L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Bean Collection의 기록을 다시 만들기(Recreate the History of your Bean Collection)
-- MAGIC 
-- MAGIC 아래 셀은 지난 강의의 모든 작업을 하나의 셀로 응축합니다(최종 **`DROP TABLE`** 문 제외). <br/>
-- MAGIC This lab picks up where the last lab left off. The cell below condenses all the operations from the last lab into a single cell (other than the final **`DROP TABLE`** statement).
-- MAGIC 
-- MAGIC **`beans`** 테이블의 스키마는 다음과 같습니다: <br>
-- MAGIC For quick reference, the schema of the **`beans`** table created is:
-- MAGIC 
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 테이블 기록 검토(Review the Table History)
-- MAGIC 
-- MAGIC 델타 레이크의 트랜잭션 로그는 테이블의 내용 또는 설정을 수정하는 각 트랜잭션에 대한 정보를 저장합니다. <br/>
-- MAGIC Delta Lake's transaction log stores information about each transaction that modifies a table's contents or settings.
-- MAGIC 
-- MAGIC 아래 **`beans`** 테이블의 history를 검토하십시오. <br/>
-- MAGIC Review the history of the **`beans`** table below.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 모든 이전 작업이 제대로 완료된 경우 7개의 row가 version 6까지 표시됩니다(**참고**: 델타 레이크 버전은 0으로 시작하므로 최대 버전 번호는 6이 됩니다. <br/>
-- MAGIC If all the previous operations were completed as described you should see 7 versions of the table (**NOTE**: Delta Lake versioning starts with 0, so the max version number will be 6).
-- MAGIC 
-- MAGIC 작업은 다음과 같아야 합니다: <br/>
-- MAGIC The operations should be as follows:
-- MAGIC 
-- MAGIC | version | operation |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC 
-- MAGIC **`operationsParameters`** 열을 사용하여 update, delete 및 merge에 사용된 predicates를 리뷰할 수 있습니다. **`operationMetrics`** 열은 각 작업에 추가되는 행 및 파일 수를 표시합니다. <br/>
-- MAGIC The **`operationsParameters`** column will let you review predicates used for updates, deletes, and merges. The **`operationMetrics`** column indicates how many rows and files are added in each operation.
-- MAGIC 
-- MAGIC 주어진 트랜잭션과 일치하는 테이블 버전을 이해하기 위해 Delta Lake 기록을 검토하는 데 시간을 할애합니다.<br/>
-- MAGIC Spend some time reviewing the Delta Lake history to understand which table version matches with a given transaction.
-- MAGIC 
-- MAGIC **참고**: **`version`** 열은 지정된 트랜잭션이 완료되면 테이블의 상태를 지정합니다.<br>
-- MAGIC **`readVersion`** 열은 작업이 실행된 테이블의 버전을 나타냅니다. <br>
-- MAGIC (동시 트랜잭션이 없는) 이 간단한 데모에서 이 관계는 항상 1씩 증가해야 합니다.<br/>
-- MAGIC **NOTE**: The **`version`** column designates the state of a table once a given transaction completes. The **`readVersion`** column indicates the version of the table an operation executed against. In this simple demo (with no concurrent transactions), this relationship should always increment by 1.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 특정 버전 쿼리(Query a Specific Version)
-- MAGIC 
-- MAGIC 테이블 기록을 검토한 후 첫 번째 데이터가 삽입된 후 테이블의 상태를 보기로 결정합니다. <br/>
-- MAGIC After reviewing the table history, you decide you want to view the state of your table after your very first data was inserted.
-- MAGIC 
-- MAGIC 아래 쿼리를 실행하여 확인하십시오.<br/>
-- MAGIC Run the query below to see this.

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이제 데이터의 현재 상태를 검토하십시오.<br/>
-- MAGIC And now review the current state of your data.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 레코드를 삭제하기 전에 콩의 무게를 검토하려고 합니다. <br/>
-- MAGIC You want to review the weights of your beans before you deleted any records.
-- MAGIC 
-- MAGIC 아래 문을 입력하여 데이터가 삭제되기 직전 버전의 임시 보기를 등록한 후 다음 셀을 실행하여 보기를 쿼리하십시오.<br/>
-- MAGIC Fill in the statement below to register a temporary view of the version just before data was deleted, then run the following cell to query the view.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
<FILL-IN>

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 올바른 버전을 캡처했는지 확인하십시오. <br/>
-- MAGIC Run the cell below to check that you have captured the correct version.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 이전 버전 복원(Restore a Previous Version)
-- MAGIC 
-- MAGIC 문제가 있었던 것 같습니다; 당신의 친구가 당신에게 준 콩은 당신이 소장품으로 Merge하기 위한 것이 콩들은 당신이 보관하기 위한 것이 아니다. <br/>
-- MAGIC Apparently there was a misunderstanding; the beans your friend gave you that you merged into your collection were not intended for you to keep.
-- MAGIC 
-- MAGIC 이 **`MERGE`** 문이 완료되기 전 버전으로 테이블을 되돌립니다. <br/>
-- MAGIC Revert your table to the version before this **`MERGE`** statement completed.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 표의 기록을 검토합니다. 이전 버전으로 복원하면 다른 테이블 버전이 추가됩니다. <br/>
-- MAGIC Review the history of your table. Make note of the fact that restoring to a previous version adds another table version.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 파일 압축(File Compaction)
-- MAGIC 복구하는 동안 트랜잭션 메트릭을 살펴보면, 이렇게 적은 양의 데이터 모음을 위한 파일이 몇 개 있다는 사실이 놀랍습니다. <br/>
-- MAGIC Looking at the transaction metrics during your reversion, you are surprised you have some many files for such a small collection of data.
-- MAGIC 
-- MAGIC 이 크기의 테이블에서 인덱싱을 수행해도 성능이 향상되지는 않지만, 콩 컬렉션이 시간이 지남에 따라 기하급수적으로 증가할 것으로 예상하여 **`name`** 필드에 Z-order index를 추가하기로 결정했습니다. <br/>
-- MAGIC While indexing on a table of this size is unlikely to improve performance, you decide to add a Z-order index on the **`name`** field in anticipation of your bean collection growing exponentially over time.
-- MAGIC 
-- MAGIC 아래 셀을 사용하여 파일 압축 및 Z-order indexing을 수행합니다. <br/>
-- MAGIC Use the cell below to perform file compaction and Z-order indexing.

-- COMMAND ----------

-- TODO
<FILL-IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 데이터가 단일 파일로 압축되어 있어야 합니다. 다음 셀을 실행하여 수동으로 확인하십시오. <br/>
-- MAGIC Your data should have been compacted to a single file; confirm this manually by running the following cell.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 테이블을 성공적으로 최적화하고 색인화했는지 확인하십시오. <br/>
-- MAGIC Run the cell below to check that you've successfully optimized and indexed your table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 오래된 데이터 파일 정리(Cleaning Up Stale Data Files)
-- MAGIC 
-- MAGIC 이제 모든 데이터가 하나의 데이터 파일에 저장되지만 이전 버전의 테이블의 데이터 파일은 여전히 이 파일과 함께 저장됩니다. 테이블에서 **`VACUUM`** 을 실행하여 이러한 파일을 제거하고 테이블의 이전 버전에 대한 액세스를 제거하려고 합니다. <br/>
-- MAGIC You know that while all your data now resides in 1 data file, the data files from previous versions of your table are still being stored alongside this. You wish to remove these files and remove access to previous versions of the table by running **`VACUUM`** on the table.
-- MAGIC 
-- MAGIC **`VACUUM`** 을 실행하면 테이블 디렉토리에서 가비지 정리가 수행됩니다. 기본적으로 7일의 보존 임계값이 적용됩니다. <br/>
-- MAGIC Executing **`VACUUM`** performs garbage cleanup on the table directory. By default, a retention threshold of 7 days will be enforced.
-- MAGIC 
-- MAGIC 아래 셀은 일부 스파크 구성을 수정합니다. 첫 번째 명령은 데이터의 영구 제거를 시연할 수 있도록 보존 임계값 검사를 재정의합니다. <br/>
-- MAGIC The cell below modifies some Spark configurations. The first command overrides the retention threshold check to allow us to demonstrate permanent removal of data. 
-- MAGIC 
-- MAGIC **참고**: 보존 기간이 짧은 프로덕션 테이블을 진공 상태로 만들면 데이터가 손상되거나 장기간 실행되는 쿼리가 실패할 수 있습니다. 이는 시연용으로만 사용되며 이 설정을 비활성화할 때는 매우 주의해야 합니다. <br/>
-- MAGIC **NOTE**: Vacuuming a production table with a short retention can lead to data corruption and/or failure of long-running queries. This is for demonstration purposes only and extreme caution should be used when disabling this setting.
-- MAGIC 
-- MAGIC 두 번째 명령은 **`VACUUM`** 작업이 트랜잭션 로그에 기록되도록 **`spark.databricks.delta.vacuum.logging.enabled`** 를 **`true`** 로 설정합니다. <br/>
-- MAGIC The second command sets **`spark.databricks.delta.vacuum.logging.enabled`** to **`true`** to ensure that the **`VACUUM`** operation is recorded in the transaction log.
-- MAGIC 
-- MAGIC **참고**: 다양한 클라우드의 스토리지 프로토콜이 약간 다르기 때문에 DBR 9.1 현재 일부 클라우드에서는 로깅 **`VACUUM`** 명령이 기본적으로 설정되어 있지 않습니다. <br/>
-- MAGIC **NOTE**: Because of slight differences in storage protocols on various clouds, logging **`VACUUM`** commands is not on by default for some clouds as of DBR 9.1.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 데이터 파일을 영구적으로 삭제하기 전에 **`DRY RUN`** 옵션을 사용하여 수동으로 검토하십시오. <br/>
-- MAGIC Before permanently deleting data files, review them manually using the **`DRY RUN`** option.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블의 현재 버전에 없는 모든 데이터 파일이 위의 미리 보기에 표시됩니다 <br/>
-- MAGIC All data files not in the current version of the table will be shown in the preview above.
-- MAGIC 
-- MAGIC 이러한 파일을 완전히 삭제하려면 **`DRY RUN`** 없이 명령을 다시 실행하십시오.<br/>
-- MAGIC Run the command again without **`DRY RUN`** to permanently delete these files.
-- MAGIC 
-- MAGIC **참고** : 모든 이전 버전의 테이블에 더 이상 액세스할 수 없습니다. <br>
-- MAGIC **NOTE**: All previous versions of the table will no longer be accessible.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`VACUUM`** 은 중요한 데이터 세트에 매우 파괴적인 작업이 될 수 있으므로 보존 기간 검사를 다시 설정하는 것이 항상 좋습니다. 아래 셀을 실행하여 이 설정을 활성화하십시오. <br/>
-- MAGIC Because **`VACUUM`** can be such a destructive act for important datasets, it's always a good idea to turn the retention duration check back on. Run the cell below to reactive this setting.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블 기록에는 **`VACUUM`** 작업을 완료한 사용자, 삭제된 파일 수 및 이 작업 동안 보존 검사가 비활성화되었음을 기록합니다. <br>
-- MAGIC Note that the table history will indicate the user that completed the **`VACUUM`** operation, the number of files deleted, and log that the retention check was disabled during this operation.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블을 다시 쿼리하여 현재 버전에 대한 액세스 권한이 있는지 확인합니다.<br>
-- MAGIC Query your table again to confirm you still have access to the current version.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png">델타 캐시는 현재 활성 클러스터에 배포된 스토리지 볼륨에 현재 세션에서 쿼리된 파일의 복사본을 저장하므로 이전 테이블 버전에 일시적으로 액세스할 수 있습니다(시스템이 이러한 동작을 예상하도록 설계되어서는 안 됩니다). <br>
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> Because Delta Cache stores copies of files queried in the current session on storage volumes deployed to your currently active cluster, you may still be able to temporarily access previous table versions (though systems should **not** be designed to expect this behavior). 
-- MAGIC 
-- MAGIC 클러스터를 다시 시작하면 캐시된 데이터 파일이 영구적으로 제거됩니다.<br>
-- MAGIC Restarting the cluster will ensure that these cached data files are permanently purged.
-- MAGIC 
-- MAGIC 캐시 상태에 따라 실패할 수도 있고 실패하지 않을 수도 있는 다음 셀의 주석을 제거하고 실행하면 이러한 예를 볼 수 있습니다. <br>
-- MAGIC You can see an example of this by uncommenting and running the following cell that may, or may not, fail
-- MAGIC (depending on the state of the cache).

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이 실습을 완료하면 다음과 같은 편안함을 느낄 수 있습니다:
-- MAGIC * 표준 Delta Lake 테이블 생성 및 데이터 조작 명령 완료
-- MAGIC * 테이블 기록을 포함한 테이블 메타데이터 검토
-- MAGIC * 스냅샷 쿼리 및 롤백에 델타 레이크 버전 활용
-- MAGIC * 작은 파일 및 인덱싱 테이블 압축
-- MAGIC * **`VACUUM`** 을 사용하여 삭제 표시된 파일 검토 및 삭제 커밋
-- MAGIC 
-- MAGIC By completing this lab, you should now feel comfortable:
-- MAGIC * Completing standard Delta Lake table creation and data manipulation commands
-- MAGIC * Reviewing table metadata including table history
-- MAGIC * Leverage Delta Lake versioning for snapshot queries and rollbacks
-- MAGIC * Compacting small files and indexing tables
-- MAGIC * Using **`VACUUM`** to review files marked for deletion and committing these deletes

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