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
-- MAGIC # 델타 테이블 관리(Managing Delta Tables)
-- MAGIC
-- MAGIC 이 노트북에서는 Databricks에서 SQL을 사용하여 데이터와 테이블을 기본적으로 조작하는 방법에 대해 알아보겠습니다. <br/>
-- MAGIC
-- MAGIC Delta Lake는 Datbricks로 작성된 모든 테이블의 기본 형식입니다. <br>
-- MAGIC Delta Lake에서 SQL 문을 실행한 경우 이미 Delta Lake로 작업하고 있을 가능성이 높습니다. <br/>
-- MAGIC
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC * 델타 레이크 테이블 작성 (Create Delta Lake tables)
-- MAGIC * Delta Lake 테이블에서 데이터 쿼리 (Query data from Delta Lake tables)
-- MAGIC * 델타 레이크 테이블에 레코드 삽입, 업데이트 및 삭제 (Insert, update, and delete records in Delta Lake tables)
-- MAGIC * 델타 레이크와 함께 업스타트 문 작성 (Write upsert statements with Delta Lake)
-- MAGIC * 델타 레이크 테이블 삭제 (Drop Delta Lake tables)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC 가장 먼저 할 일은 설치 스크립트를 실행하는 것입니다. 사용자 이름, 사용자 홈 및 각 사용자에 대한 범위가 지정된 데이터베이스를 정의합니다. <br/>
-- MAGIC The first thing we're going to do is run a setup script. It will define a username, userhome, and database that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 델타 테이블 생성(Creating a Delta Table)
-- MAGIC
-- MAGIC 델타 레이크로 테이블을 만들기 위해 작성해야 하는 코드는 많지 않습니다. <br>
-- MAGIC 델타 레이크 표를 만드는 방법에는 여러 가지가 있습니다. <br>
-- MAGIC 이 표는 아래 과정을 통해 확인할 수 있습니다.<br>
-- MAGIC 가장 쉬운 방법 중 하나로 시작하겠습니다. <br>
-- MAGIC 비어 있는 델타 레이크 테이블을 등록하는 것입니다. <br/>
-- MAGIC
-- MAGIC We need: 
-- MAGIC - **`CREATE TABLE`** 문 [ **`CREATE TABLE`** statement)]
-- MAGIC - 테이블 이름[A table name (below we use **`students`**)]
-- MAGIC - 스키마 (A schema)
-- MAGIC
-- MAGIC 참고: **Databricks Runtime 8.0 이상**에서는 델타 레이크가 기본 형식이므로 **델타를 사용할 필요가 없습니다.** <br/>
-- MAGIC **NOTE:** In Databricks Runtime 8.0 and above, Delta Lake is the default format and you don’t need **`USING DELTA`**.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 만약 우리가 다시 돌아가서 그 셀을 실행하려고 한다면 테이블이 이미 존재하기 때문에 오류가 발생합니다. <br/>
-- MAGIC
-- MAGIC 테이블이 있는지 확인하는 인수가 없으면 인수를 추가할 수 있습니다. 이것은 우리의 실수를 극복할 것이다. <br/>

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 데이터 삽입(Inserting Data)
-- MAGIC 대부분의 경우 데이터는 다른 원본에서 쿼리한 결과로 테이블에 삽입됩니다. <br/>
-- MAGIC
-- MAGIC 그러나 표준 SQL에서와 마찬가지로 다음과 같이 직접 값을 삽입할 수도 있습니다. <br/>

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 위의 셀에서 우리는 세 개의 개별 **`INSERT`** 문을 완성했습니다. 이들 각각은 자체 ACID 보증이 있는 별도의 거래로 처리된다. 대부분의 경우 단일 트랜잭션에 많은 레코드를 삽입합니다. <br/>

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Databricks에는 **`COMMIT`** 키워드가 없습니다. 트랜잭션은 실행되는 즉시 실행되고 성공하면 커밋됩니다. <br/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 델타 테이블 쿼리(Querying a Delta Table)
-- MAGIC
-- MAGIC Delta Lake 테이블을 쿼리하는 것은 표준 SELECT 문을 사용하는 것만큼 쉽습니다. <br/>

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 놀랄 만한 것은 델타 레이크가 테이블에 대한 읽기가 **항상 최신 버전의 테이블을 반환**하고 진행 중인 작업으로 인해 교착 상태가 발생하지 않도록 보장한다는 것입니다. <br/>
-- MAGIC What may surprise you is that Delta Lake guarantees that any read against a table will **always** return the most recent version of the table, and that you'll never encounter a state of deadlock due to ongoing operations.
-- MAGIC
-- MAGIC 반복하기: 테이블 읽기는 다른 작업과 충돌할 수 없으며, 데이터의 최신 버전은 레이크하우스를 쿼리할 수 있는 모든 클라이언트에서 즉시 사용할 수 있습니다. 모든 트랜잭션 정보는 데이터 파일과 함께 클라우드 객체 스토리지에 저장되므로 Delta Lake 테이블의 동시 읽기는 클라우드 공급업체의 객체 스토리지 하드 한계에 의해서만 제한됩니다. (**참고**: 무한하지는 않지만 초당 최소 수천 개의 읽기가 가능합니다.) <br/>
-- MAGIC To repeat: table reads can never conflict with other operations, and the newest version of your data is immediately available to all clients that can query your lakehouse. Because all transaction information is stored in cloud object storage alongside your data files, concurrent reads on Delta Lake tables is limited only by the hard limits of object storage on cloud vendors. (**NOTE**: It's not infinite, but it's at least thousands of reads per second.)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 레코드 업데이트(Updating Records)
-- MAGIC
-- MAGIC 레코드를 업데이트하면 현재 버전의 테이블을 스냅샷으로 읽고 WHERE 절과 일치하는 모든 필드를 찾은 다음 설명된 대로 변경 사항을 적용할 수 있습니다. <br/>
-- MAGIC Updating records provides atomic guarantees as well: we perform a snapshot read of the current version of our table, find all fields that match our **`WHERE`** clause, and then apply the changes as described.
-- MAGIC
-- MAGIC 아래에서는 문자 T로 시작하는 이름을 가진 모든 학생을 찾아 값 열의 숫자에 1을 추가합니다. <br/>
-- MAGIC Below, we find all students that have a name starting with the letter **T** and add 1 to the number in their **`value`** column.

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 테이블을 다시 쿼리하여 변경사항이 적용되었는지 확인합니다. <br/>
-- MAGIC Query the table again to see these changes applied.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 레코드 삭제(Deleting Records)
-- MAGIC
-- MAGIC 삭제도 원자적이므로 데이터 레이크 하우스에서 데이터를 제거할 때 부분적으로만 성공할 위험이 없습니다. <br/>
-- MAGIC Deletes are also atomic, so there's no risk of only partially succeeding when removing data from your data lakehouse.
-- MAGIC
-- MAGIC DELETE 문은 하나 이상의 레코드를 제거할 수 있지만 항상 단일 트랜잭션이 발생합니다. <br/>
-- MAGIC A **`DELETE`** statement can remove one or many records, but will always result in a single transaction.

-- COMMAND ----------

DELETE FROM students 
WHERE value > 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 병합 사용(Using Merge)
-- MAGIC
-- MAGIC 일부 SQL 시스템은 업데이트, 삽입 및 기타 데이터 조작을 단일 명령으로 실행할 수 있는 upsert 개념을 가지고 있습니다. <br/>
-- MAGIC Some SQL systems have the concept of an upsert, which allows updates, inserts, and other data manipulations to be run as a single command.
-- MAGIC
-- MAGIC Databricks는 MERGE 키워드를 사용하여 이 작업을 수행합니다. <br/>
-- MAGIC Databricks uses the **`MERGE`** keyword to perform this operation.
-- MAGIC
-- MAGIC CDC(Change Data Capture) 피드에서 출력할 수 있는 4개의 레코드가 포함된 다음 임시 보기를 고려해 보십시오. <br/>
-- MAGIC Consider the following temporary view, which contains 4 records that might be output by a Change Data Capture (CDC) feed.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC 지금까지 살펴본 구문을 사용하여 이 보기에서 유형별로 필터링하여 레코드를 삽입, 업데이트 및 삭제하는 3개의 문을 작성할 수 있습니다. 그러나 이렇게 하면 세 개의 개별 트랜잭션이 발생합니다. 이러한 트랜잭션 중 하나라도 실패하면 데이터가 잘못된 상태로 남을 수 있습니다. <br/>
-- MAGIC Using the syntax we've seen so far, we could filter from this view by type to write 3 statements, one each to insert, update, and delete records. But this would result in 3 separate transactions; if any of these transactions were to fail, it might leave our data in an invalid state.
-- MAGIC
-- MAGIC 대신, 우리는 이러한 행동을 단일 원자적 거래로 결합하여 세 가지 유형의 변경 사항을 모두 함께 적용한다. <br/>
-- MAGIC Instead, we combine these actions into a single atomic transaction, applying all 3 types of changes together.
-- MAGIC
-- MAGIC **`MERGE`** 문에는 일치시킬 필드가 하나 이상 있어야 하며, 일치할 때 또는 일치하지 않을 때 각 절에는 임의의 수의 추가 조건문이 있을 수 있습니다. <br/>
-- MAGIC **`MERGE`** statements must have at least one field to match on, and each **`WHEN MATCHED`** or **`WHEN NOT MATCHED`** clause can have any number of additional conditional statements.
-- MAGIC
-- MAGIC 여기서 **`id`** 필드를 일치시킨 다음 **`type`** 필드를 필터링하여 레코드를 적절히 업데이트, 삭제 또는 삽입합니다. <br/>
-- MAGIC Here, we match on our **`id`** field and then filter on the **`type`** field to appropriately update, delete, or insert our records.

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC MERGE 문의 영향을 받은 레코드는 3개뿐입니다. <br>
-- MAGIC 업데이트 테이블의 레코드 중 하나는 학생 테이블에 일치하는 ID가 없지만 업데이트로 표시되어 있습니다. <br>
-- MAGIC 사용자 지정 논리에 따라 이 레코드를 삽입하기보다는 무시했습니다. <br/>
-- MAGIC Note that only 3 records were impacted by our **`MERGE`** statement; one of the records in our updates table did not have a matching **`id`** in the students table but was marked as an **`update`**. Based on our custom logic, we ignored this record rather than inserting it. 
-- MAGIC
-- MAGIC 최종 INSERT 절에 일치하지 않는 **`update`** 로 표시된 레코드를 포함하도록 위의 문장을 어떻게 수정하시겠습니까? <br/>
-- MAGIC How would you modify the above statement to include unmatched records marked **`update`** in the final **`INSERT`** clause?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## 테이블 DROP(Dropping a Table)
-- MAGIC
-- MAGIC 대상 테이블에 대한 적절한 권한이 있다고 가정하면 **`DROP TABLE`** 명령을 사용하여 레이크 하우스의 데이터를 영구적으로 삭제할 수 있습니다. <br/>
-- MAGIC Assuming that you have proper permissions on the target table, you can permanently delete data in the lakehouse using a **`DROP TABLE`** command.
-- MAGIC
-- MAGIC 참고: 나중에 ACL(테이블 액세스 제어 목록)과 기본 권한에 대해 설명합니다. 적절하게 구성된 레이크 하우스에서 사용자는 생산 테이블을 삭제할 수 없어야 한다. <br/>
-- MAGIC **NOTE**: Later in the course, we'll discuss Table Access Control Lists (ACLs) and default permissions. In a properly configured lakehouse, users should **not** be able to delete production tables.

-- COMMAND ----------

DROP TABLE students

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
