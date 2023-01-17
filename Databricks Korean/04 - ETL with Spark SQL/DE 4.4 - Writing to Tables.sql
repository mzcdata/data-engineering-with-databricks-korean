-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 델타 테이블에 쓰기
-- MAGIC Delta Lake 테이블은 클라우드 객체 스토리지의 데이터 파일로 백업되는 테이블에 ACID 호환 업데이트를 제공합니다.
-- MAGIC 
-- MAGIC 이 노트북에서는 Delta Lake를 사용하여 업데이트를 처리하는 SQL 구문에 대해 알아보겠습니다. 많은 작업이 표준 SQL이지만 스파크와 델타레이크 실행을 수용하기 위해 약간의 변형이 존재한다.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC - **`INSERT OVERWRITE`** 를 사용하여 데이터 테이블 덮어쓰기
-- MAGIC - **`INSERT OVERWRITE`** 를 사용하여 표에 추가합니다
-- MAGIC - **`MERGE INTO`** 를 사용하여 테이블을 추가, 업데이트 및 삭제합니다
-- MAGIC - **`COPY INTO`** 를 사용하여 점진적으로 테이블로 데이터 수집
-- MAGIC  
-- MAGIC # Writing to Delta Tables
-- MAGIC Delta Lake tables provide ACID compliant updates to tables backed by data files in cloud object storage.
-- MAGIC 
-- MAGIC In this notebook, we'll explore SQL syntax to process updates with Delta Lake. While many operations are standard SQL, slight variations exist to accommodate Spark and Delta Lake execution.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Overwrite data tables using **`INSERT OVERWRITE`**
-- MAGIC - Append to a table using **`INSERT INTO`**
-- MAGIC - Append, update, and delete from a table using **`MERGE INTO`**
-- MAGIC - Ingest data incrementally into tables using **`COPY INTO`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 설치 스크립트가 데이터를 생성하고 이 노트북의 나머지 부분을 실행하는 데 필요한 값을 선언합니다.
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 전체 덮어쓰기
-- MAGIC 
-- MAGIC 덮어쓰기를 사용하여 테이블의 모든 데이터를 원자적으로 대체할 수 있습니다. 테이블을 삭제하고 다시 만드는 대신 테이블을 덮어쓰는 것은 여러 가지 이점이 있습니다:
-- MAGIC - 디렉터리를 재귀적으로 나열하거나 파일을 삭제할 필요가 없기 때문에 테이블을 덮어쓰는 것이 훨씬 빠릅니다.
-- MAGIC - 테이블의 이전 버전이 여전히 존재합니다. 시간 여행을 사용하여 이전 데이터를 쉽게 검색할 수 있습니다.
-- MAGIC - 이건 원자 작전이야. 테이블을 삭제하는 동안에도 동시 쿼리가 테이블을 읽을 수 있습니다.
-- MAGIC - ACID 트랜잭션 보증으로 인해 테이블을 덮어쓰지 못할 경우 테이블은 이전 상태가 됩니다.
-- MAGIC 
-- MAGIC Spark SQL은 완전한 덮어쓰기를 수행하는 두 가지 쉬운 방법을 제공합니다.
-- MAGIC 
-- MAGIC 일부 학생들은 CTAS 문에 대한 이전 수업에서 실제로 CRAS 문을 사용했다는 것을 알아차렸을 수 있다(셀이 여러 번 실행된 경우 잠재적인 오류를 방지하기 위해).
-- MAGIC 
-- MAGIC **`CREATE OR REPLACE TABLE`** (CRAS) 문은 실행될 때마다 테이블의 내용을 완전히 대체합니다.
-- MAGIC  
-- MAGIC ## Complete Overwrites
-- MAGIC 
-- MAGIC We can use overwrites to atomically replace all of the data in a table. There are multiple benefits to overwriting tables instead of deleting and recreating tables:
-- MAGIC - Overwriting a table is much faster because it doesn’t need to list the directory recursively or delete any files.
-- MAGIC - The old version of the table still exists; can easily retrieve the old data using Time Travel.
-- MAGIC - It’s an atomic operation. Concurrent queries can still read the table while you are deleting the table.
-- MAGIC - Due to ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
-- MAGIC 
-- MAGIC Spark SQL provides two easy methods to accomplish complete overwrites.
-- MAGIC 
-- MAGIC Some students may have noticed previous lesson on CTAS statements actually used CRAS statements (to avoid potential errors if a cell was run multiple times).
-- MAGIC 
-- MAGIC **`CREATE OR REPLACE TABLE`** (CRAS) statements fully replace the contents of a table each time they execute.

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/events-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 테이블 기록을 검토하면 이 테이블의 이전 버전이 교체된 것으로 표시됩니다.
-- MAGIC 
-- MAGIC Reviewing the table history shows a previous version of this table was replaced.

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`INSERT OVERWRITE`** 는 위와 거의 동일한 결과를 제공합니다. 대상 테이블의 데이터는 쿼리의 데이터로 대체됩니다.
-- MAGIC 
-- MAGIC **`INSERT OVERWRITE`**:
-- MAGIC - 기존 테이블만 덮어쓸 수 있으며 CRAS 문과 같은 새 테이블은 만들 수 없습니다
-- MAGIC - 현재 테이블 스키마와 일치하는 새 레코드로만 덮어쓸 수 있으므로 다운스트림 소비자를 방해하지 않고 기존 테이블을 덮어쓸 수 있는 "안전한" 기술이 될 수 있습니다
-- MAGIC - 개별 파티션을 덮어쓸 수 있음
-- MAGIC 
-- MAGIC **`INSERT OVERWRITE`** provides a nearly identical outcome as above: data in the target table will be replaced by data from the query. 
-- MAGIC 
-- MAGIC **`INSERT OVERWRITE`**:
-- MAGIC 
-- MAGIC - Can only overwrite an existing table, not create a new one like our CRAS statement
-- MAGIC - Can overwrite only with new records that match the current table schema -- and thus can be a "safer" technique for overwriting an existing table without disrupting downstream consumers
-- MAGIC - Can overwrite individual partitions

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC CRAS 문과는 다른 메트릭이 표시됩니다. 또한 테이블 기록은 작업을 다르게 기록합니다.
-- MAGIC 
-- MAGIC Note that different metrics are displayed than a CRAS statement; the table history also records the operation differently.

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 여기서의 주요 차이점은 델타 레이크가 쓰기에 스키마를 적용하는 방식과 관련이 있다.
-- MAGIC 
-- MAGIC CRAS 문을 사용하면 대상 테이블의 내용을 완전히 재정의할 수 있지만 **`INSERT OVERWRITE`** 는 스키마를 변경하려고 하면 실패합니다(선택적 설정을 제공하지 않는 한).
-- MAGIC 
-- MAGIC 주석을 제거하고 아래 셀을 실행하여 예상된 오류 메시지를 생성합니다.
-- MAGIC 
-- MAGIC A primary difference here has to do with how Delta Lake enforces schema on write.
-- MAGIC 
-- MAGIC Whereas a CRAS statement will allow us to completely redefine the contents of our target table, **`INSERT OVERWRITE`** will fail if we try to change our schema (unless we provide optional settings). 
-- MAGIC 
-- MAGIC Uncomment and run the cell below to generated an expected error message.

-- COMMAND ----------

-- INSERT OVERWRITE sales
-- SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/raw/sales-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 행 추가
-- MAGIC 
-- MAGIC **`INSERT INTO`** 를 사용하여 기존 델타 테이블에 새 행을 원자적으로 추가할 수 있습니다. 이렇게 하면 기존 테이블을 증분 업데이트할 수 있으며, 매번 덮어쓰는 것보다 훨씬 효율적입니다.
-- MAGIC 
-- MAGIC **`INSERT INTO`** 를 사용하여 **`sales`** 표에 새 판매 기록을 추가합니다.
-- MAGIC 
-- MAGIC ## Append Rows
-- MAGIC 
-- MAGIC We can use **`INSERT INTO`** to atomically append new rows to an existing Delta table. This allows for incremental updates to existing tables, which is much more efficient than overwriting each time.
-- MAGIC 
-- MAGIC Append new sale records to the **`sales`** table using **`INSERT INTO`**.

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`INSERT INTO`** 에는 동일한 레코드를 여러 번 삽입하지 못하도록 하는 기본 제공 보증이 없습니다. 위의 셀을 다시 실행하면 동일한 레코드가 대상 테이블에 기록되어 레코드가 중복됩니다.
-- MAGIC 
-- MAGIC Note that **`INSERT INTO`** does not have any built-in guarantees to prevent inserting the same records multiple times. Re-executing the above cell would write the same records to the target table, resulting in duplicate records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 업데이트 병합
-- MAGIC 
-- MAGIC **`MERGE`** SQL 작업을 사용하여 소스 테이블, 보기 또는 데이터 프레임의 데이터를 대상 델타 테이블로 업스트림할 수 있습니다. Delta Lake는 **`MERGE`** 에서 삽입, 업데이트 및 삭제를 지원하며 SQL 표준 이상의 확장 구문을 지원하여 고급 사용 사례를 지원합니다.
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC **`MERGE`** 작업을 사용하여 업데이트된 이메일 및 새 사용자로 기록 사용자 데이터를 업데이트합니다.
-- MAGIC 
-- MAGIC ## Merge Updates
-- MAGIC 
-- MAGIC You can upsert data from a source table, view, or DataFrame into a target Delta table using the **`MERGE`** SQL operation. Delta Lake supports inserts, updates and deletes in **`MERGE`**, and supports extended syntax beyond the SQL standards to facilitate advanced use cases.
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC We will use the **`MERGE`** operation to update historic users data with updated emails and new users.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/raw/users-30m`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`MERGE`** 의 주요 이점:
-- MAGIC * 업데이트, 삽입 및 삭제가 단일 트랜잭션으로 완료됨
-- MAGIC * 일치하는 필드에 여러 조건을 추가할 수 있습니다
-- MAGIC * 사용자 지정 로직을 구현하기 위한 광범위한 옵션을 제공합니다
-- MAGIC 
-- MAGIC 아래에서는 현재 행에 **`NULL`** 이메일이 있고 새 행에는 없는 경우에만 레코드를 업데이트합니다.
-- MAGIC 
-- MAGIC 새 배치에서 일치하지 않는 모든 레코드가 삽입됩니다.
-- MAGIC  
-- MAGIC The main benefits of **`MERGE`**:
-- MAGIC * updates, inserts, and deletes are completed as a single transaction
-- MAGIC * multiple conditionals can be added in addition to matching fields
-- MAGIC * provides extensive options for implementing custom logic
-- MAGIC 
-- MAGIC Below, we'll only update records if the current row has a **`NULL`** email and the new row does not. 
-- MAGIC 
-- MAGIC All unmatched records from the new batch will be inserted.

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 우리는 **`MATCHED`** 와 **`NOT MATCHED`** 조건 모두에 대해 이 함수의 동작을 명시적으로 지정한다는 점에 유의한다. 여기서 보여주는 예는 모든 **`MERGE`** 동작을 나타내기보다는 적용할 수 있는 논리의 예일 뿐이다.
-- MAGIC 
-- MAGIC Note that we explicitly specify the behavior of this function for both the **`MATCHED`** and **`NOT MATCHED`** conditions; the example demonstrated here is just an example of logic that can be applied, rather than indicative of all **`MERGE`** behavior.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 중복제거를 위한 삽입 전용 병합
-- MAGIC 
-- MAGIC 일반적인 ETL 사용 사례는 일련의 추가 작업을 통해 로그 또는 기타 모든 추가 데이터 세트를 델타 테이블로 수집하는 것입니다.
-- MAGIC 
-- MAGIC 많은 소스 시스템이 중복 레코드를 생성할 수 있습니다. 병합을 사용하면 삽입 전용 병합을 수행하여 중복 레코드를 삽입하지 않을 수 있습니다.
-- MAGIC 
-- MAGIC 이 최적화된 명령은 동일한 **`MERGE`** 구문을 사용하지만 **`WHEN NOT MATCHED`** 절만 제공합니다.
-- MAGIC 
-- MAGIC 아래에서는 동일한 **`user_id`** 및 **`event_timestamp`** 의 레코드가 이미 **`events`** 테이블에 없는지 확인하기 위해 이를 사용한다.
-- MAGIC 
-- MAGIC ## Insert-Only Merge for Deduplication
-- MAGIC 
-- MAGIC A common ETL use case is to collect logs or other every-appending datasets into a Delta table through a series of append operations. 
-- MAGIC 
-- MAGIC Many source systems can generate duplicate records. With merge, you can avoid inserting the duplicate records by performing an insert-only merge.
-- MAGIC 
-- MAGIC This optimized command uses the same **`MERGE`** syntax but only provided a **`WHEN NOT MATCHED`** clause.
-- MAGIC 
-- MAGIC Below, we use this to confirm that records with the same **`user_id`** and **`event_timestamp`** aren't already in the **`events`** table.

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 증분 로드
-- MAGIC 
-- MAGIC **`COPY INTO`** 는 SQL 엔지니어에게 외부 시스템에서 데이터를 점진적으로 수집할 수 있는 이상적인 옵션을 제공합니다.
-- MAGIC 
-- MAGIC 이 작업에는 다음과 같은 몇 가지 예상 사항이 작업에는 다음과 같습니다:
-- MAGIC - 데이터 스키마가 일관되어야 함
-- MAGIC - 중복된 레코드를 제외하거나 다운스트림에서 처리해야 합니다
-- MAGIC 
-- MAGIC 이 작업은 예측 가능하게 증가하는 데이터에 대한 전체 테이블 검색보다 훨씬 저렴할 수 있습니다.
-- MAGIC 
-- MAGIC 여기서는 정적 디렉토리에서 간단한 실행을 보여주겠지만, 실제 값은 소스의 새 파일을 자동으로 픽업하는 시간 경과에 따른 여러 실행에 있습니다.
-- MAGIC  
-- MAGIC ## Load Incrementally
-- MAGIC 
-- MAGIC **`COPY INTO`** provides SQL engineers an idempotent option to incrementally ingest data from external systems.
-- MAGIC 
-- MAGIC Note that this operation does have some expectations:
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC 
-- MAGIC This operation is potentially much cheaper than full table scans for data that grows predictably.
-- MAGIC 
-- MAGIC While here we'll show simple execution on a static directory, the real value is in multiple executions over time picking up new files in the source automatically.

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다.
-- MAGIC  
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