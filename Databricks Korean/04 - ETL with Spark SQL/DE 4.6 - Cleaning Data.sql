-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 데이터 정리
-- MAGIC 
-- MAGIC Spark SQL로 완료된 대부분의 변환은 SQL에 정통한 개발자들에게 친숙할 것이다.
-- MAGIC 
-- MAGIC 데이터를 검사하고 정리할 때 데이터 세트에 적용할 변환을 표현하기 위해 다양한 열 표현식과 쿼리를 구성해야 한다.  
-- MAGIC 
-- MAGIC 열 표현식은 기존 열, 연산자 및 내장 Spark SQL 함수로 구성됩니다. 그것들은 데이터 세트에서 새로운 열을 생성하는 변환을 표현하기 위해 **'SELECT'**문에 사용될 수 있다. 
-- MAGIC 
-- MAGIC **`SELECT`** 와 함께 **`WHERE`**, **`DISTINCT`**, **`ORDER BY`**, **`GROUP BY`** 등 많은 추가 쿼리 명령을 사용하여 스파크 SQL의 변환을 표현할 수 있다.
-- MAGIC 
-- MAGIC 이 노트북에서는 사용자가 익숙한 다른 시스템과 다를 수 있는 몇 가지 개념과 일반적인 작업에 유용한 몇 가지 기능을 설명합니다.
-- MAGIC 
-- MAGIC 우리는 **`NULL`** 값 주변의 동작뿐만 아니라 문자열 및 날짜 시간 필드의 형식을 지정하는 데 특히 주의를 기울일 것이다.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC - 데이터셋 요약 및 null 동작 설명
-- MAGIC - 중복 항목 검색 및 제거
-- MAGIC - 예상 카운트, 결측값 및 중복 레코드에 대한 데이터 세트 검증
-- MAGIC - 데이터 정리 및 변환에 공통 변환 적용
-- MAGIC  
-- MAGIC # Cleaning Data
-- MAGIC 
-- MAGIC Most transformations completed with Spark SQL will be familiar to SQL-savvy developers.
-- MAGIC 
-- MAGIC As we inspect and clean our data, we'll need to construct various column expressions and queries to express transformations to apply on our dataset.  
-- MAGIC 
-- MAGIC Column expressions are constructed from existing columns, operators, and built-in Spark SQL functions. They can be used in **`SELECT`** statements to express transformations that create new columns from datasets. 
-- MAGIC 
-- MAGIC Along with **`SELECT`**, many additional query commands can be used to express transformations in Spark SQL, including **`WHERE`**, **`DISTINCT`**, **`ORDER BY`**, **`GROUP BY`**, etc.
-- MAGIC 
-- MAGIC In this notebook, we'll review a few concepts that might differ from other systems you're used to, as well as calling out a few useful functions for common operations.
-- MAGIC 
-- MAGIC We'll pay special attention to behaviors around **`NULL`** values, as well as formatting strings and datetime fields.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Summarize datasets and describe null behaviors
-- MAGIC - Retrieve and removing duplicates
-- MAGIC - Validate datasets for expected counts, missing values, and duplicate records
-- MAGIC - Apply common transformations to clean and transform data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC 설치 스크립트가 데이터를 생성하고 이 노트북의 나머지 부분을 실행하는 데 필요한 값을 선언합니다.
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이 수업을 위해 **`users_dirty`** 표에 있는 새로운 사용자 레코드로 작업하겠습니다.
-- MAGIC 
-- MAGIC We'll work with new users records in **`users_dirty`** table for this lesson.

-- COMMAND ----------

SELECT * FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 데이터 검사
-- MAGIC 
-- MAGIC 먼저 데이터의 각 필드에서 값을 세어 보겠습니다.
-- MAGIC  
-- MAGIC ## Inspect Data
-- MAGIC 
-- MAGIC Let's start by counting values in each field of our data.

-- COMMAND ----------

SELECT count(user_id), count(user_first_touch_timestamp), count(email), count(updated), count(*)
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`count(col)`** 는 특정 열 또는 식을 계산할 때 **`NULL`** 값을 건너뜁니다.
-- MAGIC 
-- MAGIC 그러나 **`count(*)`** 는 총 행 수(**`NULL`** 값만 있는 행 포함)를 카운트하는 특수한 경우입니다.
-- MAGIC 
-- MAGIC null 값을 계산하려면 **`count_if`** 함수 또는 **`WHERE`** 절을 사용하여 값 **`IS NULL`** 인 레코드를 필터링하는 조건을 제공합니다.
-- MAGIC  
-- MAGIC Note that **`count(col)`** skips **`NULL`** values when counting specific columns or expressions.
-- MAGIC 
-- MAGIC However, **`count(*)`** is a special case that counts the total number of rows (including rows that are only **`NULL`** values).
-- MAGIC 
-- MAGIC To count null values, use the **`count_if`** function or **`WHERE`** clause to provide a condition that filters for records where the value **`IS NULL`**.

-- COMMAND ----------

SELECT
  count_if(user_id IS NULL) AS missing_user_ids, 
  count_if(user_first_touch_timestamp IS NULL) AS missing_timestamps, 
  count_if(email IS NULL) AS missing_emails,
  count_if(updated IS NULL) AS missing_updates
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 분명히 우리의 모든 필드에는 최소한 소수의 null 값이 있다. 무엇이 이것을 야기하는지 알아보도록 하자.
-- MAGIC 
-- MAGIC Clearly there are at least a handful of null values in all of our fields. Let's try to discover what is causing this.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 고유 레코드
-- MAGIC 
-- MAGIC 다른 행을 찾는 것부터 시작합니다.
-- MAGIC 
-- MAGIC ## Distinct Records
-- MAGIC 
-- MAGIC Start by looking for distinct rows.

-- COMMAND ----------

SELECT count(DISTINCT(*))
FROM users_dirty

-- COMMAND ----------

SELECT count(DISTINCT(user_id))
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`user_id`** 는 **`user_first_touch_timestamp`** 와 함께 생성되므로 이러한 필드는 항상 카운트에 대해 동등해야 합니다.
-- MAGIC 
-- MAGIC Because **`user_id`** is generated alongside the **`user_first_touch_timestamp`**, these fields should always be in parity for counts.

-- COMMAND ----------

SELECT count(DISTINCT(user_first_touch_timestamp))
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 여기서 우리는 우리의 총 행 수와 관련된 일부 중복된 레코드가 있지만, 우리는 훨씬 더 많은 고유한 값을 가지고 있다는 것에 주목한다.
-- MAGIC 
-- MAGIC 계속해서 우리의 고유한 카운트를 기둥 모양의 카운트와 결합하여 이러한 값을 나란히 봅시다.
-- MAGIC 
-- MAGIC Here we note that while there are some duplicate records relative to our total row count, we have a much higher number of distinct values.
-- MAGIC 
-- MAGIC Let's go ahead and combine our distinct counts with columnar counts to see these values side-by-side.

-- COMMAND ----------

SELECT 
  count(user_id) AS total_ids,
  count(DISTINCT user_id) AS unique_ids,
  count(email) AS total_emails,
  count(DISTINCT email) AS unique_emails,
  count(updated) AS total_updates,
  count(DISTINCT(updated)) AS unique_updates,
  count(*) AS total_rows, 
  count(DISTINCT(*)) AS unique_non_null_rows
FROM users_dirty

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 위의 요약을 바탕으로 다음을 알 수 있습니다:
-- MAGIC * 우리의 모든 이메일은 독특하다
-- MAGIC * 이메일에 가장 많은 null 값이 포함되어 있습니다
-- MAGIC * **`updated`** 열에는 하나의 고유 값만 포함되지만 대부분이 null이 아닙니다
-- MAGIC 
-- MAGIC Based on the above summary, we know:
-- MAGIC * All of our emails are unique
-- MAGIC * Our emails contain the largest number of null values
-- MAGIC * The **`updated`** column contains only 1 distinct value, but most are non-null

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 행 중복 제거
-- MAGIC 위의 동작을 바탕으로 **`DISTINCT *`** 를 사용하여 중복된 레코드를 제거하면 어떤 일이 발생할 것으로 예상하십니까?
-- MAGIC  
-- MAGIC ## Deduplicate Rows
-- MAGIC Based on the above behavior, what do you expect will happen if we use **`DISTINCT *`** to try to remove duplicate records?

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_deduped AS
  SELECT DISTINCT(*) FROM users_dirty;

SELECT * FROM users_deduped

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 위의 미리 보기에서 **`COUNT(DISTINCT(*))`** 가 이러한 null을 무시했음에도 null 값이 있는 것으로 보인다는 점에 유의하십시오.
-- MAGIC 
-- MAGIC 이 **`DISTINCT`** 명령을 통과한 행은 몇 개입니까?
-- MAGIC 
-- MAGIC Note in the preview above that there appears to be null values, even though our **`COUNT(DISTINCT(*))`** ignored these nulls.
-- MAGIC 
-- MAGIC How many rows do you expect passed through this **`DISTINCT`** command?

-- COMMAND ----------

SELECT COUNT(*) FROM users_deduped

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이제 완전히 새로운 번호를 갖게 되었습니다.
-- MAGIC 
-- MAGIC 스파크는 열에서 값을 세거나 필드에 대해 고유한 값을 세는 동안 null 값을 건너뛰지만 **`DISTINCT`** 쿼리에서 null이 있는 행을 생략하지 않습니다.
-- MAGIC 
-- MAGIC 실제로 이전 카운트보다 1이 높은 새 숫자가 표시되는 이유는 모두 null인 행이 3개 있기 때문입니다(여기에는 하나의 개별 행으로 포함됨).
-- MAGIC 
-- MAGIC Note that we now have a completely new number.
-- MAGIC 
-- MAGIC Spark skips null values while counting values in a column or counting distinct values for a field, but does not omit rows with nulls from a **`DISTINCT`** query.
-- MAGIC 
-- MAGIC Indeed, the reason we're seeing a new number that is 1 higher than previous counts is because we have 3 rows that are all nulls (here included as a single distinct row).

-- COMMAND ----------

SELECT * FROM users_dirty
WHERE
  user_id IS NULL AND
  user_first_touch_timestamp IS NULL AND
  email IS NULL AND
  updated IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 특정 열을 기준으로 중복 제거
-- MAGIC 
-- MAGIC **`user_id`** 와 **`user_first_touch_timestamp`** 는 모두 주어진 사용자를 처음 만났을 때 생성되므로 고유한 튜플을 형성해야 한다는 것을 기억하십시오.
-- MAGIC 
-- MAGIC 이러한 각 필드에 null 값이 있음을 알 수 있습니다. 이러한 필드에 대해 고유한 쌍 수를 세는 null을 제외하면 테이블에서 고유한 값에 대한 올바른 카운트를 얻을 수 있습니다.
-- MAGIC   
-- MAGIC ## Deduplicate Based on Specific Columns
-- MAGIC 
-- MAGIC Recall that **`user_id`** and **`user_first_touch_timestamp`** should form unique tuples, as they are both generated when a given user is first encountered.
-- MAGIC 
-- MAGIC We can see that we have some null values in each of these fields; exclude nulls counting the distinct number of pairs for these fields will get us the correct count for distinct values in our table.

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 여기서는 이러한 고유한 쌍을 사용하여 데이터에서 원하지 않는 행을 제거합니다.
-- MAGIC 
-- MAGIC 아래 코드는 **`GROUP BY`** 를 사용하여 **`user_id`** 및 **`user_first_touch_timestamp`** 에 기반한 중복 레코드를 제거합니다.
-- MAGIC 
-- MAGIC **`max()`** 집계 함수는 여러 레코드가 있을 때 null이 아닌 이메일을 캡처하기 위한 해킹으로 **`email`** 컬럼에 사용됩니다. 이번 배치에서는 모든 **`updated`** 값이 동일했지만, 이 값을 다음 그룹의 결과로 유지하려면 집계 함수를 사용해야 합니다.
-- MAGIC 
-- MAGIC Here, we'll use these distinct pairs to remove unwanted rows from our data.
-- MAGIC 
-- MAGIC The code below uses **`GROUP BY`** to remove duplicate records based on **`user_id`** and **`user_first_touch_timestamp`**.
-- MAGIC 
-- MAGIC The **`max()`** aggregate function is used on the **`email`** column as a hack to capture non-null emails when multiple records are present; in this batch, all **`updated`** values were equivalent, but we need to use an aggregate function to keep this value in the result of our group by.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 데이터 세트 검증
-- MAGIC 우리는 수동 검토를 기반으로 우리의 카운트가 예상대로라는 것을 시각적으로 확인했다.
-- MAGIC 
-- MAGIC 아래에서는 간단한 필터와 **`WHERE`** 절을 사용하여 프로그래밍 방식으로 몇 가지 검증을 수행한다.
-- MAGIC 
-- MAGIC 각 행에 대한 **`user_id`** 가 고유한지 확인합니다.
-- MAGIC  
-- MAGIC ## Validate Datasets
-- MAGIC We've visually confirmed that our counts are as expected, based our manual review.
-- MAGIC  
-- MAGIC Below, we programmatically do some validation using simple filters and **`WHERE`** clauses.
-- MAGIC 
-- MAGIC Validate that the **`user_id`** for each row is unique.

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 각 전자 메일이 최대 하나의 **`user_id`** 와 연결되어 있는지 확인합니다.
-- MAGIC 
-- MAGIC Confirm that each email is associated with at most one **`user_id`**.

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 날짜 형식 및 정규식
-- MAGIC Null 필드를 제거하고 중복 항목을 제거했으므로 데이터에서 추가 값을 추출할 수 있습니다.
-- MAGIC 
-- MAGIC 아래 코드:
-- MAGIC - **`user_first_touch_timestamp`** 의 배율을 올바르게 조정하고 유효한 타임스탬프로 캐스팅합니다
-- MAGIC - 사람이 읽을 수 있는 형식으로 이 타임스탬프에 대한 달력 데이터 및 시계 시간을 추출합니다
-- MAGIC - **`regexp_extract`** 를 사용하여 정규식을 사용하여 이메일 열에서 도메인을 추출합니다
-- MAGIC  
-- MAGIC ## Date Format and Regex
-- MAGIC Now that we've removed null fields and eliminated duplicates, we may wish to extract further value out of the data.
-- MAGIC 
-- MAGIC The code below:
-- MAGIC - Correctly scales and casts the **`user_first_touch_timestamp`** to a valid timestamp
-- MAGIC - Extracts the calendar data and clock time for this timestamp in human readable format
-- MAGIC - Uses **`regexp_extract`** to extract the domains from the email column using regex

-- COMMAND ----------

SELECT *,
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %md
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