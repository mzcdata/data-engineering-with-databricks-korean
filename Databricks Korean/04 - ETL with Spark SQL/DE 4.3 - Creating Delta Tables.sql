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
-- MAGIC # 델타 테이블 만들기(Creating Delta Tables)
-- MAGIC 
-- MAGIC 외부 데이터 소스에서 데이터를 추출한 후 데이터를 Lakehouse에 로드하여 Databricks 플랫폼의 모든 이점을 충분히 활용할 수 있도록 합니다.<br>
-- MAGIC After extracting data from external data sources, load data into the Lakehouse to ensure that all of the benefits of the Databricks platform can be fully leveraged.
-- MAGIC 
-- MAGIC 조직마다 데이터를 처음에 Databrick에 로드하는 방법에 대한 정책이 다를 수 있지만, 일반적으로 초기 표는 대부분 원시 버전의 데이터를 나타내며 검증 및 강화는 이후 단계에서 수행하는 것이 좋습니다. 이 패턴은 데이터가 데이터 유형 또는 열 이름과 관련된 예상과 일치하지 않더라도 데이터가 손실되지 않도록 보장합니다. 즉, 프로그래밍 방식 또는 수동 개입을 통해 부분적으로 손상되거나 잘못된 상태로 데이터를 복구할 수 있습니다.<br>
-- MAGIC While different organizations may have varying policies for how data is initially loaded into Databricks, we typically recommend that early tables represent a mostly raw version of the data, and that validation and enrichment occur in later stages. This pattern ensures that even if data doesn't match expectations with regards to data types or column names, no data will be dropped, meaning that programmatic or manual intervention can still salvage data in a partially corrupted or invalid state.
-- MAGIC 
-- MAGIC 이 과정에서는 대부분의 테이블을 만드는 데 사용되는 패턴인 **`CREATE TABLE_ASSELECT`**(CTAS) 문에 중점을 둡니다.<br>
-- MAGIC This lesson will focus primarily on the pattern used to create most tables, **`CREATE TABLE _ AS SELECT`** (CTAS) statements.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC - CTAS 문을 사용하여 Delta Lake 테이블 만들기
-- MAGIC - 기존 뷰 또는 테이블에서 새 테이블 만들기
-- MAGIC - 추가 메타데이터를 사용하여 로드된 데이터 강화
-- MAGIC - 생성된 열 및 설명 설명이 포함된 테이블 스키마 선언
-- MAGIC - 데이터 위치, 품질 시행 및 파티셔닝을 제어하기 위한 고급 옵션 설정
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use CTAS statements to create Delta Lake tables
-- MAGIC - Create new tables from existing views or tables
-- MAGIC - Enrich loaded data with additional metadata
-- MAGIC - Declare table schema with generated columns and descriptive comments
-- MAGIC - Set advanced options to control data location, quality enforcement, and partitioning

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC 설치 스크립트가 데이터를 생성하고 이 노트북의 나머지 부분을 실행하는 데 필요한 값을 선언합니다.<br>
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Select로 Table 만들기 - Create Table as Select (CTAS)
-- MAGIC 
-- MAGIC **`CREATE TABLE AS SELECT`** 문은 입력 쿼리에서 검색된 데이터를 사용하여 델타 테이블을 만들고 채웁니다. <br>
-- MAGIC **`CREATE TABLE AS SELECT`** statements create and populate Delta tables using data retrieved from an input query.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${da.paths.datasets}/raw/sales-historical/`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC CTAS 문은 쿼리 결과에서 스키마 정보를 자동으로 유추하며 수동 스키마 선언을 지원하지 않습니다. <br>
-- MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
-- MAGIC 
-- MAGIC 즉, CTAS 문은 Parquet 파일 및 테이블과 같이 스키마가 잘 정의된 소스에서 외부 데이터를 수집하는 데 유용합니다.<br>
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.
-- MAGIC 
-- MAGIC 또한 CTAS 문은 추가 파일 옵션 지정을 지원하지 않습니다.<br>
-- MAGIC CTAS statements also do not support specifying additional file options.
-- MAGIC 
-- MAGIC CSV 파일에서 데이터를 수집할 때 이 방법이 얼마나 큰 제한을 초래하는지 알 수 있습니다.<br>
-- MAGIC We can see how this would present significant limitations when trying to ingest data from CSV files.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/raw/sales-csv/`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 델타 레이크 테이블에 이 데이터를 올바르게 수집하려면 옵션을 지정할 수 있는 파일에 대한 참조를 사용해야 합니다.
-- MAGIC 
-- MAGIC 이전 수업에서는 외부 테이블을 등록하여 이를 수행하는 것을 보여주었습니다. 여기서는 이 구문을 약간 발전시켜 옵션을 임시 보기로 지정한 다음 이 임시 보기를 CTAS 문의 소스로 사용하여 델타 테이블을 성공적으로 등록할 것입니다.
-- MAGIC 
-- MAGIC To correctly ingest this data to a Delta Lake table, we'll need to use a reference to the files that allows us to specify options.
-- MAGIC 
-- MAGIC In the previous lesson, we showed doing this by registering an external table. Here, we'll slightly evolve this syntax to specify the options to a temporary view, and then use this temp view as the source for a CTAS statement to successfully register the Delta table.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 기존 테이블에서 열 필터링 및 이름 바꾸기
-- MAGIC 
-- MAGIC 테이블을 작성하는 동안 열 이름을 변경하거나 대상 테이블에서 열을 생략하는 등의 간단한 변환을 쉽게 수행할 수 있습니다.
-- MAGIC 
-- MAGIC 다음 문은 **`sales`** 테이블의 열 부분 집합을 포함하는 새 테이블을 생성합니다.
-- MAGIC 
-- MAGIC 여기서는 잠재적으로 사용자를 식별하거나 항목별 구매 세부 정보를 제공하는 정보를 의도적으로 제외하고 있다고 가정합니다. 또한 다운스트림 시스템이 소스 데이터와 다른 명명 규칙을 가지고 있다는 가정 하에 필드의 이름을 변경합니다.
-- MAGIC  
-- MAGIC ## Filtering and Renaming Columns from Existing Tables
-- MAGIC 
-- MAGIC Simple transformations like changing column names or omitting columns from target tables can be easily accomplished during table creation.
-- MAGIC 
-- MAGIC The following statement creates a new table containing a subset of columns from the **`sales`** table. 
-- MAGIC 
-- MAGIC Here, we'll presume that we're intentionally leaving out information that potentially identifies the user or that provides itemized purchase details. We'll also rename our fields with the assumption that a downstream system has different naming conventions than our source data.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래와 같이 보기를 통해 동일한 목표를 달성할 수 있었습니다.<br>
-- MAGIC Note that we could have accomplished this same goal with a view, as shown below.

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 생성된 열로 스키마 선언
-- MAGIC 
-- MAGIC 앞서 언급한 바와 같이 CTAS 문은 스키마 선언을 지원하지 않습니다. 우리는 위에서 타임스탬프 열이 Unix 타임스탬프의 일부 변형으로 보이며, 이는 분석가들이 통찰력을 도출하는 데 가장 유용하지 않을 수 있다는 점에 주목한다. 생성된 열이 유용할 수 있는 상황입니다.
-- MAGIC 
-- MAGIC 생성된 열은 델타 테이블의 다른 열에 대해 사용자가 지정한 함수를 기반으로 값이 자동으로 생성되는 특수 유형의 열입니다(DBR 8.3에서 도입됨).
-- MAGIC 
-- MAGIC 아래 코드는 새 테이블을 만드는 방법을 보여줍니다:
-- MAGIC 1. 열 이름 및 유형 지정
-- MAGIC 1. <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank"> 생성된 열을 추가하여 날짜를 계산합니다
-- MAGIC 1. 생성된 열에 대한 설명 열 설명 제공
-- MAGIC  
-- MAGIC ## Declare Schema with Generated Columns
-- MAGIC 
-- MAGIC As noted previously, CTAS statements do not support schema declaration. We note above that the timestamp column appears to be some variant of a Unix timestamp, which may not be the most useful for our analysts to derive insights. This is a situation where generated columns would be beneficial.
-- MAGIC 
-- MAGIC Generated columns are a special type of column whose values are automatically generated based on a user-specified function over other columns in the Delta table (introduced in DBR 8.3).
-- MAGIC 
-- MAGIC The code below demonstrates creating a new table while:
-- MAGIC 1. Specifying column names and types
-- MAGIC 1. Adding a <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">generated column</a> to calculate the date
-- MAGIC 1. Providing a descriptive column comment for the generated column

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  
-- MAGIC  **`date`** 는 생성된 열이기 때문에 **`date`** 열에 값을 제공하지 않고 **`purchase_dates`** 에 쓰면 델타레이크가 자동으로 계산합니다.
-- MAGIC 
-- MAGIC **참고**: 아래 셀은 델타 레이크 **`MERGE`** 문을 사용할 때 열을 생성할 수 있도록 설정을 구성합니다. 이 구문에 대한 자세한 내용은 이 과정의 뒷부분에서 확인하십시오.
-- MAGIC 
-- MAGIC Because **`date`** is a generated column, if we write to **`purchase_dates`** without providing values for the **`date`** column, Delta Lake automatically computes them.
-- MAGIC 
-- MAGIC **NOTE**: The cell below configures a setting to allow for generating columns when using a Delta Lake **`MERGE`** statement. We'll see more on this syntax later in the course.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래에서 모든 날짜가 데이터가 삽입되었을 때 올바르게 계산되었음을 알 수 있습니다. 그러나 원본 데이터나 삽입 쿼리에서 이 필드의 값을 지정하지 않았습니다.
-- MAGIC 
-- MAGIC 델타 레이크 소스와 마찬가지로 쿼리는 모든 쿼리에 대한 테이블의 최신 스냅샷을 자동으로 읽으므로 **`REFRESH TABLE`** 을 실행할 필요가 없다.
-- MAGIC  
-- MAGIC We can see below that all dates were computed correctly as data was inserted, although neither our source data or insert query specified the values in this field.
-- MAGIC 
-- MAGIC As with any Delta Lake source, the query automatically reads the most recent snapshot of the table for any query; you never need to run **`REFRESH TABLE`**.

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 그렇지 않으면 생성될 필드가 테이블 삽입에 포함된 경우, 제공된 값이 생성된 열을 정의하는 데 사용되는 논리에 의해 파생되는 값과 정확하게 일치하지 않으면 이 삽입이 실패합니다.
-- MAGIC 
-- MAGIC 아래 셀의 주석을 제거하고 실행하면 이 오류를 확인할 수 있습니다:
-- MAGIC 
-- MAGIC 
-- MAGIC It's important to note that if a field that would otherwise be generated is included in an insert to a table, this insert will fail if the value provided does not exactly match the value that would be derived by the logic used to define the generated column.
-- MAGIC 
-- MAGIC We can see this error by uncommenting and running the cell below:

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 테이블 제약 조건 추가
-- MAGIC 
-- MAGIC 위의 오류 메시지는 **'CHECK 제약 조건'**을 참조합니다. 생성된 열은 검사 제약 조건의 특수 구현입니다.
-- MAGIC 
-- MAGIC 델타 레이크는 쓰기에 스키마를 적용하기 때문에 데이터브릭은 표준 SQL 제약 관리 조항을 지원하여 테이블에 추가된 데이터의 품질과 무결성을 보장할 수 있습니다.
-- MAGIC 
-- MAGIC 데이터브릭은 현재 두 가지 유형의 제약 조건을 지원합니다:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** 제약 조건 </a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** 제약 조건 </a>
-- MAGIC 
-- MAGIC 두 경우 모두 제약 조건을 정의하기 전에 테이블에 제약 조건을 위반하는 데이터가 없는지 확인해야 합니다. 제약 조건이 테이블에 추가되면 제약 조건을 위반하는 데이터는 쓰기 오류를 발생시킵니다.
-- MAGIC 
-- MAGIC 아래 표의 **`date`** 열에 **`CHECK`** 제약 조건을 추가합니다. **`CHECK`** 제약 조건은 데이터 세트를 필터링하는 데 사용할 수 있는 표준 **`WHERE`** 절과 유사합니다.
-- MAGIC 
-- MAGIC ## Add a Table Constraint
-- MAGIC 
-- MAGIC The error message above refers to a **`CHECK constraint`**. Generated columns are a special implementation of check constraints.
-- MAGIC 
-- MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
-- MAGIC 
-- MAGIC Databricks currently support two types of constraints:
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** constraints</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** constraints</a>
-- MAGIC 
-- MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure.
-- MAGIC 
-- MAGIC Below, we'll add a **`CHECK`** constraint to the **`date`** column of our table. Note that **`CHECK`** constraints look like standard **`WHERE`** clauses you might use to filter a dataset.

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블 제약 조건은 **'TBL PROPERTIES'** 필드에 표시됩니다. <br>
-- MAGIC Table constraints are shown in the **`TBLPROPERTIES`** field.

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 추가 옵션 및 메타데이터를 사용하여 테이블 강화
-- MAGIC 
-- MAGIC 지금까지 우리는 델타 레이크 테이블을 풍부하게 하는 옵션까지 표면을 긁었을 뿐입니다.
-- MAGIC 
-- MAGIC 아래에서는 다수의 추가 구성 및 메타데이터를 포함하도록 진화하는 CTAS 문을 보여준다.
-- MAGIC 
-- MAGIC 우리의 **`CREATE TABLE`** 조항은 파일 수집에 유용한 두 가지 내장 Spark SQL 명령을 활용한다:
-- MAGIC * **`current_timestamp()`** 는 로직이 실행될 때 타임스탬프를 기록합니다
-- MAGIC * **`input_file_name()`** 은 테이블의 각 레코드에 대한 소스 데이터 파일을 기록합니다
-- MAGIC 
-- MAGIC 또한 소스의 타임스탬프 데이터에서 파생된 새 날짜 열을 생성하는 논리도 포함합니다.
-- MAGIC 
-- MAGIC **`CREATE TABLE`** 절에는 다음과 같은 몇 가지 옵션이 포함되어 있습니다:
-- MAGIC * 테이블 내용을 더 쉽게 검색할 수 있도록 **`COMMENT`** 가 추가되었습니다
-- MAGIC * **`LOCATION`** 이(가) 지정되어 관리되지 않는 외부 테이블이 생성됩니다
-- MAGIC * 테이블은 **`PARTITIONED BY`** 날짜 열입니다. 이는 각 데이터의 데이터가 대상 저장 위치의 자체 디렉터리 내에 존재함을 의미합니다
-- MAGIC 
-- MAGIC **참고**: 파티션은 주로 구문과 영향을 보여주기 위해 여기에 표시됩니다. 대부분의 델타 레이크 테이블(특히 중소형 데이터)은 파티셔닝의 혜택을 받지 못할 것이다. 파티셔닝은 물리적으로 데이터 파일을 분리하기 때문에 이 방법은 작은 파일 문제를 초래할 수 있으며 파일 압축과 효율적인 데이터 건너뛰기를 방지할 수 있습니다. Hive 또는 HDFS에서 관찰되는 이점은 델타 레이크로 변환되지 않으며, 테이블을 분할하기 전에 경험이 풍부한 델타 레이크 설계자와 상의해야 합니다.
-- MAGIC 
-- MAGIC **Delta Lake 작업 시 대부분의 사용 사례에서 파티션이 없는 테이블을 기본값으로 설정하는 것이 좋습니다.**
-- MAGIC 
-- MAGIC ## Enrich Tables with Additional Options and Metadata
-- MAGIC 
-- MAGIC So far we've only scratched the surface as far as the options for enriching Delta Lake tables.
-- MAGIC 
-- MAGIC Below, we show evolving a CTAS statement to include a number of additional configurations and metadata.
-- MAGIC 
-- MAGIC Our **`SELECT`** clause leverages two built-in Spark SQL commands useful for file ingestion:
-- MAGIC * **`current_timestamp()`** records the timestamp when the logic is executed
-- MAGIC * **`input_file_name()`** records the source data file for each record in the table
-- MAGIC 
-- MAGIC We also include logic to create a new date column derived from timestamp data in the source.
-- MAGIC 
-- MAGIC The **`CREATE TABLE`** clause contains several options:
-- MAGIC * A **`COMMENT`** is added to allow for easier discovery of table contents
-- MAGIC * A **`LOCATION`** is specified, which will result in an external (rather than managed) table
-- MAGIC * The table is **`PARTITIONED BY`** a date column; this means that the data from each data will exist within its own directory in the target storage location
-- MAGIC 
-- MAGIC **NOTE**: Partitioning is shown here primarily to demonstrate syntax and impact. Most Delta Lake tables (especially small-to-medium sized data) will not benefit from partitioning. Because partitioning physically separates data files, this approach can result in a small files problem and prevent file compaction and efficient data skipping. The benefits observed in Hive or HDFS do not translate to Delta Lake, and you should consult with an experienced Delta Lake architect before partitioning tables.
-- MAGIC 
-- MAGIC **As a best practice, you should default to non-partitioned tables for most use cases when working with Delta Lake.**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블에 추가된 메타데이터 필드는 레코드가 삽입된 시기와 위치를 이해하는 데 유용한 정보를 제공합니다. 이 기능은 원본 데이터의 문제를 해결해야 할 경우 특히 유용합니다.
-- MAGIC 
-- MAGIC 주어진 표에 대한 모든 주석과 속성은 **`DESCRIBE TABLE EXTENDED`** 를 사용하여 검토할 수 있다.
-- MAGIC 
-- MAGIC **참고**: Delta Lake는 테이블 작성 시 여러 테이블 속성을 자동으로 추가합니다.
-- MAGIC  
-- MAGIC The metadata fields added to the table provide useful information to understand when records were inserted and from where. This can be especially helpful if troubleshooting problems in the source data becomes necessary.
-- MAGIC 
-- MAGIC All of the comments and properties for a given table can be reviewed using **`DESCRIBE TABLE EXTENDED`**.
-- MAGIC 
-- MAGIC **NOTE**: Delta Lake automatically adds several table properties on table creation.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블에 사용된 위치를 나열하면 파티션 열 **`first_touch_date`** 의 고유 값이 데이터 디렉토리를 만드는 데 사용된다는 것을 알 수 있습니다.<br>
-- MAGIC Listing the location used for the table reveals that the unique values in the partition column **`first_touch_date`** are used to create data directories.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 델타 레이크 테이블 복제
-- MAGIC 델타 레이크에는 델타 레이크 테이블을 효율적으로 복사할 수 있는 두 가지 옵션이 있습니다.
-- MAGIC 
-- MAGIC **`DEEP CLONE`** 은(는) 데이터와 메타데이터를 소스 테이블에서 대상으로 완전히 복사합니다. 이 복사본은 점진적으로 발생하므로 이 명령을 다시 실행하면 원본에서 대상 위치로 변경사항을 동기화할 수 있습니다.
-- MAGIC 
-- MAGIC ## Cloning Delta Lake Tables
-- MAGIC Delta Lake has two options for efficiently copying Delta Lake tables.
-- MAGIC 
-- MAGIC **`DEEP CLONE`** fully copies data and metadata from a source table to a target. This copy occurs incrementally, so executing this command again can sync changes from the source to the target location.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 모든 데이터 파일을 복사해야 하므로 대규모 데이터 세트의 경우 이 작업에 상당한 시간이 걸릴 수 있습니다.
-- MAGIC 
-- MAGIC 현재 테이블을 수정할 위험 없이 변경 사항을 적용하는 테스트를 위해 테이블의 복사본을 빨리 생성하려면 **`SHALLOW CLONE`** 을 사용하는 것이 좋습니다. 얕은 클론은 델타 트랜잭션 로그만 복사하므로 데이터가 이동하지 않습니다.
-- MAGIC 
-- MAGIC Because all the data files must be copied over, this can take quite a while for large datasets.
-- MAGIC 
-- MAGIC If you wish to create a copy of a table quickly to test out applying changes without the risk of modifying the current table, **`SHALLOW CLONE`** can be a good option. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 두 경우 모두 복제된 테이블 버전에 적용된 데이터 수정 사항이 추적되고 원본과 별도로 저장됩니다. 복제는 개발 중인 SQL 코드를 테스트하기 위한 테이블을 설정할 수 있는 좋은 방법입니다. <br>
-- MAGIC In either case, data modifications applied to the cloned version of the table will be tracked and stored separately from the source. Cloning is a great way to set up tables for testing SQL code while still in development.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC 
-- MAGIC 이 노트북에서는 주로 DDL과 델타 레이크 테이블을 생성하기 위한 구문에 초점을 맞췄다. 다음 노트북에서는 테이블에 업데이트를 쓰는 옵션에 대해 알아보겠습니다.<br>
-- MAGIC In this notebook, we focused primarily on DDL and syntax for creating Delta Lake tables. In the next notebook, we'll explore options for writing updates to tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다.<br>
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