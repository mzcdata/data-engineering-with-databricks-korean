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
-- MAGIC # 외부 소스에 대한 옵션 제공(Providing Options for External Sources)
-- MAGIC 
-- MAGIC 파일을 직접 쿼리하는 것은 자기 기술 형식에 적합하지만, 많은 데이터 소스는 레코드를 적절하게 수집하기 위해 추가 구성이나 스키마 선언을 필요로 한다.<br>
-- MAGIC While directly querying files works well for self-describing formats, many data sources require additional configurations or schema declaration to properly ingest records.
-- MAGIC 
-- MAGIC 이 과정에서는 외부 데이터 소스를 사용하여 표를 만들 것입니다. 이러한 테이블은 아직 델타 레이크 형식으로 저장되지는 않지만(따라서 레이크 하우스에 최적화되지는 않음), 이 기술은 다양한 외부 시스템에서 데이터를 쉽게 추출하는 데 도움이 된다.<br>
-- MAGIC In this lesson, we will create tables using external data sources. While these tables will not yet be stored in the Delta Lake format (and therefore not be optimized for the Lakehouse), this technique helps to facilitate extracting data from diverse external systems.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC - Spark SQL을 사용하여 외부 소스에서 데이터 추출 옵션 구성
-- MAGIC - 다양한 파일 형식에 대한 외부 데이터 원본에 대한 테이블 만들기
-- MAGIC - 외부 소스에 대해 정의된 테이블을 쿼리할 때의 기본 동작 설명
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use Spark SQL to configure options for extracting data from external sources
-- MAGIC - Create tables against external data sources for various file formats
-- MAGIC - Describe default behavior when querying tables defined against external sources

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC 설치 스크립트가 데이터를 생성하고 이 노트북의 나머지 부분을 실행하는 데 필요한 값을 선언합니다.<br>
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 직접 쿼리가 작동하지 않는 경우(When Direct Queries Don't Work)
-- MAGIC 세션 간에 파일에 대한 직접 쿼리를 유지하는 데 뷰를 사용할 수 있지만, 이 접근 방식은 제한적인 유용성을 가지고 있습니다.<br>
-- MAGIC While views can be used to persist direct queries against files between sessions, this approach has limited utility.
-- MAGIC 
-- MAGIC CSV 파일은 가장 일반적인 파일 형식 중 하나이지만 이러한 파일에 대해 직접 쿼리를 수행해도 원하는 결과가 반환되는 경우는 거의 없습니다.<br>
-- MAGIC CSV files are one of the most common file formats, but a direct query against these files rarely returns the desired results.

-- COMMAND ----------

SELECT * FROM csv.`${da.paths.working_dir}/sales-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 위에서 우리는 다음을 볼 수 있다:
-- MAGIC 1. 헤더 행이 테이블 행으로 추출되고 있습니다
-- MAGIC 1. 모든 열이 단일 열로 로드됩니다
-- MAGIC 1. 파일이 파이프로 구분되어 있습니다(**`|`**)
-- MAGIC 1. 마지막 열에 잘리고 있는 중첩된 데이터가 포함된 것으로 나타납니다
-- MAGIC 
-- MAGIC We can see from the above that:
-- MAGIC 1. The header row is being extracted as a table row
-- MAGIC 1. All columns are being loaded as a single column
-- MAGIC 1. The file is pipe-delimited (**`|`**)
-- MAGIC 1. The final column appears to contain nested data that is being truncated

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 읽기 옵션을 사용하여 외부 데이터에 테이블 등록(Registering Tables on External Data with Read Options)
-- MAGIC 
-- MAGIC 스파크는 기본 설정을 사용하여 효율적으로 자신을 설명하는 일부 데이터 소스를 추출하지만, 많은 형식은 스키마나 다른 옵션의 선언을 필요로 한다.<br>
-- MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
-- MAGIC 
-- MAGIC 외부 소스에 대한 테이블을 만드는 동안 설정할 수 있는 추가 구성이 많지만, 아래 구문은 대부분의 형식에서 데이터를 추출하는 데 필요한 필수 요소를 보여줍니다.<br>
-- MAGIC While there are many <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">additional configurations</a> you can set while creating tables against external sources, the syntax below demonstrates the essentials required to extract data from most formats.
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC 옵션은 따옴표로 묶이지 않은 텍스트와 따옴표로 묶인 값으로 전달됩니다. 스파크는 사용자 지정 옵션을 통해 많은 데이터 소스를 지원하며, 추가 시스템은 외부 <a href="https://docs.databricks.com/libraries/index.html" target="_blank">library</a>를 통해 비공식적으로 지원할 수 있다.<br>
-- MAGIC Note that options are passed with keys as unquoted text and values in quotes. Spark supports many <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">data sources</a> with custom options, and additional systems may have unofficial support through external <a href="https://docs.databricks.com/libraries/index.html" target="_blank">libraries</a>. 
-- MAGIC 
-- MAGIC **참고**: 작업 공간 설정에 따라 라이브러리를 로드하고 일부 데이터 소스에 필요한 보안 설정을 구성하려면 관리자의 도움이 필요할 수 있습니다.<br>
-- MAGIC **NOTE**: Depending on your workspace settings, you may need administrator assistance to load libraries and configure the requisite security settings for some data sources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 아래 셀은 Spark SQL DDL을 사용하여 외부 CSV 소스에 대한 테이블을 만드는 방법을 보여줍니다:
-- MAGIC 1. 열 이름 및 유형
-- MAGIC 1. 파일 형식
-- MAGIC 1. 필드를 구분하는 데 사용되는 구분 기호
-- MAGIC 1. 헤더의 존재
-- MAGIC 1. 이 데이터가 저장되는 경로
-- MAGIC 
-- MAGIC The cell below demonstrates using Spark SQL DDL to create a table against an external CSV source, specifying:
-- MAGIC 1. The column names and types
-- MAGIC 1. The file format
-- MAGIC 1. The delimiter used to separate fields
-- MAGIC 1. The presence of a header
-- MAGIC 1. The path to where this data is stored

-- COMMAND ----------

CREATE TABLE sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${da.paths.working_dir}/sales-csv"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블 선언 중에 데이터가 이동하지 않았습니다. 파일을 직접 쿼리하고 보기를 만들 때와 마찬가지로 여전히 외부 위치에 저장된 파일을 가리키고 있습니다.<br>
-- MAGIC Note that no data has moved during table declaration. Similar to when we directly queried our files and created a view, we are still just pointing to files stored in an external location.
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 이제 데이터가 올바르게 로드되고 있는지 확인합니다.<br>
-- MAGIC Run the following cell to confirm that data is now being loaded correctly.

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블 선언 중에 전달된 모든 메타데이터 및 옵션은 해당 위치의 데이터가 항상 이러한 옵션으로 읽혀지도록 메타포에 유지됩니다.<br>
-- MAGIC All the metadata and options passed during table declaration will be persisted to the metastore, ensuring that data in the location will always be read with these options.
-- MAGIC 
-- MAGIC 참고: CSV를 데이터 소스로 사용할 때는 소스 디렉토리에 데이터 파일을 추가하는 경우 열 순서가 변경되지 않도록 하는 것이 중요합니다. 데이터 형식에는 강력한 스키마 적용이 없기 때문에 스파크는 열을 로드하고 테이블 선언 중 지정된 순서대로 열 이름과 데이터 유형을 적용합니다.<br>
-- MAGIC **NOTE**: When working with CSVs as a data source, it's important to ensure that column order does not change if additional data files will be added to the source directory. Because the data format does not have strong schema enforcement, Spark will load columns and apply column names and data types in the order specified during table declaration.
-- MAGIC 
-- MAGIC 테이블에서 **`DESCRIBE EXTENDED`** 을 실행하면 테이블 정의와 관련된 모든 메타데이터가 표시됩니다.<br>
-- MAGIC Running **`DESCRIBE EXTENDED`** on a table will show all of the metadata associated with the table definition.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 외부 데이터 원본이 있는 테이블의 Limits(Limits of Tables with External Data Sources)
-- MAGIC 
-- MAGIC Datbricks에 대한 다른 과정을 수강했거나 당사의 자료를 검토한 적이 있다면 Delta Lake와 Lakehouse에 대해 들어본 적이 있을 것입니다. 외부 데이터 소스에 대해 테이블이나 쿼리를 정의할 때마다 Delta Lake 및 Lakehouse와 관련된 성능 보장을 기대할 수 없습니다.<br>
-- MAGIC If you've taken other courses on Databricks or reviewed any of our company literature, you may have heard about Delta Lake and the Lakehouse. Note that whenever we're defining tables or queries against external data sources, we **cannot** expect the performance guarantees associated with Delta Lake and Lakehouse.
-- MAGIC 
-- MAGIC 예를 들어 Delta Lake 테이블을 사용하면 항상 최신 버전의 원본 데이터를 쿼리할 수 있지만 다른 데이터 원본에 대해 등록된 테이블은 캐시된 이전 버전을 나타낼 수 있습니다.<br>
-- MAGIC For example: while Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.
-- MAGIC 
-- MAGIC 아래의 셀은 우리가 우리의 테이블의 기초가 되는 파일들을 직접 업데이트하는 외부 시스템을 나타내는 것으로 생각할 수 있는 몇 가지 논리를 실행한다.<br>
-- MAGIC The cell below executes some logic that we can think of as just representing an external system directly updating the files underlying our table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("sales_csv")
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(f"{DA.paths.working_dir}/sales-csv"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 표의 현재 레코드 수를 보면 새로 삽입된 행이 표시되지 않습니다.<br>
-- MAGIC If we look at the current count of records in our table, the number we see will not reflect these newly inserted rows.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이전에 이 데이터 소스를 쿼리할 때 Spark는 자동으로 로컬 스토리지에 기본 데이터를 캐시했습니다. 이렇게 하면 후속 쿼리에서 스파크가 이 로컬 캐시를 쿼리하는 것만으로 최적의 성능을 제공할 수 있습니다.<br>
-- MAGIC At the time we previously queried this data source, Spark automatically cached the underlying data in local storage. This ensures that on subsequent queries, Spark will provide the optimal performance by just querying this local cache.
-- MAGIC 
-- MAGIC 당사의 외부 데이터 소스가 Spark에게 이 데이터를 새로 고치도록 구성되어 있지 않습니다.<br>
-- MAGIC Our external data source is not configured to tell Spark that it should refresh this data. 
-- MAGIC 
-- MAGIC **`REFRESH TABLE`** 명령을 실행하여 데이터 캐시를 수동으로 새로 고칠 수 **있습니다.**<br>
-- MAGIC We **can** manually refresh the cache of our data by running the **`REFRESH TABLE`** command.

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블을 새로 고치면 캐시가 무효화되므로 원래 데이터 소스를 다시 검색하고 모든 데이터를 메모리로 가져와야 합니다.<br>
-- MAGIC Note that refreshing our table will invalidate our cache, meaning that we'll need to rescan our original data source and pull all data back into memory. 
-- MAGIC 
-- MAGIC 매우 큰 데이터 세트의 경우 이 작업에 상당한 시간이 걸릴 수 있습니다.<br>
-- MAGIC For very large datasets, this may take a significant amount of time.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## SQL 데이터베이스에서 데이터 추출(Extracting Data from SQL Databases)
-- MAGIC SQL 데이터베이스는 매우 일반적인 데이터 소스이며, 데이터브릭에는 다양한 SQL 버전과 연결하기 위한 표준 JDBC 드라이버가 있습니다.<br>
-- MAGIC SQL databases are an extremely common data source, and Databricks has a standard JDBC driver for connecting with many flavors of SQL.
-- MAGIC 
-- MAGIC 이러한 연결을 만드는 일반적인 구문은 다음과 같습니다:<br>
-- MAGIC The general syntax for creating these connections is:
-- MAGIC 
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC 
-- MAGIC   아래 코드 샘플에서, 우리는 <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>로 연결할 것이다.
-- MAGIC In the code sample below, we'll connect with <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>.
-- MAGIC   
-- MAGIC **참고:** SQLite는 로컬 파일을 사용하여 데이터베이스를 저장하며 포트, 사용자 이름 또는 암호가 필요하지 않습니다.<br>
-- MAGIC **NOTE:** SQLite uses a local file to store a database, and doesn't require a port, username, or password.  
-- MAGIC   
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> **WARNING*: JDBC 서버의 백엔드 구성은 사용자가 이 노트북을 단일 노드 클러스터에서 실행하고 있다고 가정합니다. 여러 작업자가 있는 클러스터에서 실행 중인 경우, 실행기에서 실행 중인 클라이언트는 드라이버에 연결할 수 없습니다.<br>
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> **WARNING**: The backend-configuration of the JDBC server assume you are running this notebook on a single-node cluster. If you are running on a cluster with multiple workers, the client running in the executors will not be able to connect to the driver.

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:/${da.username}_ecommerce.db",
  dbtable = "users"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이제 이 테이블을 로컬로 정의된 것처럼 쿼리할 수 있습니다.<br>
-- MAGIC Now we can query this table as if it were defined locally.

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블 메타데이터를 보면 외부 시스템에서 스키마 정보를 캡처했다는 것을 알 수 있다. 스토리지 속성(연결과 관련된 사용자 이름 및 암호 포함)은 자동으로 수정됩니다.<br>
-- MAGIC Looking at the table metadata reveals that we have captured the schema information from the external system. Storage properties (which would include the username and password associated with the connection) are automatically redacted.

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 테이블이 **`MANAGED`** 로 나열되어 있지만 지정된 위치의 내용을 나열하면 로컬에서 유지되는 데이터가 없음을 확인할 수 있습니다. <br>
-- MAGIC While the table is listed as **`MANAGED`**, listing the contents of the specified location confirms that no data is being persisted locally.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC jdbc_users_path = f"{DA.paths.user_db}/users_jdbc/"
-- MAGIC print(jdbc_users_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(jdbc_users_path)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 데이터 웨어하우스와 같은 일부 SQL 시스템에는 사용자 지정 드라이버가 있습니다. 스파크는 다양한 외부 데이터베이스와 다르게 상호 작용하지만, 두 가지 기본 접근 방식은 다음과 같이 요약할 수 있다:
-- MAGIC 1. 전체 원본 테이블을 Datbricks로 이동한 다음 현재 활성 클러스터에서 논리 실행
-- MAGIC 1. 쿼리를 외부 SQL 데이터베이스로 푸시하고 결과만 Datbricks로 다시 전송
-- MAGIC 
-- MAGIC 두 경우 모두 외부 SQL 데이터베이스에서 매우 큰 데이터셋을 사용하면 다음과 같은 이유로 상당한 오버헤드가 발생할 수 있습니다:
-- MAGIC 1. 공용 인터넷을 통해 모든 데이터 이동과 관련된 네트워크 전송 지연 시간
-- MAGIC 1. 빅데이터 쿼리에 최적화되지 않은 소스 시스템에서 쿼리 로직 실행
-- MAGIC 
-- MAGIC Note that some SQL systems such as data warehouses will have custom drivers. Spark will interact with various external databases differently, but the two basic approaches can be summarized as either:
-- MAGIC 1. Moving the entire source table(s) to Databricks and then executing logic on the currently active cluster
-- MAGIC 1. Pushing down the query to the external SQL database and only transferring the results back to Databricks
-- MAGIC 
-- MAGIC In either case, working with very large datasets in external SQL databases can incur significant overhead because of either:
-- MAGIC 1. Network transfer latency associated with moving all data over the public internet
-- MAGIC 1. Execution of query logic in source systems not optimized for big data queries

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