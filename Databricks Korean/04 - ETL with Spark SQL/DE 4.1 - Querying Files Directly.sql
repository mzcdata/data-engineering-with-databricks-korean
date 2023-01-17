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
-- MAGIC # 파일에서 직접 데이터 추출(Extracting Data Directly from Files)
-- MAGIC 
-- MAGIC 이 노트북에서는 데이터브릭스의 스파크 SQL을 사용하여 파일에서 직접 데이터를 추출하는 방법에 대해 알아봅니다.<br>
-- MAGIC In this notebook, you'll learn to extract data directly from files using Spark SQL on Databricks.
-- MAGIC 
-- MAGIC 많은 파일 형식이 이 옵션을 지원하지만 데이터 형식(예: parquet 및 JSON)을 자체적으로 설명하는 데 가장 유용합니다.<br>
-- MAGIC A number of file formats support this option, but it is most useful for self-describing data formats (such as parquet and JSON).
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC - Spark SQL을 사용하여 데이터 파일을 직접 쿼리합니다
-- MAGIC - **`텍스트`** 및 **`이진 파일`** method 를 활용하여 raw file 내용 검토
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use Spark SQL to directly query data files
-- MAGIC - Leverage **`text`** and **`binaryFile`** methods to review raw file contents

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 
-- MAGIC 설치 스크립트가 데이터를 생성하고 이 노트북의 나머지 부분을 실행하는 데 필요한 값을 선언합니다.<br>
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 데이터 개요(Data Overview)
-- MAGIC 
-- MAGIC 이 예에서는 JSON 파일로 작성된 raw Kafka 데이터의 샘플로 작업합니다.<br>
-- MAGIC In this example, we'll work with a sample of raw Kafka data written as JSON files. 
-- MAGIC 
-- MAGIC 각 파일은 5초 간격 동안 소비된 모든 레코드를 포함하며, 전체 Kafka 스키마와 함께 다중 레코드 JSON 파일로 저장됩니다.<br>
-- MAGIC Each file contains all records consumed during a 5-second interval, stored with the full Kafka schema as a multiple-record JSON file.
-- MAGIC 
-- MAGIC | field | type | description |
-- MAGIC | --- | --- | --- |
-- MAGIC | key | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | timestamp | LONG | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 소스 디렉토리에는 많은 JSON 파일이 있습니다.<br>
-- MAGIC Note that our source directory contains many JSON files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path = f"{DA.paths.datasets}/raw/events-kafka"
-- MAGIC print(dataset_path)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(dataset_path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 여기서는 DBFS 루트에 기록된 데이터에 대한 상대 파일 경로를 사용합니다.<br>
-- MAGIC Here, we'll be using relative file paths to data that's been written to the DBFS root. 
-- MAGIC 
-- MAGIC 대부분의 워크플로우는 사용자가 외부 클라우드 저장 위치에서 데이터에 액세스해야 합니다.<br>
-- MAGIC Most workflows will require users to access data from external cloud storage locations. 
-- MAGIC 
-- MAGIC 대부분의 회사에서는 작업 공간 관리자가 이러한 스토리지 위치에 대한 액세스를 구성합니다.<br>
-- MAGIC In most companies, a workspace administrator will be responsible for configuring access to these storage locations.
-- MAGIC 
-- MAGIC 이러한 위치를 구성하고 액세스하는 방법에 대한 지침은 "클라우드 아키텍처 및 시스템 통합"이라는 클라우드 공급업체별 자체 과정에서 확인할 수 있습니다.<br>
-- MAGIC Instructions for configuring and accessing these locations can be found in the cloud-vendor specific self-paced courses titled "Cloud Architecture & Systems Integrations".

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 단일 파일 쿼리(Query a Single File)
-- MAGIC 
-- MAGIC 단일 파일에 포함된 데이터를 쿼리하려면 다음 패턴으로 쿼리를 실행하십시오:<br>
-- MAGIC To query the data contained in a single file, execute the query with the following pattern:
-- MAGIC 
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC 
-- MAGIC 경로 주변에 back-ticks(단순 따옴표가 아닌)을 사용하는 것에 특별히 주의하십시오. <br>
-- MAGIC Make special note of the use of back-ticks (not single quotes) around the path.

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 미리 보기는 소스 파일의 321개 행을 모두 표시합니다.<br>
-- MAGIC Note that our preview displays all 321 rows of our source file.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 파일 디렉터리 쿼리(Query a Directory of Files)
-- MAGIC 
-- MAGIC 디렉터리에 있는 모든 파일의 형식과 스키마가 동일하다고 가정하면 개별 파일이 아닌 디렉터리 경로를 지정하여 모든 파일을 동시에 쿼리할 수 있습니다.<br>
-- MAGIC Assuming all of the files in a directory have the same format and schema, all files can be queried simultaneously by specifying the directory path rather than an individual file.

-- COMMAND ----------

SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 기본적으로 이 쿼리는 첫 번째 1000개 행만 표시합니다.<br>
-- MAGIC By default, this query will only show the first 1000 rows.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 파일에 대한 참조 만들기(Create References to Files)
-- MAGIC 파일과 디렉터리를 직접 쿼리할 수 있는 이 기능은 추가 스파크 논리를 파일에 대한 쿼리에 연결할 수 있음을 의미합니다.<br>
-- MAGIC This ability to directly query files and directories means that additional Spark logic can be chained to queries against files.
-- MAGIC 
-- MAGIC 경로에 대한 쿼리에서 뷰를 만들 때 나중에 쿼리에서 이 뷰를 참조할 수 있습니다. 여기서는 임시 보기를 만들지만 일반 보기를 사용하여 영구 참조를 만들 수도 있습니다.<br>
-- MAGIC When we create a view from a query against a path, we can reference this view in later queries. Here, we'll create a temporary view, but you can also create a permanent reference with regular view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;

SELECT * FROM events_temp_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 원시 문자열로 텍스트 파일 추출(Extract Text Files as Raw Strings)
-- MAGIC 
-- MAGIC 텍스트 기반 파일(JSON, CSV, TSV 및 TXT 형식 포함)로 작업할 때 텍스트 형식을 사용하여 파일의 각 행을 값이라는 문자열 열이 있는 행으로 로드할 수 있습니다. 이는 데이터 소스가 손상되기 쉽고 사용자 지정 텍스트 구문 분석 기능을 사용하여 텍스트 필드에서 값을 추출할 때 유용합니다.<br>
-- MAGIC When working with text-based files (which include JSON, CSV, TSV, and TXT formats), you can use the **`text`** format to load each line of the file as a row with one string column named **`value`**. This can be useful when data sources are prone to corruption and custom text parsing functions will be used to extract value from text fields.

-- COMMAND ----------

SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 파일의 원시 바이트 및 메타데이터 추출(Extract the Raw Bytes and Metadata of a File)
-- MAGIC 
-- MAGIC 일부 워크플로우는 이미지 또는 비정형 데이터를 처리할 때와 같이 전체 파일을 사용해야 할 수 있습니다. 바이너리 파일을 사용하여 디렉토리를 쿼리하면 파일 내용의 바이너리 표현과 함께 파일 메타데이터가 제공됩니다.<br>
-- MAGIC Some workflows may require working with entire files, such as when dealing with images or unstructured data. Using **`binaryFile`** to query a directory will provide file metadata alongside the binary representation of the file contents.
-- MAGIC 
-- MAGIC 특히, 생성된 필드는 **`path`**, **`modificationTime`**, **`length`**, and **`content`** 을 나타냅니다.<br>
-- MAGIC Specifically, the fields created will indicate the **`path`**, **`modificationTime`**, **`length`**, and **`content`**.

-- COMMAND ----------

SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
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