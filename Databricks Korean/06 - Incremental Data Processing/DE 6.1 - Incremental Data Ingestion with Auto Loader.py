# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 자동 로더를 사용한 증분 데이터 수집
# MAGIC 
# MAGIC 증분 ETL은 마지막 섭취 이후 발생한 새로운 데이터만 처리할 수 있기 때문에 중요하다. 새로운 데이터만 안정적으로 처리하면 중복 처리를 줄이고 기업이 데이터 파이프라인을 안정적으로 확장할 수 있습니다.
# MAGIC 
# MAGIC 성공적인 데이터 레이크하우스 구현을 위한 첫 번째 단계는 클라우드 스토리지에서 델타 레이크 테이블로 수집하는 것입니다. 
# MAGIC 
# MAGIC 과거에는 데이터 레이크에서 데이터베이스로 파일을 수집하는 과정이 복잡했습니다.
# MAGIC 
# MAGIC Databricks Auto Loader는 새로운 데이터 파일이 클라우드 파일 스토리지에 도착할 때 점진적이고 효율적으로 처리할 수 있는 사용하기 쉬운 메커니즘을 제공합니다. 이 노트북에서는 자동 로더가 작동하는 것을 볼 수 있습니다.
# MAGIC 
# MAGIC Auto Loader가 제공하는 이점과 확장성 때문에 Databricks는 클라우드 객체 스토리지에서 데이터를 수집할 때 일반적인 **best practice** 로 사용할 것을 권장합니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * 자동 로더 코드를 실행하여 클라우드 스토리지에서 Delta Lake로 점진적으로 데이터 수집
# MAGIC * 자동 로더용으로 구성된 디렉터리에 새 파일이 도착할 때 발생하는 작업 설명
# MAGIC * 스트리밍 자동 로더 쿼리에서 제공하는 테이블 쿼리
# MAGIC 
# MAGIC # Incremental Data Ingestion with Auto Loader
# MAGIC 
# MAGIC Incremental ETL is important since it allows us to deal solely with new data that has been encountered since the last ingestion. Reliably processing only the new data reduces redundant processing and helps enterprises reliably scale data pipelines.
# MAGIC 
# MAGIC The first step for any successful data lakehouse implementation is ingesting into a Delta Lake table from cloud storage. 
# MAGIC 
# MAGIC Historically, ingesting files from a data lake into a database has been a complicated process.
# MAGIC 
# MAGIC Databricks Auto Loader provides an easy-to-use mechanism for incrementally and efficiently processing new data files as they arrive in cloud file storage. In this notebook, you'll see Auto Loader in action.
# MAGIC 
# MAGIC Due to the benefits and scalability that Auto Loader delivers, Databricks recommends its use as general **best practice** when ingesting data from cloud object storage.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Execute Auto Loader code to incrementally ingest data from cloud storage to Delta Lake
# MAGIC * Describe what happens when a new file arrives in a directory configured for Auto Loader
# MAGIC * Query a table fed by a streaming Auto Loader query
# MAGIC 
# MAGIC ## 사용된 데이터 집합
# MAGIC 이 데모에서는 JSON 형식으로 제공되는 심박수 기록을 나타내는 단순화된 인공적으로 생성된 의료 데이터를 사용합니다.
# MAGIC 
# MAGIC ## Dataset Used
# MAGIC This demo uses simplified artificially generated medical data representing heart rate recordings delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC 다음 셀을 실행하여 데모를 재설정하고 필요한 변수와 도움말 기능을 구성합니다.
# MAGIC 
# MAGIC Run the following cell to reset the demo and configure required variables and help functions.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 자동 로더 사용
# MAGIC 
# MAGIC 아래 셀에서는 PySpark API와 함께 Datbricks Auto Loader를 사용하여 시연하는 기능이 정의되어 있습니다. 이 코드에는 구조화된 스트리밍 읽기 및 쓰기가 모두 포함됩니다.
# MAGIC 
# MAGIC 다음 노트북은 Structured Streaming에 대한 보다 강력한 개요를 제공합니다. 자동 로더 옵션에 대한 자세한 내용은 <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank"> 설명서 </a>를 참조하십시오.
# MAGIC 
# MAGIC 자동 <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank"> 무작위 추론 및 진화 </a>와 함께 자동 로더를 사용할 때, 여기에 표시된 4개의 인수는 대부분의 데이터 세트의 수집을 허용해야 한다. 이러한 주장은 아래에 설명되어 있습니다.
# MAGIC 
# MAGIC 
# MAGIC ## Using Auto Loader
# MAGIC 
# MAGIC In the cell below, a function is defined to demonstrate using Databricks Auto Loader with the PySpark API. This code includes both a Structured Streaming read and write.
# MAGIC 
# MAGIC The following notebook will provide a more robust overview of Structured Streaming. If you wish to learn more about Auto Loader options, refer to the <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">documentation</a>.
# MAGIC 
# MAGIC Note that when using Auto Loader with automatic <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html" target="_blank">schema inference and evolution</a>, the 4 arguments shown here should allow ingestion of most datasets. These arguments are explained below.
# MAGIC 
# MAGIC | argument | what it is | how it's used |
# MAGIC | --- | --- | --- |
# MAGIC | **`data_source`** | The directory of the source data | Auto Loader will detect new files as they arrive in this location and queue them for ingestion; passed to the **`.load()`** method |
# MAGIC | **`source_format`** | The format of the source data |  While the format for all Auto Loader queries will be **`cloudFiles`**, the format of the source data should always be specified for the **`cloudFiles.format`** option |
# MAGIC | **`table_name`** | The name of the target table | Spark Structured Streaming supports writing directly to Delta Lake tables by passing a table name as a string to the **`.table()`** method. Note that you can either append to an existing table or create a new table |
# MAGIC | **`checkpoint_directory`** | The location for storing metadata about the stream | This argument is pass to the **`checkpointLocation`** and **`cloudFiles.schemaLocation`** options. Checkpoints keep track of streaming progress, while the schema location tracks updates to the fields in the source dataset |
# MAGIC 
# MAGIC **참고**: 아래 코드는 자동 로더 기능을 보여주기 위해 간소화되었습니다. 소스 데이터를 Delta Lake에 저장하기 전에 소스 데이터에 추가 변환을 적용할 수 있음을 나중에 학습합니다.
# MAGIC 
# MAGIC **NOTE**: The code below has been streamlined to demonstrate Auto Loader functionality. We'll see in later lessons that additional transformations can be applied to source data before saving them to Delta Lake.

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀에서는 이전에 정의된 함수와 설정 스크립트에 정의된 일부 경로 변수를 사용하여 자동 로더 스트림을 시작합니다.
# MAGIC 
# MAGIC JSON 파일의 원본 디렉터리를 읽고 있습니다.
# MAGIC 
# MAGIC In the following cell, we use the previously defined function and some path variables defined in the setup script to begin an Auto Loader stream.
# MAGIC 
# MAGIC Here, we're reading from a source directory of JSON files.

# COMMAND ----------

query = autoload_to_table(data_source = f"{DA.paths.working_dir}/tracker",
                          source_format = "json",
                          table_name = "target_table",
                          checkpoint_directory = f"{DA.paths.checkpoints}/target_table")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Auto Loader는 Spark Structured Streaming을 사용하여 데이터를 점진적으로 로드하기 때문에 위의 코드 실행이 완료되지 않은 것 같습니다.
# MAGIC 
# MAGIC 이는 **지속적으로 활성화된 쿼리**로 생각할 수 있습니다. 이것은 새로운 데이터가 우리의 데이터 소스에 도착하자마자, 그것은 우리의 논리를 통해 처리되고 우리의 목표 테이블에 로드될 것이라는 것을 의미한다. 잠시 후에 이것을 살펴보도록 하겠습니다.
# MAGIC 
# MAGIC Because Auto Loader uses Spark Structured Streaming to load data incrementally, the code above doesn't appear to finish executing.
# MAGIC 
# MAGIC We can think of this as a **continuously active query**. This means that as soon as new data arrives in our data source, it will be processed through our logic and loaded into our target table. We'll explore this in just a second.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 스트리밍 수업을 위한 도우미 기능
# MAGIC 
# MAGIC 노트북 기반 수업은 스트리밍 기능을 배치 및 스트리밍 쿼리와 결합하여 이러한 작업의 결과에 대비합니다. 이러한 노트북은 교육용이며 대화형 셀별 실행을 위한 것입니다. 이 패턴은 생산용이 아닙니다.
# MAGIC 
# MAGIC 아래에서, 우리는 주어진 스트리밍 쿼리에 의해 데이터가 기록되었는지 확인할 수 있을 정도로 충분히 오랫동안 노트북이 다음 셀을 실행하지 못하도록 하는 도우미 기능을 정의한다. 프로덕션 작업에는 이 코드가 필요하지 않습니다.
# MAGIC 
# MAGIC ## Helper Function for Streaming Lessons
# MAGIC 
# MAGIC Our notebook-based lessons combine streaming functions with batch and streaming queries against the results of those operations. These notebooks are for instructional purposes and intended for interactive, cell-by-cell execution. This pattern is not intended for production.
# MAGIC 
# MAGIC Below, we define a helper function that prevents our notebook from executing the next cell just long enough to ensure data has been written out by a given streaming query. This code should not be necessary in a production job.

# COMMAND ----------

def block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")

block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 대상 테이블 쿼리
# MAGIC 
# MAGIC 데이터가 자동 로더를 사용하여 델타 레이크에 수집되면 사용자는 테이블과 동일한 방식으로 데이터와 상호 작용할 수 있습니다.
# MAGIC 
# MAGIC ## Query Target Table
# MAGIC 
# MAGIC Once data has been ingested to Delta Lake with Auto Loader, users can interact with it the same way they would any table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM target_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **`_rescued_data`** 열은 자동 로더에 의해 자동으로 추가되어 형식이 잘못되어 테이블에 맞지 않을 수 있는 모든 데이터를 캡처합니다.
# MAGIC 
# MAGIC 오토 로더는 데이터에 대한 필드 이름을 올바르게 캡처했지만 모든 필드를 **`STRING`** 유형으로 인코딩했습니다. JSON은 텍스트 기반 형식이기 때문에 형식 불일치로 인해 수집 시 최소한의 데이터가 삭제되거나 무시되도록 하는 가장 안전하고 허용적인 형식입니다.
# MAGIC 
# MAGIC Note that the **`_rescued_data`** column is added by Auto Loader automatically to capture any data that might be malformed and not fit into the table otherwise.
# MAGIC 
# MAGIC While Auto Loader captured the field names for our data correctly, note that it encoded all fields as **`STRING`** type. Because JSON is a text-based format, this is the safest and most permissive type, ensuring that the least amount of data is dropped or ignored at ingestion due to type mismatch.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE target_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래 셀을 사용하여 대상 테이블에 기록된 임시 보기를 정의합니다.
# MAGIC 
# MAGIC Auto 로더와 함께 자동으로 요청한 데이터를 자동으로 테스트합니다.
# MAGIC 
# MAGIC Use the cell below to define a temporary view that summarizes the recordings in our target table.
# MAGIC 
# MAGIC We'll use this view below to demonstrate how new data is automatically ingested with Auto Loader.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts AS
# MAGIC   SELECT device_id, count(*) total_recordings
# MAGIC   FROM target_table
# MAGIC   GROUP BY device_id;
# MAGIC   
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Land New Data
# MAGIC 
# MAGIC 앞에서 언급한 바와 같이, 자동 로더는 클라우드 객체 저장소의 디렉터리에서 델타 레이크 테이블로 파일을 점진적으로 처리하도록 구성된다.
# MAGIC 
# MAGIC 우리는 **`source_path`** 가 지정한 위치에서 **`target_table`** 이라는 테이블로 JSON 파일을 처리하기 위한 쿼리를 구성하고 현재 실행 중이다. **`source_path`** 디렉토리의 내용을 검토해 보겠습니다.
# MAGIC 
# MAGIC ## Land New Data
# MAGIC 
# MAGIC As mentioned previously, Auto Loader is configured to incrementally process files from a directory in cloud object storage into a Delta Lake table.
# MAGIC 
# MAGIC We have configured and are currently executing a query to process JSON files from the location specified by **`source_path`** into a table named **`target_table`**. Let's review the contents of the **`source_path`** directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 현재 이 위치에 하나의 JSON 파일이 나열되어 있습니다.
# MAGIC 
# MAGIC 아래 셀의 메서드는 이 디렉터리에 데이터를 쓰는 외부 시스템을 모델링할 수 있도록 설정 스크립트에서 구성되었습니다. 아래 셀을 실행할 때마다 새 파일이 **`source_path`** 디렉토리에 저장됩니다.
# MAGIC 
# MAGIC At present, you should see a single JSON file listed in this location.
# MAGIC 
# MAGIC The method in the cell below was configured in our setup script to allow us to model an external system writing data to this directory. Each time you execute the cell below, a new file will land in the **`source_path`** directory.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래 셀을 사용하여 **`source_path`** 의 내용을 다시 나열하십시오. 이전 셀을 실행할 때마다 추가 JSON 파일이 표시됩니다.
# MAGIC 
# MAGIC List the contents of the **`source_path`** again using the cell below. You should see an additional JSON file for each time you ran the previous cell.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.working_dir}/tracker")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 수집 진행률 추적
# MAGIC 
# MAGIC 과거에는 대부분의 시스템이 소스 디렉토리의 모든 레코드를 재처리하여 현재 결과를 계산하거나 데이터 엔지니어가 마지막으로 테이블을 업데이트한 이후에 도착한 새 데이터를 식별하기 위한 사용자 지정 논리를 구현하도록 구성되어 있었습니다.
# MAGIC 
# MAGIC 자동 로더를 사용하여 테이블이 이미 업데이트되었습니다.
# MAGIC 
# MAGIC 아래 쿼리를 실행하여 새 데이터가 수집되었는지 확인하십시오.
# MAGIC 
# MAGIC ## Tracking Ingestion Progress
# MAGIC 
# MAGIC Historically, many systems have been configured to either reprocess all records in a source directory to calculate current results or require data engineers to implement custom logic to identify new data that's arrived since the last time a table was updated.
# MAGIC 
# MAGIC With Auto Loader, your table has already been updated.
# MAGIC 
# MAGIC Run the query below to confirm that new data has been ingested.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이전에 구성한 Auto Loader 쿼리는 원본 디렉토리의 레코드를 자동으로 검색하여 대상 테이블로 처리합니다. 레코드를 수집할 때 약간의 지연이 있지만 기본 스트리밍 구성으로 실행되는 자동 로더 쿼리는 거의 실시간으로 결과를 업데이트합니다.
# MAGIC 
# MAGIC 아래 쿼리는 테이블 기록을 보여줍니다. 각 **`STREAMING UPDATE`** 에 대해 새 테이블 버전이 표시되어야 합니다. 이러한 업데이트 이벤트는 소스에 도착하는 새 데이터 배치와 일치합니다.
# MAGIC 
# MAGIC The Auto Loader query we configured earlier automatically detects and processes records from the source directory into the target table. There is a slight delay as records are ingested, but an Auto Loader query executing with default streaming configuration should update results in near real time.
# MAGIC 
# MAGIC The query below shows the table history. A new table version should be indicated for each **`STREAMING UPDATE`**. These update events coincide with new batches of data arriving at the source.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY target_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 정리
# MAGIC 위의 셀을 사용하여 새로운 데이터를 계속해서 다운로드하고 표 결과를 살펴보십시오.
# MAGIC 
# MAGIC 작업을 마치면 다음 셀을 실행하여 모든 활성 스트림을 중지하고 생성된 리소스를 제거한 후 계속하십시오.
# MAGIC 
# MAGIC ## Clean Up
# MAGIC Feel free to continue landing new data and exploring the table results with the cells above.
# MAGIC 
# MAGIC When you're finished, run the following cell to stop all active streams and remove created resources before continuing.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>