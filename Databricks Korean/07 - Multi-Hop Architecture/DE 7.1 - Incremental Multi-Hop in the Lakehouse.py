# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 레이크하우스의 증분형 멀티홉
# MAGIC 
# MAGIC 이제 Structured Streaming API와 Spark SQL을 결합하여 증분 데이터 처리로 작업하는 방법을 더 잘 이해할 수 있으므로 Structured Streaming과 Delta Lake 간의 긴밀한 통합을 살펴볼 수 있습니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * Bronze, Silver 및 Gold 표 설명
# MAGIC * Delta Lake 멀티홉 파이프라인 생성
# MAGIC 
# MAGIC # Incremental Multi-Hop in the Lakehouse
# MAGIC 
# MAGIC Now that we have a better understanding of how to work with incremental data processing by combining Structured Streaming APIs and Spark SQL, we can explore the tight integration between Structured Streaming and Delta Lake.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe Bronze, Silver, and Gold tables
# MAGIC * Create a Delta Lake multi-hop pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lakehouse의 증분 업데이트
# MAGIC 
# MAGIC 델타 레이크는 사용자들이 스트리밍과 배치 워크로드를 통합된 멀티홉 파이프라인에서 쉽게 결합할 수 있게 해준다. 파이프라인의 각 단계는 비즈니스 내에서 핵심 사용 사례를 추진하는 데 유용한 데이터의 상태를 나타냅니다. 모든 데이터와 메타데이터가 클라우드의 객체 스토리지에 저장되므로, 여러 사용자와 애플리케이션이 거의 실시간으로 데이터에 액세스할 수 있으므로 분석가가 최신 데이터에 액세스할 수 있습니다
# MAGIC 
# MAGIC ## Incremental Updates in the Lakehouse
# MAGIC 
# MAGIC Delta Lake allows users to easily combine streaming and batch workloads in a unified multi-hop pipeline. Each stage of the pipeline represents a state of our data valuable to driving core use cases within the business. Because all data and metadata lives in object storage in the cloud, multiple users and applications can access data in near-real time, allowing analysts to access the freshest data as it's being processed.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC 
# MAGIC - **Bronze** 테이블에는 다양한 소스(JSON 파일, RDBMS 데이터, IoT 데이터)에서 수집된 원시 데이터가 포함되어 있습니다.
# MAGIC 
# MAGIC - **Silver** 표를 사용하면 데이터를 보다 정교하게 볼 수 있습니다. 다양한 브론즈 테이블의 필드를 결합하여 스트리밍 레코드를 강화하거나 최근 활동을 기반으로 계정 상태를 업데이트할 수 있습니다.
# MAGIC 
# MAGIC - **Gold** 테이블은 보고 및 대시보드에 자주 사용되는 비즈니스 레벨 집계를 제공합니다. 여기에는 일일 활성 웹 사이트 사용자, 상점당 주간 매출 또는 부서별 분기당 총 매출과 같은 집계가 포함됩니다. 
# MAGIC 
# MAGIC 최종 결과물은 비즈니스 메트릭에 대한 실행 가능한 통찰력, 대시보드 및 보고서입니다.
# MAGIC 
# MAGIC ETL 파이프라인의 모든 단계에서 비즈니스 로직을 고려함으로써 불필요한 데이터 중복을 줄이고 전체 기록 데이터에 대한 임시 쿼리를 제한하여 스토리지 및 컴퓨팅 비용을 최적화할 수 있습니다.
# MAGIC 
# MAGIC 각 단계는 배치 또는 스트리밍 작업으로 구성할 수 있으며, ACID 트랜잭션은 우리가 완전히 성공하거나 실패하도록 보장합니다.
# MAGIC 
# MAGIC - **Bronze** tables contain raw data ingested from various sources (JSON files, RDBMS data,  IoT data, to name a few examples).
# MAGIC 
# MAGIC - **Silver** tables provide a more refined view of our data. We can join fields from various bronze tables to enrich streaming records, or update account statuses based on recent activity.
# MAGIC 
# MAGIC - **Gold** tables provide business level aggregates often used for reporting and dashboarding. This would include aggregations such as daily active website users, weekly sales per store, or gross revenue per quarter by department. 
# MAGIC 
# MAGIC The end outputs are actionable insights, dashboards and reports of business metrics.
# MAGIC 
# MAGIC By considering our business logic at all steps of the ETL pipeline, we can ensure that storage and compute costs are optimized by reducing unnecessary duplication of data and limiting ad hoc querying against full historic data.
# MAGIC 
# MAGIC Each stage can be configured as a batch or streaming job, and ACID transactions ensure that we succeed or fail completely.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 사용된 데이터 세트
# MAGIC 
# MAGIC 이 데모는 인공적으로 생성된 단순화된 의료 데이터를 사용한다. 우리의 두 데이터 세트의 스키마는 아래와 같다. 다양한 단계에서 이러한 스키마를 조작합니다.
# MAGIC 
# MAGIC #### Recordings
# MAGIC 주 데이터 세트는 JSON 형식으로 제공된 의료 기기의 심박수 기록을 사용한다.
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified artificially generated medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.
# MAGIC 
# MAGIC #### Recordings
# MAGIC The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC 이러한 데이터는 나중에 이름별로 환자를 식별하기 위해 외부 시스템에 저장된 환자 정보의 정적 테이블과 결합됩니다.
# MAGIC 
# MAGIC #### PII
# MAGIC These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 다음 셀을 실행하여 랩 환경을 구성합니다.
# MAGIC 
# MAGIC Run the following cell to configure the lab environment.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데이터 시뮬레이터
# MAGIC Databricks Auto Loader는 파일이 클라우드 객체 저장소에 저장될 때 자동으로 파일을 처리할 수 있습니다. 
# MAGIC 
# MAGIC 이 프로세스를 시뮬레이션하기 위해 코스 전체에 걸쳐 다음 작업을 여러 번 실행하라는 메시지가 표시됩니다.
# MAGIC 
# MAGIC ## Data Simulator
# MAGIC Databricks Auto Loader can automatically process files as they land in your cloud object stores. 
# MAGIC 
# MAGIC To simulate this process, you will be asked to run the following operation several times throughout the course.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Bronze Table: 원시 JSON 기록 수집
# MAGIC 
# MAGIC 아래에서는 스키마 추론과 함께 자동 로더를 사용하여 원시 JSON 소스에 대한 읽기를 구성한다.
# MAGIC 
# MAGIC Spark DataFrame API를 사용하여 증분 읽기를 설정해야 하지만 구성한 후에는 즉시 임시 보기를 등록하여 데이터의 스트리밍 변환에 Spark SQL을 활용할 수 있습니다.
# MAGIC 
# MAGIC **참고**: JSON 데이터 소스의 경우 자동 로더는 기본적으로 각 열을 문자열로 유추합니다. 여기서, 우리는 **`cloudFiles.schemaHints`** 옵션을 사용하여 **`time`** 컬럼에 대한 데이터 유형을 지정하는 것을 시연한다. 필드에 잘못된 형식을 지정하면 null 값이 발생합니다.
# MAGIC 
# MAGIC ## Bronze Table: Ingesting Raw JSON Recordings
# MAGIC 
# MAGIC Below, we configure a read on a raw JSON source using Auto Loader with schema inference.
# MAGIC 
# MAGIC Note that while you need to use the Spark DataFrame API to set up an incremental read, once configured you can immediately register a temp view to leverage Spark SQL for streaming transformations on your data.
# MAGIC 
# MAGIC **NOTE**: For a JSON data source, Auto Loader will default to inferring each column as a string. Here, we demonstrate specifying the data type for the **`time`** column using the **`cloudFiles.schemaHints`** option. Note that specifying improper types for a field will result in null values.

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 여기서는 원본 파일과 수집된 시간을 설명하는 추가 메타데이터를 사용하여 원시 데이터를 풍부하게 만들 것입니다. 이 추가 메타데이터는 다운스트림 처리 중에 무시할 수 있으며 손상된 데이터가 발생할 경우 오류 문제 해결에 유용한 정보를 제공합니다.
# MAGIC 
# MAGIC Here, we'll enrich our raw data with additional metadata describing the source file and the time it was ingested. This additional metadata can be ignored during downstream processing while providing useful information for troubleshooting errors if corrupt data is encountered.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래 코드는 Delta Lake 테이블에 증분 쓰기를 처리하기 위해 풍부한 원시 데이터를 PySpark API로 다시 전달합니다.
# MAGIC 
# MAGIC The code below passes our enriched raw data back to PySpark API to process an incremental write to a Delta Lake table.

# COMMAND ----------

(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀과 함께 다른 파일 도착을 트리거하면 작성한 스트리밍 쿼리에 의해 즉시 변경 사항이 감지됩니다.
# MAGIC 
# MAGIC Trigger another file arrival with the following cell and you'll see the changes immediately detected by the streaming query you've written.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 정적 조회 테이블 로드
# MAGIC Delta Lake가 데이터에 제공하는 ACID는 테이블 수준에서 관리되며, 완전히 성공적인 커밋만 테이블에 반영되도록 보장합니다. 이러한 데이터를 다른 데이터 원본과 병합하도록 선택한 경우 해당 원본이 데이터를 어떻게 버전화하고 어떤 종류의 일관성을 보장하는지 알아야 합니다.
# MAGIC 
# MAGIC 이 단순화된 데모에서는 기록에 환자 데이터를 추가하기 위해 정적 CSV 파일을 로드합니다. 운영 환경에서는 Databricks의 <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> 기능을 사용하여 델타 레이크에 있는 이러한 데이터의 최신 보기를 유지할 수 있습니다.
# MAGIC 
# MAGIC ### Load Static Lookup Table
# MAGIC The ACID guarantees that Delta Lake brings to your data are managed at the table level, ensuring that only fully successfully commits are reflected in your tables. If you choose to merge these data with other data sources, be aware of how those sources version data and what sort of consistency guarantees they have.
# MAGIC 
# MAGIC In this simplified demo, we are loading a static CSV file to add patient data to our recordings. In production, we could use Databricks' <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> feature to keep an up-to-date view of these data in our Delta Lake.

# COMMAND ----------

(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.data_source}/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 실버 테이블: 풍부한 기록 데이터
# MAGIC 실버 레벨의 두 번째 홉으로, 우리는 다음과 같은 농축과 점검을 할 것이다:
# MAGIC - 기록 데이터가 PII와 결합되어 환자 이름을 추가합니다
# MAGIC - 녹화 시간은 사람이 읽을 수 있도록 **`'yyyy-MM-dd HH:mm:ss'`** 형식으로 구문 분석됩니다
# MAGIC - <= 0인 심박수는 환자의 부재 또는 전달 오류를 나타내는 것으로 알고 있으므로 제외합니다
# MAGIC 
# MAGIC ## Silver Table: Enriched Recording Data
# MAGIC As a second hop in our silver level, we will do the follow enrichments and checks:
# MAGIC - Our recordings data will be joined with the PII to add patient names
# MAGIC - The time for our recordings will be parsed to the format **`'yyyy-MM-dd HH:mm:ss'`** to be human-readable
# MAGIC - We will exclude heart rates that are <= 0, as we know that these either represent the absence of the patient or an error in transmission

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM bronze_tmp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다른 새 파일을 트리거하고 이전 쿼리를 통해 전파될 때까지 기다립니다.
# MAGIC 
# MAGIC Trigger another new file and wait for it propagate through both previous queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Gold Table: 일 평균
# MAGIC 
# MAGIC 여기서 우리는 **`recordings_enriched`** 의 데이터 스트림을 읽고 또 다른 스트림을 작성하여 각 환자의 일일 평균을 집계한 골드 테이블을 만든다.
# MAGIC 
# MAGIC ## Gold Table: Daily Averages
# MAGIC 
# MAGIC Here we read a stream of data from **`recordings_enriched`** and write another stream to create an aggregate gold table of daily averages for each patient.

# COMMAND ----------

(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래의 **`.trigger(availableNow=True)`** 를 사용하고 있습니다. 이를 통해 구조화된 스트리밍의 장점을 계속 사용하면서 이 작업을 한 번에 트리거하여 사용 가능한 모든 데이터를 마이크로 배치로 처리할 수 있다. 요약하면 다음과 같은 장점이 있습니다:
# MAGIC - 단대단 폴트 톨러런트 처리를 정확히 한 번만 수행할 수 있습니다
# MAGIC - 업스트림 데이터 원본의 변경 내용 자동 탐지
# MAGIC 
# MAGIC 데이터가 증가하는 대략적인 속도를 알면 이 작업을 위해 예약한 클러스터의 크기를 적절하게 조정하여 빠르고 비용 효율적인 처리를 보장할 수 있습니다. 고객은 데이터 비용에 대한 최종 집계 보기를 얼마나 업데이트하는지 평가하고 이 작업을 얼마나 자주 실행해야 하는지에 대한 정보에 입각한 결정을 내릴 수 있습니다.
# MAGIC 
# MAGIC 이 테이블을 구독하는 다운스트림 프로세스는 값비싼 집계를 다시 실행할 필요가 없습니다. 오히려 파일을 역직렬화하면 포함된 필드를 기반으로 한 쿼리가 이미 집계된 이 소스에 대해 빠르게 푸시 다운될 수 있습니다.
# MAGIC 
# MAGIC Note that we're using **`.trigger(availableNow=True)`** below. This provides us the ability to continue to use the strengths of structured streaming while trigger this job one-time to process all available data in micro-batches. To recap, these strengths include:
# MAGIC - exactly once end-to-end fault tolerant processing
# MAGIC - automatic detection of changes in upstream data sources
# MAGIC 
# MAGIC If we know the approximate rate at which our data grows, we can appropriately size the cluster we schedule for this job to ensure fast, cost-effective processing. The customer will be able to evaluate how much updating this final aggregate view of their data costs and make informed decisions about how frequently this operation needs to be run.
# MAGIC 
# MAGIC Downstream processes subscribing to this table do not need to re-run any expensive aggregations. Rather, files just need to be de-serialized and then queries based on included fields can quickly be pushed down against this already-aggregated source.

# COMMAND ----------

(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 델타를 사용한 전체 출력에 대한 중요 고려 사항
# MAGIC 
# MAGIC **'complete'** 출력 모드를 사용할 때는 논리가 실행될 때마다 테이블의 전체 상태를 다시 작성한다. 이는 집계 계산에 이상적이지만 Structured Streaming은 데이터가 업스트림 로직에만 추가되는 것으로 가정하기 때문에 **할 수 없습니다.
# MAGIC 
# MAGIC **참고** : 특정 옵션을 설정하여 이 동작을 변경할 수 있지만 다른 제한이 있습니다. 자세한 내용은 <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">델타 스트리밍: 업데이트 및 삭제를 무시합니다.</a>
# MAGIC   
# MAGIC 방금 등록한 골드 델타 테이블은 다음 쿼리를 실행할 때마다 데이터의 현재 상태에 대한 정적 읽기를 수행합니다.
# MAGIC   
# MAGIC #### Important Considerations for complete Output with Delta
# MAGIC 
# MAGIC When using **`complete`** output mode, we rewrite the entire state of our table each time our logic runs. While this is ideal for calculating aggregates, we **cannot** read a stream from this directory, as Structured Streaming assumes data is only being appended in the upstream logic.
# MAGIC 
# MAGIC **NOTE**: Certain options can be set to change this behavior, but have other limitations attached. For more details, refer to <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Delta Streaming: Ignoring Updates and Deletes</a>.
# MAGIC 
# MAGIC The gold Delta table we have just registered will perform a static read of the current state of the data each time we run the following query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 위 표에는 모든 사용자에 대한 모든 날짜가 포함되어 있습니다. 애드혹 쿼리에 대한 술어가 여기서 인코딩된 데이터와 일치하면 술어를 소스의 파일로 밀어넣고 더 제한된 집계 보기를 매우 빠르게 생성할 수 있다.
# MAGIC 
# MAGIC Note the above table includes all days for all users. If the predicates for our ad hoc queries match the data encoded here, we can push down our predicates to files at the source and very quickly generate more limited aggregate views.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 남은 레코드 처리
# MAGIC 다음 셀은 2020년 남은 기간 동안 소스 디렉토리에 추가 파일을 저장합니다. 델타 레이크의 처음 3개 테이블을 통해 이러한 프로세스를 볼 수 있지만 이 쿼리는 현재 사용 가능한 트리거 구문을 사용하기 때문에 **`daily_patient_avg`** 테이블을 업데이트하려면 최종 쿼리를 다시 실행해야 합니다.
# MAGIC 
# MAGIC ## Process Remaining Records
# MAGIC The following cell will land additional files for the rest of 2020 in your source directory. You'll be able to see these process through the first 3 tables in your Delta Lake, but will need to re-run your final query to update your **`daily_patient_avg`** table, since this query uses the trigger available now syntax.

# COMMAND ----------

DA.data_factory.load(continuous=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC 마지막으로 모든 스트림이 중지되었는지 확인합니다.
# MAGIC 
# MAGIC Finally, make sure all streams are stopped.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 요약
# MAGIC 
# MAGIC Delta Lake와 Structured Streaming이 결합되어 레이크 하우스의 데이터에 대한 거의 실시간 분석 액세스를 제공합니다.
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC Delta Lake and Structured Streaming combine to provide near real-time analytic access to data in the lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">Table Streaming Reads and Writes</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Create a Kafka Source Stream</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>