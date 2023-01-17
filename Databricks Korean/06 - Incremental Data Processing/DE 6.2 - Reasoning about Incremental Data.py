# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 증분 데이터에 대한 추론
# MAGIC 
# MAGIC Spark Structured Streaming은 Apache Spark의 기능을 확장하여 증분 데이터 세트를 처리할 때 구성과 부기를 단순화할 수 있도록 합니다. 과거에는 빅 데이터를 사용한 스트리밍에 대한 강조의 상당 부분이 거의 실시간 분석 통찰력을 제공하기 위해 대기 시간을 줄이는 데 초점을 맞추었다. Structured Streaming은 이러한 목표를 달성하는 데 있어 탁월한 성능을 제공하지만, 이 과정에서는 증분 데이터 처리의 응용 프로그램에 더 중점을 둡니다.
# MAGIC 
# MAGIC 데이터 레이크하우스에서 성공적으로 작업하기 위해 증분 처리가 반드시 필요한 것은 아니지만, 세계 최대 기업 중 일부가 세계 최대 데이터셋에서 통찰력을 얻을 수 있도록 지원한 경험을 통해 많은 워크로드가 증분 처리 접근 방식을 통해 상당한 이점을 얻을 수 있다는 결론을 도출했습니다. Databricks의 핵심 기능 중 많은 부분은 계속 증가하는 이러한 데이터 세트를 처리하기 위해 특별히 최적화되었습니다.
# MAGIC 
# MAGIC 다음과 같은 데이터 세트 및 사용 사례를 고려해 보십시오:
# MAGIC * 데이터 과학자는 운영 데이터베이스에서 자주 업데이트되는 레코드에 대한 안전하고 식별되지 않은 버전의 액세스가 필요합니다
# MAGIC * 신용 카드 거래를 과거 고객의 행동과 비교하여 부정 행위를 식별하고 표시해야 함
# MAGIC * 다국적 소매업체가 구매 내역을 사용하여 맞춤형 제품 권장 사항을 제공하려고 합니다
# MAGIC * 분산 시스템의 로그 파일을 분석하여 불안정성을 감지하고 대응해야 함
# MAGIC * 수백만 명의 온라인 쇼핑객이 제공하는 클릭스트림 데이터를 UX의 A/B 테스트에 활용해야 함
# MAGIC 
# MAGIC 위의 내용은 시간이 지남에 따라 점진적으로 무한히 증가하는 데이터 세트의 작은 샘플에 불과하다.
# MAGIC 
# MAGIC 이 과정에서는 Spark Structured Streaming을 사용하여 데이터의 증분 처리를 허용하는 기본 사항에 대해 알아보겠습니다. 다음 단원에서는 이 증분 처리 모델이 데이터 레이크 하우스에서 데이터 처리를 간소화하는 방법에 대해 자세히 설명합니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * Spark Structured Streaming에서 사용하는 프로그래밍 모델 설명
# MAGIC * 소스에서 스트리밍 읽기를 수행하는 데 필요한 옵션 구성
# MAGIC * 엔드 투 엔드 내결함성에 대한 요구 사항 설명
# MAGIC * 싱크에 스트리밍 쓰기를 수행하는 데 필요한 옵션 구성
# MAGIC 
# MAGIC # Reasoning about Incremental Data
# MAGIC 
# MAGIC Spark Structured Streaming extends the functionality of Apache Spark to allow for simplified configuration and bookkeeping when processing incremental datasets. In the past, much of the emphasis for streaming with big data has focused on reducing latency to provide near real time analytic insights. While Structured Streaming provides exceptional performance in achieving these goals, this lesson will focus more on the applications of incremental data processing.
# MAGIC 
# MAGIC While incremental processing is not absolutely necessary to work successfully in the data lakehouse, our experience helping some of the world's largest companies derive insights from the world's largest datasets has led to the conclusion that many workloads can benefit substantially from an incremental processing approach. Many of the core features at the heart of Databricks have been optimized specifically to handle these ever-growing datasets.
# MAGIC 
# MAGIC Consider the following datasets and use cases:
# MAGIC * Data scientists need secure, de-identified, versioned access to frequently updated records in an operational database
# MAGIC * Credit card transactions need to be compared to past customer behavior to identify and flag fraud
# MAGIC * A multi-national retailer seeks to serve custom product recommendations using purchase history
# MAGIC * Log files from distributed systems need to be analayzed to detect and respond to instabilities
# MAGIC * Clickstream data from millions of online shoppers needs to be leveraged for A/B testing of UX
# MAGIC 
# MAGIC The above are just a small sample of datasets that grow incrementally and infinitely over time.
# MAGIC 
# MAGIC In this lesson, we'll explore the basics of working with Spark Structured Streaming to allow incremental processing of data. In the next lesson, we'll talk more about how this incremental processing model simplifies data processing in the data lakehouse.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the programming model used by Spark Structured Streaming
# MAGIC * Configure required options to perform a streaming read on a source
# MAGIC * Describe the requirements for end-to-end fault tolerance
# MAGIC * Configure required options to perform a streaming write to a sink

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC 다음 셀을 실행하여 "교실"을 구성합니다
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-6.2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 무한 확장 데이터를 테이블로 처리
# MAGIC 
# MAGIC Spark Structured Streaming 이면의 마법은 사용자들이 마치 정적인 레코드 테이블처럼 계속 증가하는 데이터 소스와 상호 작용할 수 있게 해준다는 것이다.
# MAGIC 
# MAGIC ## Treating Infinite Data as a Table
# MAGIC 
# MAGIC The magic behind Spark Structured Streaming is that it allows users to interact with ever-growing data sources as if they were just a static table of records.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png" width="800"/>
# MAGIC 
# MAGIC 위 그림에서 **데이터 스트림**은 시간이 지남에 따라 증가하는 모든 데이터 소스를 설명합니다. 데이터 스트림의 새 데이터는 다음과 같습니다:
# MAGIC * 새로운 JSON 로그 파일이 클라우드 스토리지에 제공됨
# MAGIC * CDC 피드에 캡처된 데이터베이스 업데이트
# MAGIC * 주점/하위 메시징 피드에 대기 중인 이벤트
# MAGIC * CSV 판매 파일이 전날 마감되었습니다
# MAGIC 
# MAGIC 많은 조직은 전통적으로 결과를 업데이트할 때마다 전체 소스 데이터 세트를 재처리하는 방식을 취해 왔습니다. 또 다른 방법은 업데이트가 마지막으로 실행된 이후 추가된 파일이나 레코드만 캡처하도록 사용자 지정 논리를 작성하는 것입니다.
# MAGIC 
# MAGIC Structured Streaming을 사용하면 데이터 소스에 대한 쿼리를 정의하고 새 레코드를 자동으로 감지하여 이전에 정의된 논리를 통해 전파할 수 있습니다. 
# MAGIC 
# MAGIC **Spark Structured Streaming은 Datbricks에 최적화되어 Delta Lake 및 Auto Loader와 긴밀하게 통합됩니다.**
# MAGIC 
# MAGIC In the graphic above, a **data stream** describes any data source that grows over time. New data in a data stream might correspond to:
# MAGIC * A new JSON log file landing in cloud storage
# MAGIC * Updates to a database captured in a CDC feed
# MAGIC * Events queued in a pub/sub messaging feed
# MAGIC * A CSV file of sales closed the previous day
# MAGIC 
# MAGIC Many organizations have traditionally taken an approach of reprocessing the entire source dataset each time they want to update their results. Another approach would be to write custom logic to only capture those files or records that have been added since the last time an update was run.
# MAGIC 
# MAGIC Structured Streaming lets us define a query against the data source and automatically detect new records and propagate them through previously defined logic. 
# MAGIC 
# MAGIC **Spark Structured Streaming is optimized on Databricks to integrate closely with Delta Lake and Auto Loader.**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 기본 개념
# MAGIC 
# MAGIC - 개발자는 **소스**에 대해 스트리밍 읽기를 구성하여 **입력 테이블**을 정의합니다. 이 작업을 수행하기 위한 구문은 정적 데이터로 작업하는 것과 비슷합니다.
# MAGIC - **query**는 입력 테이블에 대해 정의됩니다. DataFrames API와 Spark SQL을 사용하여 입력 테이블에 대한 변환 및 작업을 쉽게 정의할 수 있습니다.
# MAGIC - 입력 테이블의 이 논리적 쿼리는 **결과 테이블**을 생성합니다. 결과 테이블에는 스트림의 증분 상태 정보가 포함됩니다.
# MAGIC - 스트리밍 파이프라인의 **출력*은 외부 **싱크**에 기록하여 결과 테이블에 대한 업데이트를 유지합니다. 일반적으로 싱크대는 파일이나 펍/서브 메시징 버스와 같은 내구성 있는 시스템입니다.
# MAGIC - 각 **트리거 간격**에 대해 새 행이 입력 테이블에 추가됩니다. 이러한 새 행은 기본적으로 마이크로 배치 트랜잭션과 유사하며 결과 테이블을 통해 싱크로 자동 전파됩니다.
# MAGIC 
# MAGIC ## Basic Concepts
# MAGIC 
# MAGIC - The developer defines an **input table** by configuring a streaming read against a **source**. The syntax for doing this is similar to working with static data.
# MAGIC - A **query** is defined against the input table. Both the DataFrames API and Spark SQL can be used to easily define transformations and actions against the input table.
# MAGIC - This logical query on the input table generates the **results table**. The results table contains the incremental state information of the stream.
# MAGIC - The **output** of a streaming pipeline will persist updates to the results table by writing to an external **sink**. Generally, a sink will be a durable system such as files or a pub/sub messaging bus.
# MAGIC - New rows are appended to the input table for each **trigger interval**. These new rows are essentially analogous to micro-batch transactions and will be automatically propagated through the results table to the sink.
# MAGIC 
# MAGIC <img src="http://spark.apache.org/docs/latest/img/structured-streaming-model.png" width="800"/>
# MAGIC 
# MAGIC 자세한 내용은 <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts" target="_blank">구조화 스트리밍 프로그래밍 가이드</a> 의 유사 섹션을 참조하십시오 (여러 이미지를 차용한 것).
# MAGIC 
# MAGIC For more information, see the analogous section in the <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts" target="_blank">Structured Streaming Programming Guide</a> (from which several images have been borrowed).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 엔드 투 엔드 Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming은 _checkpointing_(아래 설명) 및 <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">미리 로그 쓰기</a>를 통해 엔드 투 엔드로 정확하게 한 번의 장애 복구를 보장합니다.
# MAGIC 
# MAGIC 구조화된 스트리밍 소스, 싱크 및 기본 실행 엔진이 함께 작동하여 스트림 처리의 진행률을 추적합니다. 오류가 발생하면 스트리밍 엔진이 데이터를 다시 시작하거나 재처리하려고 시도합니다.
# MAGIC 실패한 스트리밍 쿼리에서 복구하는 모범 사례는 <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-from-query-failures" target="_blank">docs</a>를 참조하십시오.
# MAGIC 
# MAGIC 이 접근 방식은 스트리밍 소스가 재생 가능한 경우에만 작동합니다. 재생 가능한 소스에는 클라우드 기반 객체 스토리지와 pub/sub 메시징 서비스가 포함됩니다.
# MAGIC 
# MAGIC 높은 수준에서 기본 스트리밍 메커니즘은 다음과 같은 몇 가지 접근 방식에 의존합니다:
# MAGIC 
# MAGIC * 첫째, Structured Streaming은 체크포인트 및 미리 쓰기 로그를 사용하여 각 트리거 간격 동안 처리 중인 데이터의 오프셋 범위를 기록합니다.
# MAGIC * 다음으로, 스트리밍 싱크는 _idempotent_가 되도록 설계된다. 즉, (오프셋에 의해 식별된 것처럼) 동일한 데이터의 다중 쓰기는 _not_를 싱크에 쓰는 결과를 낳는다.
# MAGIC 
# MAGIC Structured Streaming은 재생 가능한 데이터 소스와 idempotent Sink를 함께 사용하여 어떠한 장애 조건에서도 *엔드 투 엔드, 정확하게 한 번의 의미론*을 보장할 수 있습니다.
# MAGIC   
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ (discussed below) and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="https://docs.databricks.com/spark/latest/structured-streaming/production.html#recover-from-query-failures" target="_blank">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable; replayable sources include cloud-based object storage and pub/sub messaging services.
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple of approaches:
# MAGIC 
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_ - that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 스트림 읽기
# MAGIC 
# MAGIC **`spark.readStream()`** 메서드는 스트림을 구성하고 쿼리하는 데 사용되는 **`DataStreamReader`** 를 반환합니다.
# MAGIC 
# MAGIC 이전 레슨에서는 자동 로더를 사용하여 증분 읽기용으로 구성된 코드를 보았습니다. 여기서는 델타 레이크 표를 증분으로 읽는 것이 얼마나 쉬운지 보여드리겠습니다.
# MAGIC 
# MAGIC 이 코드는 PySpark API를 사용하여 **`bronze`** 라는 델타 레이크 테이블을 점진적으로 읽고 **`streaming_tmp_vw`** 라는 스트리밍 온도 뷰를 등록한다.
# MAGIC 
# MAGIC **참고**: 증분 읽기를 구성할 때 여러 가지 선택적 구성(여기에 표시되지 않음)을 설정할 수 있으며, 그 중 가장 중요한 구성은 <a href="https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate" target="_blank"> 입력 속도를 제한할 수 있습니다.</a>.
# MAGIC 
# MAGIC ## Reading a Stream
# MAGIC 
# MAGIC The **`spark.readStream()`** method returns a **`DataStreamReader`** used to configure and query the stream.
# MAGIC 
# MAGIC In the previous lesson, we saw code configured for incrementally reading with Auto Loader. Here, we'll show how easy it is to incrementally read a Delta Lake table.
# MAGIC 
# MAGIC The code uses the PySpark API to incrementally read a Delta Lake table named **`bronze`** and register a streaming temp view named **`streaming_tmp_vw`**.
# MAGIC 
# MAGIC **NOTE**: A number of optional configurations (not shown here) can be set when configuring incremental reads, the most important of which allows you to <a href="https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate" target="_blank">limit the input rate</a>.

# COMMAND ----------

(spark.readStream
    .table("bronze")
    .createOrReplaceTempView("streaming_tmp_vw"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 스트리밍 임시 보기에서 쿼리를 실행할 때 새 데이터가 소스에 도착할 때 쿼리 결과를 계속 업데이트합니다.
# MAGIC 
# MAGIC 스트리밍 임시 보기에 대해 실행된 쿼리를 **항상 켜짐 증분 쿼리**로 생각해 보십시오.
# MAGIC 
# MAGIC **참고**: 일반적으로 개발 또는 라이브 대시보드 중에 사용자가 쿼리 출력을 적극적으로 모니터링하지 않는 한 스트리밍 결과를 노트북으로 반환하지 않습니다.
# MAGIC 
# MAGIC When we execute a query on a streaming temporary view, we'll continue to update the results of the query as new data arrives in the source.
# MAGIC 
# MAGIC Think of a query executed against a streaming temp view as an **always-on incremental query**.
# MAGIC 
# MAGIC **NOTE**: Generally speaking, unless a human is actively monitoring the output of a query during development or live dashboarding, we won't return streaming results to a notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 데이터가 이전 강의에서 작성한 델타 표와 동일하다는 것을 알게 될 것입니다.
# MAGIC 
# MAGIC 계속하기 전에 노트북 맨 위에 있는 **`Stop Execution`** 를 클릭하거나 셀 바로 아래에 있는 **`Cancel`** 를 클릭하거나 다음 셀을 실행하여 모든 활성 스트리밍 쿼리를 중지합니다.
# MAGIC 
# MAGIC You will recognize the data as being the same as the Delta table written out in our previous lesson.
# MAGIC 
# MAGIC Before continuing, click **`Stop Execution`** at the top of the notebook, **`Cancel`** immediately under the cell, or run the following cell to stop all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 스트리밍 데이터 작업
# MAGIC 정적 데이터와 동일한 방식으로 스트리밍 온도 뷰에 대해 대부분의 변환을 실행할 수 있습니다. 여기서는 각 **`device_id`** 에 대한 레코드 수를 얻기 위해 간단한 집계를 실행할 것이다.
# MAGIC 
# MAGIC 스트리밍 임시 뷰를 쿼리하기 때문에, 이것은 단일 결과 집합을 검색한 후 완료하는 것이 아니라 무제한으로 실행되는 스트리밍 쿼리가 된다. 이와 같은 스트리밍 쿼리의 경우 Datbricks 노트북에는 사용자가 스트리밍 성능을 모니터링할 수 있는 대화형 대시보드가 포함되어 있습니다. 아래에서 이 정보를 살펴보십시오.
# MAGIC 
# MAGIC 이 예제와 관련된 한 가지 중요한 참고 사항은 스트림에서 볼 수 있는 입력의 집합을 표시하는 것입니다. **이 기록은 현재 어디에서도 지속되지 않고 있습니다.**
# MAGIC 
# MAGIC ## Working with Streaming Data
# MAGIC We can execute most transformation against streaming temp views the same way we would with static data. Here, we'll run a simple aggregation to get counts of records for each **`device_id`**.
# MAGIC 
# MAGIC Because we are querying a streaming temp view, this becomes a streaming query that executes indefinitely, rather than completing after retrieving a single set of results. For streaming queries like this, Databricks Notebooks include interactive dashboards that allow users to monitor streaming performance. Explore this below.
# MAGIC 
# MAGIC One important note regarding this example: this is merely displaying an aggregation of input as seen by the stream. **None of these records are being persisted anywhere at this point.**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT device_id, count(device_id) AS total_recordings
# MAGIC FROM streaming_tmp_vw
# MAGIC GROUP BY device_id

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 계속하기 전에 노트북 맨 위에 있는 **`Stop Execution`** 를 클릭하거나 셀 바로 아래에 있는 **`Cancel`** 를 클릭하거나 다음 셀을 실행하여 모든 활성 스트리밍 쿼리를 중지합니다.
# MAGIC 
# MAGIC Before continuing, click **`Stop Execution`** at the top of the notebook, **`Cancel`** immediately under the cell, or run the following cell to stop all active streaming queries.

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 지원되지 않는 작업
# MAGIC 
# MAGIC 스트리밍 데이터 프레임에서의 대부분의 동작은 정적 데이터 프레임과 동일하다. 여기에는 <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank"> 몇 가지 예외가 있습니다</a>.
# MAGIC 
# MAGIC 데이터 모형을 지속적으로 추가되는 표로 간주합니다. 정렬은 스트리밍 데이터로 작업할 때 너무 복잡하거나 논리적으로 수행할 수 없는 몇 안 되는 작업 중 하나입니다.
# MAGIC 
# MAGIC 이러한 예외에 대한 전체적인 논의는 본 코스의 범위를 벗어납니다. 윈도우 설정 및 워터마킹과 같은 고급 스트리밍 방법을 사용하여 증분 워크로드에 추가 기능을 추가할 수 있습니다.
# MAGIC 
# MAGIC 이 오류가 나타나는 방법을 설명하고 다음 셀을 실행합니다:
# MAGIC   
# MAGIC ## Unsupported Operations
# MAGIC 
# MAGIC Most operations on a streaming DataFrame are identical to a static DataFrame. There are <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations" target="_blank">some exceptions to this</a>.
# MAGIC 
# MAGIC Consider the model of the data as a constantly appending table. Sorting is one of a handful of operations that is either too complex or logically not possible to do when working with streaming data.
# MAGIC 
# MAGIC A full discussion of these exceptions is out of scope for this course. Note that advanced streaming methods like windowing and watermarking can be used to add additional functionality to incremental workloads.
# MAGIC 
# MAGIC Uncomment and run the following cell how this failure may appear:

# COMMAND ----------

# %sql
# SELECT * 
# FROM streaming_tmp_vw
# ORDER BY time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 지속적인 스트리밍 결과
# MAGIC 
# MAGIC 증분 결과를 지속하려면 PySpark Structured Streaming DataFrames API로 논리를 다시 전달해야 합니다.
# MAGIC 
# MAGIC 위에서, 우리는 PySpark 스트리밍 데이터 프레임에서 임시 뷰를 만들었다. 스트리밍 임시 보기에 대한 쿼리 결과에서 다른 임시 보기를 만들면 다시 스트리밍 임시 보기가 표시됩니다.
# MAGIC 
# MAGIC ## Persisting Streaming Results
# MAGIC 
# MAGIC In order to persist incremental results, we need to pass our logic back to the PySpark Structured Streaming DataFrames API.
# MAGIC 
# MAGIC Above, we created a temp view from a PySpark streaming DataFrame. If we create another temp view from the results of a query against a streaming temp view, we'll again have a streaming temp view.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW device_counts_tmp_vw AS (
# MAGIC   SELECT device_id, COUNT(device_id) AS total_recordings
# MAGIC   FROM streaming_tmp_vw
# MAGIC   GROUP BY device_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 스트림 쓰기
# MAGIC 
# MAGIC 스트리밍 쿼리의 결과를 유지하려면 내구성이 뛰어난 스토리지에 기록해야 합니다. **`DataFrame.writeStream`** 메서드는 출력을 구성하는 데 사용되는  **`DataStreamWriter`** 를 반환합니다.
# MAGIC 
# MAGIC Delta Lake 테이블에 쓸 때 일반적으로 여기서 논의하는 3가지 설정에 대해서만 걱정하면 됩니다.
# MAGIC 
# MAGIC ### 체크포인트
# MAGIC 
# MAGIC 데이터브릭은 스트리밍 작업의 현재 상태를 클라우드 저장소에 저장하여 체크포인트를 생성합니다.
# MAGIC 
# MAGIC 체크포인트는 종료된 스트림을 다시 시작하고 중단된 위치에서 계속할 수 있도록 미리 쓰기 로그와 결합됩니다.
# MAGIC 
# MAGIC 별도의 스트림 간에 체크포인트를 공유할 수 없습니다. 모든 스트리밍 쓰기에 대해 처리 보증을 보장하기 위해 체크포인트가 필요합니다.
# MAGIC 
# MAGIC ### 출력 모드
# MAGIC 
# MAGIC 스트리밍 작업의 출력 모드는 정적/배치 워크로드와 유사합니다. <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes" target="_blank">자세한 내용은 여기를 참조하십시오.
# MAGIC 
# MAGIC ## Writing a Stream
# MAGIC 
# MAGIC To persist the results of a streaming query, we must write them out to durable storage. The **`DataFrame.writeStream`** method returns a **`DataStreamWriter`** used to configure the output.
# MAGIC 
# MAGIC When writing to Delta Lake tables, we typically will only need to worry about 3 settings, discussed here.
# MAGIC 
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC Databricks creates checkpoints by storing the current state of your streaming job to cloud storage.
# MAGIC 
# MAGIC Checkpointing combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off.
# MAGIC 
# MAGIC Checkpoints cannot be shared between separate streams. A checkpoint is required for every streaming write to ensure processing guarantees.
# MAGIC 
# MAGIC ### Output Modes
# MAGIC 
# MAGIC Streaming jobs have output modes similar to static/batch workloads. <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes" target="_blank">More details here</a>.
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- | --- |
# MAGIC | **Append** | **`.outputMode("append")`**     | **This is the default.** Only newly appended rows are incrementally appended to the target table with each batch |
# MAGIC | **Complete** | **`.outputMode("complete")`** | The Results Table is recalculated each time a write is triggered; the target table is overwritten with each batch |
# MAGIC 
# MAGIC ### 트리거 간격
# MAGIC 
# MAGIC 스트리밍 쓰기를 정의할 때 **`trigger`** 메서드는 시스템이 다음 데이터 세트를 처리해야 하는 시기를 지정합니다..
# MAGIC   
# MAGIC ### Trigger Intervals
# MAGIC 
# MAGIC When defining a streaming write, the **`trigger`** method specifies when the system should process the next set of data..
# MAGIC 
# MAGIC 
# MAGIC | Trigger Type                           | Example | Behavior |
# MAGIC |----------------------------------------|----------|----------|
# MAGIC | Unspecified                 |  | **This is the default.** This is equivalent to using **`processingTime="500ms"`** |
# MAGIC | Fixed interval micro-batches      | **`.trigger(processingTime="2 minutes")`** | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | Triggered micro-batch               | **`.trigger(once=True)`** | The query will execute a single micro-batch to process all the available data and then stop on its own |
# MAGIC | Triggered micro-batches       | **`.trigger(availableNow=True)`** | The query will execute multiple micro-batches to process all the available data and then stop on its own |
# MAGIC 
# MAGIC 트리거는 싱크에 데이터를 기록하는 방법을 정의하고 마이크로 배치 빈도를 제어할 때 지정됩니다. 기본적으로 스파크는 마지막 트리거 이후 추가된 소스의 모든 데이터를 자동으로 감지하고 처리합니다.
# MAGIC 
# MAGIC **참고:** **`Trigger.AvailableNow`** 는 DBR 10.1에서만 스칼라를 사용할 수 있고 DBR 10.2 이상에서는 파이썬 및 스칼라를 사용할 수 있는 새로운 트리거 유형입니다.
# MAGIC 
# MAGIC Triggers are specified when defining how data will be written to a sink and control the frequency of micro-batches. By default, Spark will automatically detect and process all data in the source that has been added since the last trigger.
# MAGIC 
# MAGIC **NOTE:** **`Trigger.AvailableNow`**</a> is a new trigger type that is available in DBR 10.1 for Scala only and available in DBR 10.2 and above for Python and Scala.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 모든 것을 함께 끌어모으기
# MAGIC 
# MAGIC 아래 코드는 **`spark.table()`** 을 사용하여 스트리밍 온도 뷰에서 데이터를 데이터 프레임으로 다시 로드하는 방법을 보여줍니다. 스파크는 항상 스트리밍 데이터 프레임으로 스트리밍 뷰를 로드하고 정적 뷰는 정적 데이터 프레임으로 로드합니다(증분 쓰기를 지원하려면 증분 처리를 읽기 논리로 정의해야 함).
# MAGIC 
# MAGIC 이 첫 번째 쿼리에서는 **`trigger(availableNow=True)`** 증분 배치 처리를 수행합니다.
# MAGIC 
# MAGIC ## Pulling It All Together
# MAGIC 
# MAGIC The code below demonstrates using **`spark.table()`** to load data from a streaming temp view back to a DataFrame. Note that Spark will always load streaming views as a streaming DataFrame and static views as static DataFrames (meaning that incremental processing must be defined with read logic to support incremental writing).
# MAGIC 
# MAGIC In this first query, we'll demonstrate using **`trigger(availableNow=True)`** to perform incremental batch processing.

# COMMAND ----------

(spark.table("device_counts_tmp_vw")                               
    .writeStream                                                
    .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
    .outputMode("complete")
    .trigger(availableNow=True)
    .table("device_counts")
    .awaitTermination() # This optional method blocks execution of the next cell until the incremental batch write has succeeded
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래에서는 트리거 방법을 변경하여 이 쿼리를 트리거된 증분 배치에서 4초마다 트리거된 항상 켜져 있는 쿼리로 변경합니다.
# MAGIC 
# MAGIC **참고**: 이 쿼리를 시작할 때 원본 테이블에 새 레코드가 없습니다. 곧 새로운 데이터를 추가하겠습니다.
# MAGIC 
# MAGIC Below, we change our trigger method to change this query from a triggered incremental batch to an always-on query triggered every 4 seconds.
# MAGIC 
# MAGIC **NOTE**: As we start this query, no new records exist in our source table. We'll add new data shortly.

# COMMAND ----------

query = (spark.table("device_counts_tmp_vw")                               
              .writeStream                                                
              .option("checkpointLocation", f"{DA.paths.checkpoints}/silver")
              .outputMode("complete")
              .trigger(processingTime='4 seconds')
              .table("device_counts"))

# Like before, wait until our stream has processed some data
DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 출력 쿼리
# MAGIC 이제 SQL에서 작성한 출력을 쿼리해 보겠습니다. 결과가 표이기 때문에 결과를 반환하려면 데이터를 역직렬화하기만 하면 됩니다.
# MAGIC 
# MAGIC 이제 테이블(스트리밍 데이터 프레임이 아님)을 쿼리하고 있으므로 다음은 **스트리밍 쿼리가 아닙니다**.
# MAGIC 
# MAGIC ## Querying the Output
# MAGIC Now let's query the output we've written from SQL. Because the result is a table, we only need to deserialize the data to return the results.
# MAGIC 
# MAGIC Because we are now querying a table (not a streaming DataFrame), the following will **not** be a streaming query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Land New Data
# MAGIC 
# MAGIC 이전 레슨에서와 마찬가지로 새로운 레코드를 소스 테이블로 처리하는 도우미 기능을 구성했습니다.
# MAGIC 
# MAGIC 아래 셀을 실행하여 다른 데이터 배치를 배치합니다.
# MAGIC 
# MAGIC ## Land New Data
# MAGIC 
# MAGIC As in our previous lesson, we have configured a helper function to process new records into our source table.
# MAGIC 
# MAGIC Run the cell below to land another batch of data.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 대상 테이블을 다시 쿼리하여 각 **`device_id`** 에 대한 업데이트된 카운트를 확인합니다.
# MAGIC 
# MAGIC Query the target table again to see the updated counts for each **`device_id`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM device_counts

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