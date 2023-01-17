-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # 델타 라이브 테이블용 SQL
-- MAGIC 
-- MAGIC 지난 수업에서는 이 노트북을 DLT(Delta Live Table) 파이프라인으로 예약하는 과정을 설명했습니다. 이제 Delta Live Table에서 사용하는 구문을 더 잘 이해하기 위해 이 노트북의 내용을 살펴보겠습니다.
-- MAGIC 
-- MAGIC 이 노트북은 SQL을 사용하여 데이터브릭 워크스페이스에 기본적으로 로드된 데이터브릭 제공 예제 데이터셋을 기반으로 간단한 멀티홉 아키텍처를 구현하는 델타 라이브 테이블을 선언합니다.
-- MAGIC 
-- MAGIC 간단히 말해 DLT SQL은 기존 CTAS 문을 약간 수정한 것이라고 생각할 수 있습니다. DLT 테이블과 뷰는 항상 **`LIVE`** 키워드 앞에 나옵니다.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC * 델타 라이브 테이블로 테이블 및 뷰 정의
-- MAGIC * SQL을 사용하여 자동 로더를 사용하여 원시 데이터 증분 수집
-- MAGIC * SQL을 사용하여 델타 테이블에서 증분 읽기 수행
-- MAGIC * 코드 업데이트 및 파이프라인 다시 배포
-- MAGIC 
-- MAGIC # SQL for Delta Live Tables
-- MAGIC 
-- MAGIC In the last lesson, we walked through the process of scheduling this notebook as a Delta Live Table (DLT) pipeline. Now we'll explore the contents of this notebook to better understand the syntax used by Delta Live Tables.
-- MAGIC 
-- MAGIC This notebook uses SQL to declare Delta Live Tables that together implement a simple multi-hop architecture based on a Databricks-provided example dataset loaded by default into Databricks workspaces.
-- MAGIC 
-- MAGIC At its simplest, you can think of DLT SQL as a slight modification to traditional CTAS statements. DLT tables and views will always be preceded by the **`LIVE`** keyword.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Define tables and views with Delta Live Tables
-- MAGIC * Use SQL to incrementally ingest raw data with Auto Loader
-- MAGIC * Perform incremental reads on Delta tables with SQL
-- MAGIC * Update code and redeploy a pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 청동 레이어 테이블 선언
-- MAGIC 
-- MAGIC 아래에서 우리는 청동층을 구현하는 두 개의 표를 선언한다. 이는 데이터를 가장 원시적인 형태로 나타내지만, 델타 레이크가 제공해야 하는 성능과 이점을 무한정 유지하고 쿼리할 수 있는 형식으로 캡처됩니다.
-- MAGIC 
-- MAGIC ## Declare Bronze Layer Tables
-- MAGIC 
-- MAGIC Below we declare two tables implementing the bronze layer. This represents data in its rawest form, but captured in a format that can be retained indefinitely and queried with the performance and benefits that Delta Lake has to offer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### sales_orders_raw
-- MAGIC 
-- MAGIC **`sales_orders_raw`** 는 */databricks-datasets/retail-org/sales_orders/* 에 있는 예제 데이터 세트에서 JSON 데이터를 점진적으로 수집한다.
-- MAGIC 
-- MAGIC <a herf="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a>를 통한 증분 처리(구조화 스트리밍과 동일한 처리 모델을 사용)는 아래와 같이 선언에 **`STREAMING`** 키워드를 추가해야 합니다. **`cloud_files()`** 메서드를 사용하면 SQL에서 자동 로더를 기본적으로 사용할 수 있습니다. 이 메서드는 다음 위치 매개 변수를 사용합니다:
-- MAGIC * 위에서 언급한 바와 같이 소스 위치
-- MAGIC * 원본 데이터 형식(이 경우 JSON)
-- MAGIC * 임의 크기의 선택적 판독기 옵션 배열입니다. 이 경우 **`cloudFiles.inferColumnTypes`** 를 **`true`** 로 설정합니다
-- MAGIC 
-- MAGIC 다음 선언은 또한 데이터 카탈로그를 탐색하는 모든 사용자가 볼 수 있는 추가 테이블 메타데이터(이 경우 주석 및 속성)의 선언을 보여줍니다.
-- MAGIC 
-- MAGIC ### sales_orders_raw
-- MAGIC 
-- MAGIC **`sales_orders_raw`** ingests JSON data incrementally from the example dataset found in  */databricks-datasets/retail-org/sales_orders/*.
-- MAGIC 
-- MAGIC Incremental processing via <a herf="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> (which uses the same processing model as Structured Streaming), requires the addition of the **`STREAMING`** keyword in the declaration as seen below. The **`cloud_files()`** method enables Auto Loader to be used natively with SQL. This method takes the following positional parameters:
-- MAGIC * The source location, as mentioned above
-- MAGIC * The source data format, which is JSON in this case
-- MAGIC * An arbitrarily sized array of optional reader options. In this case, we set **`cloudFiles.inferColumnTypes`** to **`true`**
-- MAGIC 
-- MAGIC The following declaration also demonstrates the declaration of additional table metadata (a comment and properties in this case) that would be visible to anyone exploring the data catalog.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### customers
-- MAGIC 
-- MAGIC **`customers`** 는 */databricks-datasets/retail-org/customers/* 에서 찾을 수 있는 CSV 고객 데이터를 제시합니다. 이 표는 곧 판매 기록을 기반으로 고객 데이터를 조회하는 공동 작업에 사용될 예정입니다.
-- MAGIC 
-- MAGIC ### customers
-- MAGIC 
-- MAGIC **`customers`** presents CSV customer data found in */databricks-datasets/retail-org/customers/*. This table will soon be used in a join operation to look up customer data based on sales records.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from /databricks-datasets."
AS SELECT * FROM cloud_files("/databricks-datasets/retail-org/customers/", "csv");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 실버 레이어 테이블 선언
-- MAGIC 
-- MAGIC 이제 실버 레이어를 구현하는 테이블을 선언합니다. 이 계층은 다운스트림 애플리케이션을 최적화하기 위해 브론즈 계층의 정제된 데이터 복사본을 나타냅니다. 이 수준에서는 데이터 정리 및 강화와 같은 작업을 적용합니다.
-- MAGIC 
-- MAGIC ## Declare Silver Layer Tables
-- MAGIC 
-- MAGIC Now we declare tables implementing the silver layer. This layer represents a refined copy of data from the bronze layer, with the intention of optimizing downstream applications. At this level we apply operations like data cleansing and enrichment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### 판매_주문_판매
-- MAGIC 
-- MAGIC 여기서 우리는 주문 번호가 null인 레코드를 거부하여 품질 관리를 구현할 뿐만 아니라 고객 정보로 판매 거래 데이터를 풍부하게 하는 첫 번째 실버 테이블을 선언합니다.
-- MAGIC 
-- MAGIC 이 선언은 많은 새로운 개념들을 소개한다.
-- MAGIC 
-- MAGIC #### 품질 관리
-- MAGIC 
-- MAGIC **`CONSTRAINT`** 키워드는 품질 관리를 도입합니다. 기존의 **`WHERE`** 조항과 기능 면에서 유사한 **`CONSTRAINT`** 는 DLT와 통합되어 제약 조건 위반에 대한 메트릭을 수집할 수 있다. 제약 조건은 제약 조건을 위반하는 레코드에 대해 수행할 작업을 지정하는 **`ON VIOLATION`** 절(선택 사항)을 제공합니다. DLT에서 현재 지원하는 세 가지 모드는 다음과 같습니다:
-- MAGIC 
-- MAGIC ### sales_orders_cleaned
-- MAGIC 
-- MAGIC Here we declare our first silver table, which enriches the sales transaction data with customer information in addition to implementing quality control by rejecting records with a null order number.
-- MAGIC 
-- MAGIC This declaration introduces a number of new concepts.
-- MAGIC 
-- MAGIC #### Quality Control
-- MAGIC 
-- MAGIC The **`CONSTRAINT`** keyword introduces quality control. Similar in function to a traditional **`WHERE`** clause, **`CONSTRAINT`** integrates with DLT, enabling it to collect metrics on constraint violations. Constraints provide an optional **`ON VIOLATION`** clause, specifying an action to take on records that violate the constraint. The three modes currently supported by DLT include:
-- MAGIC 
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`FAIL UPDATE`** | Pipeline failure when constraint is violated |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | Omitted | Records violating constraints will be included (but violations will be reported in metrics) |
-- MAGIC 
-- MAGIC #### DLT 테이블 및 뷰에 대한 참조
-- MAGIC 다른 DLT 테이블 및 뷰에 대한 참조에는 항상 **`live.`**. 접두사가 포함됩니다. 실행 시 대상 데이터베이스 이름이 자동으로 대체되어 DEV/QA/PROD 환경 간에 파이프라인을 쉽게 마이그레이션할 수 있습니다.
-- MAGIC 
-- MAGIC #### 스트리밍 테이블에 대한 참조
-- MAGIC 
-- MAGIC 스트리밍 DLT 테이블에 대한 참조는 **`STREAM()`** 를 사용하여 테이블 이름을 인수로 제공합니다.
-- MAGIC 
-- MAGIC #### References to DLT Tables and Views
-- MAGIC References to other DLT tables and views will always include the **`live.`** prefix. A target database name will automatically be substituted at runtime, allowing for easily migration of pipelines between DEV/QA/PROD environments.
-- MAGIC 
-- MAGIC #### References to Streaming Tables
-- MAGIC 
-- MAGIC References to streaming DLT tables use the **`STREAM()`**, supplying the table name as an argument.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
AS
  SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
         timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
         date(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
         f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Gold Table 선언
-- MAGIC 
-- MAGIC 아키텍처의 가장 정교한 수준에서, 우리는 비즈니스 가치가 있는 집계, 이 경우 특정 지역을 기반으로 한 판매 주문 데이터 모음을 제공하는 표를 선언한다. 보고서는 집계 시 날짜 및 고객별 주문 수 및 총계를 생성합니다.
-- MAGIC 
-- MAGIC ## Declare Gold Table
-- MAGIC 
-- MAGIC At the most refined level of the architecture, we declare a table delivering an aggregation with business value, in this case a collection of sales order data based in a specific region. In aggregating, the report generates counts and totals of orders by date and customer.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Los Angeles')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 결과 탐색
-- MAGIC 
-- MAGIC 파이프라인에 관련된 엔티티와 이들 간의 관계를 나타내는 DAG(Directed Acyclic Graph)를 탐색합니다. 다음을 포함하는 요약을 보려면 각 항목을 클릭하십시오:
-- MAGIC * 실행 상태
-- MAGIC * 메타데이터 요약
-- MAGIC * 스키마
-- MAGIC * 데이터 품질 메트릭
-- MAGIC 
-- MAGIC 표와 로그를 검사하려면 이 <a href="$/DE 8.3 - Pipeline Results" target="_blank"> 노트북 </a>를 참조하십시오.
-- MAGIC 
-- MAGIC ## Explore Results
-- MAGIC 
-- MAGIC Explore the DAG (Directed Acyclic Graph) representing the entities involved in the pipeline and the relationships between them. Click on each to view a summary, which includes:
-- MAGIC * Run status
-- MAGIC * Metadata summary
-- MAGIC * Schema
-- MAGIC * Data quality metrics
-- MAGIC 
-- MAGIC Refer to this <a href="$./DE 8.3 - Pipeline Results" target="_blank">companion notebook</a> to inspect tables and logs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 파이프라인 업데이트
-- MAGIC 
-- MAGIC 다음 셀의 주석을 제거하여 다른 골드 테이블을 선언합니다. 이전 골드 테이블 선언과 마찬가지로, 이 필터는 시카고의 **`city`**  를 필터링합니다. 
-- MAGIC 
-- MAGIC 파이프라인을 다시 실행하여 업데이트된 결과를 검사합니다. 
-- MAGIC 
-- MAGIC 예상대로 작동하나요? 
-- MAGIC 
-- MAGIC 어떤 문제라도 식별할 수 있습니까?
-- MAGIC 
-- MAGIC ## Update Pipeline
-- MAGIC 
-- MAGIC Uncomment the following cell to declare another gold table. Similar to the previous gold table declaration, this filters for the **`city`** of Chicago. 
-- MAGIC 
-- MAGIC Re-run your pipeline to examine the updated results. 
-- MAGIC 
-- MAGIC Does it run as expected? 
-- MAGIC 
-- MAGIC Can you identify any issues?

-- COMMAND ----------

-- TODO
-- CREATE OR REFRESH LIVE TABLE sales_order_in_chicago
-- COMMENT "Sales orders in Chicago."
-- AS
--   SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
--          sum(ordered_products_explode.price) as sales, 
--          sum(ordered_products_explode.qty) as quantity, 
--          count(ordered_products_explode.id) as product_count
--   FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
--         FROM sales_orders_cleaned 
--         WHERE city = 'Chicago')
--   GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>