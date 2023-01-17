-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # 고급 SQL 변환
-- MAGIC 
-- MAGIC Spark SQL을 사용하여 데이터 레이크하우스에 저장된 표 형식 데이터를 쉽고 효율적이며 빠르게 쿼리할 수 있습니다.
-- MAGIC 
-- MAGIC 데이터 구조가 규칙적이지 않게 되거나, 단일 쿼리에서 많은 테이블을 사용해야 하거나, 데이터의 모양을 크게 변경해야 할 때 이 문제는 더욱 복잡해집니다. 이 노트북은 Spark SQL에 있는 여러 기능을 소개하여 엔지니어가 가장 복잡한 변환도 완료할 수 있도록 도와줍니다.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC - `.` 및 `:` 구문을 사용하여 중첩된 데이터를 쿼리합니다
-- MAGIC - JSON 작업
-- MAGIC - arrays 및 structs의 플랫화 및 언팩
-- MAGIC - 조인 및 설정 연산자를 사용하여 데이터 세트 결합
-- MAGIC - 피벗 테이블을 사용하여 데이터 모양 바꾸기
-- MAGIC - arrays 작업에 고차 함수 사용
-- MAGIC 
-- MAGIC # Advanced SQL Transformations
-- MAGIC 
-- MAGIC Querying tabular data stored in the data lakehouse with Spark SQL is easy, efficient, and fast.
-- MAGIC 
-- MAGIC This gets more complicated as the data structure becomes less regular, when many tables need to be used in a single query, or when the shape of data needs to be changed dramatically. This notebook introduces a number of functions present in Spark SQL to help engineers complete even the most complicated transformations.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use **`.`** and **`:`** syntax to query nested data
-- MAGIC - Work with JSON
-- MAGIC - Flatten and unpacking arrays and structs
-- MAGIC - Combine datasets using joins and set operators
-- MAGIC - Reshape data using pivot tables
-- MAGIC - Use higher order functions for working with arrays

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Run Setup
-- MAGIC 설치 스크립트가 데이터를 생성하고 이 노트북의 나머지 부분을 실행하는 데 필요한 값을 선언합니다.
-- MAGIC 
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.7

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Interacting with JSON Data
-- MAGIC 
-- MAGIC **`events_raw`** 테이블이 카프카 페이로드를 나타내는 데이터에 대해 등록되었습니다.
-- MAGIC 
-- MAGIC 대부분의 경우 카프카 데이터는 이진 인코딩된 JSON 값입니다. 아래 문자열로 **`key`* 와 **`value`*를 캐스팅하여 사람이 읽을 수 있는 형식으로 이를 살펴볼 것이다.
-- MAGIC 
-- MAGIC The **`events_raw`** table was registered against data representing a Kafka payload.
-- MAGIC 
-- MAGIC In most cases, Kafka data will be binary-encoded JSON values. We'll cast the **`key`** and **`value`** as strings below to look at these in a human-readable format.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS
  SELECT string(key), string(value) 
  FROM events_raw;
  
SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 스파크 SQL에는 문자열로 저장된 JSON 데이터와 직접 상호 작용하는 기능이 내장되어 있다. 우리는 **`:`** 구문을 사용하여 중첩된 데이터 구조를 통과할 수 있다.
-- MAGIC 
-- MAGIC Spark SQL has built-in functionality to directly interact with JSON data stored as strings. We can use the **`:`** syntax to traverse nested data structures.

-- COMMAND ----------

SELECT value:device, value:geo:city 
FROM events_strings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 스파크 SQL은 또한 JSON 객체를 구조 유형(네스트된 속성을 가진 네이티브 스파크 유형)으로 구문 분석하는 기능을 가지고 있다.
-- MAGIC 
-- MAGIC 그러나 **`from_json`** 함수에는 스키마가 필요합니다. 현재 데이터의 스키마를 도출하기 위해 null 필드가 없는 JSON 값을 반환하는 쿼리를 실행하는 것으로 시작합니다.
-- MAGIC 
-- MAGIC Spark SQL also has the ability to parse JSON objects into struct types (a native Spark type with nested attributes).
-- MAGIC 
-- MAGIC However, the **`from_json`** function requires a schema. To derive the schema of our current data, we'll start by executing a query we know will return a JSON value with no null fields.

-- COMMAND ----------

SELECT value 
FROM events_strings 
WHERE value:event_name = "finalize" 
ORDER BY key
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 스파크 SQL에는 예시에서 JSON 스키마를 도출하기 위한 **`schema_of_json`** 함수도 있다. 여기서는 예제 JSON을 함수에 복사하여 붙여넣고 이를 **`from_json`** 함수에 연결하여 **`value`** 필드를 구조 유형으로 캐스팅한다.
-- MAGIC 
-- MAGIC Spark SQL also has a **`schema_of_json`** function to derive the JSON schema from an example. Here, we copy and paste an example JSON to the function and chain it into the **`from_json`** function to cast our **`value`** field to a struct type.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS
  SELECT from_json(value, schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')) AS json 
  FROM events_strings;
  
SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC JSON 문자열이 구조 유형으로 압축 해제되면 스파크는 **`*`**(별) 압축 해제를 지원하여 필드를 열로 평평하게 만듭니다.
-- MAGIC 
-- MAGIC Once a JSON string is unpacked to a struct type, Spark supports **`*`** (star) unpacking to flatten fields into columns.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_events_final AS
  SELECT json.* 
  FROM parsed_events;
  
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 데이터 구조 탐색
-- MAGIC 
-- MAGIC 스파크 SQL은 복잡한 데이터 유형과 중첩된 데이터 유형으로 작업하기 위한 강력한 구문을 가지고 있다.
-- MAGIC 
-- MAGIC 먼저 **`events`** 테이블의 필드를 살펴봅니다.
-- MAGIC 
-- MAGIC ## Explore Data Structures
-- MAGIC 
-- MAGIC Spark SQL has robust syntax for working with complex and nested data types.
-- MAGIC 
-- MAGIC Start by looking at the fields in the **`events`** table.

-- COMMAND ----------

DESCRIBE events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **`ecommerce`** 필드는 더블과 2 롱을 포함하는 구조입니다.
-- MAGIC 
-- MAGIC 표준 **`.`** 를 사용하여 이 필드의 하위 필드와 상호 작용할 수 있습니다. JSON에서 중첩된 데이터를 통과하는 방법과 유사한 구문입니다.
-- MAGIC 
-- MAGIC The **`ecommerce`** field is a struct that contains a double and 2 longs.
-- MAGIC 
-- MAGIC We can interact with the subfields in this field using standard **`.`** syntax similar to how we might traverse nested data in JSON.

-- COMMAND ----------

SELECT ecommerce.purchase_revenue_in_usd 
FROM events
WHERE ecommerce.purchase_revenue_in_usd IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 배열 분해
-- MAGIC **`events`** 테이블의 **`items`** 필드는 구조의 배열입니다.
-- MAGIC 
-- MAGIC 스파크 SQL은 배열을 처리하기 위해 특별히 많은 기능을 가지고 있다.
-- MAGIC 
-- MAGIC **`explode`** 함수를 사용하면 각 요소를 해당 행의 배열에 넣을 수 있습니다.
-- MAGIC  
-- MAGIC ## Explode Arrays
-- MAGIC The **`items`** field in the **`events`** table is an array of structs.
-- MAGIC 
-- MAGIC Spark SQL has a number of functions specifically to deal with arrays.
-- MAGIC 
-- MAGIC The **`explode`** function lets us put each element in an array on its own row.

-- COMMAND ----------

SELECT user_id, event_timestamp, event_name, explode(items) AS item 
FROM events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 배열 수집
-- MAGIC 
-- MAGIC **`collect_set`** 함수는 배열 내의 필드를 포함하여 필드에 대한 고유한 값을 수집할 수 있습니다.
-- MAGIC 
-- MAGIC **`flatten`** 함수를 사용하면 여러 배열을 하나의 배열로 결합할 수 있습니다.
-- MAGIC 
-- MAGIC **`array_distinct`** 함수는 배열에서 중복 요소를 제거합니다.
-- MAGIC 
-- MAGIC 여기서는 이러한 쿼리를 결합하여 고유한 작업 모음과 사용자 카트의 항목을 보여주는 간단한 테이블을 만든다.
-- MAGIC  
-- MAGIC ## Collect Arrays
-- MAGIC 
-- MAGIC The **`collect_set`** function can collect unique values for a field, including fields within arrays.
-- MAGIC 
-- MAGIC The **`flatten`** function allows multiple arrays to be combined into a single array.
-- MAGIC 
-- MAGIC The **`array_distinct`** function removes duplicate elements from an array.
-- MAGIC 
-- MAGIC Here, we combine these queries to create a simple table that shows the unique collection of actions and the items in a user's cart.

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 테이블 조인
-- MAGIC 
-- MAGIC Spark SQL은 표준 조인 작업(내부, 외부, 왼쪽, 오른쪽, 안티, 크로스, 세미)을 지원합니다.
-- MAGIC 
-- MAGIC 여기서 우리는 표준 인쇄 항목 이름을 얻기 위해 **`explode`** 작업에 대한 조회 테이블과 결합을 연결한다.
-- MAGIC  
-- MAGIC ## Join Tables
-- MAGIC 
-- MAGIC Spark SQL supports standard join operations (inner, outer, left, right, anti, cross, semi).
-- MAGIC 
-- MAGIC Here we chain a join with a lookup table to an **`explode`** operation to grab the standard printed item name.

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_enriched AS
SELECT *
FROM (
  SELECT *, explode(items) AS item 
  FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM sales_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 연산자 설정
-- MAGIC Spark SQL은 **`UNION`**, **`MINUS`** 및 **`INTERCT`** 집합 연산자를 지원합니다.
-- MAGIC 
-- MAGIC **`UNION`** 은(는) 두 개의 쿼리 모음을 반환합니다.
-- MAGIC 
-- MAGIC 아래 쿼리는 **`new_events_final`** 을 **`events`** 테이블에 삽입한 경우와 동일한 결과를 반환합니다.
-- MAGIC 
-- MAGIC ## Set Operators
-- MAGIC Spark SQL supports **`UNION`**, **`MINUS`**, and **`INTERSECT`** set operators.
-- MAGIC 
-- MAGIC **`UNION`** returns the collection of two queries. 
-- MAGIC 
-- MAGIC The query below returns the same results as if we inserted our **`new_events_final`** into the **`events`** table.

-- COMMAND ----------

SELECT * FROM events 
UNION 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC **`INTERSECT`** 는 두 관계에서 발견된 모든 행을 반환합니다.
-- MAGIC 
-- MAGIC **`INTERSECT`** returns all rows found in both relations.

-- COMMAND ----------

SELECT * FROM events 
INTERSECT 
SELECT * FROM new_events_final

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 우리의 두 데이터 세트는 공통된 값이 없기 때문에 위의 쿼리는 결과를 반환하지 않는다.
-- MAGIC 
-- MAGIC **`MINUS`** 는 한 데이터 세트에서 발견된 모든 행을 반환하지만 다른 데이터 세트에서는 반환하지 않습니다. 이전 쿼리에서 공통 값이 없음을 보여주므로 여기서는 이 실행을 생략하겠습니다.
-- MAGIC 
-- MAGIC The above query returns no results because our two datasets have no values in common.
-- MAGIC 
-- MAGIC **`MINUS`** returns all the rows found in one dataset but not the other; we'll skip executing this here as our previous query demonstrates we have no values in common.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 피벗 테이블
-- MAGIC **`PIVOT`** 절은 데이터 관점에서 사용됩니다. 특정 열 값을 기반으로 집계된 값을 얻을 수 있으며, 이 값은 **`SELECT`** 절에서 사용되는 여러 열로 전환됩니다. **`PIVOT`** 절은 테이블 이름 또는 하위 쿼리 뒤에 지정할 수 있습니다.
-- MAGIC 
-- MAGIC **`SELECT * FROM ()`** : 괄호 안의 **`SELECT`** 문이 이 표의 입력입니다.
-- MAGIC 
-- MAGIC **`PIVOT`**: 절의 첫 번째 인수는 집계 함수와 집계할 열입니다. 그런 다음 **`FOR`** 하위 절에 피벗 열을 지정합니다. 더 **`IN`** 연산자에는 피벗 열 값이 포함되어 있습니다.
-- MAGIC 
-- MAGIC 여기서 우리는 **`PIVOT`** 를 사용하여 **`sales`** 표에 포함된 정보를 평평하게 하는 새로운 **`transactions`** 표를 작성한다.
-- MAGIC 
-- MAGIC 이 평평한 데이터 형식은 대시보드에 유용할 수 있지만 추론 또는 예측을 위한 기계 학습 알고리듬을 적용하는 데도 유용하다.
-- MAGIC 
-- MAGIC ## Pivot Tables
-- MAGIC The **`PIVOT`** clause is used for data perspective. We can get the aggregated values based on specific column values, which will be turned to multiple columns used in **`SELECT`** clause. The **`PIVOT`** clause can be specified after the table name or subquery.
-- MAGIC 
-- MAGIC **`SELECT * FROM ()`**: The **`SELECT`** statement inside the parentheses is the input for this table.
-- MAGIC 
-- MAGIC **`PIVOT`**: The first argument in the clause is an aggregate function and the column to be aggregated. Then, we specify the pivot column in the **`FOR`** subclause. The **`IN`** operator contains the pivot column values. 
-- MAGIC 
-- MAGIC Here we use **`PIVOT`** to create a new **`transactions`** table that flattens out the information contained in the **`sales`** table.
-- MAGIC 
-- MAGIC This flattened data format can be useful for dashboarding, but also useful for applying machine learning algorithms for inference or prediction.

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    email,
    order_id,
    transaction_timestamp,
    total_item_quantity,
    purchase_revenue_in_usd,
    unique_items,
    item.item_id AS item_id,
    item.quantity AS quantity
  FROM sales_enriched
) PIVOT (
  sum(quantity) FOR item_id in (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K'
  )
);

SELECT * FROM transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 고차 함수
-- MAGIC 스파크 SQL의 고차 함수를 사용하면 복잡한 데이터 유형을 직접 작업할 수 있습니다. 계층 데이터로 작업할 때 레코드는 배열 또는 맵 유형 개체로 저장되는 경우가 많습니다. 고차 함수를 사용하면 원래 구조를 유지하면서 데이터를 변환할 수 있습니다.
-- MAGIC 
-- MAGIC 고차 함수에는 다음이 포함됩니다:
-- MAGIC - **`FILTER`** 는 지정된 람다 함수를 사용하여 배열을 필터링합니다.
-- MAGIC - **`EXIST`** 는 배열에서 하나 이상의 요소에 대한 문이 참인지 여부를 테스트합니다.
-- MAGIC - **`TRANSFORM`** 은 주어진 람다 함수를 사용하여 배열의 모든 요소를 변환한다.
-- MAGIC - **`REDUCE`** 는 두 개의 람다 함수를 사용하여 요소를 버퍼에 병합하여 배열의 요소를 단일 값으로 줄이고 는 최종 버퍼에 마무리 함수를 적용합니다.
-- MAGIC 
-- MAGIC 
-- MAGIC ## Higher Order Functions
-- MAGIC Higher order functions in Spark SQL allow you to work directly with complex data types. When working with hierarchical data, records are frequently stored as array or map type objects. Higher-order functions allow you to transform data while preserving the original structure.
-- MAGIC 
-- MAGIC Higher order functions include:
-- MAGIC - **`FILTER`** filters an array using the given lambda function.
-- MAGIC - **`EXIST`** tests whether a statement is true for one or more elements in an array. 
-- MAGIC - **`TRANSFORM`** uses the given lambda function to transform all elements in an array.
-- MAGIC - **`REDUCE`** takes two lambda functions to reduce the elements of an array to a single value by merging the elements into a buffer, and the apply a finishing function on the final buffer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 필터
-- MAGIC **`items`** 열에 있는 모든 레코드에서 킹사이즈가 아닌 항목을 제거하십시오. **`FILTER`** 함수를 사용하여 각 배열에서 해당 값을 제외하는 새 열을 만들 수 있습니다.
-- MAGIC 
-- MAGIC **`FILTER(항목, i -> i.item_id LIKE %K) ASKING_items`**
-- MAGIC 
-- MAGIC 위의 설명에서:
-- MAGIC - **`FILTER`** : 고차 함수의 이름 <br>
-- MAGIC - **`items`** : 입력 배열의 이름 <br>
-- MAGIC - **`i`**: iterator 변수의 이름입니다. 이 이름을 선택한 다음 람다 함수에 사용합니다. 배열을 반복하여 각 값을 한 번에 하나씩 함수에 순환시킵니다.<br>
-- MAGIC - **`->`** : 함수의 시작을 나타냅니다. <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : 이것이 함수입니다. 각 값이 대문자 K로 끝나는지 확인합니다. 이 경우 새 열인 **`king_items`** 로 필터링됩니다
-- MAGIC 
-- MAGIC ## Filter
-- MAGIC Remove items that are not king-sized from all records in our **`items`** column. We can use the **`FILTER`** function to create a new column that excludes that value from each array.
-- MAGIC 
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC 
-- MAGIC In the statement above:
-- MAGIC - **`FILTER`** : the name of the higher-order function <br>
-- MAGIC - **`items`** : the name of our input array <br>
-- MAGIC - **`i`** : the name of the iterator variable. You choose this name and then use it in the lambda function. It iterates over the array, cycling each value into the function one at a time.<br>
-- MAGIC - **`->`** :  Indicates the start of a function <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : This is the function. Each value is checked to see if it ends with the capital letter K. If it is, it gets filtered into the new column, **`king_items`**

-- COMMAND ----------

-- filter for sales of only king sized items
SELECT
  order_id,
  items,
  FILTER (items, i -> i.item_id LIKE "%K") AS king_items
FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 생성된 열에 빈 배열을 많이 생성하는 필터를 작성할 수 있습니다. 이 경우 **`WHERE`** 절을 사용하여 반환된 열에 비어 있지 않은 배열 값만 표시하는 것이 유용할 수 있습니다.
-- MAGIC 
-- MAGIC 이 예에서는 하위 쿼리(쿼리 내 쿼리)를 사용하여 이를 수행합니다. 여러 단계에서 작업을 수행하는 데 유용합니다. 이 경우에는 **`WHERE`** 절과 함께 사용할 명명된 열을 만들기 위해 이 열을 사용합니다.
-- MAGIC 
-- MAGIC You may write a filter that produces a lot of empty arrays in the created column. When that happens, it can be useful to use a **`WHERE`** clause to show only non-empty array values in the returned column. 
-- MAGIC 
-- MAGIC In this example, we accomplish that by using a subquery (a query within a query). They are useful for performing an operation in multiple steps. In this case, we're using it to create the named column that we will use with a **`WHERE`** clause.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW king_size_sales AS

SELECT order_id, king_items
FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0;
  
SELECT * FROM king_size_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 변환
-- MAGIC 내장 함수는 셀 내의 단일 단순한 데이터 유형에서 작동하도록 설계되었으며 배열 값을 처리할 수 없습니다. **`TRANSFORM`** 은 배열의 각 요소에 기존 함수를 적용하려는 경우 특히 유용합니다. 
-- MAGIC 
-- MAGIC 주문당 킹사이즈 품목의 총 매출을 계산합니다.
-- MAGIC 
-- MAGIC **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`**
-- MAGIC 
-- MAGIC 위의 문장에서 입력 배열의 각 값에 대해 항목의 수익 값을 추출하고 100을 곱한 다음 결과를 정수로 캐스팅합니다. 이전 명령과 동일한 종류의 참조를 사용하지만 반복자의 이름을 새 변수 **'k'**로 지정합니다.
-- MAGIC 
-- MAGIC ## Transform
-- MAGIC Built-in functions are designed to operate on a single, simple data type within a cell; they cannot process array values. **`TRANSFORM`** can be particularly useful when you want to apply an existing function to each element in an array. 
-- MAGIC 
-- MAGIC Compute the total revenue from king-sized items per order.
-- MAGIC 
-- MAGIC **`TRANSFORM(king_items, k -> CAST(k.item_revenue_in_usd * 100 AS INT)) AS item_revenues`**
-- MAGIC 
-- MAGIC In the statement above, for each value in the input array, we extract the item's revenue value, multiply it by 100, and cast the result to integer. Note that we're using the same kind as references as in the previous command, but we name the iterator with a new variable, **`k`**.

-- COMMAND ----------

-- get total revenue from king items per order
CREATE OR REPLACE TEMP VIEW king_item_revenues AS

SELECT
  order_id,
  king_items,
  TRANSFORM (
    king_items,
    k -> CAST(k.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM king_size_sales;

SELECT * FROM king_item_revenues


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 요약
-- MAGIC Spark SQL은 고도로 중첩된 데이터와 상호 작용하고 조작하기 위한 포괄적인 기본 기능 집합을 제공합니다.
-- MAGIC 
-- MAGIC 이 기능에 대한 일부 구문은 SQL 사용자에게 생소할 수 있지만 고차 함수와 같은 내장 함수를 활용하면 SQL 엔지니어가 매우 복잡한 데이터 구조를 처리할 때 사용자 지정 논리에 의존할 필요가 없습니다.
-- MAGIC 
-- MAGIC ## Summary
-- MAGIC Spark SQL offers a comprehensive set of native functionality for interacting with and manipulating highly nested data.
-- MAGIC 
-- MAGIC While some syntax for this functionality may be unfamiliar to SQL users, leveraging built-in functions like higher order functions can prevent SQL engineers from needing to rely on custom logic when dealing with highly complex data structures.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC  다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다.
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