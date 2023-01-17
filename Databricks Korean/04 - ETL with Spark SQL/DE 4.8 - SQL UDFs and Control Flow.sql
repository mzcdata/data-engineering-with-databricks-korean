-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # SQL UDF 및 제어 흐름
-- MAGIC 
-- MAGIC 데이터브릭은 DBR 9.1부터 SQL에 네이티브로 등록된 UDF(사용자 정의 함수)에 대한 지원을 추가하였다.
-- MAGIC 
-- MAGIC 이 기능을 통해 사용자는 SQL 논리의 사용자 지정 조합을 데이터베이스의 함수로 등록할 수 있으며, 이러한 메서드는 SQL이 Datbricks에서 실행될 수 있는 모든 곳에서 재사용할 수 있습니다. 이러한 기능은 스파크 SQL을 직접 활용하여 대규모 데이터 세트에 사용자 지정 로직을 적용할 때 스파크의 모든 최적화를 유지합니다.
-- MAGIC 
-- MAGIC 이 노트에서는 먼저 이러한 방법에 대한 간단한 소개를 한 다음 이 논리를 **`CASE`** / **`WHEN`** 절과 결합하여 재사용 가능한 사용자 정의 제어 흐름 논리를 제공하는 방법을 살펴보겠습니다.
-- MAGIC 
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
-- MAGIC * SQL UDF 정의 및 등록
-- MAGIC * SQL UDF 공유에 사용되는 보안 모델 설명
-- MAGIC * SQL 코드에서 **`CASE`** / **`WHEN`** 문 사용
-- MAGIC * SQL UDF의 **`CASE`** / **`WHEN`** 문을 사용자 지정 제어 흐름에 활용
-- MAGIC 
-- MAGIC 
-- MAGIC # SQL UDFs and Control Flow
-- MAGIC 
-- MAGIC Databricks added support for User Defined Functions (UDFs) registered natively in SQL starting in DBR 9.1.
-- MAGIC 
-- MAGIC This feature allows users to register custom combinations of SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions leverage Spark SQL directly, maintaining all of the optimizations of Spark when applying your custom logic to large datasets.
-- MAGIC 
-- MAGIC In this notebook, we'll first have a simple introduction to these methods, and then explore how this logic can be combined with **`CASE`** / **`WHEN`** clauses to provide reusable custom control flow logic.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Define and registering SQL UDFs
-- MAGIC * Describe the security model used for sharing SQL UDFs
-- MAGIC * Use **`CASE`** / **`WHEN`** statements in SQL code
-- MAGIC * Leverage **`CASE`** / **`WHEN`** statements in SQL UDFs for custom control flow

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Setup
-- MAGIC 
-- MAGIC 다음 셀을 실행하여 환경을 설정합니다.
-- MAGIC Run the following cell to setup your environment.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-4.8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## 단순 데이터 집합 만들기
-- MAGIC 이 노트북의 경우 여기에 temporary view로 등록된 다음 데이터 세트를 고려해 보겠습니다.
-- MAGIC 
-- MAGIC ## Create a Simple Dataset
-- MAGIC 
-- MAGIC For this notebook, we'll consider the following dataset, registered here as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW foods(food) AS VALUES
("beef"),
("beans"),
("potatoes"),
("bread");

SELECT * FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## SQL UDF
-- MAGIC 최소한 SQL UDF에는 함수 이름, 선택적 매개 변수, 반환할 유형 및 일부 사용자 지정 논리가 필요합니다.
-- MAGIC 
-- MAGIC 아래에서 **`yelling`** 라는 간단한 함수는 **`text`** 라는 매개 변수 하나를 취한다. 끝에 느낌표 세 개가 추가된 모든 대문자로 된 문자열을 반환합니다.
-- MAGIC 
-- MAGIC ## SQL UDFs
-- MAGIC At minimum, a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.
-- MAGIC 
-- MAGIC Below, a simple function named **`yelling`** takes one parameter named **`text`**. It returns a string that will be in all uppercase letters with three exclamation points added to the end.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION yelling(text STRING)
RETURNS STRING
RETURN concat(upper(text), "!!!")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 이 기능은 스파크 처리 엔진 내에서 컬럼의 모든 값에 병렬로 적용됩니다. SQL UDF는 Datbricks에서 실행하도록 최적화된 사용자 지정 로직을 정의하는 효율적인 방법입니다.
-- MAGIC 
-- MAGIC Note that this function is applied to all values of the column in a parallel fashion within the Spark processing engine. SQL UDFs are an efficient way to define custom logic that is optimized for execution on Databricks.

-- COMMAND ----------

SELECT yelling(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## SQL UDF의 범위 지정 및 사용 권한
-- MAGIC 
-- MAGIC SQL UDF는 실행 환경(노트북, DBSQL 쿼리 및 작업 포함) 간에 유지됩니다.
-- MAGIC 
-- MAGIC 등록된 위치와 예상 입력 및 반환되는 내용에 대한 기본 정보를 확인할 수 있는 기능을 설명할 수 있습니다.
-- MAGIC 
-- MAGIC ## Scoping and Permissions of SQL UDFs
-- MAGIC 
-- MAGIC Note that SQL UDFs will persist between execution environments (which can include notebooks, DBSQL queries, and jobs).
-- MAGIC 
-- MAGIC We can describe the function to see where it was registered and basic information about expected inputs and what is returned.

-- COMMAND ----------

DESCRIBE FUNCTION yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 확장된 설명을 통해, 우리는 훨씬 더 많은 정보를 얻을 수 있다.
-- MAGIC 
-- MAGIC 함수 설명 하단의 **`Body`** 필드에는 함수 자체에 사용되는 SQL 논리가 표시됩니다.
-- MAGIC 
-- MAGIC By describing extended, we can get even more information. 
-- MAGIC 
-- MAGIC Note that the **`Body`** field at the bottom of the function description shows the SQL logic used in the function itself.

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED yelling

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC SQL UDF는 전이 영역의 개체로 존재하며 데이터베이스, 테이블 또는 보기와 동일한 테이블 ACL에 의해 제어됩니다.
-- MAGIC 
-- MAGIC SQL UDF를 사용하려면 사용자에게 함수에 대한 **`USAGE`** 및 **`SELECT`** 권한이 있어야 합니다.
-- MAGIC 
-- MAGIC SQL UDFs exist as objects in the metastore and are governed by the same Table ACLs as databases, tables, or views.
-- MAGIC 
-- MAGIC In order to use a SQL UDF, a user must have **`USAGE`** and **`SELECT`** permissions on the function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 사례/시기
-- MAGIC 
-- MAGIC 표준 SQL 구문 구성 **`CASE`** / **`WHEN`** 을 사용하면 표 내용을 기반으로 여러 조건문을 대체 결과로 평가할 수 있다.
-- MAGIC 
-- MAGIC 다시 말하지만, 모든 것은 스파크에서 기본적으로 평가되므로 병렬 실행에 최적화된다.
-- MAGIC 
-- MAGIC ## CASE/WHEN
-- MAGIC 
-- MAGIC The standard SQL syntactic construct **`CASE`** / **`WHEN`** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.
-- MAGIC 
-- MAGIC Again, everything is evaluated natively in Spark, and so is optimized for parallel execution.

-- COMMAND ----------

SELECT *,
  CASE 
    WHEN food = "beans" THEN "I love beans"
    WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
    WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
    ELSE concat("I don't eat ", food)
  END
FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 단순 제어 흐름 기능
-- MAGIC 
-- MAGIC SQL UDF를 **`CASE`** / **`WHEN`** 절 형태의 제어 흐름과 결합하면 SQL 워크로드 내의 제어 흐름에 최적화된 실행을 제공한다.
-- MAGIC 
-- MAGIC 여기서는 SQL을 실행할 수 있는 모든 곳에서 재사용할 수 있는 기능으로 이전 로직을 래핑하는 것을 시연합니다.
-- MAGIC 
-- MAGIC ## Simple Control Flow Functions
-- MAGIC 
-- MAGIC Combining SQL UDFs with control flow in the form of **`CASE`** / **`WHEN`** clauses provides optimized execution for control flows within SQL workloads.
-- MAGIC 
-- MAGIC Here, we demonstrate wrapping the previous logic in a function that will be reusable anywhere we can execute SQL.

-- COMMAND ----------

CREATE FUNCTION foods_i_like(food STRING)
RETURNS STRING
RETURN CASE 
  WHEN food = "beans" THEN "I love beans"
  WHEN food = "potatoes" THEN "My favorite vegetable is potatoes"
  WHEN food <> "beef" THEN concat("Do you have any good recipes for ", food ,"?")
  ELSE concat("I don't eat ", food)
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 우리의 데이터에 이 방법을 사용하면 원하는 결과를 얻을 수 있다. <br>
-- MAGIC Using this method on our data provides the desired outcome.

-- COMMAND ----------

SELECT foods_i_like(food) FROM foods

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 여기에 제공된 예는 단순한 문자열 방법이지만, 이러한 기본 원칙은 스파크 SQL에서 네이티브 실행을 위한 사용자 지정 계산과 로직을 추가하는 데 사용될 수 있습니다.
-- MAGIC 
-- MAGIC 특히 정의된 절차나 사용자 정의 공식이 많은 시스템에서 사용자를 마이그레이션하는 기업의 경우 SQL UDF를 사용하면 소수의 사용자가 일반적인 보고 및 분석 쿼리에 필요한 복잡한 논리를 정의할 수 있습니다.
-- MAGIC 
-- MAGIC While the example provided here are simple string methods, these same basic principles can be used to add custom computations and logic for native execution in Spark SQL. 
-- MAGIC 
-- MAGIC Especially for enterprises that might be migrating users from systems with many defined procedures or custom-defined formulas, SQL UDFs can allow a handful of users to define the complex logic needed for common reporting and analytic queries.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이 교훈과 관련된 테이블을 삭제하려면 다음 셀 및 파일을 삭제합니다.
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