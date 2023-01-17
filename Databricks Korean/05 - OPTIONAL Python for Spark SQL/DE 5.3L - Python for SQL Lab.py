# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks SQL Lab을 위한 충분한 Python
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
# MAGIC * 기본 Python 코드 검토 및 코드 실행의 예상 결과 설명
# MAGIC * Python 함수에서 제어 흐름 문을 통한 이유
# MAGIC * SQL 쿼리를 Python 함수로 래핑하여 매개 변수 추가
# MAGIC 
# MAGIC # Just Enough Python for Databricks SQL Lab
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Review basic Python code and describe expected outcomes of code execution
# MAGIC * Reason through control flow statements in Python functions
# MAGIC * Add parameters to a SQL query by wrapping it in a Python function

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-5.3L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Python 기본 사항 검토
# MAGIC 
# MAGIC 이전 노트북에서는 **`spark.sql()`** 를 사용하여 파이썬에서 임의 SQL 명령을 실행하는 방법을 간략하게 살펴보았다.
# MAGIC 
# MAGIC 다음 세 개의 셀을 보세요. 각 셀을 실행하기 전에 다음을 확인하십시오:
# MAGIC 1. 셀 실행의 예상 출력
# MAGIC 1. 어떤 논리가 실행되고 있는가
# MAGIC 1. 결과적인 환경 상태의 변화
# MAGIC 
# MAGIC 그런 다음 셀을 실행하고 결과를 예상과 비교한 후 아래 설명을 참조하십시오.
# MAGIC 
# MAGIC # Reviewing Python Basics
# MAGIC 
# MAGIC In the previous notebook, we briefly explored using **`spark.sql()`** to execute arbitrary SQL commands from Python.
# MAGIC 
# MAGIC Look at the following 3 cells. Before executing each cell, identify:
# MAGIC 1. The expected output of cell execution
# MAGIC 1. What logic is being executed
# MAGIC 1. Changes to the resultant state of the environment
# MAGIC 
# MAGIC Then execute the cells, compare the results to your expectations, and see the explanations below.

# COMMAND ----------

course = "dewd"

# COMMAND ----------

spark.sql(f"SELECT '{course}' AS course_name")

# COMMAND ----------

df = spark.sql(f"SELECT '{course}' AS course_name")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 1. **Cmd 5**는 문자열을 변수에 할당합니다. 변수 할당이 성공하면 노트북에 출력이 표시되지 않습니다. 현재 실행 환경에 새 변수가 추가되었습니다.
# MAGIC 1. **Cmd 6**는 SQL 쿼리를 실행하고 **`DataFrame`** 이라는 단어와 함께 DataFrame에 대한 스키마를 표시합니다. 이 경우 SQL 쿼리는 문자열을 선택하기 위한 것일 뿐이므로 환경에 대한 변경은 발생하지 않습니다. 
# MAGIC 1. **Cmd 7**는 동일한 SQL 쿼리를 실행하고 데이터 프레임의 출력을 표시합니다. **`display()`** 와 **`spark.sql()`** 의 이러한 조합은 **`%sql`** 셀에서 실행 로직을 가장 가깝게 반영한다. 결과는 항상 쿼리에 의해 결과가 반환된다고 가정하여 형식이 지정된 테이블에 인쇄된다. 일부 쿼리는 대신 테이블이나 데이터베이스를 조작하고, 이 경우 **`OK`** 작업은 성공적인 실행을 보여주기 위해 인쇄된다. 이 경우 이 코드를 실행하면 환경이 변경되지 않습니다.
# MAGIC 
# MAGIC 1. **Cmd 5** assigns a string to a variable. When a variable assignment is successful, no output is displayed to the notebook. A new variable is added to the current execution environment.
# MAGIC 1. **Cmd 6** executes a SQL query and displays the schema for the DataFrame alongside the word **`DataFrame`**. In this case, the SQL query is just to select a string, so no changes to our environment occur. 
# MAGIC 1. **Cmd 7** executes the same SQL query and displays the output of the DataFrame. This combination of **`display()`** and **`spark.sql()`** most closely mirrors executing logic in a **`%sql`** cell; the results will always be printed in a formatted table, assuming results are returned by the query; some queries will instead manipulate tables or databases, in which case the work **`OK`** will print to show successful execution. In this case, no changes to our environment occur from running this code.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 개발 환경 설정
# MAGIC 
# MAGIC 이 과정 내내, 우리는 현재 노트북을 실행하고 있는 사용자에 대한 정보를 캡처하고 분리된 개발 데이터베이스를 만들기 위해 다음 셀과 유사한 논리를 사용한다.
# MAGIC 
# MAGIC **`re`** 라이브러리는 <a href="https://timeout.python.org/3/library/re.html" target="_blank"> standard Python library for regex</a> 입니다..
# MAGIC 
# MAGIC 데이터브릭 SQL은 **`current_user()`** 의 사용자 이름을 캡처하는 특별한 방법을 가지고 있으며, **`spark.sql()`** 로 실행된 쿼리의 첫 번째 열의 첫 번째 행을 캡처하는 빠른 해킹입니다(이 경우에는 행과 열이 1개만 있음을 알고 안전하게 수행합니다).
# MAGIC 
# MAGIC 아래의 다른 모든 논리는 문자열 형식일 뿐입니다.
# MAGIC 
# MAGIC ## Setting Up a Development Environment
# MAGIC 
# MAGIC Throughout this course, we use logic similar to the follow cell to capture information about the user currently executing the notebook and create an isolated development database.
# MAGIC 
# MAGIC The **`re`** library is the <a href="https://docs.python.org/3/library/re.html" target="_blank">standard Python library for regex</a>.
# MAGIC 
# MAGIC Databricks SQL has a special method to capture the username of the **`current_user()`**; and the **`.first()[0]`** code is a quick hack to capture the first row of the first column of a query executed with **`spark.sql()`** (in this case, we do this safely knowing that there will only be 1 row and 1 column).
# MAGIC 
# MAGIC All other logic below is just string formatting.

# COMMAND ----------

import re

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
db_name = f"dbacademy_{clean_username}_{course}_5_3l"
working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

print(f"username:    {username}")
print(f"db_name:     {db_name}")
print(f"working_dir: {working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래에서, 우리는 이 사용자별 데이터베이스를 만들고 사용하기 위해 이 논리에 간단한 제어 흐름 문을 추가한다. 
# MAGIC 
# MAGIC 선택적으로 이 데이터베이스를 재설정하고 반복 실행 시 모든 내용을 삭제합니다. (파라미터 **`reset`** 의 기본값은 **`True`** 입니다.).
# MAGIC 
# MAGIC Below, we add a simple control flow statement to this logic to create and use this user-specific database. 
# MAGIC 
# MAGIC Optionally, we will reset this database and drop all of the contents on repeat execution. (Note the the default value for the parameter **`reset`** is **`True`**).

# COMMAND ----------

def create_database(course, reset=True):
    import re

    username = spark.sql("SELECT current_user()").first()[0]
    clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
    db_name = f"dbacademy_{clean_username}_{course}_5_3l"
    working_dir = f"dbfs:/user/{username}/dbacademy/{course}/5.3l"

    print(f"username:    {username}")
    print(f"db_name:     {db_name}")
    print(f"working_dir: {working_dir}")

    if reset:
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
        dbutils.fs.rm(working_dir, True)
        
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{working_dir}/{db_name}.db'")
    spark.sql(f"USE {db_name}")
    
create_database(course)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 정의된 이 논리는 교육 목적을 위해 공유 작업 공간에서 학생을 격리하는 것을 목표로 하지만, 생산을 추진하기 전에 격리된 환경에서 새로운 논리를 테스트하는 데 동일한 기본 설계를 활용할 수 있다.
# MAGIC 
# MAGIC While this logic as defined is geared toward isolating students in shared workspaces for instructional purposes, the same basic design could be leveraged for testing new logic in an isolated environment before pushing to production.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 오류를 정상적으로 처리
# MAGIC 
# MAGIC 아래 함수의 논리를 검토하십시오.
# MAGIC 
# MAGIC 방금 테이블이 없는 새 데이터베이스를 선언했습니다.
# MAGIC 
# MAGIC ## Handling Errors Gracefully
# MAGIC 
# MAGIC Review the logic in the function below.
# MAGIC 
# MAGIC Note that we've just declared a new database that currently contains no tables.

# COMMAND ----------

def query_or_make_demo_table(table_name):
    try:
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Displayed results for the table {table_name}")
        
    except:
        spark.sql(f"CREATE TABLE {table_name} (id INT, name STRING, value DOUBLE, state STRING)")
        spark.sql(f"""INSERT INTO {table_name}
                      VALUES (1, "Yve", 1.0, "CA"),
                             (2, "Omar", 2.5, "NY"),
                             (3, "Elia", 3.3, "OH"),
                             (4, "Rebecca", 4.7, "TX"),
                             (5, "Ameena", 5.3, "CA"),
                             (6, "Ling", 6.6, "NY"),
                             (7, "Pedro", 7.1, "KY")""")
        
        display(spark.sql(f"SELECT * FROM {table_name}"))
        print(f"Created the table {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀을 실행하기 전에 다음 항목을 확인하십시오:
# MAGIC 1. 셀 실행의 예상 출력
# MAGIC 1. 어떤 논리가 실행되고 있는가
# MAGIC 1. 결과적인 환경 상태의 변화
# MAGIC 
# MAGIC Try to identify the following before executing the next cell:
# MAGIC 1. The expected output of cell execution
# MAGIC 1. What logic is being executed
# MAGIC 1. Changes to the resultant state of the environment

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이제 아래의 동일한 쿼리를 실행하기 전에 동일한 세 가지 질문에 답하십시오.
# MAGIC 
# MAGIC Now answer the same three questions before running the same query below.

# COMMAND ----------

query_or_make_demo_table("demo_table")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - 첫 번째 실행 시 **`demo_table`** 테이블이 아직 존재하지 않습니다. 이와 같이 테이블의 내용을 반환하려는 시도는 오류를 발생시켰고, 이로 인해 **`except`** 의 논리 블록이 실행되었다. 이 블록:
# MAGIC   1. 테이블을 만들었습니다
# MAGIC   1. 삽입된 값
# MAGIC   1. 표의 내용을 인쇄하거나 표시한다
# MAGIC - 두 번째 실행 시 **`demo_table`** 테이블이 이미 존재하므로 **`try`** 블록의 첫 번째 쿼리가 오류 없이 실행됩니다. 따라서 환경을 수정하지 않고 쿼리 결과만 표시합니다.
# MAGIC 
# MAGIC - On the first execution, the table **`demo_table`** did not yet exist. As such, the attempt to return the contents of the table created an error, which resulted in our **`except`** block of logic executing. This block:
# MAGIC   1. Created the table
# MAGIC   1. Inserted values
# MAGIC   1. Printed or displayed the contents of the table
# MAGIC - On the second execution, the table **`demo_table`** already exists, and so the first query in the **`try`** block executes without error. As a result, we just display the results of the query without modifying anything in our environment.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL을 Python에 적용
# MAGIC 위에서 작성한 데모 테이블에 대해 다음 SQL 쿼리를 고려해 보겠습니다.
# MAGIC 
# MAGIC ## Adapting SQL to Python
# MAGIC Let's consider the following SQL query against our demo table created above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, value 
# MAGIC FROM demo_table
# MAGIC WHERE state = "CA"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 값은 다음과 같이 PySpark API와 **`display`** 함수를 사용하여 표현할 수도 있습니다:
# MAGIC 
# MAGIC which can also be expressed using the PySpark API and the **`display`** function as seen here:

# COMMAND ----------

results = spark.sql("SELECT id, value FROM demo_table WHERE state = 'CA'")
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 간단한 예를 사용하여 옵션 기능을 추가하는 Python 함수를 만드는 연습을 해봅시다.
# MAGIC 
# MAGIC 대상 기능은 다음과 같습니다:
# MAGIC * **`demo_table`** 이라는 테이블의 **`id`** 및 **`value`** 열만 포함하는 쿼리를 기반으로 합니다
# MAGIC * 기본 동작이 모든 상태를 포함하는 **`state`** 에 의해 해당 쿼리를 필터링할 수 있음
# MAGIC * 기본 동작이 렌더링되지 않는 **`display`** 함수를 사용하여 쿼리 결과를 선택적으로 렌더링합니다
# MAGIC * 반환 예정:
# MAGIC   * **`render_results`** 가 False인 경우 쿼리 결과 개체(PySpark DataFrame)
# MAGIC   * **`render_results`** 가 True인 경우 **`None`** 값
# MAGIC 
# MAGIC 목표 달성:
# MAGIC * assert 문을 추가하여 **`state`** 매개 변수에 전달된 값에 두 개의 대문자가 포함되어 있는지 확인합니다
# MAGIC 
# MAGIC 아래에는 몇 가지 시동 논리가 제공되어 있습니다:
# MAGIC 
# MAGIC Let's use this simple example to practice creating a Python function that adds optional functionality.
# MAGIC 
# MAGIC Our target function will:
# MAGIC * Be based upon a query that only includes the **`id`** and **`value`** columns from the a table named **`demo_table`**
# MAGIC * Will allow filtering of that query by **`state`** where the the default behavior is to include all states
# MAGIC * Will optionally render the results of the query using the **`display`** function where the default behavior is to not render
# MAGIC * Will return:
# MAGIC   * The query result object (a PySpark DataFrame) if **`render_results`** is False
# MAGIC   * The **`None`** value  if **`render_results`** is True
# MAGIC 
# MAGIC Stretch Goal:
# MAGIC * Add an assert statement to verify that the value passed for the **`state`** parameter contains two, uppercase letters
# MAGIC 
# MAGIC Some starter logic has been provided below:

# COMMAND ----------

# TODO
def preview_values(state=<FILL-IN>, render_results=<FILL-IN>):
    query = <FILL-IN>

    if state is not None:
        <FILL-IN>

    if render_results
        <FILL-IN>


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래의 단언문은 당신의 기능이 의도한 대로 작동하는지 여부를 확인하는 데 사용될 수 있다.
# MAGIC 
# MAGIC The assert statements below can be used to check whether or not your function works as intended.

# COMMAND ----------

import pyspark.sql.dataframe

assert type(preview_values()) == pyspark.sql.dataframe.DataFrame, "Function should return the results as a DataFrame"
assert preview_values().columns == ["id", "value"], "Query should only return **`id`** and **`value`** columns"

assert preview_values(render_results=True) is None, "Function should not return None when rendering"
assert preview_values(render_results=False) is not None, "Function should return DataFrame when not rendering"

assert preview_values(state=None).count() == 7, "Function should allow no state"
assert preview_values(state="NY").count() == 2, "Function should allow filtering by state"
assert preview_values(state="CA").count() == 2, "Function should allow filtering by state"
assert preview_values(state="OH").count() == 1, "Function should allow filtering by state"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>