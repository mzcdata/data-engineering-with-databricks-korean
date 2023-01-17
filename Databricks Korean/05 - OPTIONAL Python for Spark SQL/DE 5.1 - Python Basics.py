# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 데이터브릭 SQL을 위한 충분한 Python
# MAGIC 
# MAGIC Databricks SQL은 ANSI 호환 SQL을 제공하는 반면, 일부 시스템에서 마이그레이션하는 사용자는 특히 제어 흐름 및 오류 처리와 관련하여 누락된 기능에 직면할 수 있다.
# MAGIC 
# MAGIC 데이터브릭 노트북은 사용자가 SQL과 파이썬을 작성하고 셀 단위로 논리를 실행할 수 있게 해준다. PySpark는 SQL 쿼리 실행을 광범위하게 지원하며 테이블 및 임시 뷰와 데이터를 쉽게 교환할 수 있습니다.
# MAGIC 
# MAGIC Python 개념을 조금만 숙달하면 SQL에 능숙한 엔지니어와 분석가를 위한 강력한 새로운 설계 관행을 실현할 수 있습니다. 이 수업에서는 전체 언어를 가르치려고 하지 않고 Databricks에서 더 확장 가능한 SQL 프로그램을 작성하는 데 즉시 활용할 수 있는 기능에 초점을 맞춥니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * 다중 라인 Python 문자열 인쇄 및 조작
# MAGIC * 변수 및 함수 정의
# MAGIC * 변수 대체에 f-string 사용
# MAGIC 
# MAGIC # Just Enough Python for Databricks SQL
# MAGIC 
# MAGIC While Databricks SQL provides an ANSI-compliant flavor of SQL with many additional custom methods (including the entire Delta Lake SQL syntax), users migrating from some systems may run into missing features, especially around control flow and error handling.
# MAGIC 
# MAGIC Databricks notebooks allow users to write SQL and Python and execute logic cell-by-cell. PySpark has extensive support for executing SQL queries, and can easily exchange data with tables and temporary views.
# MAGIC 
# MAGIC Mastering just a handful of Python concepts will unlock powerful new design practices for engineers and analysts proficient in SQL. Rather than trying to teach the entire language, this lesson focuses on those features that can immediately be leveraged to write more extensible SQL programs on Databricks.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Print and manipulate multi-line Python strings
# MAGIC * Define variables and functions
# MAGIC * Use f-strings for variable substitution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strings
# MAGIC 단일(**`'`**) 또는 이중(**`"`**) 따옴표로 둘러싸인 문자는 문자열로 간주됩니다.
# MAGIC 
# MAGIC ## Strings
# MAGIC Characters enclosed in single (**`'`**) or double (**`"`**) quotes are considered strings.

# COMMAND ----------

"This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 문자열이 어떻게 렌더링되는지 미리 보기 위해 **`print()`** 를 호출할 수 있습니다.
# MAGIC 
# MAGIC To preview how a string will render, we can call **`print()`**.

# COMMAND ----------

print("This is a string")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 문자열을 세 개의 따옴표(**`"""`**)로 묶으면 여러 줄을 사용할 수 있습니다.
# MAGIC 
# MAGIC By wrapping a string in triple quotes (**`"""`**), it's possible to use multiple lines.

# COMMAND ----------

print("""
This 
is 
a 
multi-line 
string
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이것은 SQL 쿼리를 파이썬 문자열로 전환하기 쉽다.
# MAGIC 
# MAGIC This makes it easy to turn SQL queries into Python strings.

# COMMAND ----------

print("""
SELECT *
FROM test_table
""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Python 셀에서 SQL을 실행할 때 문자열을 인수로 **`spark.sql()`** 에 전달합니다.
# MAGIC 
# MAGIC When we execute SQL from a Python cell, we will pass a string as an argument to **`spark.sql()`**.

# COMMAND ----------

spark.sql("SELECT 1 AS test")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 일반 SQL 노트북에 표시되는 방식으로 쿼리를 렌더링하려면 이 함수에 대해 **`display()`** 를 호출합니다.
# MAGIC 
# MAGIC To render a query the way it would appear in a normal SQL notebook, we call **`display()`** on this function.

# COMMAND ----------

display(spark.sql("SELECT 1 AS test"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **참고**: Python 문자열만 포함된 셀을 실행하면 문자열이 인쇄됩니다. 문자열과 함께 **`print()`** 를 사용하면 노트북으로 되돌아갑니다.
# MAGIC 
# MAGIC Python을 사용하여 SQL을 포함하는 문자열을 실행하려면 **`spark.sql()`** 호출 내에 전달해야 합니다.
# MAGIC 
# MAGIC **NOTE**: Executing a cell with only a Python string in it will just print the string. Using **`print()`** with a string just renders it back to the notebook.
# MAGIC 
# MAGIC To execute a string that contains SQL using Python, it must be passed within a call to **`spark.sql()`**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 변수
# MAGIC 파이썬 변수는 **`=`** 를 사용하여 할당된다.
# MAGIC 
# MAGIC Python 변수 이름은 문자로 시작해야 하며 문자, 숫자 및 밑줄만 포함할 수 있습니다. (밑줄로 시작하는 변수 이름은 유효하지만 일반적으로 특수한 사용 사례를 위해 예약됩니다.)
# MAGIC 
# MAGIC 많은 파이썬 프로그래머들은 모든 변수에 소문자와 밑줄만 사용하는 스네이크 케이싱을 선호한다.
# MAGIC 
# MAGIC 아래 셀은 **`my_string`** 변수를 생성합니다.
# MAGIC 
# MAGIC ## Variables
# MAGIC Python variables are assigned using the **`=`**.
# MAGIC 
# MAGIC Python variable names need to start with a letter, and can only contain letters, numbers, and underscores. (Variable names starting with underscores are valid but typically reserved for special use cases.)
# MAGIC 
# MAGIC Many Python programmers favor snake casing, which uses only lowercase letters and underscores for all variables.
# MAGIC 
# MAGIC The cell below creates the variable **`my_string`**.

# COMMAND ----------

my_string = "This is a string"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 변수로 셀을 실행하면 값이 반환됩니다.
# MAGIC 
# MAGIC Executing a cell with this variable will return its value.

# COMMAND ----------

my_string

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 여기의 출력은 우리가 **`"This is a string"`** 을 셀에 입력하고 실행한 것과 같다.
# MAGIC 
# MAGIC 따옴표는 인쇄할 때 표시된 대로 문자열의 일부가 아닙니다.
# MAGIC 
# MAGIC The output here is the same as if we typed **`"This is a string"`** into the cell and ran it.
# MAGIC 
# MAGIC Note that the quotation marks aren't part of the string, as shown when we print it.

# COMMAND ----------

print(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 변수는 문자열과 동일한 방식으로 사용할 수 있습니다.
# MAGIC 
# MAGIC 문자열 연결(문자열에 함께 결합)은 **`+`** 를 사용하여 수행할 수 있습니다.
# MAGIC 
# MAGIC This variable can be used the same way a string would be.
# MAGIC 
# MAGIC String concatenation (joining to strings together) can be performed with a **`+`**.

# COMMAND ----------

print("This is a new string and " + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 문자열 변수를 다른 문자열 변수와 결합할 수 있습니다.
# MAGIC 
# MAGIC We can join string variables with other string variables.

# COMMAND ----------

new_string = "This is a new string and "
print(new_string + my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Functions
# MAGIC 함수를 사용하면 로컬 변수를 인수로 지정한 다음 사용자 지정 논리를 적용할 수 있습니다. 키워드 **`def`** 뒤에 함수 이름과 괄호로 묶은 변수 인수를 사용하여 함수를 정의한다. 마지막으로 함수 헤더 끝에 **`:`** 가 있다.
# MAGIC 
# MAGIC 참고: Python에서는 들여쓰기가 중요합니다. 아래 셀에서 함수의 논리가 왼쪽 여백에서 들여쓰기된 것을 볼 수 있습니다. 이 수준으로 들여쓰기된 코드는 함수의 일부입니다.
# MAGIC 
# MAGIC 아래 함수는 하나의 인수(**`arg`**)를 가져와서 인쇄합니다.
# MAGIC 
# MAGIC ## Functions
# MAGIC Functions allow you to specify local variables as arguments and then apply custom logic. We define a function using the keyword **`def`** followed by the function name and, enclosed in parentheses, any variable arguments we wish to pass into the function. Finally, the function header has a **`:`** at the end.
# MAGIC 
# MAGIC Note: In Python, indentation matters. You can see in the cell below that the logic of the function is indented in from the left margin. Any code that is indented to this level is part of the function.
# MAGIC 
# MAGIC The function below takes one argument (**`arg`**) and then prints it.

# COMMAND ----------

def print_string(arg):
    print(arg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 문자열을 인수로 전달하면 인쇄됩니다.
# MAGIC 
# MAGIC When we pass a string as the argument, it will be printed.

# COMMAND ----------

print_string("foo")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 변수를 인수로 전달할 수도 있습니다.
# MAGIC 
# MAGIC We can also pass a variable as an argument.

# COMMAND ----------

print_string(my_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 종종 우리는 다른 곳에서 사용하기 위해 우리의 기능의 결과를 반환하기를 원한다. 이를 위해 우리는 **`return`** 키워드를 사용한다.
# MAGIC 
# MAGIC 아래 함수는 우리의 인수를 연결하여 새로운 문자열을 구성한다. 함수와 인수 모두 변수와 마찬가지로 임의의 이름을 가질 수 있으며 동일한 규칙을 따릅니다.
# MAGIC 
# MAGIC Oftentimes we want to return the results of our function for use elsewhere. For this we use the **`return`** keyword.
# MAGIC 
# MAGIC The function below constructs a new string by concatenating our argument. Note that both functions and arguments can have arbitrary names, just like variables (and follow the same rules).

# COMMAND ----------

def return_new_string(string_arg):
    return "The string passed to this function was " + string_arg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 기능을 실행하면 출력이 반환됩니다.
# MAGIC 
# MAGIC Running this function returns the output.

# COMMAND ----------

return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 변수에 할당하면 다른 곳에서 재사용할 수 있도록 출력이 캡처됩니다.
# MAGIC 
# MAGIC Assigning it to a variable captures the output for reuse elsewhere.

# COMMAND ----------

function_output = return_new_string("foobar")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 변수는 함수를 포함하지 않고 함수(문자열)의 결과만 포함합니다.
# MAGIC 
# MAGIC This variable doesn't contain our function, just the results of our function (a string).

# COMMAND ----------

function_output

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## F-strings
# MAGIC 파이썬 문자열 앞에 **`f`** 라는 문자를 추가하면 변수를 주입하거나 curly braces(**`{}`**) 안에 삽입하여 파이썬 코드를 평가할 수 있다.
# MAGIC 
# MAGIC 문자열 변수 대체를 보려면 아래 셀을 평가하십시오.
# MAGIC 
# MAGIC ## F-strings
# MAGIC By adding the letter **`f`** before a Python string, you can inject variables or evaluated Python code by inserted them inside curly braces (**`{}`**).
# MAGIC 
# MAGIC Evaluate the cell below to see string variable substitution.

# COMMAND ----------

f"I can substitute {my_string} here"

# COMMAND ----------

# MAGIC %md
# MAGIC 다음 셀은 함수에 의해 반환된 문자열을 삽입합니다.
# MAGIC 
# MAGIC The following cell inserts the string returned by a function.

# COMMAND ----------

f"I can substitute functions like {return_new_string('foobar')} here"

# COMMAND ----------

# MAGIC %md
# MAGIC 이것을 세 개의 따옴표와 결합하면 아래와 같이 문단이나 목록의 형식을 지정할 수 있습니다.
# MAGIC 
# MAGIC Combine this with triple quotes and you can format a paragraph or list, like below.

# COMMAND ----------

multi_line_string = f"""
I can have many lines of text with variable substitution:
  - A variable: {my_string}
  - A function output: {return_new_string('foobar')}
"""

print(multi_line_string)

# COMMAND ----------

# MAGIC %md
# MAGIC 또는 SQL 쿼리를 포맷할 수 있습니다.
# MAGIC 
# MAGIC Or you could format a SQL query.

# COMMAND ----------

table_name = "users"
filter_clause = "WHERE state = 'CA'"

query = f"""
SELECT *
FROM {table_name}
{filter_clause}
"""

print(query)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>