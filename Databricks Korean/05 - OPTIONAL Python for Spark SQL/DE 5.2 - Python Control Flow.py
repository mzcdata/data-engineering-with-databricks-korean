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
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * **`if`** / **`else`** 활용
# MAGIC * 오류가 노트북 실행에 미치는 영향 설명
# MAGIC * **`assert`**로 간단한 테스트 작성
# MAGIC * 오류를 처리하려면 **`try`** / **`except`** 를 사용하십시오
# MAGIC 
# MAGIC # Just Enough Python for Databricks SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Leverage **`if`** / **`else`**
# MAGIC * Describe how errors impact notebook execution
# MAGIC * Write simple tests with **`assert`**
# MAGIC * Use **`try`** / **`except`** to handle errors

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 만약/아마도
# MAGIC 
# MAGIC **`if`** / **`else`** 절들은 많은 프로그래밍 언어에서 일반적이다.
# MAGIC 
# MAGIC 유사하게 SQL에는 **`CASE WHEN ... ELSE`** 구조가 있다는 것을 참고하세요
# MAGIC 
# MAGIC <strong>테이블 또는 쿼리 내의 조건을 평가하려는 경우 **`CASE WHEN`** 을 사용하십시오.</strong>
# MAGIC 
# MAGIC Python 제어 흐름은 쿼리 외부의 조건을 평가하기 위해 예약되어야 합니다.
# MAGIC 
# MAGIC 나중에 이것에 대해 더 자세히. 먼저 **`"beans"`** 의 예를 들어 보겠습니다.
# MAGIC 
# MAGIC ## if/else
# MAGIC 
# MAGIC **`if`** / **`else`** clauses are common in many programming languages.
# MAGIC 
# MAGIC Note that SQL has the **`CASE WHEN ... ELSE`** construct, which is similar.
# MAGIC 
# MAGIC <strong>If you're seeking to evaluate conditions within your tables or queries, use **`CASE WHEN`**.</strong>
# MAGIC 
# MAGIC Python control flow should be reserved for evaluating conditions outside of your query.
# MAGIC 
# MAGIC More on this later. First, an example with **`"beans"`**.

# COMMAND ----------

food = "beans"

# COMMAND ----------

# MAGIC %md
# MAGIC **`if`** 및 **`else`** 와 함께 작업하는 것은 실행 환경에서 특정 조건이 사실인지 여부를 평가하는 것입니다.
# MAGIC 
# MAGIC 파이썬에는 다음과 같은 비교 연산자가 있다:
# MAGIC 
# MAGIC Working with **`if`** and **`else`** is all about evaluating whether or not certain conditions are true in your execution environment.
# MAGIC 
# MAGIC Note that in Python, we have the following comparison operators:
# MAGIC 
# MAGIC | Syntax | Operation |
# MAGIC | --- | --- |
# MAGIC | **`==`** | equals |
# MAGIC | **`>`** | greater than |
# MAGIC | **`<`** | less than |
# MAGIC | **`>=`** | greater than or equal |
# MAGIC | **`<=`** | less than or equal |
# MAGIC | **`!=`** | not equal |
# MAGIC 
# MAGIC 아래 문장을 큰 소리로 읽으면 프로그램의 제어 흐름을 설명하게 됩니다.
# MAGIC 
# MAGIC If you read the sentence below out loud, you will be describing the control flow of your program.

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 예상대로 변수 **`food`** 는 문자열 리터럴 **`"beans"`** 이기 때문에 **`if`** 문은 **`True`** 로 평가되고 첫 번째 인쇄문은 평가됩니다.
# MAGIC 
# MAGIC 변수에 다른 값을 할당합시다.
# MAGIC 
# MAGIC As expected, because the variable **`food`** is the string literal **`"beans"`**, the **`if`** statement evaluated to **`True`** and the first print statement evaluated.
# MAGIC 
# MAGIC Let's assign a different value to the variable.

# COMMAND ----------

food = "beef"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이제 첫 번째 조건은 **`False`** 로 평가됩니다. 
# MAGIC 
# MAGIC 다음 셀을 실행하면 어떻게 될 것 같습니까?
# MAGIC 
# MAGIC Now the first condition will evaluate as **`False`**. 
# MAGIC 
# MAGIC What do you think will happen when you run the following cell?

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 변수에 새 값을 할당할 때마다 이전 변수가 완전히 지워집니다.
# MAGIC 
# MAGIC Note that each time we assign a new value to a variable, this completely erases the old variable.

# COMMAND ----------

food = "potatoes"
print(food)

# COMMAND ----------

# MAGIC %md
# MAGIC Python 키워드 **`elif`** (**`else`** + **`if`** 의 줄임말)를 사용하면 여러 조건을 평가할 수 있다.
# MAGIC 
# MAGIC 조건은 위에서 아래로 평가됩니다. 조건이 참으로 평가되면 더 이상의 조건은 평가되지 않습니다.
# MAGIC 
# MAGIC **`if`** / **`else`** 제어 흐름 패턴:
# MAGIC 1. **`if`** 조항을 포함해야 합니다
# MAGIC 1. 임의의 수의 **`elif`** 절을 포함할 수 있습니다
# MAGIC 1. 최대 하나의 **`else`** 절을 포함할 수 있습니다
# MAGIC 
# MAGIC The Python keyword **`elif`** (short for **`else`** + **`if`**) allows us to evaluate multiple conditions.
# MAGIC 
# MAGIC Note that conditions are evaluated from top to bottom. Once a condition evaluates to true, no further conditions will be evaluated.
# MAGIC 
# MAGIC **`if`** / **`else`** control flow patterns:
# MAGIC 1. Must contain an **`if`** clause
# MAGIC 1. Can contain any number of **`elif`** clauses
# MAGIC 1. Can contain at most one **`else`** clause

# COMMAND ----------

if food == "beans":
    print(f"I love {food}")
elif food == "potatoes":
    print(f"My favorite vegetable is {food}")
elif food != "beef":
    print(f"Do you have any good recipes for {food}?")
else:
    print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 위의 논리를 함수에 캡슐화함으로써, 우리는 이 논리를 재사용할 수 있고 전역적으로 정의된 변수를 참조하는 대신 임의의 인수로 포맷할 수 있다.
# MAGIC 
# MAGIC By encapsulating the above logic in a function, we can reuse this logic and formatting with arbitrary arguments rather than referencing globally-defined variables.

# COMMAND ----------

def foods_i_like(food):
    if food == "beans":
        print(f"I love {food}")
    elif food == "potatoes":
        print(f"My favorite vegetable is {food}")
    elif food != "beef":
        print(f"Do you have any good recipes for {food}?")
    else:
        print(f"I don't eat {food}")

# COMMAND ----------

# MAGIC %md
# MAGIC 여기서, 우리는 **`"bread"`** 라는 문자열을 함수에 전달한다.
# MAGIC 
# MAGIC Here, we pass the string **`"bread"`** to the function.

# COMMAND ----------

foods_i_like("bread")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 함수를 평가할 때, 우리는 **`food`** 변수에 **`"bread"`** 라는 문자열을 로컬로 할당하고, 논리는 예상대로 작동한다.
# MAGIC 
# MAGIC 노트에 정의된 **`food`** 변수의 값은 덮어쓰지 않습니다.
# MAGIC 
# MAGIC As we evaluate the function, we locally assign the string **`"bread"`** to the **`food`** variable, and the logic behaves as expected.
# MAGIC 
# MAGIC Note that we don't overwrite the value of the **`food`** variable as previously defined in the notebook.

# COMMAND ----------

food

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## try/except
# MAGIC 
# MAGIC **`if`** / **`else`** 조항은 조건문 평가를 기반으로 조건부 논리를 정의할 수 있지만 **`try`** / **`except`** 는 강력한 오류 처리를 제공하는 데 중점을 둔다.
# MAGIC 
# MAGIC 간단한 기능을 고려하는 것으로 시작합시다.
# MAGIC 
# MAGIC ## try/except
# MAGIC 
# MAGIC While **`if`** / **`else`** clauses allow us to define conditional logic based on evaluating conditional statements, **`try`** / **`except`** focuses on providing robust error handling.
# MAGIC 
# MAGIC Let's begin by considering a simple function.

# COMMAND ----------

def three_times(number):
    return number * 3

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 함수를 사용하여 정수 값에 3을 곱한다고 가정합니다.
# MAGIC 
# MAGIC 아래 셀은 이 동작을 보여줍니다.
# MAGIC 
# MAGIC Let's assume that the desired use of this function is to multiply an integer value by 3.
# MAGIC 
# MAGIC The below cell demonstrates this behavior.

# COMMAND ----------

three_times(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 문자열이 함수에 전달될 경우 어떤 일이 발생하는지 확인합니다.
# MAGIC 
# MAGIC Note what happens if a string is passed to the function.

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 경우 오류가 발생하지는 않지만 원하는 결과를 얻을 수도 없습니다.
# MAGIC 
# MAGIC **`assert`** 문을 사용하면 Python 코드의 간단한 테스트를 실행할 수 있다. **`assert`** 의 진술이 사실로 평가되면 아무 일도 일어나지 않습니다. 
# MAGIC 
# MAGIC false로 평가되면 오류가 발생합니다.
# MAGIC 
# MAGIC 다음 셀을 실행하여 숫자 **'2'*가 다음과 같다고 주장합니다
# MAGIC 
# MAGIC In this case, we don't get an error, but we also do not get the desired outcome.
# MAGIC 
# MAGIC **`assert`** statements allow us to run simple tests of Python code. If an **`assert`** statement evaluates to true, nothing happens. 
# MAGIC 
# MAGIC If it evaluates to false, an error is raised.
# MAGIC 
# MAGIC Run the following cell to assert that the number **`2`** is an integer

# COMMAND ----------

assert type(2) == int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀의 주석을 제거한 후 실행하여 문자열 **`"2"`"** 이 정수임을 주장합니다.
# MAGIC 
# MAGIC 그것은 **`AssertionError`** 를 던질 것이다.
# MAGIC 
# MAGIC Uncomment the following cell and then run it to assert that the string **`"2"`"** is an integer.
# MAGIC 
# MAGIC It should throw an **`AssertionError`**.

# COMMAND ----------

# assert type("2") == int

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 예상대로 **`"2"`** 문자열은 정수가 아닙니다.
# MAGIC 
# MAGIC 파이썬 문자열은 아래와 같이 숫자 값으로 안전하게 캐스팅할 수 있는지 여부를 보고하는 속성이 있습니다.
# MAGIC 
# MAGIC As expected, the string **`"2"`** is not an integer.
# MAGIC 
# MAGIC Python strings have a property to report whether or not they can be safely cast as numeric value as seen below.

# COMMAND ----------

assert "2".isnumeric()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 문자열 번호는 API 쿼리의 결과, JSON 또는 CSV 파일의 원시 레코드 또는 SQL 쿼리에 의해 반환되는 결과로 볼 수 있습니다.
# MAGIC 
# MAGIC **`int()`** 와 **`float()`** 는 수치 유형에 값을 캐스팅하는 두 가지 일반적인 방법입니다. 
# MAGIC 
# MAGIC **`int`**  는 항상 정수이고 **`float`** 는 항상 소수입니다.
# MAGIC 
# MAGIC String numbers are common; you may see them as results from an API query, raw records in a JSON or CSV file, or returned by a SQL query.
# MAGIC 
# MAGIC **`int()`** and **`float()`** are two common methods for casting values to numeric types. 
# MAGIC 
# MAGIC An **`int`** will always be a whole number, while a **`float`** will always have a decimal.

# COMMAND ----------

int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC Python은 숫자 문자가 포함된 문자열을 숫자 형식으로 기꺼이 캐스팅하지만 다른 문자열을 숫자로 변경할 수는 없습니다.
# MAGIC 
# MAGIC 다음 셀에 대한 주석을 제거하고 시도하십시오:
# MAGIC 
# MAGIC While Python will gladly cast a string containing numeric characters to a numeric type, it will not allow you to change other strings to numbers.
# MAGIC 
# MAGIC Uncomment the following cell and give it a try:

# COMMAND ----------

# int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 오류가 발생하면 노트북 스크립트의 실행이 중지됩니다. 오류 이후의 모든 셀은 노트북을 프로덕션 작업으로 예약할 때 건너뜁니다.
# MAGIC 
# MAGIC 오류를 발생시킬 수 있는 코드를 **`try`** 문에 동봉하면 오류가 발생했을 때 대체 논리를 정의할 수 있다.
# MAGIC 
# MAGIC 아래는 이를 보여주는 간단한 기능입니다.
# MAGIC 
# MAGIC Note that errors will stop the execution of a notebook script; all cells after an error will be skipped when a notebook is scheduled as a production job.
# MAGIC 
# MAGIC If we enclose code that might throw an error in a **`try`** statement, we can define alternate logic when an error is encountered.
# MAGIC 
# MAGIC Below is a simple function that demonstrates this.

# COMMAND ----------

def try_int(num_string):
    try:
        int(num_string)
        result = f"{num_string} is a number."
    except:
        result = f"{num_string} is not a number!"
        
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 숫자 문자열이 전달되면 함수는 결과를 정수로 반환합니다.
# MAGIC 
# MAGIC When a numeric string is passed, the function will return the result as an integer.

# COMMAND ----------

try_int("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 숫자가 아닌 문자열이 전달되면 유용한 메시지가 출력됩니다.
# MAGIC 
# MAGIC **참고**: 오류가 발생하여 값이 반환되지 않았음에도 불구하고 **오류가 발생하지 않았습니다**. 오류를 억제하는 논리를 구현하면 논리가 묵묵히 실패할 수 있다.
# MAGIC 
# MAGIC When a non-numeric string is passed, an informative message is printed out.
# MAGIC 
# MAGIC **NOTE**: An error is **not** raised, even though an error occurred, and no value was returned. Implementing logic that suppresses errors can lead to logic silently failing.

# COMMAND ----------

try_int("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 아래에서 이전 기능은 정보 메시지를 반환하기 위한 오류 처리 로직을 포함하도록 업데이트되었습니다.
# MAGIC 
# MAGIC Below, our earlier function is updated to include logic for handling errors to return an informative message.

# COMMAND ----------

def three_times(number):
    try:
        return int(number) * 3
    except ValueError as e:
        print(f"You passed the string variable '{number}'.\n")
        print(f"Try passing an integer instead.")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 우리의 기능은 문자열로 전달된 숫자를 처리할 수 있다.
# MAGIC 
# MAGIC Now our function can process numbers passed as strings.

# COMMAND ----------

three_times("2")

# COMMAND ----------

# MAGIC %md
# MAGIC 그리고 문자열이 전달될 때 유용한 메시지를 출력합니다.
# MAGIC 
# MAGIC And prints an informative message when a string is passed.

# COMMAND ----------

three_times("two")

# COMMAND ----------

# MAGIC %md
# MAGIC 이 논리가 구현되면 이 논리의 대화형 실행에만 유용할 것입니다(메시지는 현재 아무 곳에도 기록되지 않으며 코드는 원하는 형식으로 데이터를 반환하지 않습니다. 인쇄된 메시지에 대해 작업하려면 사람의 개입이 필요합니다).
# MAGIC 
# MAGIC Note that as implemented, this logic would only be useful for interactive execution of this logic (the message isn't currently being logged anywhere, and the code will not return the data in the desired format; human intervention would be required to act upon the printed message).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SQL 쿼리에 Python 제어 흐름 적용
# MAGIC 
# MAGIC 위의 예는 Python에서 이러한 설계를 사용하는 기본 원칙을 보여주지만, 이 수업의 목표는 이러한 개념을 Datbricks에서 SQL 논리를 실행하는 데 적용하는 방법을 배우는 것이다.
# MAGIC 
# MAGIC Python에서 실행할 SQL 셀 변환을 다시 살펴보겠습니다.
# MAGIC 
# MAGIC **참고**: 다음 설치 스크립트는 분리된 실행 환경을 보장합니다.
# MAGIC 
# MAGIC ## Applying Python Control Flow for SQL Queries
# MAGIC 
# MAGIC While the above examples demonstrate the basic principles of using these designs in Python, the goal of this lesson is to learn how to apply these concepts to executing SQL logic on Databricks.
# MAGIC 
# MAGIC Let's revisit converting a SQL cell to execute in Python.
# MAGIC 
# MAGIC **NOTE**: The following setup script ensures an isolated execution environment.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW demo_tmp_vw(id, name, value) AS VALUES
# MAGIC   (1, "Yve", 1.0),
# MAGIC   (2, "Omar", 2.5),
# MAGIC   (3, "Elia", 3.3);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 임시 보기의 내용을 미리 보려면 아래의 SQL 셀을 실행하십시오.
# MAGIC 
# MAGIC Run the SQL cell below to preview the contents of this temp view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC Python 셀에서 SQL을 실행하려면 문자열 쿼리를 **`spark.sql()`** 로 전달하기만 하면 됩니다.
# MAGIC 
# MAGIC Running SQL in a Python cell simply requires passing the string query to **`spark.sql()`**.

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 그러나 **`spark.sql()`** 로 쿼리를 실행하면 결과가 표시되지 않고 데이터 프레임으로 반환됩니다. 아래에서 코드는 결과를 캡처하여 표시하도록 확장됩니다.
# MAGIC 
# MAGIC But recall that executing a query with **`spark.sql()`** returns the results as a DataFrame rather than displaying them; below, the code is augmented to capture the result and display it.

# COMMAND ----------

query = "SELECT * FROM demo_tmp_vw"
result = spark.sql(query)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 함수와 함께 간단한 **`if`** 절을 사용하면 임의의 SQL 쿼리를 실행하고, 선택적으로 결과를 표시하고, 항상 결과 데이터 프레임을 반환할 수 있다.
# MAGIC 
# MAGIC Using a simple **`if`** clause with a function allows us to execute arbitrary SQL queries, optionally displaying the results, and always returning the resultant DataFrame.

# COMMAND ----------

def simple_query_function(query, preview=True):
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

result = simple_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 아래에서는 다른 쿼리를 실행하고 쿼리의 목적이 데이터의 미리 보기를 반환하는 것이 아니라 임시 보기를 만드는 것이기 때문에 미리 보기를 **`False`** 로 설정한다.
# MAGIC 
# MAGIC Below, we execute a different query and set preview to **`False`**, as the purpose of the query is to create a temp view rather than return a preview of data.

# COMMAND ----------

new_query = "CREATE OR REPLACE TEMP VIEW id_name_tmp_vw AS SELECT id, name FROM demo_tmp_vw"

simple_query_function(new_query, preview=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 조직의 필요에 따라 추가로 매개 변수화할 수 있는 단순한 확장 가능 기능이 있습니다.
# MAGIC 
# MAGIC 예를 들어, 아래 쿼리와 같이 악의적인 SQL로부터 회사를 보호한다고 가정해 보겠습니다.
# MAGIC 
# MAGIC We now have a simple extensible function that could be further parameterized depending on the needs of our organization.
# MAGIC 
# MAGIC For example, suppose we want to protect our company from malicious SQL, like the query below.

# COMMAND ----------

injection_query = "SELECT * FROM demo_tmp_vw; DROP DATABASE prod_db CASCADE; SELECT * FROM demo_tmp_vw"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **`find()`** method를 사용하여 세미콜론을 찾아 여러 SQL 문을 테스트할 수 있습니다.
# MAGIC 
# MAGIC We can use the **`find()`** method to test for multiple SQL statements by looking for a semicolon.

# COMMAND ----------

injection_query.find(";")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 발견되지 않으면 **`-1`** 이 반환됩니다
# MAGIC 
# MAGIC If it's not found it will return **`-1`**

# COMMAND ----------

injection_query.find("x")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이러한 지식을 통해 쿼리 문자열에서 세미콜론에 대한 간단한 검색을 정의하고 발견된 경우 사용자 지정 오류 메시지를 발생시킬 수 있습니다(**`-1`** 는 아님)
# MAGIC 
# MAGIC With that knowledge, we can define a simple search for a semicolon in the query string and raise a custom error message if it was found (not **`-1`**)

# COMMAND ----------

def injection_check(query):
    semicolon_index = query.find(";")
    if semicolon_index >= 0:
        raise ValueError(f"Query contains semi-colon at index {semicolon_index}\nBlocking execution to avoid SQL injection attack")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **참고**: 여기에 표시된 예는 복잡하지 않지만 일반적인 원리를 보여주려고 합니다. 
# MAGIC 
# MAGIC 신뢰할 수 없는 사용자가 SQL 쿼리로 전달될 텍스트를 전달할 수 있도록 항상 주의하십시오. 
# MAGIC 
# MAGIC 또한 **`spark.sql()`** 를 사용하여 하나의 쿼리만 실행할 수 있으므로 세미콜론이 있는 텍스트는 항상 오류를 발생시킵니다.
# MAGIC 
# MAGIC 
# MAGIC **NOTE**: The example shown here is not sophisticated, but seeks to demonstrate a general principle. 
# MAGIC 
# MAGIC Always be wary of allowing untrusted users to pass text that will be passed to SQL queries. 
# MAGIC 
# MAGIC Also note that only one query can be executed using **`spark.sql()`**, so text with a semi-colon will always throw an error.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀에 대한 주석을 제거하고 시도하십시오:
# MAGIC 
# MAGIC Uncomment the following cell and give it a try:

# COMMAND ----------

# injection_check(injection_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 방법을 이전 쿼리 기능에 추가하면 실행 전에 잠재적 위협에 대해 각 쿼리를 평가하는 더 강력한 기능을 갖게 된다.
# MAGIC 
# MAGIC If we add this method to our earlier query function, we now have a more robust function that will assess each query for potential threats before execution.

# COMMAND ----------

def secure_query_function(query, preview=True):
    injection_check(query)
    query_result = spark.sql(query)
    if preview:
        display(query_result)
    return query_result

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 예상대로 안전한 쿼리를 사용하면 정상적인 성능을 볼 수 있습니다.
# MAGIC 
# MAGIC As expected, we see normal performance with a safe query.

# COMMAND ----------

secure_query_function(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 그러나 잘못된 논리가 실행될 때 실행을 방지합니다.
# MAGIC 
# MAGIC But prevent execution when when bad logic is run.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>