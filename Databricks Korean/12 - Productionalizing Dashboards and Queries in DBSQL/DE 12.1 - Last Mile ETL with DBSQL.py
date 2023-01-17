# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Databricks SQL이 포함된 Last Mile ETL
# MAGIC 
# MAGIC 계속하기 전에 지금까지 배운 몇 가지 사항을 요약해 보겠습니다:
# MAGIC 1. Databricks 작업 공간에는 데이터 엔지니어링 개발 라이프사이클을 단순화할 수 있는 도구 모음이 포함되어 있습니다
# MAGIC 1. Databricks 노트북을 사용하면 SQL을 다른 프로그래밍 언어와 혼합하여 ETL 워크로드를 정의할 수 있습니다
# MAGIC 1. Delta Lake는 ACID 준수 트랜잭션을 제공하고 Lakehouse에서 증분 데이터 처리를 쉽게 합니다
# MAGIC 1. Delta Live Tables는 SQL 구문을 확장하여 Lakehouse의 다양한 설계 패턴을 지원하고 인프라 구축을 단순화합니다
# MAGIC 1. 멀티태스킹 작업을 통해 전체 작업 조정이 가능하며, 노트북과 DLT 파이프라인의 혼합을 예약하면서 종속성을 추가할 수 있습니다
# MAGIC 1. Databricks SQL을 통해 사용자는 SQL 쿼리를 편집 및 실행하고 시각화를 구축하고 대시보드를 정의할 수 있습니다
# MAGIC 1. Data Explorer는 테이블 ACL 관리를 간소화하여 SQL 분석가가 Lakehouse 데이터를 사용할 수 있도록 지원합니다(Unity Catalog를 통해 곧 대폭 확장 예정)
# MAGIC 
# MAGIC 이 섹션에서는 프로덕션 워크로드를 지원하기 위해 더 많은 DBSQL 기능을 살펴봅니다. 
# MAGIC 
# MAGIC 먼저 Databricks SQL을 활용하여 분석을 위한 Lastmile ETL을 지원하는 쿼리를 구성하는 것에 중점을 두겠습니다. 이 데모에 Databricks SQL UI를 사용하는 동안 SQL Endpoints <a href="https://docs.databricks.com/integrations/partners.html" target="_blank">는 외부 쿼리 실행을 허용할 뿐만 아니라 전체 API를 지원할 수 있는 여러 다른 도구와 함께 사용됩니다 임의 쿼리를 프로그래밍 방식으로 실행합니다.
# MAGIC 
# MAGIC 이러한 쿼리 결과에서 일련의 시각화를 생성하여 대시보드로 결합합니다.
# MAGIC 
# MAGIC 마지막으로 쿼리 및 대시보드에 대한 업데이트 스케줄링 과정을 살펴보고, 시간 경과에 따른 프로덕션 데이터셋의 상태를 모니터링하는 데 도움이 되는 설정 경고를 시연합니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * 분석 워크로드를 지원하는 프로덕션 ETL 작업을 지원하는 도구로 Datbricks SQL 사용
# MAGIC * Databricks SQL Editor를 사용하여 SQL 쿼리 및 시각화 구성
# MAGIC * Databricks SQL에서 대시보드 만들기
# MAGIC * 쿼리 및 대시보드에 대한 업데이트 예약
# MAGIC * SQL 쿼리에 대한 알림 설정
# MAGIC   
# MAGIC # Last Mile ETL with Databricks SQL
# MAGIC 
# MAGIC Before we continue, let's do a recap of some of the things we've learned so far:
# MAGIC 1. The Databricks workspace contains a suite of tools to simplify the data engineering development lifecycle
# MAGIC 1. Databricks notebooks allow users to mix SQL with other programming languages to define ETL workloads
# MAGIC 1. Delta Lake provides ACID compliant transactions and makes incremental data processing easy in the Lakehouse
# MAGIC 1. Delta Live Tables extends the SQL syntax to support many design patterns in the Lakehouse, and simplifies infrastructure deployment
# MAGIC 1. Multi-task jobs allows for full task orchestration, adding dependencies while scheduling a mix of notebooks and DLT pipelines
# MAGIC 1. Databricks SQL allows users to edit and execute SQL queries, build visualizations, and define dashboards
# MAGIC 1. Data Explorer simplifies managing Table ACLs, making Lakehouse data available to SQL analysts (soon to be expanded greatly by Unity Catalog)
# MAGIC 
# MAGIC In this section, we'll focus on exploring more DBSQL functionality to support production workloads. 
# MAGIC 
# MAGIC We'll start by focusing on leveraging Databricks SQL to configure queries that support last mile ETL for analytics. Note that while we'll be using the Databricks SQL UI for this demo, SQL Endpoints <a href="https://docs.databricks.com/integrations/partners.html" target="_blank">integrate with a number of other tools to allow external query execution</a>, as well as having <a href="https://docs.databricks.com/sql/api/index.html" target="_blank">full API support for executing arbitrary queries programmatically</a>.
# MAGIC 
# MAGIC From these query results, we'll generate a series of visualizations, which we'll combine into a dashboard.
# MAGIC 
# MAGIC Finally, we'll walk through scheduling updates for queries and dashboards, and demonstrate setting alerts to help monitor the state of production datasets over time.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Use Databricks SQL as a tool to support production ETL tasks backing analytic workloads
# MAGIC * Configure SQL queries and visualizations with the Databricks SQL Editor
# MAGIC * Create dashboards in Databricks SQL
# MAGIC * Schedule updates for queries and dashboards
# MAGIC * Set alerts for SQL queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## 설치 스크립트 실행
# MAGIC 다음 셀은 SQL 쿼리를 생성하는 데 사용할 클래스를 정의하는 노트북을 실행합니다.
# MAGIC 
# MAGIC ## Run Setup Script
# MAGIC The following cells runs a notebook that defines a class we'll use to generate SQL queries.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-12.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데모 데이터베이스 만들기
# MAGIC 다음 셀을 실행하고 결과를 Databricks SQL Editor에 복사합니다.
# MAGIC 
# MAGIC 다음 쿼리:
# MAGIC * 새 데이터베이스 만들기
# MAGIC * 두 테이블 선언(데이터 로드에 사용)
# MAGIC * 두 함수 선언(데이터 생성에 사용)
# MAGIC 
# MAGIC 복사가 완료되면 **Run** 버튼을 사용하여 쿼리를 실행합니다.
# MAGIC 
# MAGIC ## Create a Demo Database
# MAGIC Execute the following cell and copy the results into the Databricks SQL Editor.
# MAGIC 
# MAGIC These queries:
# MAGIC * Create a new database
# MAGIC * Declare two tables (we'll use these for loading data)
# MAGIC * Declare two functions (we'll use these for generating data)
# MAGIC 
# MAGIC Once copied, execute the query using the **Run** button.

# COMMAND ----------

DA.generate_config()

# COMMAND ----------

# MAGIC %md
# MAGIC **참고**: 위의 쿼리는 환경을 재구성하기 위해 데모를 완전히 재설정한 후 한 번만 실행되도록 설계되었습니다. 카탈로그를 실행하려면 사용자에게 **`CREATE`** 및 **`USAGE`** 권한이 있어야 합니다.
# MAGIC 
# MAGIC **NOTE**: The queries above are only designed to be run once after resetting the demo completely to reconfigure the environment. Users will need to have **`CREATE`** and **`USAGE`** permissions on the catalog to execute them.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 
# MAGIC **경고:** **`USE`** 문이 아직 쿼리를 실행할 데이터베이스를 변경하지 않았기 때문에 계속하기 전에 데이터베이스를 선택해야 합니다 
# MAGIC <br>
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 
# MAGIC **WARNING:** Make sure to select your database before proceeding as the **`USE`** statement<br/>doesn't yet change the database against which your queries will execute

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데이터를 로드할 쿼리 만들기
# MAGIC 단계:
# MAGIC 1. 아래 셀을 실행하여 이전 단계에서 생성된 **`user_ping`** 테이블에 데이터를 로드하기 위한 포맷된 SQL 쿼리를 출력합니다.
# MAGIC 1. 이 쿼리를 **Load Ping Data**라는 이름으로 저장합니다.
# MAGIC 1. 이 쿼리를 실행하여 데이터 배치를 로드합니다.
# MAGIC 
# MAGIC ## Create a Query to Load Data
# MAGIC Steps:
# MAGIC 1. Execute the cell below to print out a formatted SQL query for loading data in the **`user_ping`** table created in the previous step.
# MAGIC 1. Save this query with the name **Load Ping Data**.
# MAGIC 1. Run this query to load a batch of data.

# COMMAND ----------

DA.generate_load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 쿼리를 실행하면 일부 데이터가 로드되고 테이블에 있는 데이터의 미리 보기가 반환됩니다.
# MAGIC 
# MAGIC **참고**: 랜덤 숫자를 사용하여 데이터를 정의하고 로드하므로 각 사용자의 값이 조금씩 다릅니다.
# MAGIC 
# MAGIC Executing the query should load some data and return a preview of the data in the table.
# MAGIC 
# MAGIC **NOTE**: Random numbers are being used to define and load data, so each user will have slightly different values present.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 쿼리 새로 고침 예약 설정
# MAGIC 
# MAGIC 단계:
# MAGIC 1. SQL 쿼리 편집기 상자의 오른쪽 아래에 있는 **Refresh Schedule** 필드를 찾은 후 파란색 **Never**을 클릭합니다
# MAGIC 1. 드롭다운을 사용하여 **1 minute**마다 새로 고침으로 변경합니다
# MAGIC 1. **Ends**의 경우 **On** 라디오 버튼을 클릭합니다
# MAGIC 1. 내일 날짜를 선택
# MAGIC 1. **OK**을 클릭합니다
# MAGIC 
# MAGIC ## Set a Query Refresh Schedule
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Locate the **Refresh Schedule** field at the bottom right of the SQL query editor box; click the blue **Never**
# MAGIC 1. Use the drop down to change to Refresh every **1 minute**
# MAGIC 1. For **Ends**, click the **On** radio button
# MAGIC 1. Select tomorrow's date
# MAGIC 1. Click **OK**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 총 레코드를 추적하기 위한 쿼리 만들기
# MAGIC 단계:
# MAGIC 1. 아래 셀을 실행합니다.
# MAGIC 1. 이 쿼리를 **User Counts**라는 이름으로 저장합니다.
# MAGIC 1. 쿼리를 실행하여 현재 결과를 계산합니다.
# MAGIC 
# MAGIC ## Create a Query to Track Total Records
# MAGIC Steps:
# MAGIC 1. Execute the cell below.
# MAGIC 1. Save this query with the name **User Counts**.
# MAGIC 1. Run the query to calculate the current results.

# COMMAND ----------

DA.generate_user_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 막대 그래프 시각화 만들기
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 쿼리 창의 오른쪽 하단 모서리에 있는 새로 고침 예약 버튼 아래에 있는 **Add Visualization** 버튼을 클릭합니다
# MAGIC 1. 이름(기본값은 **`Visualization 1`** )을 클릭하고 이름을 **Total User Records**로 변경합니다
# MAGIC 1. **X Column**에 **`user_id`** 를 설정합니다
# MAGIC 1. **Y Columns**에 대해 **`total_records`** 를 설정합니다
# MAGIC 1. **Save**을 클릭합니다
# MAGIC 
# MAGIC ## Create a Bar Graph Visualization
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Add Visualization** button, located beneath the Refresh Schedule button in the bottom right-hand corner of the query window
# MAGIC 1. Click on the name (should default to something like **`Visualization 1`**) and change the name to **Total User Records**
# MAGIC 1. Set **`user_id`** for the **X Column**
# MAGIC 1. Set **`total_records`** for the **Y Columns**
# MAGIC 1. Click **Save**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 새 대시보드 만들기
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 화면 하단에 세 개의 수직 점이 있는 버튼을 클릭하고 **대시보드에 추가**를 선택합니다.
# MAGIC 1. **새 대시보드 만들기** 옵션을 클릭합니다
# MAGIC 1. 대시보드 이름을 지정합니다. <strong>사용자 핑 요약 **`<your_initials_here>`**</strong>
# MAGIC 1. **Save**을 클릭하여 새 대시보드를 만듭니다
# MAGIC 1. 이제 새로 생성한 대시보드를 대상으로 선택해야 합니다. 시각화를 추가하려면 **OK** 을 클릭하십시오
# MAGIC 
# MAGIC ## Create a New Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC 1. Click the **Create new dashboard** option
# MAGIC 1. Name your dashboard <strong>User Ping Summary **`<your_initials_here>`**</strong>
# MAGIC 1. Click **Save** to create the new dashboard
# MAGIC 1. Your newly created dashboard should now be selected as the target; click **OK** to add your visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 최근 평균 ping을 계산하기 위한 쿼리 생성
# MAGIC 단계:
# MAGIC 1. 아래 셀을 실행하여 포맷된 SQL 쿼리를 출력합니다.
# MAGIC 1. 이 쿼리를 **Avg Ping** 이름으로 저장합니다.
# MAGIC 1. 쿼리를 실행하여 현재 결과를 계산합니다.
# MAGIC 
# MAGIC ## Create a Query to Calculate the Recent Average Ping
# MAGIC Steps:
# MAGIC 1. Execute the cell below to print out the formatted SQL query.
# MAGIC 1. Save this query with the name **Avg Ping**.
# MAGIC 1. Run the query to calculate the current results.

# COMMAND ----------

DA.generate_avg_ping()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 대시보드에 선 그림 시각화 추가
# MAGIC 
# MAGIC 단계:
# MAGIC 1. **Add Visualization** 버튼을 클릭합니다
# MAGIC 1. 이름(기본값은 **`Visualization 1`**)을 클릭하고 이름을 **Avg User Ping**으로 변경합니다
# MAGIC 1. **Visualization Type**에서 **`Line`** 을 선택합니다
# MAGIC 1. **X Column**에 **`end_time`** 을 설정합니다
# MAGIC 1. **Y Columns**에 대해 **`avg_ping`** 을 설정합니다
# MAGIC 1. **Group by**에 대해 **`user_id`**를 설정합니다
# MAGIC 1. **Save**을 클릭합니다
# MAGIC 1. 화면 하단에 세 개의 수직 점이 있는 버튼을 클릭하고 **Add to Dashboard**를 선택합니다.
# MAGIC 1. 이전에 생성한 대시보드를 선택합니다
# MAGIC 1. **OK**을 클릭하여 시각화를 추가합니다
# MAGIC 
# MAGIC ## Add a Line Plot Visualization to your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Add Visualization** button
# MAGIC 1. Click on the name (should default to something like **`Visualization 1`**) and change the name to **Avg User Ping**
# MAGIC 1. Select **`Line`** for the **Visualization Type**
# MAGIC 1. Set **`end_time`** for the **X Column**
# MAGIC 1. Set **`avg_ping`** for the **Y Columns**
# MAGIC 1. Set **`user_id`** for the **Group by**
# MAGIC 1. Click **Save**
# MAGIC 1. Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC 1. Select the dashboard you created earlier
# MAGIC 1. Click **OK** to add your visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 요약 통계를 보고하는 쿼리 만들기
# MAGIC 단계:
# MAGIC 1. 아래 셀을 실행합니다.
# MAGIC 1. 이 쿼리를 **Ping Summary** 이름으로 저장합니다.
# MAGIC 1. 쿼리를 실행하여 현재 결과를 계산합니다.
# MAGIC 
# MAGIC ## Create a Query to Report Summary Statistics
# MAGIC Steps:
# MAGIC 1. Execute the cell below.
# MAGIC 1. Save this query with the name **Ping Summary**.
# MAGIC 1. Run the query to calculate the current results.

# COMMAND ----------

DA.generate_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 대시보드에 요약 테이블 추가
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 화면 하단에 세 개의 수직 점이 있는 버튼을 클릭하고 **Add to Dashboard** 를 선택합니다.
# MAGIC 1. 이전에 생성한 대시보드를 선택합니다
# MAGIC 1. **OK** 을 클릭하여 시각화를 추가합니다
# MAGIC 
# MAGIC ## Add the Summary Table to your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the button with three vertical dots at the bottom of the screen and select **Add to Dashboard**.
# MAGIC 1. Select the dashboard you created earlier
# MAGIC 1. Click **OK** to add your visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 대시보드 검토 및 새로 고침
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 왼쪽 사이드바를 사용하여 **Dashboards*로 이동합니다
# MAGIC 1. 쿼리를 추가한 대시보드 찾기
# MAGIC 1. 파란색 **Refresh** 버튼을 클릭하여 대시보드를 업데이트합니다
# MAGIC 1. **Schedule** 버튼을 클릭하여 대시보드 스케줄링 옵션을 검토합니다
# MAGIC   * 업데이트할 대시보드를 예약하면 해당 대시보드와 관련된 모든 쿼리가 실행됩니다
# MAGIC   * 지금은 대시보드 예약 안 함
# MAGIC 
# MAGIC ## Review and Refresh your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Use the left side bar to navigate to **Dashboards**
# MAGIC 1. Find the dashboard you've added your queries to
# MAGIC 1. Click the blue **Refresh** button to update your dashboard
# MAGIC 1. Click the **Schedule** button to review dashboard scheduling options
# MAGIC   * Note that scheduling a dashboard to update will execute all queries associated with that dashboard
# MAGIC   * Do not schedule the dashboard at this time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 대시보드 공유
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 파란색 **Share** 버튼을 클릭합니다
# MAGIC 1. 맨 위 필드에서  **All Users**를 선택합니다
# MAGIC 1. 오른쪽 필드에서 **Can Run**을 선택합니다
# MAGIC 1. **Add**를 클릭합니다
# MAGIC 1. **Credentials*를 **viewer로 실행*으로 변경합니다
# MAGIC 
# MAGIC **참고**: 다른 사용자는 테이블 ACL을 사용하여 기본 데이터베이스 및 테이블에 대한 권한이 부여되지 않았기 때문에 현재 대시보드를 실행할 수 있는 권한이 없어야 합니다. 다른 사용자가 대시보드에 대한 업데이트를 트리거할 수 있도록 하려면 **Run as owner**에 대한 권한을 부여하거나 쿼리에서 참조하는 테이블에 대한 권한을 추가해야 합니다.
# MAGIC 
# MAGIC ## Share your Dashboard
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the blue **Share** button
# MAGIC 1. Select **All Users** from the top field
# MAGIC 1. Choose **Can Run** from the right field
# MAGIC 1. Click **Add**
# MAGIC 1. Change the **Credentials** to **Run as viewer**
# MAGIC 
# MAGIC **NOTE**: At present, no other users should have any permissions to run your dashboard, as they have not been granted permissions to the underlying databases and tables using Table ACLs. If you wish other users to be able to trigger updates to your dashboard, you will either need to grant them permissions to **Run as owner** or add permissions for the tables referenced in your queries.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 알림 설정
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 왼쪽 사이드바를 사용하여 **Alerts**로 이동합니다
# MAGIC 1. 오른쪽 상단의 **Create Alert**를 클릭합니다
# MAGIC 1. 화면 왼쪽 위에 있는 필드를 클릭하여 경고 이름을 **`<your_initials> Count Check`** 로 지정합니다
# MAGIC 1. **User Counts** 쿼리를 선택합니다
# MAGIC 1. **Trigger when** 옵션의 경우 다음을 구성합니다:
# MAGIC   * **Value column**: **`total_records`**
# MAGIC   * **Condition**: **`>`**
# MAGIC   * **Threshold**: **`15`**
# MAGIC 1. **Refresh**의 경우 **Never**를 선택합니다
# MAGIC 1. **Create Alert**을 클릭합니다
# MAGIC 1. 다음 화면에서 오른쪽 상단의 파란색 **Refresh**를 클릭하여 경고를 평가합니다
# MAGIC 
# MAGIC ## Set Up an Alert
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Use the left side bar to navigate to **Alerts**
# MAGIC 1. Click **Create Alert** in the top right
# MAGIC 1. Click the field at the top left of the screen to give the alert a name **`<your_initials> Count Check`**
# MAGIC 1. Select your **User Counts** query
# MAGIC 1. For the **Trigger when** options, configure:
# MAGIC   * **Value column**: **`total_records`**
# MAGIC   * **Condition**: **`>`**
# MAGIC   * **Threshold**: **`15`**
# MAGIC 1. For **Refresh**, select **Never**
# MAGIC 1. Click **Create Alert**
# MAGIC 1. On the next screen, click the blue **Refresh** in the top right to evaluate the alert

# COMMAND ----------

# MAGIC %md
# MAGIC ## 경고 대상 옵션 검토
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 경고 미리 보기에서 화면 오른쪽의 **Destinations** 오른쪽에 있는 파란색 **Add** 버튼을 클릭합니다
# MAGIC 1. 팝업 창 하단에서 를 찾아 **Create new destinations in Alert Destinations** 메시지의 파란색 텍스트를 클릭합니다
# MAGIC 1. 사용 가능한 경고 옵션 검토
# MAGIC 
# MAGIC 
# MAGIC ## Review Alert Destination Options
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. From the preview of your alert, click the blue **Add** button to the right of **Destinations** on the right side of the screen
# MAGIC 1. At the bottom of the window that pops up, locate the and click the blue text in the message **Create new destinations in Alert Destinations**
# MAGIC 1. Review the available alerting options

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>