# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Databrick SQL 탐색 및 엔드포인트 연결
# MAGIC 
# MAGIC * Databricks SQL로 이동  
# MAGIC   * 사이드바의 작업영역 옵션(데이터브릭스 로고 바로 아래)에서 SQL을 선택했는지 확인합니다
# MAGIC * SQL 끝점이 켜져 있고 액세스할 수 있는지 확인합니다
# MAGIC   * 사이드바에서 SQL 엔드포인트로 이동
# MAGIC   * SQL 끝점이 존재하고 상태가 **`Running`** 인 경우 이 끝점을 사용합니다
# MAGIC   * SQL 끝점이 있지만 **`Stopped`** 인 경우 이 옵션이 있으면 **`Start`** 버튼을 클릭합니다(**참고** : 사용 가능한 가장 작은 끝점 시작) 
# MAGIC   * 끝점이 없고 옵션이 있으면 **`Create SQL Endpoint`** 를 클릭하고 인식할 끝점의 이름을 지정하고 클러스터 크기를 2X-Small로 설정합니다. 다른 모든 옵션은 기본값으로 유지합니다.
# MAGIC   * SQL 끝점을 만들거나 SQL 끝점에 연결할 방법이 없는 경우 작업 공간 관리자에게 문의하여 계속하려면 Datbricks SQL의 계산 리소스에 대한 액세스를 요청해야 합니다.
# MAGIC * Databricks SQL의 홈 페이지로 이동
# MAGIC   * 측면 탐색 모음의 맨 위에 있는 Datbricks 로고를 클릭합니다
# MAGIC * **Sample dashboards**를 찾은 후 **`Visit gallery`** 을 클릭합니다
# MAGIC * **Retail Revenue & Supply Chain** 옵션 옆에 있는 **`Import`** 를 클릭합니다
# MAGIC   * SQL 끝점을 사용할 수 있다고 가정하면 대시보드를 로드하고 즉시 결과를 표시합니다
# MAGIC   * 오른쪽 상단에서 **Refresh**를 클릭합니다(기본 데이터는 변경되지 않았지만 변경 사항을 선택하는 데 사용되는 버튼입니다)
# MAGIC   
# MAGIC # Navigating Databricks SQL and Attaching to Endpoints
# MAGIC 
# MAGIC * Navigate to Databricks SQL  
# MAGIC   * Make sure that SQL is selected from the workspace option in the sidebar (directly below the Databricks logo)
# MAGIC * Make sure a SQL endpoint is on and accessible
# MAGIC   * Navigate to SQL endpoints in the sidebar
# MAGIC   * If a SQL endpoint exists and has the State **`Running`**, you'll use this endpoint
# MAGIC   * If a SQL endpoint exists but is **`Stopped`**, click the **`Start`** button if you have this option (**NOTE**: Start the smallest endpoint you have available to you) 
# MAGIC   * If no endpoints exist and you have the option, click **`Create SQL Endpoint`**; name the endpoint something you'll recognize and set the cluster size to 2X-Small. Leave all other options as default.
# MAGIC   * If you have no way to create or attach to a SQL endpoint, you'll need to contact a workspace administrator and request access to compute resources in Databricks SQL to continue.
# MAGIC * Navigate to home page in Databricks SQL
# MAGIC   * Click the Databricks logo at the top of the side nav bar
# MAGIC * Locate the **Sample dashboards** and click **`Visit gallery`**
# MAGIC * Click **`Import`** next to the **Retail Revenue & Supply Chain** option
# MAGIC   * Assuming you have a SQL endpoint available, this should load a dashboard and immediately display results
# MAGIC   * Click **Refresh** in the top right (the underlying data has not changed, but this is the button that would be used to pick up changes)
# MAGIC 
# MAGIC 
# MAGIC # DBSQL 대시보드 업데이트
# MAGIC * 사이드바 탐색기를 사용하여 **Dashboards**를 찾습니다
# MAGIC   * 방금 로드한 샘플 대시보드를 찾으십시오. 대시보드의 이름은 **Retail Revenue & Supply Chain**이어야 하며 **`Created By`**  필드 아래에 사용자 이름이 있어야 합니다. **참고**: 오른쪽에 있는 **My Dashboards** 옵션은 작업 공간에서 다른 대시보드를 필터링하는 바로 가기 역할을 할 수 있습니다
# MAGIC   * 대시보드 이름을 클릭하여 봅니다
# MAGIC * **Shifts in Pricing Priorities**  그림 뒤에 있는 쿼리 보기
# MAGIC   * 그림 위에 마우스를 놓으면 세 개의 수직 점이 나타납니다. 이것들을
# MAGIC   * 나타나는 메뉴에서 **View Query**를 선택합니다
# MAGIC * 이 그림을 채우는 데 사용되는 SQL 코드 검토
# MAGIC   * 소스 테이블을 식별하는 데 3계층 네임스페이스가 사용됩니다. 이는 Unity 카탈로그에서 지원되는 새로운 기능의 미리 보기입니다
# MAGIC   * 질의 결과를 미리 보려면 화면 오른쪽 상단의 **`Run`** 을 클릭하십시오
# MAGIC * 시각화 검토
# MAGIC   * 쿼리에서 **Table** 탭을 선택해야 합니다. **Price by Priority over Time**을 클릭하여 플롯의 미리 보기로 전환합니다
# MAGIC   * 화면 하단의 **Edit Visualization**을 클릭하여 설정을 검토합니다
# MAGIC   * 설정 변경이 시각화에 어떤 영향을 미치는지 알아보기
# MAGIC   * 변경 내용을 적용하려면 **Save**을 클릭하고, 그렇지 않으면 **Cancel**를 클릭하십시오
# MAGIC * 쿼리 편집기로 돌아가 시각화 이름 오른쪽에 있는 **Add Visualization** 버튼을 클릭합니다
# MAGIC   * 막대 그래프 만들기
# MAGIC   * **X Column**을 **`Date`** 로 설정합니다
# MAGIC   * **Y Column**을 **`Total Price`** 로 설정합니다
# MAGIC   * **Group by** **`Priority`**
# MAGIC   * **Stacking**을 **`Stack`** 으로 설정합니다
# MAGIC   * 다른 모든 설정을 기본값으로 유지
# MAGIC   * **Save**을 클릭합니다
# MAGIC * 쿼리 편집기로 돌아가 편집할 이 시각화의 기본 이름을 클릭하고 시각화 이름을 **`Stacked Price`** 로 변경합니다
# MAGIC * 화면 하단을 추가하고 **`Edit Visualization`** 버튼 왼쪽에 있는 세 개의 수직 점을 클릭합니다
# MAGIC   * 메뉴에서 **Add to Dashboard**를 선택합니다
# MAGIC   * **`Retail Revenue & Supply Chain`** 대시보드 선택
# MAGIC * 이 변경 내용을 보려면 대시보드로 다시 이동하십시오
# MAGIC 
# MAGIC # Updating a DBSQL Dashboard
# MAGIC * Use the sidebar navigator to find the **Dashboards**
# MAGIC   * Locate the sample dashboard you just loaded; it should be called **Retail Revenue & Supply Chain** and have your username under the **`Created By`** field. **NOTE**: the **My Dashboards** option on the right hand side can serve as a shortcut to filter out other dashboards in the workspace
# MAGIC   * Click on the dashboard name to view it
# MAGIC * View the query behind the **Shifts in Pricing Priorities** plot
# MAGIC   * Hover over the plot; three vertical dots should appear. Click on these
# MAGIC   * Select **View Query** from the menu that appears
# MAGIC * Review the SQL code used to populate this plot
# MAGIC   * Note that 3 tier namespacing is used to identify the source table; this is a preview of new functionality to be supported by Unity Catalog
# MAGIC   * Click **`Run`** in the top right of the screen to preview the results of the query
# MAGIC * Review the visualization
# MAGIC   * Under the query, a tab named **Table** should be selected; click **Price by Priority over Time** to switch to a preview of your plot
# MAGIC   * Click **Edit Visualization** at the bottom of the screen to review settings
# MAGIC   * Explore how changing settings impacts your visualization
# MAGIC   * If you wish to apply your changes, click **Save**; otherwise, click **Cancel**
# MAGIC * Back in the query editor, click the **Add Visualization** button to the right of the visualization name
# MAGIC   * Create a bar graph
# MAGIC   * Set the **X Column** as **`Date`**
# MAGIC   * Set the **Y Column** as **`Total Price`**
# MAGIC   * **Group by** **`Priority`**
# MAGIC   * Set **Stacking** to **`Stack`**
# MAGIC   * Leave all other settings as defaults
# MAGIC   * Click **Save**
# MAGIC * Back in the query editor, click the default name for this visualization to edit it; change the visualization name to **`Stacked Price`**
# MAGIC * Add the bottom of the screen, click the three vertical dots to the left of the **`Edit Visualization`** button
# MAGIC   * Select **Add to Dashboard** from the menu
# MAGIC   * Select your **`Retail Revenue & Supply Chain`** dashboard
# MAGIC * Navigate back to your dashboard to view this change
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # 새 쿼리 만들기
# MAGIC 
# MAGIC * 사이드바를 사용하여 **Queries*로 이동합니다
# MAGIC * **`Create Query`** 버튼을 클릭합니다
# MAGIC * 끝점에 연결되어 있는지 확인합니다. **Schema Browser**에서 현재 전이를 클릭하고 **`samples`** 을 선택합니다. 
# MAGIC   * **`tpch`** 데이터베이스 선택
# MAGIC   * 스키마의 미리 보기를 보려면 **`partsupp`** 테이블을 클릭하십시오
# MAGIC   * **`partsupp`** 테이블 이름 위를 마우스로 가리키면서 **>>** 버튼을 클릭하여 쿼리 텍스트에 테이블 이름을 삽입합니다
# MAGIC * 첫 번째 쿼리를 작성합니다:
# MAGIC   * **`SELECT * FROM`** 마지막 단계에서 가져온 전체 이름을 사용하여 **`partsupp`** 테이블을 선택한 후 **Run**을 클릭하여 결과를 미리 봅니다
# MAGIC   * 이 쿼리를 **`GROUP BY ps_partkey`** 로 수정하고 **`ps_partkey`** 및 **`sum(ps_availqty)`** 를 반환하십시오. 결과를 미리 보려면 **Run**을 클릭하십시오
# MAGIC   * 쿼리를 업데이트하여 **`total_availqty`** 라는 이름의 두 번째 열을 별칭으로 지정하고 쿼리를 다시 실행하십시오
# MAGIC * 쿼리 저장
# MAGIC   * 화면 오른쪽 상단 근처의 **Run** 옆에 있는 **Save** 버튼을 클릭합니다
# MAGIC   * 쿼리에 기억할 이름을 지정합니다
# MAGIC * 대시보드에 쿼리 추가
# MAGIC   * 화면 하단에 있는 세 개의 세로 단추를 클릭합니다
# MAGIC   * **Add to Dashboard**를 클릭합니다
# MAGIC   * **`Retail Revenue & Supply Chain`** 대시보드 선택
# MAGIC * 이 변경 내용을 보려면 대시보드로 다시 이동하십시오
# MAGIC   * 시각화 구성을 변경하려면 화면 오른쪽 상단에 있는 세 개의 세로 단추를 클릭하고 나타나는 메뉴에서 **Edit**을 클릭하면 시각화를 끌어 크기를 조정할 수 있습니다
# MAGIC 
# MAGIC # Create a New Query
# MAGIC 
# MAGIC * Use the sidebar to navigate to **Queries**
# MAGIC * Click the **`Create Query`** button
# MAGIC * Make sure you are connected to an endpoint. In the **Schema Browser**, click on the current metastore and select **`samples`**. 
# MAGIC   * Select the **`tpch`** database
# MAGIC   * Click on the **`partsupp`** table to get a preview of the schema
# MAGIC   * While hovering over the **`partsupp`** table name, click the **>>** button to insert the table name into your query text
# MAGIC * Write your first query:
# MAGIC   * **`SELECT * FROM`** the **`partsupp`** table using the full name imported in the last step; click **Run** to preview results
# MAGIC   * Modify this query to **`GROUP BY ps_partkey`** and return the **`ps_partkey`** and **`sum(ps_availqty)`**; click **Run** to preview results
# MAGIC   * Update your query to alias the 2nd column to be named **`total_availqty`** and re-execute the query
# MAGIC * Save your query
# MAGIC   * Click the **Save** button next to **Run** near the top right of the screen
# MAGIC   * Give the query a name you'll remember
# MAGIC * Add the query to your dashboard
# MAGIC   * Click the three vertical buttons at the bottom of the screen
# MAGIC   * Click **Add to Dashboard**
# MAGIC   * Select your **`Retail Revenue & Supply Chain`** dashboard
# MAGIC * Navigate back to your dashboard to view this change
# MAGIC   * If you wish to change the organization of visualizations, click the three vertical buttons in the top right of the screen; click **Edit** in the menu that appears and you'll be able to drag and resize visualizations

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>