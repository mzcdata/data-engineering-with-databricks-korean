# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lakehouse의 엔드 투 엔드 ETL
# MAGIC 
# MAGIC 이 노트북에서는 과정을 통해 학습한 개념을 종합하여 데이터 파이프라인 예제를 완성합니다.
# MAGIC 
# MAGIC 다음은 이 실습을 성공적으로 완료하는 데 필요한 기술 및 과제의 전체 목록입니다:
# MAGIC * Databricks 노트북을 사용하여 SQL 및 Python에서 쿼리 작성
# MAGIC * 데이터베이스, 표 및 보기 작성 및 수정
# MAGIC * 멀티홉 아키텍처에서 증분 데이터 처리를 위한 자동 로더 및 스파크 구조화 스트리밍 사용
# MAGIC * 델타 라이브 테이블 SQL 구문 사용
# MAGIC * 연속 처리를 위해 델타 라이브 테이블 파이프라인 구성
# MAGIC * Datbricks 작업을 사용하여 Repos에 저장된 노트북에서 태스크 조정
# MAGIC * Datbricks 작업에 대한 시간순 예약 설정
# MAGIC * Databricks SQL에서 쿼리 정의
# MAGIC * Databricks SQL에서 시각화 만들기
# MAGIC * 메트릭 및 결과를 검토하기 위한 Datbricks SQL 대시보드 정의
# MAGIC 
# MAGIC ## End-to-End ETL in the Lakehouse
# MAGIC 
# MAGIC In this notebook, you will pull together concepts learned throughout the course to complete an example data pipeline.
# MAGIC 
# MAGIC The following is a non-exhaustive list of skills and tasks necessary to successfully complete this exercise:
# MAGIC * Using Databricks notebooks to write queries in SQL and Python
# MAGIC * Creating and modifying databases, tables, and views
# MAGIC * Using Auto Loader and Spark Structured Streaming for incremental data processing in a multi-hop architecture
# MAGIC * Using Delta Live Table SQL syntax
# MAGIC * Configuring a Delta Live Table pipeline for continuous processing
# MAGIC * Using Databricks Jobs to orchestrate tasks from notebooks stored in Repos
# MAGIC * Setting chronological scheduling for Databricks Jobs
# MAGIC * Defining queries in Databricks SQL
# MAGIC * Creating visualizations in Databricks SQL
# MAGIC * Defining Databricks SQL dashboards to review metrics and results

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Run Setup
# MAGIC 다음 셀을 실행하여 이 실습과 관련된 모든 데이터베이스 및 디렉토리를 재설정합니다.
# MAGIC 
# MAGIC Run the following cell to reset all the databases and directories associated with this lab.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC ## 토지 초기 데이터
# MAGIC 계속하기 전에 일부 데이터로 착륙 구역을 시드하십시오.
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT 파이프라인 생성 및 구성
# MAGIC **참고**: 여기에 있는 지침과 DLT를 사용하는 이전 연구소의 지침의 주요 차이점은 이 경우 **Production** 모드에서 **Continuous** 실행을 위한 파이프라인을 설정한다는 것입니다.
# MAGIC 
# MAGIC ## Create and Configure a DLT Pipeline
# MAGIC **NOTE**: The main difference between the instructions here and in previous labs with DLT is that in this instance, we will be setting up our pipeline for **Continuous** execution in **Production** mode.

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 단계:
# MAGIC 1. 사이드바에서 **Jobs** 버튼을 클릭합니다.
# MAGIC 1. **Delta Live Tables** 탭을 선택합니다.
# MAGIC 1. **Create Pipeline**을 클릭합니다.
# MAGIC 1. **Pipeline Name** 을 입력하십시오. 이러한 이름은 고유해야 하므로 위의 셀에 제공된 **Pipeline Name** 을 사용하는 것이 좋습니다.
# MAGIC 1. **Notebook Libraries**의 경우 탐색기를 사용하여 노트북 **DE 12.2.2L - DLT Task**를 찾아 선택합니다
# MAGIC     * 또는 위에서 지정한 **Notebook Path**를 복사하여 제공된 필드에 붙여넣을 수 있습니다.
# MAGIC 1. 소스 구성
# MAGIC     * **`Add configuration`** 를 클릭합니다
# MAGIC     * **Key** 필드에 **`source`** 를 입력합니다
# MAGIC     * 위에서 지정한 **Source* 값을 **`Value`** 필드에 입력합니다
# MAGIC 1. **Target** 필드에서 위 셀의 **Target** 옆에 인쇄된 데이터베이스 이름을 지정합니다.<br/>
# MAGIC 이것은 **`dbacademy_<username>_dewd_cap_12`** 패턴을 따라야 합니다
# MAGIC 1. **Storage location** 필드에서 위에 인쇄된 디렉토리를 복사합니다.
# MAGIC 1. **Pipeline Mode**의 경우 **Continuous*를 선택합니다
# MAGIC 1. **Enable autoscaling** 상자의 선택을 취소합니다
# MAGIC 1. 작업자 수를 **`1`**(1명)으로 설정합니다
# MAGIC 1. **Create**를 클릭합니다.
# MAGIC 1. UI 업데이트 후 **Development** 모드에서 **Production** 모드로 변경합니다
# MAGIC 
# MAGIC 이것은 인프라 구축을 시작해야 한다.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar.
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook **DE 12.2.2L - DLT Task**.
# MAGIC     * Alternatively, you can copy the **Notebook Path** specified above and paste it into the field provided.
# MAGIC 1. Configure the Source
# MAGIC     * Click **`Add configuration`**
# MAGIC     * Enter the word **`source`** in the **Key** field
# MAGIC     * Enter the **Source** value specified above to the **`Value`** field
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above.<br/>
# MAGIC This should follow the pattern **`dbacademy_<username>_dewd_cap_12`**
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC 1. For **Pipeline Mode**, select **Continuous**
# MAGIC 1. Uncheck the **Enable autoscaling** box
# MAGIC 1. Set the number of workers to **`1`** (one)
# MAGIC 1. Click **Create**.
# MAGIC 1. After the UI updates, change from **Development** to **Production** mode
# MAGIC 
# MAGIC This should begin the deployment of infrastructure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 노트북 작업 예약
# MAGIC 
# MAGIC 우리의 DLT 파이프라인은 데이터가 도착하는 대로 데이터를 처리하도록 설정되어 있습니다. 
# MAGIC 
# MAGIC 이 기능이 작동하는 것을 확인할 수 있도록 매 분마다 새로운 데이터 배치를 제공하도록 노트북을 예약할 것입니다.
# MAGIC 
# MAGIC 시작하기 전에 다음 셀을 실행하여 이 단계에 사용된 값을 가져옵니다.
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC Our DLT pipeline is setup to process data as soon as it arrives. 
# MAGIC 
# MAGIC We'll schedule a notebook to land a new batch of data each minute so we can see this functionality in action.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 단계:
# MAGIC 1. Databricks 왼쪽 탐색 모음을 사용하여 Jobs UI로 이동합니다.
# MAGIC 1. 파란색 **Create Job** 버튼을 클릭합니다
# MAGIC 1. 작업을 구성합니다:
# MAGIC     1. 작업 이름에 **Land-Data**를 입력합니다
# MAGIC     1. 노트북 피커를 사용하여 노트북 **DE 12.2.3L - Land New Data**를 선택합니다.
# MAGIC     1. **Cluster** 드롭다운의 **Existing All Purpose Cluster**에서 클러스터를 선택합니다
# MAGIC     1. **Create**을 클릭합니다
# MAGIC 1. 화면 왼쪽 위에서 작업 이름(작업이 아닌)을 **`Land-Data`** (기본값)에서 이전 셀에 제공된 **Job Name**으로 변경합니다.    
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **참고**: 만능 클러스터를 선택하면 만능 컴퓨팅으로 청구되는 방법에 대한 경고가 표시됩니다. 프로덕션 작업은 훨씬 낮은 비율로 청구되므로 워크로드에 적합한 크기의 새 작업 클러스터에 대해 항상 예약해야 합니다.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Land-Data** for the task name
# MAGIC     1. Select the notebook **DE 12.2.3L - Land New Data** using the notebook picker
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Cluster**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen rename the job (not the task) from **`Land-Data`** (the defaulted value) to the **Job Name** provided for you in the previous cell.    
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 작업의 시간순 스케줄 설정
# MAGIC 단계:
# MAGIC 1. **Jobs UI**로 이동하여 방금 만든 작업을 클릭합니다.
# MAGIC 1. 오른쪽 측면 패널에서 **Schedule** 섹션을 찾습니다.
# MAGIC 1. **Edit schedule** 버튼을 클릭하여 예약 옵션을 살펴보십시오.
# MAGIC 1. **Schedule type** 필드를 **Manual**에서 **Scheduled**로 변경합니다. 그러면 시간 예약 UI가 표시됩니다.
# MAGIC 1. **00**에서 **Every 2**, **Minutes**을 업데이트하도록 일정을 설정합니다 
# MAGIC 1. **Save**을 클릭합니다
# MAGIC 
# MAGIC **참고**: 원하는 경우 **지금 실행**을 클릭하여 첫 번째 실행을 트리거하거나, 다음 분이 다 될 때까지 기다려 예약이 성공적으로 작동했는지 확인할 수 있습니다.
# MAGIC 
# MAGIC ## Set a Chronological Schedule for your Job
# MAGIC Steps:
# MAGIC 1. Navigate to the **Jobs UI** and click on the job you just created.
# MAGIC 1. Locate the **Schedule** section in the side panel on the right.
# MAGIC 1. Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 1. Change the **Schedule type** field from **Manual** to **Scheduled**, which will bring up a chron scheduling UI.
# MAGIC 1. Set the schedule to update **Every 2**, **Minutes** from **00** 
# MAGIC 1. Click **Save**
# MAGIC 
# MAGIC **NOTE**: If you wish, you can click **Run now** to trigger the first run, or wait until the top of the next minute to make sure your scheduling has worked successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DBSQL로 쿼리하기 위한 DLT 이벤트 메트릭 등록
# MAGIC 
# MAGIC 다음 셀은 SQL 문을 출력하여 DBSQL에서 쿼리하기 위해 DLT 이벤트 로그를 대상 데이터베이스에 등록합니다.
# MAGIC 
# MAGIC DBSQL Query Editor(DBSQL 쿼리 편집기)를 사용하여 출력 코드를 실행하여 이러한 테이블과 보기를 등록합니다. 
# MAGIC 
# MAGIC 각 항목을 탐색하고 기록된 이벤트 메트릭을 기록합니다.
# MAGIC 
# MAGIC ## Register DLT Event Metrics for Querying with DBSQL
# MAGIC 
# MAGIC The following cell prints out SQL statements to register the DLT event logs to your target database for querying in DBSQL.
# MAGIC 
# MAGIC Execute the output code with the DBSQL Query Editor to register these tables and views. 
# MAGIC 
# MAGIC Explore each and make note of the logged event metrics.

# COMMAND ----------

DA.generate_register_dlt_event_metrics_sql()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 골드 테이블에 쿼리 정의
# MAGIC 
# MAGIC **daily_patient_avg** 테이블은 DLT 파이프라인을 통해 새 데이터 배치가 처리될 때마다 자동으로 업데이트됩니다. 이 테이블에 대해 쿼리가 실행될 때마다 DBSQL은 최신 버전이 있는지 확인한 다음 사용 가능한 최신 버전의 결과를 구체화합니다.
# MAGIC 
# MAGIC 다음 셀을 실행하여 데이터베이스 이름으로 쿼리를 인쇄합니다. DBSQL 쿼리로 저장합니다.
# MAGIC 
# MAGIC ## Define a Query on the Gold Table
# MAGIC 
# MAGIC The **daily_patient_avg** table is automatically updated each time a new batch of data is processed through the DLT pipeline. Each time a query is executed against this table, DBSQL will confirm if there is a newer version and then materialize results from the newest available version.
# MAGIC 
# MAGIC Run the following cell to print out a query with your database name. Save this as a DBSQL query.

# COMMAND ----------

DA.generate_daily_patient_avg()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 선 그림 시각화 추가
# MAGIC 
# MAGIC 시간 경과에 따른 환자 평균의 추세를 추적하려면 선 그림을 만들어 새 대시보드에 추가합니다.
# MAGIC 
# MAGIC 다음 설정을 사용하여 선 그림을 만듭니다:
# MAGIC * **X Column**: **`date`**
# MAGIC * **Y Column**: **`avg_heartrate`**
# MAGIC * **Group By**: **`name`**
# MAGIC 
# MAGIC 이 시각화를 대시보드에 추가합니다.
# MAGIC 
# MAGIC ## Add a Line Plot Visualization
# MAGIC 
# MAGIC To track trends in patient averages over time, create a line plot and add it to a new dashboard.
# MAGIC 
# MAGIC Create a line plot with the following settings:
# MAGIC * **X Column**: **`date`**
# MAGIC * **Y Column**: **`avg_heartrate`**
# MAGIC * **Group By**: **`name`**
# MAGIC 
# MAGIC Add this visualization to a dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 데이터 처리 진행률 추적
# MAGIC 
# MAGIC 아래 코드는 DLT 이벤트 로그에서 **`flow_name`**, **`timestamp`**, **`num_output_rows`** 를 추출합니다.
# MAGIC 
# MAGIC 이 쿼리를 DBSQL에 저장한 다음 다음을 보여주는 막대 그래프 시각화를 정의합니다:
# MAGIC * **X Column**: **`timestamp`**
# MAGIC * **Y Column**: **`num_output_rows`**
# MAGIC * **Group By**: **`flow_name`**
# MAGIC 
# MAGIC 대시보드에 시각화를 추가합니다.
# MAGIC 
# MAGIC ## Track Data Processing Progress
# MAGIC 
# MAGIC The code below extracts the **`flow_name`**, **`timestamp`**, and **`num_output_rows`** from the DLT event logs.
# MAGIC 
# MAGIC Save this query in DBSQL, then define a bar plot visualization that shows:
# MAGIC * **X Column**: **`timestamp`**
# MAGIC * **Y Column**: **`num_output_rows`**
# MAGIC * **Group By**: **`flow_name`**
# MAGIC 
# MAGIC Add your visualization to your dashboard.

# COMMAND ----------

DA.generate_visualization_query()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 대시보드 새로 고침 및 결과 추적
# MAGIC 
# MAGIC 위의 Jobs와 함께 예약된 **Land-Data** 노트북에는 12개의 데이터 배치가 있으며, 각 데이터는 환자의 소규모 표본 추출을 위한 한 달 간의 기록을 나타냅니다. 지침에 따라 구성된 대로 이러한 모든 데이터 배치가 트리거되고 처리되는 데는 20분 이상이 소요됩니다(데이터브릭 작업이 2분마다 실행되도록 예약했으며 데이터 배치는 초기 수집 후 파이프라인을 통해 매우 빠르게 처리됩니다).
# MAGIC 
# MAGIC 대시보드를 새로 고치고 시각화를 검토하여 처리된 데이터 배치 수를 확인합니다. 여기에 설명된 지침을 따른 경우 DLT 메트릭으로 추적되는 12개의 고유한 흐름 업데이트가 있어야 합니다 모든 원본 데이터가 아직 처리되지 않은 경우 Databricks Jobs UI로 돌아가서 수동으로 추가 배치를 트리거할 수 있습니다.
# MAGIC 
# MAGIC ## Refresh your Dashboard and Track Results
# MAGIC 
# MAGIC The **Land-Data** notebook scheduled with Jobs above has 12 batches of data, each representing a month of recordings for our small sampling of patients. As configured per our instructions, it should take just over 20 minutes for all of these batches of data to be triggered and processed (we scheduled the Databricks Job to run every 2 minutes, and batches of data will process through our pipeline very quickly after initial ingestion).
# MAGIC 
# MAGIC Refresh your dashboard and review your visualizations to see how many batches of data have been processed. (If you followed the instructions as outlined here, there should be 12 distinct flow updates tracked by your DLT metrics.) If all source data has not yet been processed, you can go back to the Databricks Jobs UI and manually trigger additional batches.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 모든 구성이 완료되면 이제 노트북[DE 12.2.4L - Final Steps]($/DE 12.2.4L - Final Steps)에서 랩의 마지막 부분으로 이동할 수 있습니다
# MAGIC 
# MAGIC With everything configured, you can now continue to the final part of your lab in the notebook [DE 12.2.4L - Final Steps]($./DE 12.2.4L - Final Steps)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>