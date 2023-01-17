# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Lab: 데이터브릭을 사용하여 작업 조정
# MAGIC 
# MAGIC 이 실습에서는 다음으로 구성된 멀티태스킹 작업을 구성합니다:
# MAGIC * 저장소 디렉터리에 새 데이터 배치를 저장하는 노트북
# MAGIC * 일련의 테이블을 통해 이 데이터를 처리하는 델타 라이브 테이블 파이프라인
# MAGIC * 이 파이프라인에서 생성한 골드 테이블과 DLT에서 출력한 다양한 메트릭을 쿼리하는 노트북
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
# MAGIC * 노트북을 데이터브릭 작업으로 예약
# MAGIC * DLT 파이프라인을 데이터브릭 작업으로 예약
# MAGIC * Databricks Jobs UI를 사용하여 태스크 간의 선형 종속성 구성
# MAGIC 
# MAGIC # Lab: Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC In this lab, you'll be configuring a multi-task job comprising of:
# MAGIC * A notebook that lands a new batch of data in a storage directory
# MAGIC * A Delta Live Table pipeline that processes this data through a series of tables
# MAGIC * A notebook that queries the gold table produced by this pipeline as well as various metrics output by DLT
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Schedule a notebook as a Databricks Job
# MAGIC * Schedule a DLT pipeline as a Databricks Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Jobs UI

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-9.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC 계속하기 전에 일부 데이터로 착륙 구역을 시드하십시오. 나중에 추가 데이터를 저장하려면 이 명령을 다시 실행합니다.
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. You will re-run this command to land additional data later.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 파이프라인 생성 및 구성
# MAGIC 
# MAGIC 여기서 생성하는 파이프라인은 이전 장치의 파이프라인과 거의 같습니다.
# MAGIC 
# MAGIC 이 과정에서는 예약된 작업의 일부로 사용합니다.
# MAGIC 
# MAGIC 다음 셀을 실행하여 다음 구성 단계에서 사용할 값을 출력합니다.
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC The pipeline we create here is nearly identical to the one in the previous unit.
# MAGIC 
# MAGIC We will use it as part of a scheduled job in this lesson.
# MAGIC 
# MAGIC Execute the following cell to print out the values that will be used during the following configuration steps.

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 사이드바에서 **Jobs** 버튼을 클릭합니다.
# MAGIC 1. **Delta Live Tables** 탭을 선택합니다.
# MAGIC 1. **Create Pipeline**을 클릭합니다.
# MAGIC 1. **Pipeline Name**을 입력하십시오. 이러한 이름은 고유해야 하므로 위의 셀에 제공된 **Pipeline Name**을 사용하는 것이 좋습니다.
# MAGIC 1. **Notebook Libraries**의 경우 탐색기를 사용하여 **DE 9.2.3L - DLT Job**이라는 지원 노트북을 찾아 선택합니다.
# MAGIC     * 또는 위에서 지정한 **Notebook Path**를 복사하여 제공된 필드에 붙여넣을 수 있습니다.
# MAGIC 1. 소스 구성
# MAGIC     * **`Add configuration`** 를 클릭합니다
# MAGIC     * **Key** 필드에 **`source`** 를 입력합니다
# MAGIC     * 위에서 지정한 **Source* 값을 **`Value`** 필드에 입력합니다
# MAGIC 1. **Target** 필드에서 위 셀의 **Target** 옆에 인쇄된 데이터베이스 이름을 지정합니다.<br/>
# MAGIC 이는 **`dbacademy_<username>_dewd_jobs_lab_92`** 패턴을 따라야 합니다
# MAGIC 1. **Storage location** 필드에서 위에 인쇄된 디렉토리를 복사합니다.
# MAGIC 1. **Pipeline Mode**의 경우 **Triggered**을 선택합니다
# MAGIC 1. **Enable autoscaling** 상자의 선택을 취소합니다
# MAGIC 1. 작업자 수를 **`1`**(1명)으로 설정합니다
# MAGIC 1. **Create**를 클릭합니다.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **참고**: 이 수업 후반부에 작업에 의해 실행되므로 이 파이프라인을 직접 실행하지는 않을 것입니다.<br/>
# MAGIC 빠른 테스트를 원한다면 지금 **Start** 버튼을 클릭하면 됩니다.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar.
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipeline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the companion notebook called **DE 9.2.3L - DLT Job**.
# MAGIC     * Alternatively, you can copy the **Notebook Path** specified above and paste it into the field provided.
# MAGIC 1. Configure the Source
# MAGIC     * Click **`Add configuration`**
# MAGIC     * Enter the word **`source`** in the **Key** field
# MAGIC     * Enter the **Source** value specified above to the **`Value`** field
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above.<br/>
# MAGIC This should follow the pattern **`dbacademy_<username>_dewd_jobs_lab_92`**
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC 1. For **Pipeline Mode**, select **Triggered**
# MAGIC 1. Uncheck the **Enable autoscaling** box
# MAGIC 1. Set the number of workers to **`1`** (one)
# MAGIC 1. Click **Create**.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: we won't be executing this pipline directly as it will be executed by our job later in this lesson,<br/>
# MAGIC but if you want to test it real quick, you can click the **Start** button now.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 노트북 작업 예약
# MAGIC 
# MAGIC 작업 UI를 사용하여 작업이 여러 개인 워크로드를 조정할 때는 항상 단일 작업을 예약하는 것으로 시작합니다.
# MAGIC 
# MAGIC 시작하기 전에 다음 셀을 실행하여 이 단계에 사용된 값을 가져옵니다.
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC When using the Jobs UI to orchestrate a workload with multiple tasks, you'll always begin by scheduling a single task.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 여기서는 노트북 배치 작업을 예약하는 것으로 시작하겠습니다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. Databricks 왼쪽 탐색 모음을 사용하여 Jobs UI로 이동합니다.
# MAGIC 1. 파란색 **Create Job** 버튼을 클릭합니다
# MAGIC 1. 작업을 구성합니다:
# MAGIC     1. 작업 이름에 **Batch-Job**을 입력합니다
# MAGIC     1. 노트북 피커를 사용하여 노트북 **DE 9.2.2L - Batch Job**을 선택합니다
# MAGIC     1. **Cluster* 드롭다운의 **Existing All Purpose Cluster**에서 클러스터를 선택합니다
# MAGIC     1. **Create**을 클릭합니다
# MAGIC 1. 화면 왼쪽 위에서 작업 이름(작업이 아닌)을 **'Batch-Job'**(기본값)에서 이전 셀에 제공된 **Job Name**으로 변경합니다.    
# MAGIC 1. 작업을 정말 빠르게 테스트하려면 오른쪽 상단의 파란색 **Run now** 버튼을 클릭합니다.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **참고**: 만능 클러스터를 선택하면 만능 컴퓨팅으로 청구되는 방법에 대한 경고가 표시됩니다. 프로덕션 작업은 훨씬 낮은 비율로 청구되므로 워크로드에 적합한 크기의 새 작업 클러스터에 대해 항상 예약해야 합니다.
# MAGIC 
# MAGIC Here, we'll start by scheduling the notebook batch job.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Batch-Job** for the task name
# MAGIC     1. Select the notebook **DE 9.2.2L - Batch Job** using the notebook picker
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Cluster**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen rename the job (not the task) from **`Batch-Job`** (the defaulted value) to the **Job Name** provided for you in the previous cell.    
# MAGIC 1. Click the blue **Run now** button in the top right to start the job to test the job real quick.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DLT 파이프라인을 태스크로 예약
# MAGIC 
# MAGIC 이 단계에서는 이 과정을 시작할 때 구성한 작업이 성공한 후 실행할 DLT 파이프라인을 추가합니다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 화면 왼쪽 위에서 **Tasks** 탭이 아직 선택되지 않은 경우 클릭합니다.
# MAGIC 1. 화면 중앙 하단에 **+** 가 있는 큰 파란색 원을 클릭하여 새 작업을 추가합니다
# MAGIC     1. **Task name**을 **DLT-Pipeline*으로 지정합니다
# MAGIC     1. **Type**에서 **`Delta Live Tables pipeline`** 을 선택합니다
# MAGIC     1. **Pipeline** 필드를 클릭하고 이전에 구성한 DLT 파이프라인을 선택합니다.<br/>
# MAGIC     참고: 파이프라인은 **Jobs-Labs-92**로 시작하여 이메일 주소로 끝납니다.
# MAGIC     1. **Depends on** 필드는 이전에 정의한 작업으로 기본 설정되지만 이전에 지정한 값 **reset*에서 **Jobs-Lab-92-youremailaddress**와 같은 이름으로 이름이 변경되었을 수 있습니다.
# MAGIC     1. 파란색 **Create task** 버튼을 클릭합니다
# MAGIC 
# MAGIC 이제 상자 2개와 아래쪽 화살표가 있는 화면이 표시됩니다. 
# MAGIC 
# MAGIC **`Batch-Job`** 작업(**Jobs-Labs-92-youremailaddress**와 같은 이름으로 변경될 수 있음)이 맨 위에 있습니다, 
# MAGIC **`DLT-Pipeline`** 작업으로 이어집니다.
# MAGIC 
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, click the **Tasks** tab if it is not already selected.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC     1. Specify the **Task name** as **DLT-Pipeline**
# MAGIC     1. From **Type**, select **`Delta Live Tables pipeline`**
# MAGIC     1. Click the **Pipeline** field and select the DLT pipeline you configured previously<br/>
# MAGIC     Note: The pipeline will start with **Jobs-Labs-92** and will end with your email address.
# MAGIC     1. The **Depends on** field defaults to your previously defined task but may have renamed itself from the value **reset** that you specified previously to something like **Jobs-Lab-92-youremailaddress**.
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`Batch-Job`** task (possibly renamed to something like **Jobs-Labs-92-youremailaddress**) will be at the top, 
# MAGIC leading into your **`DLT-Pipeline`** task.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 추가 노트북 작업 예약
# MAGIC 
# MAGIC DLT 파이프라인에 정의된 일부 DLT 메트릭과 골드 테이블을 쿼리하는 추가 노트북이 제공되었습니다. 
# MAGIC 
# MAGIC 우리는 이것을 우리 일의 마지막 과제로 추가할 것이다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 화면 왼쪽 위에서 **Tasks** 탭이 아직 선택되지 않은 경우 클릭합니다.
# MAGIC 1. 화면 중앙 하단에 **+** 가 있는 큰 파란색 원을 클릭하여 새 작업을 추가합니다
# MAGIC     1. **Task name**을 **Query-Results**로 지정합니다
# MAGIC     1. **Type**을 **Notebook**으로 설정한 상태로 둡니다
# MAGIC     1. 노트북 피커를 사용하여 노트북 **DE 9.2.4L - Query Results Job**을 선택합니다
# MAGIC     1. **Depends on** 필드의 기본값은 이전에 정의한 작업 **DLT-Pipeline*입니다
# MAGIC     1. **Cluster* 드롭다운의 **Existing All Purpose Cluster**에서 클러스터를 선택합니다
# MAGIC     1. 파란색 **Create task** 버튼을 클릭합니다
# MAGIC 
# MAGIC 이 작업을 실행하려면 화면 오른쪽 상단에 있는 파란색 **Run now** 버튼을 클릭하십시오.
# MAGIC 
# MAGIC **Runs** 탭에서 **Active runs**  섹션에서 이 실행의 시작 시간을 클릭하고 작업 진행 상황을 시각적으로 추적할 수 있습니다.
# MAGIC 
# MAGIC 모든 작업이 성공했으면 각 작업의 내용을 검토하여 예상되는 동작을 확인합니다.
# MAGIC 
# MAGIC ## Schedule an Additional Notebook Task
# MAGIC 
# MAGIC An additional notebook has been provided which queries some of the DLT metrics and the gold table defined in the DLT pipeline. 
# MAGIC 
# MAGIC We'll add this as a final task in our job.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, click the **Tasks** tab if it is not already selected.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC     1. Specify the **Task name** as **Query-Results**
# MAGIC     1. Leave the **Type** set to **Notebook**
# MAGIC     1. Select the notebook **DE 9.2.4L - Query Results Job** using the notebook picker
# MAGIC     1. Note that the **Depends on** field defaults to your previously defined task, **DLT-Pipeline**
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Cluster**, select your cluster
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC Click the blue **Run now** button in the top right of the screen to run this job.
# MAGIC 
# MAGIC From the **Runs** tab, you will be able to click on the start time for this run under the **Active runs** section and visually track task progress.
# MAGIC 
# MAGIC Once all your tasks have succeeded, review the contents of each task to confirm expected behavior.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>