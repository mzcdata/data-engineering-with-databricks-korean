# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 데이터브릭을 사용하여 작업 오케스트레이션
# MAGIC 
# MAGIC Databricks Jobs UI에 대한 새로운 업데이트는 작업의 일부로 여러 작업을 예약할 수 있는 기능을 추가하여 Databricks Job이 대부분의 프로덕션 워크로드에 대한 오케스트레이션을 완전히 처리할 수 있도록 합니다.
# MAGIC 
# MAGIC 여기서는 노트북을 트리거된 독립 실행형 작업으로 예약하는 단계를 검토한 다음 DLT 파이프라인을 사용하여 종속 작업을 추가합니다. 
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * 노트북을 데이터브릭 작업으로 예약
# MAGIC * 작업 예약 옵션 및 클러스터 유형 간의 차이점 설명
# MAGIC * 작업 실행을 검토하여 진행률을 추적하고 결과를 확인합니다
# MAGIC * DLT 파이프라인을 데이터브릭 작업으로 예약
# MAGIC * Databricks Jobs UI를 사용하여 태스크 간의 선형 종속성 구성
# MAGIC 
# MAGIC # Orchestrating Jobs with Databricks
# MAGIC 
# MAGIC New updates to the Databricks Jobs UI have added the ability to schedule multiple tasks as part of a job, allowing Databricks Jobs to fully handle orchestration for most production workloads.
# MAGIC 
# MAGIC Here, we'll start by reviewing the steps for scheduling a notebook as a triggered standalone job, and then add a dependent job using a DLT pipeline. 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Schedule a notebook as a Databricks Job
# MAGIC * Describe job scheduling options and differences between cluster types
# MAGIC * Review Job Runs to track progress and see results
# MAGIC * Schedule a DLT pipeline as a Databricks Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Jobs UI

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-9.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 파이프라인 생성 및 구성
# MAGIC 여기서 생성하는 파이프라인은 이전 장치의 파이프라인과 거의 같습니다.
# MAGIC 
# MAGIC 이 과정에서는 예약된 작업의 일부로 사용합니다.
# MAGIC 
# MAGIC 다음 셀을 실행하여 다음 구성 단계에서 사용할 값을 출력합니다.
# MAGIC 
# MAGIC ## Create and configure a pipeline
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
# MAGIC ## 파이프라인 생성 및 구성
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 사이드바에서 **Jobs** 버튼을 클릭합니다,
# MAGIC 1. **Delta Live Tables** 탭을 선택합니다.
# MAGIC 1. **Create Pipeline**을 클릭합니다.
# MAGIC 1. **Pipeline Name**을 입력하십시오. 이러한 이름은 고유해야 하므로 위의 셀에 제공된 **Pipeline Name**을 사용하는 것이 좋습니다.
# MAGIC 1. **Notebook Libraries**의 경우 탐색기를 사용하여 **DE 9.1.3 - DLT Job**이라는 지원 노트북을 찾아 선택합니다. 또는 **Notebook Path**를 복사하여 제공된 필드에 붙여넣을 수 있습니다.
# MAGIC 1. **Target** 필드에서 위 셀의 **Target** 옆에 인쇄된 데이터베이스 이름을 지정합니다.<br/>
# MAGIC 이는 **`dbacademy_<username>_dewd_dlt_demo_91`** 패턴을 따라야 합니다
# MAGIC 1. **Storage location** 필드에서 위에 인쇄된 디렉토리를 복사합니다.
# MAGIC 1. **Pipeline Mode**의 경우 **Triggered**을 선택합니다
# MAGIC 1. **Enable autoscaling** 상자의 선택을 취소합니다
# MAGIC 1. 작업자 수를 **`1`**(1명)으로 설정합니다
# MAGIC 1. **Create**를 클릭합니다.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **참고**: 이 수업 후반부에 작업에 의해 실행되므로 이 파이프라인을 직접 실행하지는 않을 것입니다.<br/>
# MAGIC 빠른 테스트를 원한다면 지금 **Start** 버튼을 클릭하면 됩니다.
# MAGIC 
# MAGIC ## Create and configure a pipeline
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar,
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipeline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the companion notebook called **DE 9.1.3 - DLT Job**. Alternatively, you can copy the **Notebook Path** and paste it into the field provided.
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above.<br/>
# MAGIC This should follow the pattern **`dbacademy_<username>_dewd_dlt_demo_91`**
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC 1. For **Pipeline Mode**, select **Triggered**
# MAGIC 1. Uncheck the **Enable autoscaling** box
# MAGIC 1. Set the number of workers to **`1`** (one)
# MAGIC 1. Click **Create**.
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
# MAGIC 여기서는 다음 노트북을 예약하는 것으로 시작하겠습니다
# MAGIC 
# MAGIC 단계:
# MAGIC 1. Databricks 왼쪽 탐색 모음을 사용하여 Jobs UI로 이동합니다.
# MAGIC 1. 파란색 **'Create Job'** 버튼을 클릭합니다
# MAGIC 1. 작업을 구성합니다:
# MAGIC     1. 작업 이름에 **`reset`** 를 입력합니다
# MAGIC     1. 노트북 피커를 사용하여 노트북 **`DE 9.1.2 - Reset`** 을 선택합니다.
# MAGIC     1. **Cluster** 드롭다운의 **Existing All Purpose Cluster**에서 클러스터를 선택합니다
# MAGIC     1. **Create**을 클릭합니다
# MAGIC 1. 화면 왼쪽 위에서 작업 이름(작업이 아님)을 **`reset`**(기본값)에서 이전 셀에 제공된 **Job Name**으로 변경합니다.
# MAGIC 1. 오른쪽 상단의 파란색 **Run now** 버튼을 클릭하여 작업을 시작합니다.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **참고**: 다목적 클러스터를 선택하면 범용 컴퓨팅으로 청구되는 방법에 대한 경고가 표시됩니다. 프로덕션 작업은 훨씬 낮은 비율로 청구되므로 워크로드에 적합한 크기의 새 작업 클러스터에 대해 항상 예약해야 합니다.
# MAGIC 
# MAGIC Here, we'll start by scheduling the next notebook
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **`Create Job`** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **`reset`** for the task name
# MAGIC     1. Select the notebook **`DE 9.1.2 - Reset`** using the notebook picker.
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Clusters**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen rename the job (not the task) from **`reset`** (the defaulted value) to the **Job Name** provided for you in the previous cell.
# MAGIC 1. Click the blue **Run now** button in the top right to start the job.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데이터브릭 작업의 시간순 예약
# MAGIC 
# MAGIC 작업 UI 오른쪽의 **Job Details** 섹션 바로 아래에 **Schedule**라는 섹션이 있습니다.
# MAGIC 
# MAGIC 예약 옵션을 보려면 **Edit schedule** 버튼을 클릭하십시오.
# MAGIC 
# MAGIC **Schedule type** 필드를 **Manual**에서 **Schedule**로 변경하면 시간순 스케줄링 UI가 표시됩니다.
# MAGIC 
# MAGIC 이 UI는 작업의 시간순 예약을 설정하기 위한 광범위한 옵션을 제공합니다. UI로 구성된 설정은 시간 구문으로도 출력할 수 있으며, UI에서 사용할 수 없는 사용자 지정 구성이 필요한 경우 편집할 수 있습니다.
# MAGIC 
# MAGIC 이제 작업 세트는 **Manual** 스케줄링으로 종료하겠습니다.
# MAGIC 
# MAGIC ## Chron Scheduling of Databricks Jobs
# MAGIC 
# MAGIC Note that on the right hand side of the Jobs UI, directly under the **Job Details** section is a section labeled **Schedule**.
# MAGIC 
# MAGIC Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 
# MAGIC Changing the **Schedule type** field from **Manual** to **Scheduled** will bring up a chron scheduling UI.
# MAGIC 
# MAGIC This UI provides extensive options for setting up chronological scheduling of your Jobs. Settings configured with the UI can also be output in chron syntax, which can be edited if custom configuration not available with the UI is needed.
# MAGIC 
# MAGIC At this time, we'll leave our job set with **Manual** scheduling.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 실행 검토
# MAGIC 
# MAGIC 현재 구성된 단일 노트북은 단일 노트북만 예약할 수 있었던 기존 Databricks Jobs UI와 동일한 성능을 제공합니다.
# MAGIC 
# MAGIC 작업 실행을 검토하려면
# MAGIC 1. 화면 왼쪽 상단에서 **Runs** 탭을 선택합니다(현재 **작업** 탭에 있어야 함)
# MAGIC 1. 직장을 구하세요.  **the job is still running** **Active runs** 섹션 아래에 있습니다. **the job finished running** **Completed runs** 섹션 아래에 표시됩니다
# MAGIC 1. **Start time** 열에서 타임스탬프 필드를 클릭하여 Output 세부 정보를 엽니다
# MAGIC 1. **작업이 여전히 실행 중인 경우 **`Pending`** 또는 **`Running`** 의 **Status** 상태로 노트북의 활성 상태가 오른쪽 패널에 표시됩니다. **the job has completed** 오른쪽 패널에 **`Succeeded`** 또는 **`Failed`** 의 **Status**와 함께 노트북의 전체 실행이 표시됩니다
# MAGIC   
# MAGIC 노트북은 마법 명령 **`%run`** 을 사용하여 상대 경로를 사용하여 추가 노트북을 호출합니다. 이 과정에서는 다루지 않지만, Databricks Repos에 추가된 <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank"> 새로운 기능을 사용하면 상대 경로를 사용하여 Python 모듈을 로드할 수 있습니다.
# MAGIC 
# MAGIC 예약된 노트북의 실제 결과는 새 작업 및 파이프라인을 위해 환경을 재설정하는 것입니다.
# MAGIC   
# MAGIC ## Review Run
# MAGIC 
# MAGIC As currently configured, our single notebook provides identical performance to the legacy Databricks Jobs UI, which only allowed a single notebook to be scheduled.
# MAGIC 
# MAGIC To Review the Job Run
# MAGIC 1. Select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
# MAGIC 1. Find your job. If **the job is still running**, it will be under the **Active runs** section. If **the job finished running**, it will be under the **Completed runs** section
# MAGIC 1. Open the Output details by click on the timestamp field under the **Start time** column
# MAGIC 1. If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel. If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel
# MAGIC   
# MAGIC The notebook employs the magic command **`%run`** to call an additional notebook using a relative path. Note that while not covered in this course, <a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">new functionality added to Databricks Repos allows loading Python modules using relative paths</a>.
# MAGIC 
# MAGIC The actual outcome of the scheduled notebook is to reset the environment for our new job and pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DLT 파이프라인을 태스크로 예약
# MAGIC 
# MAGIC 이 단계에서는 이 과정을 시작할 때 구성한 작업이 성공한 후 실행할 DLT 파이프라인을 추가합니다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 화면 왼쪽 상단에 현재 **실행** 탭이 선택되어 있는 것이 표시됩니다. **작업** 탭을 클릭하십시오.
# MAGIC 1. 화면 중앙 하단에 **+** 가 있는 큰 파란색 원을 클릭하여 새 작업을 추가합니다
# MAGIC     1. **Task name**을 **`dlt`**로 지정합니다
# MAGIC     1. **Type**에서 **`Delta Live Tables pipeline`** 을 선택합니다
# MAGIC     1. **Pipeline** 필드를 클릭하고 이전에 구성한 DLT 파이프라인을 선택합니다.<br/>
# MAGIC     참고: 파이프라인은 **Jobs-Demo-91**로 시작하여 사용자의 이메일 주소로 끝납니다.
# MAGIC     1. **Depends on** 필드는 이전에 정의한 작업으로 기본 설정되지만 이전에 지정한 값 **reset** 에서 **Jobs-Demo-91-youremailaddress**와 같은 이름으로 이름이 변경되었을 수 있습니다.
# MAGIC     1. 파란색 **Create task** 버튼을 클릭합니다
# MAGIC 
# MAGIC 이제 상자 2개와 아래쪽 화살표가 있는 화면이 표시됩니다. 
# MAGIC 
# MAGIC  **`reset`** 작업(**Jobs-Demo-91-youremailaddress**와 같은 이름으로 변경될 수 있음)이 맨 위에 표시됩니다, 
# MAGIC 당신의 **`dlt`** 작업으로 이어집니다. 
# MAGIC 
# MAGIC 이 시각화는 이러한 작업 간의 종속성을 나타냅니다.
# MAGIC 
# MAGIC **Run now**을 클릭하여 작업을 실행합니다.
# MAGIC 
# MAGIC **참고**: 작업 및 파이프라인에 대한 인프라가 구축되기 때문에 몇 분 정도 기다려야 할 수도 있습니다.
# MAGIC 
# MAGIC ## Schedule a DLT Pipeline as a Task
# MAGIC 
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured at the start of this lesson.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. At the top left of your screen, you'll see the **Runs** tab is currently selected; click the **Tasks** tab.
# MAGIC 1. Click the large blue circle with a **+** at the center bottom of the screen to add a new task
# MAGIC     1. Specify the **Task name** as **`dlt`**
# MAGIC     1. From **Type**, select **`Delta Live Tables pipeline`**
# MAGIC     1. Click the **Pipeline** field and select the DLT pipeline you configured previously<br/>
# MAGIC     Note: The pipeline will start with **Jobs-Demo-91** and will end with your email address.
# MAGIC     1. The **Depends on** field defaults to your previously defined task but may have renamed itself from the value **reset** that you specified previously to something like **Jobs-Demo-91-youremailaddress**.
# MAGIC     1. Click the blue **Create task** button
# MAGIC 
# MAGIC You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC 
# MAGIC Your **`reset`** task (possibly renamed to something like **Jobs-Demo-91-youremailaddress**) will be at the top, 
# MAGIC leading into your **`dlt`** task. 
# MAGIC 
# MAGIC This visualization represents the dependencies between these tasks.
# MAGIC 
# MAGIC Click **Run now** to execute your job.
# MAGIC 
# MAGIC **NOTE**: You may need to wait a few minutes as infrastructure for your job and pipeline is deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 다중 작업 실행 결과 검토
# MAGIC 
# MAGIC 작업 완료 여부에 따라 **Runs** 탭을 다시 선택한 다음 **Active runs** 또는 **Completed runs**에서 최근 실행을 선택합니다.
# MAGIC 
# MAGIC 작업에 대한 시각화는 현재 실행 중인 작업을 반영하기 위해 실시간으로 업데이트되며 작업 실패가 발생할 경우 색이 변경됩니다. 
# MAGIC 
# MAGIC 작업 상자를 클릭하면 예약된 노트북이 UI에 렌더링됩니다. 
# MAGIC 
# MAGIC CLI 또는 REST API를 사용하여 작업을 스케줄링하는 워크로드가 있는 경우 작업을 구성하고 결과를 얻는 데 사용되는 JSON 구조를 참조하십시오. has에서 UI에 대한 유사한 업데이트를 참조하십시오.
# MAGIC 
# MAGIC **참고**: 작업으로 예약된 DLT 파이프라인은 실행 GUI에서 직접 결과를 렌더링하지 않습니다. 대신 예약된 파이프라인에 대한 DLT 파이프라인 GUI로 돌아갑니다.
# MAGIC 
# MAGIC ## Review Multi-Task Run Results
# MAGIC 
# MAGIC Select the **Runs** tab again and then the most recent run under **Active runs** or **Completed runs** depending on if the job has completed or not.
# MAGIC 
# MAGIC The visualizations for tasks will update in real time to reflect which tasks are actively running, and will change colors if task failures occur. 
# MAGIC 
# MAGIC Clicking on a task box will render the scheduled notebook in the UI. 
# MAGIC 
# MAGIC You can think of this as just an additional layer of orchestration on top of the previous Databricks Jobs UI, if that helps; note that if you have workloads scheduling jobs with the CLI or REST API, <a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">the JSON structure used to configure and get results about jobs has seen similar updates to the UI</a>.
# MAGIC 
# MAGIC **NOTE**: At this time, DLT pipelines scheduled as tasks do not directly render results in the Runs GUI; instead, you will be directed back to the DLT Pipeline GUI for the scheduled Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>