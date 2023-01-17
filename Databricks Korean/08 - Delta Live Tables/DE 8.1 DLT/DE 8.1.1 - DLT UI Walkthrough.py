# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 델타 라이브 테이블 UI 사용
# MAGIC 
# MAGIC 이 데모에서는 DLT UI에 대해 살펴보겠습니다. 
# MAGIC 
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * DLT 파이프라인 배포
# MAGIC * 결과 DAG 탐색
# MAGIC * 파이프라인 업데이트 실행
# MAGIC * 메트릭 보기
# MAGIC 
# MAGIC # Using the Delta Live Tables UI
# MAGIC 
# MAGIC This demo will explore the DLT UI. 
# MAGIC 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Deploy a DLT pipeline
# MAGIC * Explore the resultant DAG
# MAGIC * Execute an update of the pipeline
# MAGIC * Look at metrics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Run Setup
# MAGIC 다음 셀은 이 데모를 재설정하도록 구성되어 있습니다.
# MAGIC 
# MAGIC The following cell is configured to reset this demo.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.1.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 다음 셀을 실행하여 다음 구성 단계에서 사용할 값을 출력합니다.
# MAGIC 
# MAGIC Execute the following cell to print out values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 파이프라인 생성 및 구성
# MAGIC 
# MAGIC 이 섹션에서는 코스웨어와 함께 제공된 노트북을 사용하여 파이프라인을 작성합니다. 우리는 다음 수업에서 노트의 내용을 살펴볼 것입니다.
# MAGIC 
# MAGIC 1. 사이드바에서 **Jobs** 버튼을 클릭합니다.
# MAGIC 1. **Delta Live Tables** 탭을 선택합니다.
# MAGIC 1. **Create Pipeline**을 클릭합니다.
# MAGIC 1. **Product Edition**을 **Advanced**로 유지합니다.
# MAGIC 1. **Pipeline Name**을 입력하십시오. 이러한 이름은 고유해야 하므로 위 셀에서 제공하는 **`Pipeline Name`** 을 사용하는 것이 좋습니다.
# MAGIC 1. **Notebook Libraries**의 경우 탐색기를 사용하여 **DE 8.1.2 - SQL for Delta Live Tables**이라는 지원 노트북을 찾아 선택합니다.   
# MAGIC    * 또는 위의 셀에서 제공하는 **`Notebook Path`** 를 복사하여 제공된 필드에 붙여넣을 수 있습니다.
# MAGIC    * 이 문서는 표준 Datbricks Notebook이지만 SQL 구문은 DLT 테이블 선언에 특화되어 있습니다.
# MAGIC    * 우리는 다음 연습에서 구문을 탐구할 것이다.
# MAGIC 1. **Target** 필드에서 위 셀의 **`Target`** 옆에 출력된 데이터베이스 이름을 지정합니다.<br/>
# MAGIC 이는 **`dbacademy_<username>_dewd_dlt_demo_81`** 패턴을 따라야 합니다
# MAGIC    * 이 필드는 선택 사항입니다. 지정하지 않으면 테이블이 전이에 등록되지 않지만 DBFS에서 계속 사용할 수 있습니다. 이 옵션에 대한 자세한 내용은 <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank"> 설명서</a>를 참조하십시오.
# MAGIC 1. **Storage Location** 필드에서 위의 셀에서 인쇄한 **'Storage Location'** 경로를 복사합니다.
# MAGIC    * 이 선택적 필드를 사용하여 로그, 테이블 및 파이프라인 실행과 관련된 기타 정보를 저장할 위치를 지정할 수 있습니다. 
# MAGIC    * 지정하지 않으면 DLT는 자동으로 디렉토리를 생성합니다.
# MAGIC 1. **Pipeline Mode**의 경우 **Triggered**을 선택합니다
# MAGIC    * 이 필드는 파이프라인의 실행 방법을 지정합니다.
# MAGIC    * **Triggered** 파이프라인이 한 번 실행된 후 다음 수동 또는 예약된 업데이트가 있을 때까지 종료됩니다.
# MAGIC    * **Continuous** 파이프라인이 지속적으로 실행되어 새로운 데이터가 도착할 때마다 수집됩니다. 대기 시간 및 비용 요구 사항에 따라 모드를 선택합니다.
# MAGIC 1. **Enable autoscaling** 상자의 선택을 취소하고 작업자 수를 **`1`**(1명)로 설정합니다.
# MAGIC    * **Enable autoscaling**, **Min Workers* 및 **Max Workers**는 파이프라인을 처리하는 기본 클러스터에 대한 작업자 구성을 제어합니다. 제공된 DBU 추정치는 대화형 클러스터를 구성할 때 제공된 것과 유사합니다.
# MAGIC 1. **Create**를 클릭합니다.
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC In this section you will create a pipeline using a notebook provided with the courseware. We'll explore the contents of the notebook in the following lesson.
# MAGIC 
# MAGIC 1. Click the **Jobs** button on the sidebar.
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Leave **Product Edition** as **Advanced**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **`Pipeline Name`** provided by the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the companion notebook called **DE 8.1.2 - SQL for Delta Live Tables**.   
# MAGIC    * Alternatively, you can copy the **`Notebook Path`** provided by the cell above and paste it into the provided field.
# MAGIC    * Even though this document is a standard Databricks Notebook, the SQL syntax is specialized to DLT table declarations.
# MAGIC    * We will be exploring the syntax in the exercise that follows.
# MAGIC 1. In the **Target** field, specify the database name printed out next to **`Target`** in the cell above.<br/>
# MAGIC This should follow the pattern **`dbacademy_<username>_dewd_dlt_demo_81`**
# MAGIC    * This field is optional; if not specified, then tables will not be registered to a metastore, but will still be available in the DBFS. Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">documentation</a> for more information on this option.
# MAGIC 1. In the **Storage location** field, copy the **`Storage Location`** path printed by the cell above.
# MAGIC    * This optional field allows the user to specify a location to store logs, tables, and other information related to pipeline execution. 
# MAGIC    * If not specified, DLT will automatically generate a directory.
# MAGIC 1. For **Pipeline Mode**, select **Triggered**
# MAGIC    * This field specifies how the pipeline will be run.
# MAGIC    * **Triggered** pipelines run once and then shut down until the next manual or scheduled update.
# MAGIC    * **Continuous** pipelines run continuously, ingesting new data as it arrives. Choose the mode based on latency and cost requirements.
# MAGIC 1. Uncheck the **Enable autoscaling** box, and set the number of workers to **`1`** (one).
# MAGIC    * **Enable autoscaling**, **Min Workers** and **Max Workers** control the worker configuration for the underlying cluster processing the pipeline. Notice the DBU estimate provided, similar to that provided when configuring interactive clusters.
# MAGIC 1. Click **Create**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 파이프라인 실행
# MAGIC 
# MAGIC 파이프라인이 생성되면 이제 파이프라인을 실행합니다.
# MAGIC 
# MAGIC 1. **Development**를 선택하여 개발 모드에서 파이프라인을 실행합니다. 
# MAGIC   * 개발 모드는 오류를 쉽게 식별하고 수정할 수 있도록 클러스터를 재사용하고 재시도를 사용하지 않도록 설정하여 보다 신속한 반복 개발을 제공합니다.
# MAGIC   * 이 기능에 대한 자세한 내용은 <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank"> 설명서</a>를 참조하십시오.
# MAGIC 2. **Start**을 클릭합니다.
# MAGIC 
# MAGIC 클러스터가 프로비저닝되는 동안 초기 실행에는 몇 분이 걸립니다. 
# MAGIC 
# MAGIC 후속 주행은 상당히 빨라집니다.
# MAGIC 
# MAGIC ## Run a Pipeline
# MAGIC 
# MAGIC With a pipeline created, you will now run the pipeline.
# MAGIC 
# MAGIC 1. Select **Development** to run the pipeline in development mode. 
# MAGIC   * Development mode provides for more expeditious iterative development by reusing the cluster (as opposed to creating a new cluster for each run) and disabling retries so that you can readily identify and fix errors.
# MAGIC   * Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentation</a> for more information on this feature.
# MAGIC 2. Click **Start**.
# MAGIC 
# MAGIC The initial run will take several minutes while a cluster is provisioned. 
# MAGIC 
# MAGIC Subsequent runs will be appreciably quicker.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAG 탐색
# MAGIC 
# MAGIC 파이프라인이 완료되면 실행 흐름이 그래프로 표시됩니다. 
# MAGIC 
# MAGIC 테이블을 선택하면 세부 정보가 검토됩니다.
# MAGIC 
# MAGIC **sales_orders_cleaned**를 선택합니다. **Data Quality** 섹션에 보고된 결과에 주목하십시오. 이 흐름에는 선언된 데이터 기대가 있기 때문에 이러한 메트릭은 여기서 추적됩니다. 제약 조건이 위반 레코드를 출력에 포함할 수 있는 방식으로 선언되므로 레코드가 삭제되지 않습니다. 이것은 다음 연습에서 더 자세히 다룰 것이다.
# MAGIC 
# MAGIC ## Exploring the DAG
# MAGIC 
# MAGIC As the pipeline completes, the execution flow is graphed. 
# MAGIC 
# MAGIC Selecting the tables reviews the details.
# MAGIC 
# MAGIC Select **sales_orders_cleaned**. Notice the results reported in the **Data Quality** section. Because this flow has data expectations declared, those metrics are tracked here. No records are dropped because the constraint is declared in a way that allows violating records to be included in the output. This will be covered in more details in the next exercise.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>