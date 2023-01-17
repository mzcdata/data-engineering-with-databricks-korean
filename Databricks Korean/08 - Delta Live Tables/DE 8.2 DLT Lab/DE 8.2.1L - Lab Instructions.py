# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC # Lab: SQL 노트북을 델타 라이브 테이블로 마이그레이션
# MAGIC 
# MAGIC 이 노트북은 실습의 전반적인 구조를 설명하고, 실습 환경을 구성하고, 시뮬레이션 데이터 스트리밍을 제공하며, 완료되면 정리를 수행합니다. 일반적으로 프로덕션 파이프라인 시나리오에서는 이러한 노트북이 필요하지 않습니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
# MAGIC * 기존 데이터 파이프라인을 델타 라이브 테이블로 변환
# MAGIC 
# MAGIC # Lab: Migrating SQL Notebooks to Delta Live Tables
# MAGIC 
# MAGIC This notebook describes the overall structure for the lab exercise, configures the environment for the lab, provides simulated data streaming, and performs cleanup once you are done. A notebook like this is not typically needed in a production pipeline scenario.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Convert existing data pipelines to Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 사용된 데이터 세트
# MAGIC 
# MAGIC 이 데모는 인공적으로 생성된 단순화된 의료 데이터를 사용한다. 우리의 두 데이터 세트의 스키마는 아래와 같다. 다양한 단계에서 이러한 스키마를 조작합니다.
# MAGIC 
# MAGIC #### 녹음
# MAGIC 주 데이터 세트는 JSON 형식으로 제공된 의료 기기의 심박수 기록을 사용한다.
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified artificially generated medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.
# MAGIC 
# MAGIC #### Recordings
# MAGIC The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC 이러한 데이터는 나중에 이름별로 환자를 식별하기 위해 외부 시스템에 저장된 환자 정보의 정적 테이블과 결합됩니다.
# MAGIC 
# MAGIC #### PII
# MAGIC These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 먼저 다음 셀을 실행하여 실험실 환경을 구성합니다.
# MAGIC 
# MAGIC Begin by running the following cell to configure the lab environment.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.1L

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
# MAGIC 1. 사이드바에서 **Jobs** 버튼을 클릭한 다음 **Delta Live Table** 탭을 선택합니다.
# MAGIC 1. **Create Pipeline**을 클릭합니다.
# MAGIC 1. **Product Edition**을 **Advanced**로 유지합니다.
# MAGIC 1. **Pipeline Name**을 입력하십시오. 이러한 이름은 고유해야 하므로 위의 셀에 제공된 **Pipeline Name**을 사용하는 것이 좋습니다.
# MAGIC 1. **Notebook Libraries**의 경우 탐색기를 사용하여 **`DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab`** 노트북을 찾아 선택합니다.
# MAGIC 1. 소스 구성
# MAGIC     * **`Add configuration`** 를 클릭합니다
# MAGIC     * **Key** 필드에 **`source`** 를 입력합니다
# MAGIC     * 위에서 지정한 **Source* 값을 **'Value'** 필드에 입력합니다
# MAGIC 1. 아래 **Target** 필드에서 **`Target`** 옆에 인쇄된 데이터베이스 이름을 입력합니다.
# MAGIC 1. 아래 **Storage Location** 필드에 **`Storage Location`** 옆에 인쇄된 위치를 입력합니다.
# MAGIC 1. **Pipeline Mode**를 **Triggered**으로 설정합니다.
# MAGIC 1. 자동 스케일링을 비활성화합니다.
# MAGIC 1. **`workers`** 의 수를 **`1`** (1개)로 설정합니다.
# MAGIC 1. **Create**를 클릭합니다.
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC 1. Click the **Jobs** button on the sidebar, then select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Leave **Product Edition** as **Advanced**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook **`DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab`**.
# MAGIC 1. Configure the Source
# MAGIC     * Click **`Add configuration`**
# MAGIC     * Enter the word **`source`** in the **Key** field
# MAGIC     * Enter the **Source** value specified above to the **`Value`** field
# MAGIC 1. Enter the database name printed next to **`Target`** below in the **Target** field.
# MAGIC 1. Enter the location printed next to **`Storage Location`** below in the **Storage Location** field.
# MAGIC 1. Set **Pipeline Mode** to **Triggered**.
# MAGIC 1. Disable autoscaling.
# MAGIC 1. Set the number of **`workers`** to **`1`** (one).
# MAGIC 1. Click **Create**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DLT Pipeline 노트북 열기 및 완료
# MAGIC 
# MAGIC 지원 노트북[DE 8.2.2L - SQL 파이프라인을 DLT Lab으로 마이그레이션]($/DE 8.2.2L - SQL 파이프라인을 DLT Lab으로 마이그레이션), <br/>에서 작업을 수행하게 됩니다
# MAGIC 최종적으로 파이프라인으로 배포하게 됩니다.
# MAGIC 
# MAGIC 노트북을 열고 여기에 제공된 지침에 따라 메시지가 표시되는 셀을 채웁니다. <br/>
# MAGIC 이전 섹션에서 작업한 것과 유사한 멀티홉 아키텍처를 구현합니다.
# MAGIC 
# MAGIC ## Open and Complete DLT Pipeline Notebook
# MAGIC 
# MAGIC You will perform your work in the companion notebook [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab),<br/>
# MAGIC which you will ultimately deploy as a pipeline.
# MAGIC 
# MAGIC Open the Notebook and, following the guidelines provided therein, fill in the cells where prompted to<br/>
# MAGIC implement a multi-hop architecture similar to the one we worked with in the previous section.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 파이프라인 실행
# MAGIC 
# MAGIC 실행 중에 동일한 클러스터를 재사용하여 개발 라이프사이클을 가속화하는 **Development** 모드를 선택합니다.<br/>
# MAGIC 또한 작업이 실패할 때 자동 재시도를 해제합니다.
# MAGIC 
# MAGIC **Start**를 클릭하여 테이블에 대한 첫 번째 업데이트를 시작합니다.
# MAGIC 
# MAGIC 델타 라이브 테이블은 필요한 모든 인프라를 자동으로 배포하고 모든 데이터 세트 간의 종속성을 해결합니다.
# MAGIC 
# MAGIC **참고**: 첫 번째 테이블 업데이트는 관계가 해결되고 인프라가 구축되기 때문에 몇 분 정도 걸릴 수 있습니다.
# MAGIC 
# MAGIC ## Run your Pipeline
# MAGIC 
# MAGIC Select **Development** mode, which accelerates the development lifecycle by reusing the same cluster across runs.<br/>
# MAGIC It will also turn off automatic retries when jobs fail.
# MAGIC 
# MAGIC Click **Start** to begin the first update to your table.
# MAGIC 
# MAGIC Delta Live Tables will automatically deploy all the necessary infrastructure and resolve the dependencies between all datasets.
# MAGIC 
# MAGIC **NOTE**: The first table update may take several minutes as relationships are resolved and infrastructure deploys.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 개발 모드에서 코드 문제 해결
# MAGIC 
# MAGIC 파이프라인이 처음 실패하더라도 절망하지 마십시오. 델타 라이브 테이블은 현재 개발 중이며 오류 메시지는 항상 개선되고 있습니다.
# MAGIC 
# MAGIC 테이블 간의 관계는 DAG로 매핑되기 때문에 오류 메시지는 종종 데이터 세트를 찾을 수 없음을 나타냅니다.
# MAGIC 
# MAGIC 아래의 DAG를 고려해 보겠습니다:
# MAGIC 
# MAGIC ## Troubleshooting Code in Development Mode
# MAGIC 
# MAGIC Don't despair if your pipeline fails the first time. Delta Live Tables is in active development, and error messages are improving all the time.
# MAGIC 
# MAGIC Because relationships between tables are mapped as a DAG, error messages will often indicate that a dataset isn't found.
# MAGIC 
# MAGIC Let's consider our DAG below:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/dlt-dag.png">
# MAGIC 
# MAGIC **`Dataset not found: 'recordings_parsed'`** 오류 메시지가 표시되면 다음과 같은 여러 가지 원인이 있을 수 있습니다:
# MAGIC 1. **`recordings_parsed`** 를 정의하는 논리가 잘못되었습니다
# MAGIC 1. **`recordings_bronze`** 에서 읽는 중 오류가 발생했습니다
# MAGIC 1. **`recordings_parsed`** 또는 **`recordings_bronze`** 에 오타가 있습니다
# MAGIC 
# MAGIC 원인을 식별하는 가장 안전한 방법은 초기 수집 테이블에서 시작하여 DAG에 테이블/뷰 정의를 반복적으로 추가하는 것입니다. 나중에 테이블/뷰 정의를 주석 처리하고 실행 간에 주석을 제거할 수 있습니다.
# MAGIC 
# MAGIC If the error message **`Dataset not found: 'recordings_parsed'`** is raised, there may be several culprits:
# MAGIC 1. The logic defining **`recordings_parsed`** is invalid
# MAGIC 1. There is an error reading from **`recordings_bronze`**
# MAGIC 1. A typo exists in either **`recordings_parsed`** or **`recordings_bronze`**
# MAGIC 
# MAGIC The safest way to identify the culprit is to iteratively add table/view definitions back into your DAG starting from your initial ingestion tables. You can simply comment out later table/view definitions and uncomment these between runs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>