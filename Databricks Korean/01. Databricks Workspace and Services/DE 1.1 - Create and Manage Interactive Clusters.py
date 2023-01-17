# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC # 대화형 클러스터 생성 및 관리
# MAGIC 
# MAGIC Databricks 클러스터는 운영 ETL 파이프라인, 스트리밍 분석, 애드혹 분석 및 머신러닝과 같은 데이터 엔지니어링, 데이터 과학 및 데이터 분석 워크로드를 실행하는 계산 리소스 및 구성 집합입니다. 이러한 워크로드는 노트북의 명령 집합 또는 자동 작업으로 실행됩니다.
# MAGIC 
# MAGIC 데이터 블록은 다목적 클러스터와 작업 클러스터를 구분합니다.
# MAGIC - 범용 클러스터를 사용하여 대화형 노트북을 사용하여 데이터를 공동으로 분석할 수 있습니다.
# MAGIC - 작업 클러스터를 사용하여 빠르고 강력한 자동 작업을 실행할 수 있습니다.
# MAGIC 
# MAGIC 이 데모에서는 Databricks Data Science & Engineering Workspace를 사용하여 다목적 Datbricks 클러스터를 만들고 관리하는 방법에 대해 설명합니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다. : 
# MAGIC * 클러스터 UI를 사용하여 클러스터 구성 및 배포
# MAGIC * 클러스터 편집, 종료, 재시작 및 삭제

# COMMAND ----------

# MAGIC %md
# MAGIC   
# MAGIC ## 클러스터 생성
# MAGIC 
# MAGIC 현재 작업 중인 작업 공간에 따라 클러스터 생성 권한이 있을 수도 있고 없을 수도 있습니다.
# MAGIC 
# MAGIC 이 섹션의 지침에서는 클러스터 작성 **권한이 있으며 이 과정의 과정을 실행하려면 새 클러스터를 배포해야 한다고 가정합니다.**
# MAGIC 
# MAGIC **참고**: 강사 또는 플랫폼 관리자에게 문의하여 새 클러스터를 생성할지 또는 이미 배포된 클러스터에 연결할지 여부를 확인하십시오. 클러스터 정책은 클러스터 구성 옵션에 영향을 줄 수 있습니다.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. 왼쪽 사이드바를 사용하여 **Compute** (![compute](https://files.training.databricks.com/images/clusters-icon.png)) 아이콘을 클릭하여 계산 페이지로 이동합니다. 
# MAGIC 1. 파란색 **Create Cluster** 버튼을 클릭합니다.
# MAGIC 1. **Cluster Name**의 경우 사용자의 이름을 사용하여 쉽게 찾을 수 있으며 문제가 있을 경우 강사가 쉽게 식별할 수 있습니다.
# MAGIC 1. **Cluster mode**를 **Single Node**로 설정(이 과정을 실행하려면 이 모드가 필요함)
# MAGIC 1. 이 과정에 권장되는 **Databricks runtime version** 사용
# MAGIC 1. **Autopilot Options**에서 기본 설정에 대한 상자를 선택한 상태로 둡니다.
# MAGIC 1. 파란색 **Create Cluster** 버튼을 클릭합니다.
# MAGIC 
# MAGIC **참고:** 클러스터를 배포하는 데 몇 분 정도 걸릴 수 있습니다. 클러스터 배포를 마쳤으면 클러스터 생성 UI를 계속 탐색하십시오.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> 위 과정에는 Single-Node Cluster가 필요합니다.
# MAGIC **IMPORTANT:** 위 과정에는 Single-Node Cluster가 필요합니다.
# MAGIC 
# MAGIC 위의 지침에 따라 **Cluster Mode**가 아닌 **Single Node** 설정된 클러스터를 생성합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 클러스터 관리(Manage Clusters)
# MAGIC 
# MAGIC 클러스터가 생성되면 **Compute** 페이지로 돌아가 클러스터를 봅니다.
# MAGIC 
# MAGIC 현재 구성을 검토할 클러스터를 선택하십시오.
# MAGIC 
# MAGIC **Edit** 버튼을 클릭합니다. 충분한 권한이 있는 경우 대부분의 설정을 수정할 수 있습니다. 대부분의 설정을 변경하려면 실행 중인 클러스터를 다시 시작해야 합니다.
# MAGIC 
# MAGIC **참고** : 다음 과정에서는 클러스터를 사용합니다. 클러스터를 다시 시작하거나 종료하거나 삭제하면 새 리소스가 배포될 때까지 기다릴 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Restart, Terminate 및 Delete
# MAGIC 
# MAGIC **Restart**, **Terminate**, 및 **Delete**의 효과는 다르지만 모두 클러스터 종료 이벤트로 시작합니다
# MAGIC 
# MAGIC (이 설정이 사용되는 경우 클러스터도 비활성화로 인해 자동으로 종료됩니다.)
# MAGIC 
# MAGIC 클러스터가 종료되면 현재 사용 중인 모든 클라우드 리소스가 삭제됩니다. 이는 다음을 의미합니다. : 
# MAGIC * 연결된 VM 및 작동 메모리가 제거됩니다.
# MAGIC * 연결된 볼륨 저장소가 삭제됩니다.
# MAGIC * 노드 간의 네트워크 연결이 제거됩니다.
# MAGIC 
# MAGIC 즉, 이전에 컴퓨팅 환경과 연결된 모든 리소스가 완전히 제거됩니다. 이는 **지속되어야 하는 결과를 영구적인 위치에 저장해야 함**을 의미합니다
# MAGIC 
# MAGIC 코드가 손실되지 않으며, 적절하게 저장한 데이터 파일도 손실되지 않습니다.
# MAGIC 
# MAGIC **Restart** 버튼을 사용하여 클러스터를 수동으로 다시 시작할 수 있습니다. 이 기능은 클러스터의 캐시를 완전히 삭제하거나 컴퓨팅 환경을 완전히 재설정하려는 경우에 유용합니다.
# MAGIC 
# MAGIC **Terminate** 버틀을 사용하여 클러스터를 중지할 수 있습니다. 클러스터 구성 설정을 유지 관리하며, 다시 시작 버튼을 사용하여 동일한 구성을 사용하여 새로운 클라우드 리소스 세트를 배포할 수 있습니다.
# MAGIC 
# MAGIC **Delete** 버튼을 누르면 클러스터가 중지되고 클러스터 구성이 제거됩니다.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>