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
# MAGIC # Notebook 기초
# MAGIC 
# MAGIC 노트북은 데이터브릭에서 대화형으로 코드를 개발하고 실행하는 주요 수단입니다. 
# MAGIC 이 과정에서는 Databricks 노트북 작업에 대한 기본적인 소개를 제공합니다.
# MAGIC 
# MAGIC Databricks 노트북을 사용한 적이 있지만 Databricks Repos에서 노트북을 실행하는 것이 처음이라면 기본 기능이 동일하다는 것을 알 수 있습니다. <br>
# MAGIC 다음 단원에서는 Databricks Repos가 노트북에 추가하는 몇 가지 기능에 대해 살펴보겠습니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다.
# MAGIC * 노트북을 클러스터에 연결
# MAGIC * 노트북에서 셀 실행
# MAGIC * 노트북 언어 설정
# MAGIC * magic commands 설명 및 사용
# MAGIC * SQL 셀 생성 및 실행
# MAGIC * Python 셀 만들기 및 실행
# MAGIC * 마크다운 셀 만들기
# MAGIC * Databricks 노트북 Export
# MAGIC * Databricks 노트북 collection  Export

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 클러스터에 연결
# MAGIC 
# MAGIC 이전 과정에서는 클러스터를 배포 및 구성했습니다.
# MAGIC 
# MAGIC 화면 맨 위에 있는 노트북 이름 바로 아래에 있는 드롭다운 목록을 사용하여 이 노트북을 클러스터에 연결할 수 있습니다.
# MAGIC 
# MAGIC **참고**: 클러스터를 배포하는 데 몇 분 정도 걸릴 수 있습니다. 리소스가 배포되면 클러스터 이름 오른쪽에 녹색 화살표가 나타납니다. 
# MAGIC 클러스터 왼쪽에 회색 원이 있는 경우 <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">클러스터를 시작</a>하려면 지침을 따라야 합니다. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## 노트북 기본 사항
# MAGIC 
# MAGIC - 노트북은 셀 단위로 코드를 실행할 수 있습니다. 
# MAGIC - 노트북에서 여러 언어를 혼합해서 사용할 수 있습니다. 
# MAGIC - 사용자는 플롯, 이미지 및 마크다운 텍스트를 추가하여 코드를 향상시킬 수 있습니다.
# MAGIC 
# MAGIC 본 코스를 통해 노트북을 학습할 수 있습니다. <br>
# MAGIC 노트북은 Databricks를 통해 프로덕션 코드로 쉽게 배포할 수 있을 뿐만 아니라 데이터 탐색, 보고 및 대시보드를 위한 강력한 도구를 제공합니다.
# MAGIC 
# MAGIC ### Cell 실행
# MAGIC * 다음 옵션 중 하나를 사용하여 아래 셀을 실행합니다.
# MAGIC   * **CTRL+ENTER** 또는 **CTRL+RETURN**
# MAGIC   * **SHIFT+ENTER** 또는 **SHIFT+RETURN**을 눌러 셀을 실행하고 다음 셀로 이동합니다.
# MAGIC   * **Run Cell**, **Run All Above** 또는 **Run All Below** <br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png"/>

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **참고**: 셀별 코드 실행은 셀이 여러 번 실행되거나 순서가 잘못될 수 있음을 의미합니다. <br>
# MAGIC 명시적으로 지시하지 않는 한, 이 과정의 노트북은 항상 위에서 아래로 한 번에 하나의 셀을 실행하는 것으로 가정해야 합니다. <br>
# MAGIC 오류가 발생하면 문제를 해결하기 전에 셀 앞뒤의 텍스트를 읽어 원인을 파악하십시오. <br>
# MAGIC 대부분의 오류는 누락된 노트북에서 이전 셀을 실행하거나 전체 노트북을 처음부터 다시 실행하여 해결할 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### 기본 노트북 언어 설정
# MAGIC 
# MAGIC 노트북에 대한 현재 기본 언어가 Python으로 설정되어 있기 때문에 위의 셀은 Python 명령을 실행합니다.
# MAGIC 
# MAGIC 데이터브릭 노트북은 Python, SQL, Scala 및 R을 지원합니다. 노트북을 만들 때 언어를 선택할 수 있지만 언제든지 변경할 수 있습니다.
# MAGIC 
# MAGIC 기본 언어는 페이지 맨 위의 노트북 제목의 오른쪽에 직접 나타납니다. 이 과정에서는 SQL 및 Python 노트북을 함께 사용할 것입니다.
# MAGIC 
# MAGIC 노트북의 기본 언어를 SQL로 변경.
# MAGIC Steps:
# MAGIC * 화면 상단의 노트북 제목 옆에 있는 **Python**을 클릭합니다.
# MAGIC * 표시되는 UI에서 드롭다운 목록에서 **SQL**을 선택합니다.
# MAGIC 
# MAGIC **참고**: 이 셀 바로 앞에 있는 셀에는  <strong><code>&#37;python</code></strong>과 함께 새 줄이 표시됩니다. 잠시 후에 이것에 대해 논의하겠습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### SQL Cell 생성 및 실행
# MAGIC 
# MAGIC * 이 셀을 강조 표시하고 키보드의 **B** 버튼을 눌러 아래에 새 셀을 만듭니다.
# MAGIC * 다음 코드를 아래 셀에 복사한 후 셀을 실행합니다.
# MAGIC 
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC 
# MAGIC **참고**: GUI 옵션 및 키보드 단축키를 포함하여 셀을 추가, 이동 및 삭제하는 방법은 여러 가지가 있습니다. 자세한 내용은 <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">문서</a>를 참조하십시오.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Magic Commands
# MAGIC * Magic commands는 Datbricks 노트북에만 적용됩니다.
# MAGIC * 유사한 노트북 제품에서 볼 수 있는 Magic commands와 매우 유사합니다.
# MAGIC * 이러한 명령은 노트북의 언어에 관계없이 동일한 결과를 제공하는 기본 제공 명령입니다.
# MAGIC * 셀의 시작 부분에 있는 단일 백분율(%) 기호는 magic command을 나타냅니다.
# MAGIC   * Cell당 하나의 magic command만 가질 수 있습니다.
# MAGIC   * Cell에서 magic command가 첫 번째 줄에 위치해야 합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### Language Magics
# MAGIC Language magic 명령을 사용하면 노트북의 기본값이 아닌 언어로 코드를 실행할 수 있습니다. 이 과정에서는 다음과 같은 Language magic에 대해 알아봅니다.
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC 
# MAGIC 현재 설정된 노트북 유형에 Language magic을 추가할 필요가 없습니다.
# MAGIC 
# MAGIC 노트북 언어를 위의 Python에서 SQL로 변경했을 때 Python으로 작성된 기존 셀에는 <strong><code>&#37;python</code></strong> 명령어가 추가되었습니다.
# MAGIC 
# MAGIC 참고: 노트북의 기본 언어를 계속 변경하는 대신 기본 언어를 기본 언어로 유지하고 다른 언어로 코드를 실행하는 데 필요한 Language magic만 사용해야 합니다.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select "Hello SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Markdown
# MAGIC 
# MAGIC The magic command **&percnt;md** 을 사용하여 셀에서 마크다운을 렌더링할 수 있습니다.
# MAGIC * 편집을 시작하려면 이 셀을 두 번 클릭하십시오.
# MAGIC * 다음 `Esc`를 눌러 편집을 중지합니다.
# MAGIC 
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC 
# MAGIC 이것은 비상 방송 시스템의 테스트입니다. 이것은 단지 시험일 뿐이다.
# MAGIC 
# MAGIC 이것은 **굵은 글씨**가 들어간 텍스트입니다.
# MAGIC 
# MAGIC 이것은 *이탤릭체*로 된 단어가 들어 있는 텍스트이다.
# MAGIC 
# MAGIC This is an ordered list
# MAGIC 0. once
# MAGIC 0. two
# MAGIC 0. three
# MAGIC 
# MAGIC This is an unordered list
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC 
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC 
# MAGIC Images:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC 
# MAGIC And of course, tables:
# MAGIC 
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ### %run
# MAGIC * magic command **%run**을 사용하여 다른 노트북을 실행할 수 있습니다.
# MAGIC * 실행할 노트북을 상대 경로로 지정합니다.
# MAGIC * 참조된 노트북은 현재 노트북의 일부인 것처럼 실행되므로 호출 노트북에서 temp view 및 기타 로컬 선언을 사용할 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 다음 셀의 주석을 제거하고 실행하면 다음 오류가 발생합니다.<br/>
# MAGIC **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC But we can declare it and a handful of other variables and functions buy running this cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.2

# COMMAND ----------

# MAGIC %md
# MAGIC 참조한 **`../Includes/Classroom-Setup-1.2`** 노트북에는 데이터베이스를 만들고 **사용**하는 논리뿐만 아니라 임시 view **`demo_temp_vw`**를 만드는 논리가 포함되어 있습니다.
# MAGIC 
# MAGIC 다음 쿼리를 통해 현재 노트북 세션에서 이 임시 view 를 사용할 수 있습니다.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 교육 과정 전반에 걸쳐 이러한 "setup" 노트북 패턴을 사용하여 강의 및 실습 환경을 구성합니다.
# MAGIC 
# MAGIC 이러한 variables, functions 및 other objects는 **`DBAcademyHelper`**의 인스턴스인 **`DA`** object의 일부이기 때문에 쉽게 식별할 수 있어야 합니다.
# MAGIC 
# MAGIC 이를 염두에 두고 대부분의 수업에서는 파일과 데이터베이스를 구성하기 위해 사용자 이름에서 파생된 변수를 사용합니다.
# MAGIC 
# MAGIC 이 패턴을 사용하면 공유 작업 공간에서 다른 사용자와의 충돌을 방지할 수 있습니다.
# MAGIC 
# MAGIC 아래 셀은 Python을 사용하여 이전에 이 노트북의 설정 스크립트에 정의된 변수 중 일부를 출력합니다.
# MAGIC 
# MAGIC We'll use this pattern of "setup" notebooks throughout the course to help configure the environment for lessons and labs.
# MAGIC 
# MAGIC These "provided" variables, functions and other objects should be easily identifiable in that they are part of the **`DA`** object which is an instance of **`DBAcademyHelper`**.
# MAGIC 
# MAGIC With that in mind, most lessons will use variables derived from your username to organize files and databases. 
# MAGIC 
# MAGIC This pattern allows us to avoid collision with other users in shared a workspace.
# MAGIC 
# MAGIC The cell below uses Python to print some of those variables previously defined in this notebook's setup script:

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.db_name:           {DA.db_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 또한 SQL 문에서 사용할 수 있도록 SQL 컨텍스트에 동일한 변수를 "주입(injected)"합니다.
# MAGIC 
# MAGIC 이에 대해서는 나중에 자세히 설명하겠지만 다음 셀에서 간단한 예를 확인할 수 있습니다.
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> 이 두 가지 예에서 단어 **`da`** 와 **`DA`** 의 미묘하지만 중요한 차이를 주목하십시오.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.db_name}' as database_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Databricks Utilities
# MAGIC Databricks 노트북은 환경을 구성하고 상호 작용하기 위한 여러 유틸리티 명령을 제공합니다. <br>
# MAGIC <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC 
# MAGIC 이 과정을 통해 **`dbutils.fs.ls()`** 를 사용하여 Python 셀의 파일 디렉토리를 나열할 수 있습니다.

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## display()
# MAGIC 
# MAGIC 셀에서 SQL 쿼리를 실행할 때 결과는 항상 렌더링된 표 형식으로 표시됩니다.
# MAGIC 
# MAGIC 파이썬 셀이 표 형식의 데이터를 반환하면 **`display`** 를 호출하여 동일한 유형의 미리 보기를 얻을 수 있습니다.
# MAGIC 
# MAGIC 여기서는 파일 시스템의 이전 목록 명령을 **`display`** 와 함께 정리합니다.

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **`display()`** 명령에는 다음과 같은 기능 및 제한이 있습니다.
# MAGIC * 1000개 레코드로 제한된 결과 미리 보기
# MAGIC * 결과 데이터를 CSV로 다운로드할 수 있는 버튼 제공
# MAGIC * 시각화를 허용합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Notebooks 다운로드
# MAGIC 
# MAGIC 개별 노트북 또는 노트북 모음을 다운로드할 수 있는 다양한 옵션이 있습니다.
# MAGIC 
# MAGIC 이 과정에서 이 노트북과 이 과정의 모든 노트북 모음을 다운로드하는 과정을 거치게 됩니다.
# MAGIC 
# MAGIC ### 노트북 다운로드
# MAGIC 
# MAGIC Steps:
# MAGIC * 노트북 상단의 클러스터 선택 오른쪽에 있는 **File** 옵션을 클릭합니다.
# MAGIC * 나타나는 메뉴에서 **Export** 위에 마우스를 올린 다음 **Source File**을 선택합니다.
# MAGIC 
# MAGIC 노트북이 개인 랩탑으로 다운로드됩니다. 현재 노트북 이름으로 이름이 지정되고 기본 언어의 파일 확장자가 지정됩니다. 모든 파일 편집기에서 이 노트북을 열 수 있으며 Datbricks 노트북의 원시 콘텐츠를 볼 수 있습니다.
# MAGIC 
# MAGIC 이러한 원본 파일은 모든 Databricks 작업 공간에 업로드할 수 있습니다.
# MAGIC 
# MAGIC ### 노트북 Collection 다운로드
# MAGIC 
# MAGIC **참고**: 다음 지침에서는 **Repos**를 사용하여 이러한 자료를 가져온 것으로 가정합니다.
# MAGIC 
# MAGIC Steps:
# MAGIC * 왼쪽 사이드바에서 ![](https://files.training.databricks.com/images/repos-icon.png) **Repos** 를 클릭합니다.
# MAGIC   * 이 노트북의 상위 디렉터리를 미리 볼 수 있습니다.
# MAGIC * 화면 중앙의 디렉토리 미리보기 왼쪽에 왼쪽 화살표가 있어야 합니다. 파일 계층에서 위로 이동하려면 이 옵션을 클릭합니다.
# MAGIC * **Data Engineering with Datbricks**라는 디렉토리가 표시됩니다. 아래쪽 화살표/그림자를 클릭하여 메뉴를 표시합니다.
# MAGIC * 메뉴에서 **Export** 위에 마우스를 놓고 **DBC Archive**를 선택합니다.
# MAGIC 
# MAGIC 다운로드된 DBC(Databricks Cloud) 파일에는 이 과정의 디렉터리 및 노트북의 압축된 컬렉션이 포함되어 있습니다. 사용자는 이러한 DBC 파일을 로컬로 편집하지 않아야 하지만 노트북 콘텐츠를 이동하거나 공유하기 위해 안전하게 Datbricks 작업 공간에 업로드할 수 있습니다.
# MAGIC 
# MAGIC **참고**: DBC 모음을 다운로드할 때 결과 미리 보기 및 플롯도 내보냅니다. 소스 노트북을 다운로드할 때 코드만 저장됩니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Learning More
# MAGIC 
# MAGIC We like to encourage you to explore the documentation to learn more about the various features of the Databricks platform and notebooks.
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">User Guide / Notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importing notebooks - Supported Formats</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Administration Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Cluster Configuration</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Release Notes</a>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## One more note! 
# MAGIC 
# MAGIC 각 레슨이 끝나면  **`DA.cleanup()`**. 명령을 실행합니다.
# MAGIC 
# MAGIC 이 방법은 워크스페이스를 깨끗하게 유지하고 각 레슨의 불변성을 유지하기 위해 레슨별 데이터베이스와 작업 디렉토리를 삭제합니다.
# MAGIC 
# MAGIC 다음 셀을 실행하여 이 과정과 관련된 테이블 및 파일을 삭제합니다.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>