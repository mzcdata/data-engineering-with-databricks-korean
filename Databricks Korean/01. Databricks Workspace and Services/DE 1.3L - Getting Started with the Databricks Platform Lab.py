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
# MAGIC 
# MAGIC # Datbricks 플랫폼 시작하기
# MAGIC 
# MAGIC 이 노트북은 Databricks Data Science and Engineering Workspace의 기본 기능 중 일부를 직접 검토하는 기능을 제공합니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다.
# MAGIC - 노트북 이름 변경 및 기본 언어 변경
# MAGIC - 클러스터 연결
# MAGIC - **`%run`** magic command 사용
# MAGIC - Python 및 SQL cells 실행
# MAGIC - 마크다운 셀 만들기

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # 노트북 이름 바꾸기
# MAGIC 
# MAGIC 노트북의 이름을 변경하는 것은 쉽습니다. <br>
# MAGIC 이 페이지의 맨 위에 있는 이름을 클릭한 다음 이름을 변경하세요. <br>
# MAGIC 필요한 경우 이 노트북으로 다시 쉽게 이동할 수 있도록 기존 이름 끝에 짧은 테스트 문자열을 추가합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Cluster 연결
# MAGIC 
# MAGIC 노트북에서 셀을 실행하려면 클러스터에서 제공하는 컴퓨팅 리소스가 필요합니다. <br>
# MAGIC 노트북에서 셀을 처음 실행할 때 클러스터가 아직 연결되지 않은 경우, <br>
# MAGIC 클러스터에 연결하라는 메시지가 나타납니다.
# MAGIC 
# MAGIC 이 페이지의 왼쪽 상단 모서리 근처에 있는 드롭다운을 클릭하여 지금 이 노트북에 클러스터를 연결합니다. <br>
# MAGIC 이전에 만든 클러스터를 선택합니다. <br>
# MAGIC 노트북의 실행 상태를 지우고 선택한 클러스터에 노트북을 연결합니다.
# MAGIC 
# MAGIC 드롭다운 메뉴는 필요에 따라 클러스터를 시작하거나 재시작할 수 있는 옵션을 제공합니다. <br>
# MAGIC 한 번의 이동으로 클러스터를 분리했다가 다시 연결할 수도 있습니다. <br>
# MAGIC 필요할 때 실행 상태를 지우는 데 유용합니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Using %run
# MAGIC 
# MAGIC 복잡한 프로젝트는 단순하고 재사용 가능한 구성요소로 분해할 수 있습니다.
# MAGIC 
# MAGIC Databricks 노트북의 컨텍스트에서는 **`%run`** magic command를 통해 이러한 기능을 제공됩니다.
# MAGIC 
# MAGIC 이렇게 사용하면 변수, 함수 및 코드 블록이 현재 프로그래밍 Context의 일부가 됩니다.
# MAGIC 
# MAGIC 다음 예를 고려해 보십시오:
# MAGIC 
# MAGIC **`Notebook_A`** 는 4가지 명령이 있습니다.:
# MAGIC   1. **`name = "John"`**
# MAGIC   2. **`print(f"Hello {name}")`**
# MAGIC   3. **`%run ./Notebook_B`**
# MAGIC   4. **`print(f"Welcome back {full_name}`)**
# MAGIC 
# MAGIC **`Notebook_B`** 는 1개의 명령이 있습니다.:
# MAGIC   1. **`full_name = f"{name} Doe"`**
# MAGIC 
# MAGIC 만약 우리가 **`Notebook_B`** 을 실행한다면, **`name`** 이 **`Notebook_B`** 에 정의되어 있지 않기 때문에 실행은 실패할 것입니다.
# MAGIC 
# MAGIC 마찬가지로, **`Notebook_A`** 에 정의되지 않은 **`full_name`** 변수를 사용하기 때문에 **`Notebook_A`** 가 실패할 것이라고 생각할 수 있지만, 그렇지 않습니다!
# MAGIC 
# MAGIC 실제로는 아래와 같이 두 개의 노트북이 병합된 후 실행됩니다:
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Welcome back {full_name}`**
# MAGIC 
# MAGIC 따라서 예상되는 동작은:
# MAGIC * **`Hello John`**
# MAGIC * **`Welcome back John Doe`**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 이 노트북을 포함하는 폴더에  **`ExampleSetupFolder`** 라는 하위 폴더가 포함되어 있으며, 폴더에는 **`example-setup`** 설정이라는 노트북이 들어 있습니다.
# MAGIC 
# MAGIC 이 간단한 노트북은 변수 **`my_name`** 을 선언하고 **`None`** 으로 설정한 다음 **`example_df`** 라는 DataFrame을 만듭니다.
# MAGIC 
# MAGIC example-setup notebook을 열고 이름이 **`None`** 이 아니라 따옴표로 둘러싸인 사용자 이름(또는 다른 사용자의 이름)이 되도록 수정하고 다음 두 셀이 **`AssertionError`** 를 던지지 않고 실행되도록 합니다.

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Python cell 실행
# MAGIC 
# MAGIC 다음 셀을 실행하여 **`example_df`** Dataframe을 표시하여 **`example-setup`** 노트북이 실행되었는지 확인합니다. 이 표는 증가하는 값의 16개 행으로 구성됩니다.

# COMMAND ----------

display(example_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Cluster 분리 및 재연결
# MAGIC 
# MAGIC 클러스터에 연결하는 것은 상당히 일반적인 작업이지만 한 번의 작업으로 분리했다가 다시 연결하는 것이 유용할 수 있습니다.<br>
# MAGIC **그러나** 분리 후 재연결 시 실행 상태가 지워지는 부작용이 있습니다. <br>
# MAGIC 이 기능은 셀을 분리하여 테스트하는 경우 또는 단순히 실행 상태를 재설정하려는 경우에 유용합니다. <br/>
# MAGIC 
# MAGIC 클러스터 드롭다운을 선택합니다. <br>
# MAGIC 현재 연결된 클러스터를 나타내는 메뉴 항목에서 **Detach & Re-attach** 링크를 선택합니다. <br/>
# MAGIC 
# MAGIC 결과와 실행 상태는 관련이 없기 때문에 실행 상태는 삭제되지만 셀의 출력은 그대로 유지됩니다. <br>
# MAGIC 재연결 후 위의 셀을 다시 실행하면, **`example_df`** 변수가 지워졌기 때문에 실패합니다. <br/>
# MAGIC 
# MAGIC 
# MAGIC While attaching to clusters is a fairly common task, sometimes it is useful to detach and re-attach in one single operation. The main side-effect this achieves is clearing the execution state. This can be useful when you want to test cells in isolation, or you simply want to reset the execution state.
# MAGIC 
# MAGIC Revisit the cluster dropdown. In the menu item representing the currently attached cluster, select the **Detach & Re-attach** link.
# MAGIC 
# MAGIC Notice that the output from the cell above remains since results and execution state are unrelated, but the execution state is cleared. This can be verified by attempting to re-run the cell above. This fails, since the **`example_df`** variable has been cleared, along with the rest of the state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Change Language
# MAGIC 
# MAGIC 이 노트북의 기본 언어는 Python으로 설정되어 있습니다. 노트북 이름 오른쪽에 있는 **Python** 버튼을 클릭하여 변경합니다. 기본 언어를 SQL로 변경합니다. <br/>
# MAGIC Notice that the default language for this notebook is set to Python. Change this by clicking the **Python** button to the right of the notebook name. Change the default language to SQL.
# MAGIC 
# MAGIC 파이썬 셀에는 해당 셀의 유효성을 유지하기 위해 <strong><code>&#37;python</code></strong> magic command가 자동으로 추가됩니다. 이 작업은 실행 상태도 지웁니다. <br/>
# MAGIC Notice that the Python cells are automatically prepended with a <strong><code>&#37;python</code></strong> magic command to maintain validity of those cells. Notice that this operation also clears the execution state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Create a Markdown Cell
# MAGIC 
# MAGIC 이 셀 아래에 새 셀을 추가합니다. 적어도 다음 요소를 포함하는 일부 마크다운으로 채웁니다: <br/>
# MAGIC Add a new cell below this one. Populate with some Markdown that includes at least the following elements:
# MAGIC * 표제어 [A header]
# MAGIC * 글머리 기호 [Bullet points]
# MAGIC * 링크(HTML 또는 마크다운 규칙 선택 사용) [A link (using your choice of HTML or Markdown conventions)]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Run a SQL cell
# MAGIC 
# MAGIC SQL을 사용하여 델타 테이블을 쿼리하려면 다음 셀을 실행하십시오. 이렇게 하면 모든 DBFS 설치에 포함된 Databricks 제공 예제 데이터 세트가 지원하는 테이블에 대한 간단한 쿼리가 실행됩니다. <br/>
# MAGIC Run the following cell to query a Delta table using SQL. This executes a simple query against a table is backed by a Databricks-provided example dataset included in all DBFS installations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/databricks-datasets/nyctaxi-with-zipcodes/subsampled`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 이 테이블의 기본 파일들을 보려면, 다음 셀을 실행하십시오. <br/>
# MAGIC Execute the following cell to view the underlying files backing this table.

# COMMAND ----------

files = dbutils.fs.ls("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # 변경사항 검토(Review Changes)
# MAGIC Databricks Repo를 사용하여 이 자료를 작업영역으로 가져왔다고 가정하면, 이 페이지의 왼쪽 상단 모서리에 있는 게시된 분기 버튼을 클릭하여 Repo 대화상자를 엽니다. 세 가지 변경 사항이 표시됩니다: <br/>
# MAGIC 1. 이전 노트북 이름으로 **제거됨**
# MAGIC 1. 새 노트북 이름과 함께 **추가됨**
# MAGIC 1. 위의 마크다운 셀을 만들기 위해 **수정됨**
# MAGIC 
# MAGIC 대화상자를 사용하여 변경사항을 되돌리고 이 노트북을 원래 상태로 복원합니다.
# MAGIC 
# MAGIC Assuming you have imported this material into your workspace using a Databricks Repo, open the Repo dialog by clicking the **`published`** branch button at the top-left corner of this page. You should see three changes:
# MAGIC 1. **Removed** with the old notebook name
# MAGIC 1. **Added** with the new notebook name
# MAGIC 1. **Modified** for creating a markdown cell above
# MAGIC 
# MAGIC Use the dialog to revert the changes and restore this notebook to its original state.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC 이 실습을 완료하면 이제 노트북을 조작하고, 새 셀을 만들고, 노트북 내에서 노트북을 실행할 수 있습니다. <br/>
# MAGIC By completing this lab, you should now feel comfortable manipulating notebooks, creating new cells, and running notebooks within notebooks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>