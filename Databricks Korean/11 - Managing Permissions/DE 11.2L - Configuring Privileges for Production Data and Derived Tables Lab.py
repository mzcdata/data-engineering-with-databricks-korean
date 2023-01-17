# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 프로덕션 데이터 및 파생 테이블에 대한 권한 구성
# MAGIC 
# MAGIC 사용자 쌍이 Datbricks의 테이블 ACL 작동 방식을 탐색할 수 있도록 아래에 자세히 설명된 지침이 제공됩니다. Databricks SQL과 Data Explorer를 활용하여 이러한 작업을 수행하며, 두 사용자 모두 작업 공간에 대한 관리자 권한이 없는 것으로 가정합니다. 사용자가 Databricks SQL에서 데이터베이스를 만들 수 있으려면 관리자가 카탈로그에 대한 **`CREATE`** 및 **`USAGE`** 권한을 이전에 부여해야 합니다.
# MAGIC 
# MAGIC ##학습 목표
# MAGIC 
# MAGIC 이 실습을 완료하면 다음을 수행할 수 있습니다:
# MAGIC * Data Explorer를 사용하여 관계 엔티티 탐색
# MAGIC * Data Explorer를 사용하여 테이블 및 보기에 대한 권한 구성
# MAGIC * 테이블 검색 및 쿼리를 허용하는 최소 권한 구성
# MAGIC * DBSQL에서 작성된 데이터베이스, 테이블 및 보기의 소유권 변경
# MAGIC 
# MAGIC # Configuring Privileges for Production Data and Derived Tables
# MAGIC 
# MAGIC The instructions as detailed below are provided for pairs of users to explore how Table ACLs on Databricks work. It leverages Databricks SQL and the Data Explorer to accomplish these tasks, and assumes that neither user has admin privileges for the workspace. An admin will need to have previously granted **`CREATE`** and **`USAGE`** privileges on a catalog for users to be able to create databases in Databricks SQL.
# MAGIC 
# MAGIC ##Learning Objectives
# MAGIC 
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Use Data Explorer to navigate relational entities
# MAGIC * Configure permissions for tables and views with Data Explorer
# MAGIC * Configure minimal permissions to allow for table discovery and querying
# MAGIC * Change ownership for databases, tables, and views created in DBSQL

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.2L

# COMMAND ----------

# MAGIC %md
# MAGIC ## 파트너와 사용자 이름 교환
# MAGIC 사용자 이름이 전자 메일 주소와 일치하는 작업 공간에 있지 않은 경우 파트너에게 사용자 이름이 있는지 확인하십시오.
# MAGIC 
# MAGIC 권한을 할당하고 이후 단계에서 데이터베이스를 검색할 때 필요합니다.
# MAGIC 
# MAGIC 다음 셀에서 사용자 이름을 인쇄합니다.
# MAGIC 
# MAGIC ## Exchange User Names with your Partner
# MAGIC If you are not in a workspace where your usernames correspond with your email address, make sure your partner has your username.
# MAGIC 
# MAGIC They will need this when assigning privileges and searching for your database at later steps.
# MAGIC 
# MAGIC The following cell will print your username.

# COMMAND ----------

print(f"Your username: {DA.username}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Setup Statements 생성
# MAGIC 
# MAGIC 다음 셀은 Python을 사용하여 현재 사용자의 사용자 이름을 추출하고 데이터베이스, 테이블 및 보기를 만드는 데 사용되는 여러 문으로 형식을 지정합니다.
# MAGIC 
# MAGIC 두 학생 모두 다음 셀을 실행해야 합니다. 
# MAGIC 
# MAGIC 실행이 성공하면 형식이 지정된 일련의 SQL 쿼리가 출력되며, 이를 DBSQL 쿼리 편집기에 복사하여 실행할 수 있습니다.
# MAGIC 
# MAGIC ## Generate Setup Statements
# MAGIC 
# MAGIC The following cell uses Python to extract the username of the current user and format this into several statements used to create databases, tables, and views.
# MAGIC 
# MAGIC Both students should execute the following cell. 
# MAGIC 
# MAGIC Successful execution will print out a series of formatted SQL queries, which can be copied into the DBSQL query editor and executed.

# COMMAND ----------

DA.generate_query()

# COMMAND ----------

# MAGIC %md
# MAGIC 단계:
# MAGIC 1. 위의 셀을 실행합니다
# MAGIC 1. 전체 출력을 클립보드에 복사합니다
# MAGIC 1. Databricks SQL 작업 공간으로 이동합니다
# MAGIC 1. DBSQL 끝점이 실행 중인지 확인합니다
# MAGIC 1. 왼쪽 사이드바를 사용하여 **SQL Editor**를 선택합니다
# MAGIC 1. 위의 쿼리를 붙여넣고 오른쪽 상단의 파란색 **실행**을 클릭합니다
# MAGIC 
# MAGIC **참고**: 이러한 쿼리를 성공적으로 실행하려면 DBSQL 끝점에 연결되어 있어야 합니다. DBSQL 끝점에 연결할 수 없는 경우 관리자에게 문의하여 액세스 권한을 부여해야 합니다.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Run the cell above
# MAGIC 1. Copy the entire output to your clipboard
# MAGIC 1. Navigate to the Databricks SQL workspace
# MAGIC 1. Make sure that a DBSQL endpoint is running
# MAGIC 1. Use the left sidebar to select the **SQL Editor**
# MAGIC 1. Paste the query above and click the blue **Run** in the top right
# MAGIC 
# MAGIC **NOTE**: You will need to be connected to a DBSQL endpoint to execute these queries successfully. If you cannot connect to a DBSQL endpoint, you will need to contact your administrator to give you access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 데이터베이스 찾기
# MAGIC Data Explorer에서 이전에 생성한 데이터베이스를 찾습니다(이는 **`dbacademy_<username>_dewd_acls_lab`** 패턴을 따라야 합니다).
# MAGIC 
# MAGIC 데이터베이스 이름을 누르면 왼쪽에 포함된 테이블 및 보기 목록이 표시됩니다.
# MAGIC 
# MAGIC 오른쪽에는 **Owner** 및 **Location**을 포함한 데이터베이스에 대한 세부 정보가 표시됩니다.
# MAGIC 
# MAGIC **Permissions** 탭을 클릭하여 현재 권한이 있는 사용자를 검토합니다(작업영역 구성에 따라 카탈로그의 설정에서 일부 권한이 상속되었을 수 있음).
# MAGIC 
# MAGIC ## Find Your Database
# MAGIC In the Data Explorer, find the database you created earlier (this should follow the pattern **`dbacademy_<username>_dewd_acls_lab`**).
# MAGIC 
# MAGIC Clicking on the database name should display a list of the contained tables and views on the left hand side.
# MAGIC 
# MAGIC On the right, you'll see some details about the database, including the **Owner** and **Location**.
# MAGIC 
# MAGIC Click the **Permissions** tab to review who presently has permissions (depending on your workspace configuration, some permissions may have been inherited from settings on the catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데이터베이스 사용 권한 변경
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 데이터베이스에 대해 **Permissions** 탭을 선택했는지 확인합니다
# MAGIC 1. 파란색 **Grant** 버튼을 클릭합니다
# MAGIC 1. **USAGE**, **SELECT**, **READ_METADA** 옵션을 선택합니다.
# MAGIC 1. 맨 위에 있는 필드에 파트너의 사용자 이름을 입력합니다.
# MAGIC 1. **OK**을 클릭합니다
# MAGIC 
# MAGIC 파트너와 함께 서로의 데이터베이스와 표를 볼 수 있는지 확인합니다.
# MAGIC 
# MAGIC ## Change Database Permissions
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Make sure you have the **Permissions** tab selected for the database
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **USAGE**, **SELECT**, and **READ_METADATA** options
# MAGIC 1. Enter the username of your partner in the field at the top.
# MAGIC 1. Click **OK**
# MAGIC 
# MAGIC Confirm with your partner that you can each see each others' databases and tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 확인을 위해 쿼리 실행
# MAGIC 
# MAGIC **`USAGE`**, **`SELECT`**, **`READ_METADATA`** 권한을 부여함으로써 파트너는 이제 이 데이터베이스의 테이블과 뷰를 자유롭게 쿼리할 수 있지만 새 테이블을 만들거나 데이터를 수정할 수 없습니다.
# MAGIC 
# MAGIC SQL Editor에서 각 사용자는 일련의 쿼리를 실행하여 방금 추가한 데이터베이스에서 이 동작을 확인해야 합니다.
# MAGIC 
# MAGIC **아래 쿼리를 실행하는 동안 파트너의 데이터베이스를 지정해야 합니다.**
# MAGIC 
# MAGIC **참고**: 처음 3개의 쿼리는 성공하지만 마지막 쿼리는 실패합니다.
# MAGIC 
# MAGIC ## Run a Query to Confirm
# MAGIC 
# MAGIC By granting **`USAGE`**, **`SELECT`**, and **`READ_METADATA`** on your database, your partner should now be able to freely query the tables and views in this database, but will not be able to create new tables OR modify your data.
# MAGIC 
# MAGIC In the SQL Editor, each user should run a series of queries to confirm this behavior in the database they were just added to.
# MAGIC 
# MAGIC **Make sure you specify your partner's database while running the queries below.**
# MAGIC 
# MAGIC **NOTE**: These first 3 queries should succeed, but the last should fail.

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_confirmation_query("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 콩 조합을 생성하기 위한 쿼리 실행
# MAGIC 
# MAGIC 사용자 자신의 데이터베이스에 대해 아래 쿼리를 실행합니다.
# MAGIC 
# MAGIC **참고**: **`grams`** 및 **`delicious`** 열에 랜덤 값이 삽입되었기 때문에 각 **`name`**, **`color`** 쌍에 대해 2개의 행이 구별됩니다.
# MAGIC 
# MAGIC ## Execute a Query to Generate the Union of Your Beans
# MAGIC 
# MAGIC Execute the query below against your own databases.
# MAGIC 
# MAGIC **NOTE**: Because random values were inserted for the **`grams`** and **`delicious`** columns, you should see 2 distinct rows for each **`name`**, **`color`** pair.

# COMMAND ----------

DA.generate_union_query()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 데이터베이스에 파생 뷰 등록
# MAGIC 
# MAGIC 이전 쿼리의 결과를 데이터베이스에 등록하려면 아래 쿼리를 실행하십시오.
# MAGIC 
# MAGIC ## Register a Derivative View to Your Database
# MAGIC 
# MAGIC Execute the query below to register the results of the previous query to your database.

# COMMAND ----------

DA.generate_derivative_view()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 파트너 보기 쿼리
# MAGIC 
# MAGIC 파트너가 이전 단계를 성공적으로 완료했으면 각 테이블에 대해 다음 쿼리를 실행하면 동일한 결과를 얻을 수 있습니다:
# MAGIC 
# MAGIC ## Query Your Partner's View
# MAGIC 
# MAGIC Once your partner has successfully completed the previous step, run the following query against each of your tables; you should get the same results:

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_partner_view("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 수정 권한 추가
# MAGIC 
# MAGIC 이제 서로의 **`beans`** 테이블을 떨어뜨리도록 노력하세요. 
# MAGIC 
# MAGIC 지금으로서는, 이것은 효과가 없을 것이다.
# MAGIC 
# MAGIC Data Explorer를 사용하여 파트너의 **`beans`** 테이블에 **`MODIFY`** 권한을 추가합니다.
# MAGIC 
# MAGIC 다시 한 번 파트너의 **`beans`** 테이블을 떨어뜨리려고 시도합니다. 
# MAGIC 
# MAGIC 그것은 또 실패할 것이다. 
# MAGIC 
# MAGIC **테이블의 소유자만 이 명세서를 발행할 수 있습니다**.<br/>
# MAGIC (원하는 경우 소유권은 개인에서 그룹으로 이전할 수 있습니다.).
# MAGIC 
# MAGIC 대신 쿼리를 실행하여 파트너 테이블에서 레코드를 삭제하십시오:
# MAGIC 
# MAGIC ## Add Modify Permissions
# MAGIC 
# MAGIC Now try to drop each other's **`beans`** tables. 
# MAGIC 
# MAGIC At the moment, this shouldn't work.
# MAGIC 
# MAGIC Using the Data Explorer, add the **`MODIFY`** permission for your **`beans`** table for your partner.
# MAGIC 
# MAGIC Again, attempt to drop your partner's **`beans`** table. 
# MAGIC 
# MAGIC It should again fail. 
# MAGIC 
# MAGIC **Only the owner of a table should be able to issue this statement**.<br/>
# MAGIC (Note that ownership can be transferred from an individual to a group, if desired).
# MAGIC 
# MAGIC Instead, execute a query to delete records from your partner's table:

# COMMAND ----------

# Replace FILL_IN with your partner's username
DA.generate_delete_query("FILL_IN")

# COMMAND ----------

# MAGIC %md
# MAGIC 이 쿼리는 대상 테이블에서 모든 레코드를 삭제합니다.
# MAGIC 
# MAGIC 이 실습에서 이전에 쿼리한 테이블 보기에 대해 쿼리를 다시 실행해 보십시오.
# MAGIC 
# MAGIC **참고**: 단계가 성공적으로 완료된 경우 보기에서 참조한 데이터가 삭제되었기 때문에 이전 쿼리에서 결과를 반환하지 않습니다. 이는 프로덕션 애플리케이션 및 대시보드에서 사용될 데이터에 대한 사용자에게 **`MODIFY`** 권한을 제공하는 것과 관련된 위험을 보여줍니다.
# MAGIC 
# MAGIC 추가 시간이 있으면 델타 방법 **`DESCRIBE HISTORY`** 및 **`RESTORE`** 를 사용하여 테이블의 레코드를 되돌릴 수 있는지 확인하십시오.
# MAGIC 
# MAGIC This query should successfully drop all records from the target table.
# MAGIC 
# MAGIC Try to re-execute queries against any of the views of tables you'd previously queried in this lab.
# MAGIC 
# MAGIC **NOTE**: If steps were completed successfully, none of your previous queries should return results, as the data referenced by your views has been deleted. This demonstrates the risks associated with providing **`MODIFY`** privileges to users on data that will be used in production applications and dashboards.
# MAGIC 
# MAGIC If you have additional time, see if you can use the Delta methods **`DESCRIBE HISTORY`** and **`RESTORE`** to revert the records in your table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>