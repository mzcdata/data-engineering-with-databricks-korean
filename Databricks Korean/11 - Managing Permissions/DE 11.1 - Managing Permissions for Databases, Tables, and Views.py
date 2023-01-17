# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 데이터베이스, 테이블 및 보기에 대한 사용 권한 관리
# MAGIC 
# MAGIC 사용자 그룹이 Datbricks의 테이블 ACL 작동 방식을 탐색할 수 있도록 아래에 자세히 설명된 지침이 제공됩니다. Databricks SQL과 Data Explorer를 활용하여 이러한 작업을 수행하고 그룹에서 적어도 한 명의 사용자가 관리자 상태(또는 관리자가 사용자가 데이터베이스, 테이블 및 보기를 만들 수 있는 적절한 권한을 허용하도록 이전에 구성한 사용 권한)를 가지고 있다고 가정합니다. 
# MAGIC 
# MAGIC 작성된 대로 이 지침은 관리자가 작성해야 합니다. 다음 노트북에는 사용자가 쌍으로 완료해야 하는 유사한 연습이 있습니다.
# MAGIC 
# MAGIC ## 학습 목표
# MAGIC 이 과정을 통해 다음을 수행할 수 있습니다:
# MAGIC * DBSQL에서 사용자 및 관리자에 대한 기본 권한 설명
# MAGIC * DBSQL에서 작성된 데이터베이스, 테이블 및 뷰의 기본 소유자 식별 및 소유권 변경
# MAGIC * Data Explorer를 사용하여 관계 엔티티 탐색
# MAGIC * Data Explorer를 사용하여 테이블 및 보기에 대한 권한 구성
# MAGIC * 테이블 검색 및 쿼리를 허용하는 최소 권한 구성
# MAGIC 
# MAGIC # Managing Permissions for Databases, Tables, and Views
# MAGIC 
# MAGIC The instructions as detailed below are provided for groups of users to explore how Table ACLs on Databricks work. It leverages Databricks SQL and the Data Explorer to accomplish these tasks, and assumes that at least one user in the group has administrator status (or that an admin has previously configured permissions to allow proper permissions for users to create databases, tables, and views). 
# MAGIC 
# MAGIC As written, these instructions are for the admin user to complete. The following notebook will have a similar exercise for users to complete in pairs.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the default permissions for users and admins in DBSQL
# MAGIC * Identify the default owner for databases, tables, and views created in DBSQL and change ownership
# MAGIC * Use Data Explorer to navigate relational entities
# MAGIC * Configure permissions for tables and views with Data Explorer
# MAGIC * Configure minimal permissions to allow for table discovery and querying

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 설정 문 생성
# MAGIC 
# MAGIC 다음 셀은 Python을 사용하여 현재 사용자의 사용자 이름을 추출하고 데이터베이스, 테이블 및 보기를 만드는 데 사용되는 여러 문으로 형식을 지정합니다.
# MAGIC 
# MAGIC 관리자만 다음 셀을 실행하면 됩니다. 실행이 성공하면 형식이 지정된 일련의 SQL 쿼리가 출력되며, 이를 DBSQL 쿼리 편집기에 복사하여 실행할 수 있습니다.
# MAGIC 
# MAGIC ## Generate Setup Statements
# MAGIC 
# MAGIC The following cell uses Python to extract username of the current user and format this into several statements used to create databases, tables, and views.
# MAGIC 
# MAGIC Only the admin needs to execute the following cell. Successful execution will print out a series of formatted SQL queries, which can be copied into the DBSQL query editor and executed.

# COMMAND ----------

DA.generate_users_table()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 위의 셀을 실행합니다
# MAGIC 1. 전체 출력을 클립보드에 복사합니다
# MAGIC 1. Databricks SQL 작업 공간으로 이동합니다
# MAGIC 1. DBSQL 끝점이 실행 중인지 확인합니다
# MAGIC 1. 왼쪽 사이드바를 사용하여 **SQL Editor**를 선택합니다
# MAGIC 1. 위의 쿼리를 붙여넣고 오른쪽 상단의 파란색 **Run**을 클릭합니다
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
# MAGIC 
# MAGIC ## Data Explorer 사용
# MAGIC 
# MAGIC * 왼쪽 사이드바 탐색기를 사용하여 **Data** 탭을 선택합니다. 그러면 **Data Explorer**가 표시됩니다
# MAGIC 
# MAGIC ## Using Data Explorer
# MAGIC 
# MAGIC * Use the left sidebar navigator to select the **Data** tab; this places you in the **Data Explorer**
# MAGIC 
# MAGIC ## Data Explorer란 무엇입니까?
# MAGIC 
# MAGIC 데이터 탐색기를 통해 사용자와 관리자는 다음을 수행할 수 있습니다:
# MAGIC * 데이터베이스, 테이블 및 보기 탐색
# MAGIC * 데이터 스키마, 메타데이터 및 기록 탐색
# MAGIC * 관계 엔티티의 사용 권한 설정 및 수정
# MAGIC 
# MAGIC 이러한 지침이 작성되는 현재로서는 Unity 카탈로그를 일반적으로 사용할 수 없습니다. 추가하는 3계층 네임스페이스 기능은 기본 **`hive_metastore`** 와 대시보드 및 쿼리에 사용되는 **`sample`** 카탈로그 사이에서 전환하여 어느 정도 미리 볼 수 있다. Unity 카탈로그가 작업 공간에 추가됨에 따라 Data Explorer UI 및 기능이 발전할 것으로 예상합니다.
# MAGIC 
# MAGIC ## What is the Data Explorer?
# MAGIC 
# MAGIC The data explorer allows users and admins to:
# MAGIC * Navigate databases, tables, and views
# MAGIC * Explore data schema, metadata, and history
# MAGIC * Set and modify permissions of relational entities
# MAGIC 
# MAGIC Note that at the moment these instructions are being written, Unity Catalog is not yet generally available. The 3 tier namespacing functionality it adds can be previewed to an extent by switching between the default **`hive_metastore`** and the **`sample`** catalog used for example dashboards and queries. Expect the Data Explorer UI and functionality to evolve as Unity Catalog is added to workspaces.
# MAGIC 
# MAGIC ## 사용 권한 구성
# MAGIC 
# MAGIC 기본적으로 관리자는 메타포에 등록된 모든 개체를 볼 수 있으며 작업 공간에서 다른 사용자에 대한 사용 권한을 제어할 수 있습니다. 사용자는 기본적으로 DBSQL에서 생성하는 개체를 제외하고 전이에 등록된 모든 개체에 대해 **no** 권한을 가집니다. 데이터베이스, 테이블 또는 보기를 생성하려면 사용자에게 특별히 부여된 생성 및 사용 권한이 있어야 합니다.
# MAGIC 
# MAGIC 일반적으로 권한은 관리자가 구성한 그룹을 사용하여 설정되며, 다른 ID 공급자와의 SCIM 통합에서 조직 구조를 가져오는 경우가 많습니다. 이 과정에서는 사용 권한을 제어하는 데 사용되는 ACL(액세스 제어 목록)에 대해 설명하지만 그룹이 아닌 개인을 사용합니다.
# MAGIC 
# MAGIC ## Configuring Permissions
# MAGIC 
# MAGIC By default, admins will have the ability to view all objects registered to the metastore and will be able to control permissions for other users in the workspace. Users will default to having **no** permissions on anything registered to the metastore, other than objects that they create in DBSQL; note that before users can create any databases, tables, or views, they must have create and usage privileges specifically granted to them.
# MAGIC 
# MAGIC Generally, permissions will be set using Groups that have been configured by an administrator, often by importing organizational structures from SCIM integration with a different identity provider. This lesson will explore Access Control Lists (ACLs) used to control permissions, but will use individuals rather than groups.
# MAGIC 
# MAGIC ## 테이블 ACL
# MAGIC 
# MAGIC 데이터브릭을 사용하면 다음 개체에 대한 사용 권한을 구성할 수 있습니다:
# MAGIC 
# MAGIC ## Table ACLs
# MAGIC 
# MAGIC Databricks allows you to configure permissions for the following objects:
# MAGIC 
# MAGIC | Object | Scope |
# MAGIC | --- | --- |
# MAGIC | CATALOG | controls access to the entire data catalog. |
# MAGIC | DATABASE | controls access to a database. |
# MAGIC | TABLE | controls access to a managed or external table. |
# MAGIC | VIEW | controls access to SQL views. |
# MAGIC | FUNCTION | controls access to a named function. |
# MAGIC | ANY FILE | controls access to the underlying filesystem. Users granted access to ANY FILE can bypass the restrictions put on the catalog, databases, tables, and views by reading from the file system directly. |
# MAGIC 
# MAGIC **참고**: 현재 **`ANY FILE`** 개체는 Data Explorer에서 설정할 수 없습니다.
# MAGIC 
# MAGIC **NOTE**: At present, the **`ANY FILE`** object cannot be set from Data Explorer.
# MAGIC 
# MAGIC ## 권한 부여
# MAGIC 
# MAGIC 데이터브릭 관리자 및 개체 소유자는 다음 규칙에 따라 권한을 부여할 수 있습니다:
# MAGIC 
# MAGIC ## Granting Privileges
# MAGIC 
# MAGIC Databricks admins and object owners can grant privileges according to the following rules:
# MAGIC 
# MAGIC | Role | Can grant access privileges for |
# MAGIC | --- | --- |
# MAGIC | Databricks administrator | All objects in the catalog and the underlying filesystem. |
# MAGIC | Catalog owner | All objects in the catalog. |
# MAGIC | Database owner | All objects in the database. |
# MAGIC | Table owner | Only the table (similar options for views and functions). |
# MAGIC 
# MAGIC **참고**: 현재 Data Explorer는 데이터베이스, 테이블 및 보기의 소유권을 수정하는 데만 사용할 수 있습니다. 카탈로그 권한은 SQL 조회 편집기와 대화식으로 설정할 수 있습니다.
# MAGIC 
# MAGIC **NOTE**: At present, Data Explorer can only be used to modify ownership of databases, tables, and views. Catalog permissions can be set interactively with the SQL Query Editor.
# MAGIC 
# MAGIC ## 권한
# MAGIC 
# MAGIC Data Explorer에서 구성할 수 있는 권한은 다음과 같습니다:
# MAGIC 
# MAGIC ## Privileges
# MAGIC 
# MAGIC The following privileges can be configured in Data Explorer:
# MAGIC 
# MAGIC | Privilege | Ability |
# MAGIC | --- | --- |
# MAGIC | ALL PRIVILEGES | gives all privileges (is translated into all the below privileges). |
# MAGIC | SELECT | gives read access to an object. |
# MAGIC | MODIFY | gives ability to add, delete, and modify data to or from an object. |
# MAGIC | READ_METADATA | gives ability to view an object and its metadata. |
# MAGIC | USAGE | does not give any abilities, but is an additional requirement to perform any action on a database object. |
# MAGIC | CREATE | gives ability to create an object (for example, a table in a database). |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 기본 사용 권한 검토
# MAGIC Data Explorer에서 이전에 생성한 데이터베이스를 찾습니다(이는 **`dbacademy_<username>_acls_demo`** 패턴을 따라야 함).
# MAGIC 
# MAGIC 데이터베이스 이름을 누르면 왼쪽에 포함된 테이블 및 보기 목록이 표시됩니다. 오른쪽에는 **Owner** 및 **Location**을 포함한 데이터베이스에 대한 세부 정보가 표시됩니다. 
# MAGIC 
# MAGIC **Permissions** 탭을 클릭하여 현재 권한이 있는 사용자를 검토합니다(작업영역 구성에 따라 카탈로그의 설정에서 일부 권한이 상속되었을 수 있음).
# MAGIC   
# MAGIC ## Review the Default Permissions
# MAGIC In the Data Explorer, find the database you created earlier (this should follow the pattern **`dbacademy_<username>_acls_demo`**).
# MAGIC 
# MAGIC Clicking on the database name should display a list of the contained tables and views on the left hand side. On the right, you'll see some details about the database, including the **Owner** and **Location**. 
# MAGIC 
# MAGIC Click the **Permissions** tab to review who presently has permissions (depending on your workspace configuration, some permissions may have been inherited from settings on the catalog).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 소유권 할당
# MAGIC 
# MAGIC **Owner** 필드 옆에 있는 파란색 연필을 클릭합니다. 소유자는 개인 또는 그룹으로 설정할 수 있습니다. 대부분의 구현에서 하나 또는 여러 명의 신뢰할 수 있는 강력한 사용자 그룹을 소유자로 갖는 것은 중요한 데이터 세트에 대한 관리자 액세스를 제한하는 동시에 단일 사용자가 생산성에 초크 포인트를 만들지 않도록 보장한다.
# MAGIC 
# MAGIC 여기서는 모든 작업 공간 관리자를 포함하는 기본 그룹인 **Admins**로 소유자를 설정합니다.
# MAGIC 
# MAGIC ## Assigning Ownership
# MAGIC 
# MAGIC Click the blue pencil next to the **Owner** field. Note that an owner can be set as an individual OR a group. For most implementations, having one or several small groups of trusted power users as owners will limit admin access to important datasets while ensuring that a single user does not create a choke point in productivity.
# MAGIC 
# MAGIC Here, we'll set the owner to **Admins**, which is a default group containing all workspace administrators.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 데이터베이스 사용 권한 변경
# MAGIC 
# MAGIC 먼저 모든 사용자가 데이터베이스에 대한 메타데이터를 검토할 수 있도록 허용합니다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. 데이터베이스에 대해 **Permissions** 탭을 선택했는지 확인합니다
# MAGIC 1. 파란색 **Grant** 버튼을 클릭합니다
# MAGIC 1. **USAGE* 및 **READ_METADATA**  옵션을 선택합니다
# MAGIC 1. 상단의 드롭다운 메뉴에서 **All Users** 그룹을 선택합니다
# MAGIC 1. **OK**를 클릭합니다
# MAGIC 
# MAGIC 사용자가 보기를 새로 고쳐야 이러한 권한이 업데이트될 수 있습니다. 업데이트는 Data Explorer와 SQL Editor 모두에 대해 거의 실시간으로 사용자에게 반영되어야 합니다.
# MAGIC 
# MAGIC ## Change Database Permissions
# MAGIC 
# MAGIC Begin by allowing all users to review metadata about the database.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Make sure you have the **Permissions** tab selected for the database
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **USAGE** and **READ_METADATA** options
# MAGIC 1. Select the **All Users** group from the drop down menu at the top
# MAGIC 1. Click **OK**
# MAGIC 
# MAGIC Note that users may need to refresh their view to see these permissions updated. Updates should be reflected for users in near real time for both the Data Explorer and the SQL Editor.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## View Permissions 변경
# MAGIC 
# MAGIC 이제 사용자는 이 데이터베이스에 대한 정보를 볼 수 있지만 위에 명시된 보기 표와 상호 작용할 수 없습니다.
# MAGIC 
# MAGIC 사용자에게 보기를 쿼리할 수 있는 기능을 제공하는 것부터 시작해 보겠습니다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. **`ny_users_vw`** 를 선택합니다
# MAGIC 1. **Permissions** 탭을 선택합니다
# MAGIC    * 사용자는 데이터베이스 수준에서 부여된 권한을 상속받아야 합니다. 사용자가 자산에 대해 현재 가지고 있는 권한과 해당 권한이 상속된 위치를 확인할 수 있습니다
# MAGIC 1. 파란색 **Grant** 버튼을 클릭합니다
# MAGIC 1. **SELECT** 및 **READ_METADATA** 옵션을 선택합니다.
# MAGIC    * **READ_METADATA**는 사용자가 이미 데이터베이스에서 이를 상속했기 때문에 기술적으로 중복됩니다. 그러나 보기 수준에서 이 권한을 부여하면 데이터베이스 권한이 취소되더라도 사용자가 이 권한을 계속 가질 수 있습니다
# MAGIC 1. 상단의 드롭다운 메뉴에서 **All Users** 그룹을 선택합니다
# MAGIC 1. **OK**을 클릭합니다
# MAGIC 
# MAGIC ## Change View Permissions
# MAGIC 
# MAGIC While users can now see information about this database, they won't be able to interact with the table of view declared above.
# MAGIC 
# MAGIC Let's start by giving users the ability to query our view.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Select the **`ny_users_vw`**
# MAGIC 1. Select the **Permissions** tab
# MAGIC    * Users should have inherited the permissions granted at the database level; you'll be able to see which permissions users currently have on an asset, as well as where that permission is inherited from
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **SELECT** and **READ_METADATA** options
# MAGIC    * **READ_METADATA** is technically redundant, as users have already inherited this from the database. However, granting it at the view level allows us to ensure users still have this permission even if the database permissions are revoked
# MAGIC 1. Select the **All Users** group from the drop down menu at the top
# MAGIC 1. Click **OK**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 확인을 위해 쿼리 실행
# MAGIC 
# MAGIC **SQL Editor**에서 모든 사용자는 왼쪽의 **Schema Browser**를 사용하여 관리자가 제어하는 데이터베이스로 이동해야 합니다.
# MAGIC 
# MAGIC 사용자는 **`SELECT * FROM`** 을 입력하여 쿼리를 시작한 다음 뷰 이름 위에 나타나는 **>>** 를 클릭하여 쿼리에 삽입해야 합니다.
# MAGIC 
# MAGIC 이 쿼리는 2개의 결과를 반환해야 합니다.
# MAGIC 
# MAGIC **참고**: 이 보기는 권한이 설정되지 않은 **`users`** 테이블에 대해 정의됩니다. 사용자는 뷰에 정의된 필터를 통과하는 데이터의 해당 부분에만 액세스할 수 있습니다. 이 패턴은 단일 기본 테이블을 사용하여 관련 이해관계자의 데이터에 대한 제어된 액세스를 유도하는 방법을 보여줍니다.
# MAGIC 
# MAGIC ## Run a Query to Confirm
# MAGIC 
# MAGIC In the **SQL Editor**, all users should use the **Schema Browser** on the lefthand side to navigate to the database being controlled by the admin.
# MAGIC 
# MAGIC Users should start a query by typing **`SELECT * FROM`** and then click the **>>** that appears while hovering over the view name to insert it into their query.
# MAGIC 
# MAGIC This query should return 2 results.
# MAGIC 
# MAGIC **NOTE**: This view is defined against the **`users`** table, which has not had any permissions set yet. Note that users have access only to that portion of the data that passes through the filters defined on the view; this pattern demonstrates how a single underlying table can be used to drive controlled access to data for relevant stakeholders.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 테이블 사용 권한 변경
# MAGIC 
# MAGIC 위와 동일한 단계를 수행하지만 이제 **`users`** 테이블에 대해 수행합니다.
# MAGIC 
# MAGIC 단계:
# MAGIC 1. **`users`** 테이블을 선택합니다
# MAGIC 1. **Permissions** 탭을 선택합니다
# MAGIC 1. 파란색 **Grant** 버튼을 클릭합니다
# MAGIC 1. **SELECT** 및 **READ_METADATA** 옵션을 선택합니다.
# MAGIC 1. 상단의 드롭다운 메뉴에서 **All Users** 그룹을 선택합니다
# MAGIC 1. **확인**을 클릭합니다
# MAGIC 
# MAGIC ## Change Table Permissions
# MAGIC 
# MAGIC Perform the same steps as above, but now for the **`users`** table.
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Select the **`users`** table
# MAGIC 1. Select the **Permissions** tab
# MAGIC 1. Click the blue **Grant** button
# MAGIC 1. Select the **SELECT** and **READ_METADATA** options
# MAGIC 1. Select the **All Users** group from the drop down menu at the top
# MAGIC 1. Click **OK**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 사용자가 **`DROP TABLE`** 을(를) 시도하도록 함
# MAGIC 
# MAGIC **SQL Editor**에서 사용자가 이 표의 데이터를 살펴보도록 권장합니다.
# MAGIC 
# MAGIC 사용자가 여기서 데이터를 수정하도록 권장합니다. 사용 권한이 올바르게 설정되었다고 가정하면 이러한 명령은 오류가 발생합니다.
# MAGIC 
# MAGIC ## Have Users Attempt to **`DROP TABLE`**
# MAGIC 
# MAGIC In the **SQL Editor**, encourage users to explore the data in this table.
# MAGIC 
# MAGIC Encourage users to try to modify the data here; assuming permissions were set correctly, these commands should error out.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 파생 데이터 세트를 위한 데이터베이스 생성
# MAGIC 
# MAGIC 대부분의 경우 사용자는 파생 데이터 세트를 저장할 위치가 필요합니다. 현재 사용자는 작업영역의 기존 ACL과 학생들이 완료한 이전 수업 중에 작성한 데이터베이스에 따라 어느 위치에서도 새 테이블을 작성할 수 없습니다.
# MAGIC 
# MAGIC 아래 셀은 새 데이터베이스를 생성하고 모든 사용자에게 권한을 부여하기 위해 코드를 출력합니다.
# MAGIC 
# MAGIC **참고**: 여기서는 Data Explorer가 아닌 SQL Editor를 사용하여 권한을 설정합니다. 쿼리 기록을 검토하여 Data Explorer의 이전 권한 변경 내용이 모두 SQL 쿼리로 실행되어 여기에 기록되었음을 확인할 수 있습니다. 또한 Data Explorer의 대부분의 작업은 UI 필드를 채우는 데 사용되는 해당 SQL 쿼리와 함께 기록됩니다.
# MAGIC 
# MAGIC ## Create a Database for Derivative Datasets
# MAGIC 
# MAGIC In most cases users will need a location to save out derivative datasets. At present, users may not have the ability to create new tables in any location (depending on existing ACLs in the workspace and databases created during previous lessons students have completed).
# MAGIC 
# MAGIC The cell below prints out the code to generate a new database and grant permissions to all users.
# MAGIC 
# MAGIC **NOTE**: Here we set permissions using the SQL Editor rather than the Data Explorer. You can review the Query History to note that all of our previous permission changes from Data Explorer were executed as SQL queries and logged here (additionally, most actions in the Data Explorer are logged with the corresponding SQL query used to populate the UI fields).

# COMMAND ----------

DA.generate_create_database_with_grants()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 사용자가 새 테이블 또는 뷰를 작성하도록 합니다
# MAGIC 
# MAGIC 사용자가 새 데이터베이스에 표와 보기를 작성할 수 있는지 테스트할 시간을 줍니다.
# MAGIC 
# MAGIC **참고**: 사용자에게 **MODIFY** 및 **SELECT** 권한도 부여되었으므로 모든 사용자는 피어에서 만든 엔티티를 즉시 쿼리하고 수정할 수 있습니다.
# MAGIC 
# MAGIC ## Have Users Create New Tables or Views
# MAGIC 
# MAGIC Give users a moment to test that they can create tables and views in this new database.
# MAGIC 
# MAGIC **NOTE**: because users were also granted **MODIFY** and **SELECT** permissions, all users will immediately be able to query and modify entities created by their peers.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 관리자 구성
# MAGIC 
# MAGIC 현재 사용자는 기본적으로 기본 카탈로그 **`hive_metastore`** 에 부여된 테이블 ACL 권한이 없습니다. 다음 실습에서는 사용자가 데이터베이스를 작성할 수 있다고 가정합니다.
# MAGIC 
# MAGIC Databricks SQL을 사용하여 기본 카탈로그에 데이터베이스와 테이블을 만드는 기능을 사용하려면 작업영역 관리자가 DBSQL 쿼리 편집기에서 다음 명령을 실행하도록 하십시오:
# MAGIC 
# MAGIC <strong><code>GRANT usage, create ON CATALOG &#x60;hive_metastore&#x60; TO &#x60;users&#x60;</code></strong>
# MAGIC 
# MAGIC 성공적으로 실행되었는지 확인하려면 다음 쿼리를 실행하십시오:
# MAGIC 
# MAGIC <strong><code>SHOW GRANT ON CATALOG &#x60;hive_metastore&#x60;</code></strong>
# MAGIC 
# MAGIC ## Admin Configuration
# MAGIC 
# MAGIC At present, users do not have any Table ACL permissions granted on the default catalog **`hive_metastore`** by default. The next lab assumes that users will be able to create databases.
# MAGIC 
# MAGIC To enable the ability to create databases and tables in the default catalog using Databricks SQL, have a workspace admin run the following command in the DBSQL query editor:
# MAGIC 
# MAGIC <strong><code>GRANT usage, create ON CATALOG &#x60;hive_metastore&#x60; TO &#x60;users&#x60;</code></strong>
# MAGIC 
# MAGIC To confirm this has run successfully, execute the following query:
# MAGIC 
# MAGIC <strong><code>SHOW GRANT ON CATALOG &#x60;hive_metastore&#x60;</code></strong>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>