# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# #### Introduction
# 
# This set of notebooks (01 - Run in Primary and 02 - Run in DR) demonstrates an automated end-to-end BCDR process to recover supported (Git) items, lakehouses, warehouses and associated data.

# MARKDOWN ********************

# ##### Prerequisites
# Please read the disaster recovery guidance found in <a href="https://learn.microsoft.com/en-us/fabric/security/experience-specific-guidance">the documentation</a> to obtain a general understanding of how to recover lakehouse and warehouse data.
# When the OneLake DR setting has been enabled at capacity level, lakehouse and warehouse data will be geo-replicated to the secondary (paired) region. As mentioned in the documentation, this data may be inaccessible through the normal Fabric UI experience, therefore the data will need to be recovered into a corresponding (new) workspace in the DR region using the storage endpoints (abfs paths) as depicted in the image below. 
# <div>
# <img src="https://github.com/hurtn/images/blob/main/reco_from_docs.png?raw=true" width="800"/>
# </div>
# 
# To use this notebook ensure this runs in a new workspace (Reco1) attached to a new lakehouse (LH_BCDR) and ensure DR is enabled for the assocated capacity (C1).
# 
# <div>
# <img src="https://github.com/hurtn/images/blob/main/before_recovery.png?raw=true" width="800"/>
# </div>


# MARKDOWN ********************

# ###### About this notebook
# 
# This part of the BCDR process collects metadata about your Fabric environment, such as information about your  capacities, workspaces, their assocated Git connections (if connected to Git) and Fabric items. This metadata is used in second notebook "02 - Run in DR" during the recovery process. 

# MARKDOWN ********************

# ##### Imports and Utility Functions
# Ensure the cell below is runs successfully to include the supporting functions

# CELL ********************

%run workspaceutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Notebook parameters

# PARAMETERS CELL ********************

# Specify workspaces to ignore in this part of the process which collects metadata about your Fabric environment. 
# Either use exact workspace names or prefix or suffix with % as wildcard 
# This is useful when you have have many workspaces which don't need to be recovered 
# for businesss continuity purposes and collecting required metadata would take an unecessary amount of time 
# e.g. to ignore any workspaces suffixed with DEV or TEST use ['%DEV','%TEST%'] 

# These list parameters need to be in the format of ['string1','string2',...'stringN']. Use [] for an empty list.

# Specify an exact list of workspaces to ignore e.g. p_ws_ignore_list = ['Microsoft Fabric Capacity Metrics 26/02/2024 16:15:42','AdminInsights']
p_ws_ignore_list = [] 
# Specify a list with wildcards using % e.g. to ignore anything with _DEV and _TEST as a suffix p_ws_ignore_like_list = ['%_DEV%','%_TEST%']  
p_ws_ignore_like_list = [] #['%_DEV%','%_TEST%','%CLONE%']  
# Optionally specify an exact list of workspaces to recover
p_list_of_workspaces_to_recover = [] #['Prod1','Prod2'] #specify exact workspaces

# Boolean parameter to specify verbose informational messages. 
# Only set to True if additional logging information required, otherwise notebook may generate significant (print) messages.
p_logging_verbose = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Check for default lakehouse

# CELL ********************

if (notebookutils.runtime.context['defaultLakehouseId']==None):
    displayHTML('<div style="display: flex; align-items: flex-end;"><img style="float: left; margin-right: 10px;" src="https://github.com/hurtn/images/blob/main/stop.png?raw=true" width="50"><span><h4>Please set a default lakehouse before proceeding</span><img style="float: right; margin-left: 10px;" src="https://github.com/hurtn/images/blob/main/stop.png?raw=true" width="50"></div>')
    print('\n')
    raise noDefaultLakehouseException('No default lakehouse found. Please add a lakehouse to this notebook.')
else: 
    print('Default lakehouse is set to '+ notebookutils.runtime.context['defaultLakehouseName'] + '('+ notebookutils.runtime.context['defaultLakehouseId'] + ')')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Store metadata of Capacities, Workspaces and Items

# CELL ********************

print('Gathering recovery metadata...')
saveCapacityMeta()
saveWorkspaceMeta()
saveItemMeta(verbose_logging=p_logging_verbose, ws_ignore_list=p_ws_ignore_list,ws_ignore_like_list=p_ws_ignore_like_list,list_of_workspaces_to_recover=p_list_of_workspaces_to_recover)
#saveReportMeta(verbose_logging=p_logging_verbose,only_secondary=False,ws_ignore_list=p_ws_ignore_list,ws_ignore_like_list=p_ws_ignore_like_list)
print('Done')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Save Workspace Git Connection Details

# CELL ********************

from pyspark.sql.functions import lit

table_name = 'gitconnections'
spark.sql("Drop table if exists "+ table_name)
wsgitconnsql  ="SELECT distinct ID,Type,Name FROM workspaces where Type!='AdminInsights' and Name not like 'Microsoft Fabric Capacity Metrics%'"
if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        wsgitconnsql  = wsgitconnsql + " and name not like '" + notlike + "'"
if len(p_ws_ignore_list)>0:
    wsgitconnsql  = wsgitconnsql + " and name not in ('" + "', '".join(p_ws_ignore_list)+ "') "
if len(p_list_of_workspaces_to_recover)>0:
  wsgitconnsql = wsgitconnsql+" and Name in ('" +  "', '".join(p_list_of_workspaces_to_recover)+ "') "

#print(wsgitconnsql)


dfwks = spark.sql(wsgitconnsql).collect()
print("Retreiving Git details for any connected workspaces. This may take a few minutes...")
for idx,i in enumerate(dfwks):
    if i['Type'] == 'Workspace':
        url = "/v1/workspaces/" + i['ID'] + "/git/connection"
        try:
            if p_logging_verbose:
                print("Storing git details for workspace "+i['Name']+" ("+i['ID']+')...')
            response = client.get(url)

            gitProviderDetailsJSON = response.json()['gitProviderDetails']
            gitConnectionStateJSON = response.json()['gitConnectionState']
            gitSyncDetailsJSON = response.json()['gitSyncDetails']
            df = spark.createDataFrame([i['ID']],"string").toDF("Workspace_ID")
            df=df.withColumn("Workspace_Name",lit(i['Name'])).withColumn("gitConnectionState",lit(gitConnectionStateJSON)).withColumn("gitProviderDetails",lit(json.dumps(gitProviderDetailsJSON))).withColumn("gitSyncDetails",lit(json.dumps(gitSyncDetailsJSON)))

            if idx == 0:
                dfall = df
            else:
                dfall= dfall.union(df)
        except Exception as error:
            errmsg =  "Couldn't get git connection status for workspace " + i['Name'] + "("+ i['ID'] + ")."
            errmsg = errmsg + "Error: "+str(error)
            print(str(errmsg))

dfall.withColumn("metaTimestamp",current_timestamp()).write.mode('overwrite').option("mergeSchema", "true").saveAsTable(table_name)
print('Done')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bcdrmeta.gitconnections LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bcdrmeta.gitconnections LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bcdrmeta.gitconnections LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Save Workspace Role Assignments
# 
# Using workspace APIs therefore requires permissions to access each workspace. If run as a Fabric Admin, use the cell below.

# CELL ********************

# iterate through workspaces and store role assignments in a list
import ast
all_role_data = []
table_name = 'wsroleassignments'

spark.sql("Drop table if exists "+ table_name)

rolesql = "SELECT distinct ID,Type,Name FROM workspaces where Type!='AdminInsights'"

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        rolesql  = rolesql + " and name not like '" + notlike + "'"
if len(p_ws_ignore_list)>0:
    rolesql  = rolesql + " and name not in ('" + "', '".join(p_ws_ignore_list)+ "') "
if len(p_list_of_workspaces_to_recover)>0:
  rolesql = rolesql+" and Name in ('" +  "', '".join(p_list_of_workspaces_to_recover)+ "') "


print("Retreiving workspace role assignments...")

roledf = spark.sql(rolesql).collect()
for idx,i in enumerate(roledf):
    if i['Type'] == 'Workspace':
        url = "/v1/workspaces/" +i['ID'] + "/roleAssignments"
        try:
            if p_logging_verbose:
                print('Retreiving roles asignments for workspace '+ i['Name'] + '...')
            raresponse = client.get(url)
            roleassignments  =  raresponse.json()['value']
            for roleassignment in  roleassignments:
                roleassignment['workspaceName']=i['Name']
                roleassignment['workspaceId']=i['ID']
            #print(roleassignments)
            all_role_data.extend(roleassignments)
            
        except Exception as error:
            errmsg =  "Couldn't retreive role assignments for workspace " + i['Name'] + "("+ i['ID'] + "). Error: "+str(error)
            print(str(errmsg))

if all_role_data is not None and len(all_role_data)>0:
    roleassignmentdf = spark.read.json(sc.parallelize(all_role_data)) \
        .withColumn('principalId',col('principal')['id']) \
        .withColumn('displayName',col('principal')['displayName']) \
        .withColumn('principalType',col('principal')['type']) \
        .withColumn('userPrincipalName',col('principal')['userDetails']['userPrincipalName']) \
        .withColumn('workspaceId',col('workspaceId')) \
        .withColumn('workspaceName',col('workspaceName')) \
        .drop('principal')
    #display(roleassignmentdf)
    print(saveTable(roleassignmentdf,'wsroleassignments') ) 
else:
    print('No role data found.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Save Workspace Role Assignments - Using Admin APIs

# CELL ********************

# iterate through workspaces and store role assignments in a list
import ast
all_role_data = []
table_name = 'wsroleassignments'

spark.sql("Drop table if exists "+ table_name)

rolesql = "SELECT distinct ID,Type,Name FROM workspaces where Type!='AdminInsights'"

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        rolesql  = rolesql + " and name not like '" + notlike + "'"
if len(p_ws_ignore_list)>0:
    rolesql  = rolesql + " and name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

print("Retreiving workspace role assignments...")

roledf = spark.sql(rolesql).collect()
for idx,i in enumerate(roledf):
    if i['Type'] == 'Workspace':
        #url = "/v1/workspaces/" +i['ID'] + "/roleAssignments"
        url = f"/v1.0/myorg/admin/groups/{i['ID']}/users"
        try:
            if p_logging_verbose:
                print('Retreiving roles asignments for workspace '+ i['Name'] + '...')
            raresponse = pbiclient.get(url)
            roleassignments  =  raresponse.json()['value']
            for roleassignment in  roleassignments:
                roleassignment['workspaceName']=i['Name']
                roleassignment['workspaceId']=i['ID']
            #print(roleassignments)
            all_role_data.extend(roleassignments)
            
        except Exception as error:
            errmsg =  "Couldn't retreive role assignments for workspace " + i['Name'] + "("+ i['ID'] + "). Error: "+str(error)
            print(str(errmsg))

if all_role_data is not None and len(all_role_data)>0:
        #.withColumn('principalId',col('graphId')) \
        #.withColumn('displayName',col('principal')['displayName']) \
        #.withColumn('principalType',col('principalType')) \
        #.withColumn('userPrincipalName',col('displayName')) \

    roleassignmentdf = spark.read.json(sc.parallelize(all_role_data)) \
        .withColumn('workspaceId',col('workspaceId')) \
        .withColumn('workspaceName',col('workspaceName')) 
#        .drop('principal')
    #display(roleassignmentdf)
    print(saveTable(roleassignmentdf,'wsroleassignments') ) 
else:
    print('No role data found.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# MARKDOWN ********************

# ##### Complete. 
# 
# Now proceed to Stage 3 in the associated accelerator documentation.

# CELL ********************

df = spark.sql("SELECT * FROM bcdrmeta.items LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bcdrmeta.items LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bcdrmeta.workspaces LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
