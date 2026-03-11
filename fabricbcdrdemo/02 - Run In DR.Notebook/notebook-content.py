# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "74c0b0d2-afed-41e7-b116-0c5d1eafaa12",
# META       "default_lakehouse_name": "bcdr_dr_lakehouse",
# META       "default_lakehouse_workspace_id": "c70b788e-eb76-44ba-8219-6b64e1b328ea",
# META       "known_lakehouses": [
# META         {
# META           "id": "74c0b0d2-afed-41e7-b116-0c5d1eafaa12"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ##### Imports and Utility Functions
# Ensure the cell below is runs successfully to include all the helper utilities

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC   "defaultLakehouse": {
# MAGIC     "name": "bcdr_dr_lakehouse"
# MAGIC     // "id": "<optional lakehouse-id>",
# MAGIC     // "workspaceId": "<optional workspace-id>"
# MAGIC   }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run workspaceutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Prerequisites
# Please read the disaster recovery guidance found in <a href="https://learn.microsoft.com/en-us/fabric/security/experience-specific-guidance">the documentation</a> to obtain a general understanding of how to recover lakehouse and warehouse data.
# When the OneLake DR setting has been enabled at capacity level, lakehouse and warehouse data will be geo-replicated to the secondary (paired) region - which may not be accessible through the normal Fabric UI experience. Therefore, in a DR scenario, this data will need to be recovered into a corresponding (new) workspace in the DR region using the storage endpoints (abfs paths) as depicted in the image below. 
# <!--<div style="margin: 0 auto; text-align: center; overflow: hidden;">
# <div style="float: left;"> -->
# <img src="https://github.com/hurtn/images/blob/main/reco_from_docs.png?raw=true" width="800"/>
# <!--<small><b>Figure 1</b></small></div></div><br> -->
# 
# To use this recovery notebook please ensure:<p>&nbsp;</p>
# 1. You have imported and run the "01 - Run in Primary" notebook in the primary region.
# <!-- <div style="margin: 0 auto; text-align: center; overflow: hidden;">
# <div style="float: left;"> -->
# <img src="https://github.com/hurtn/images/blob/main/before_recovery.png?raw=true" width="800"/>
# <!-- <small><b>Figure 2</b></small></div></div><br> --><p>&nbsp;</p>
# 2. There is at least one capacity (C2) created in the DR region. 
# <img src="https://github.com/hurtn/images/blob/main/starting_reco_stage1v2.png?raw=true" width="800"/>
# <!--<small><b>Figure 3</b></small> </div></div><br> --><p>&nbsp;</p>
# 3. A recovery workspace (Reco2) has been created in the secondary region (C2.Reco2) which contains this notebook "02 - Run in DR" and attached to a default lakehouse (C2.Reco2.LH_BCDR).
# <img src="https://github.com/hurtn/images/blob/main/starting_reco_stage2v2.png?raw=true" width="800"/>
# <!-- <small><b>Figure 4</b></small> </div></div> -->
# <p>&nbsp;</p>
# 4. Set the values for the notebook parameters below according to your item names. Sample names are shown below if using the naming scheme in the image above. Note: If passing these parameters in from a pipeline these defaults will be overridden:<p>&nbsp;</p>
# <left>
# p_bcdr_workspace_src -> Reco1 <br>
# p_bcdr_workspace_tgt -> Reco2  <br>
# p_bcdr_lakehouse_src -> LH_BCDR <br>
# p_bcdr_lakehouse_tgt -> LH_BCDR <br>
# p_secondary_ws_suffix eg '_SECONDARY' - specify a suffix which will be appended to each workspace created in the DR region. This is to ensure uniqueness of workspace names and is useful for identifying these worksspaces when testing in a non-DR scenario also.  <br>
# p_recovered_object_suffix eg '_recovered'- specify a suffix that will be appended to each table name created in the recovery lakehouse C2.Reco2.LH_BCDR <br>
# list_of_workspaces_to_recover eg ['WS1','WS2'] - specify a list of workspaces to specifically recover. This is useful for testing in a non-DR scenario also. Leave empty [''] for all workspaces.  <br>


# PARAMETERS CELL ********************

# Specify the source and target BCDR workspaces and lakehouses
p_bcdr_workspace_src = 'BCDR_Primary' #'BCDR'
p_bcdr_workspace_tgt = 'BCDR_DR' #'BCDR_DR'
p_bcdr_lakehouse_src = 'bcdr_primary_lakehouse' #'bcdrmeta'
p_bcdr_lakehouse_tgt = 'bcdr_dr_lakehouse' #'bcdrmeta'

# Specify the DR capacity ID or name. If left blank then the capacity of this workspace will be used
target_capacity = 'be3709da-fc46-46f5-be5b-6f9bec79bbfb' 

# This variable adds a suffix to the name of the new workspaces created to ensure there are no naming conflicts with the original workspace name. 
# Ensure that you use a string that will gaurantee uniqueness rather than common terms which may be used by others in day to day activities.  
p_secondary_ws_suffix = '_DR'
p_recovered_object_suffix = '_recovered'

# Determines whether to add role assignments to the new workspaces. If you prefer to apply these at a later stage set the value to False. 
p_add_ws_role_assignments = True

# List parameters below need to be in the format of ['string1','string2',...'stringn']. Empty lists must be declared as []
# Specify the list of workspaces to recover, leave empty [] to recover all. For specific workspaces e.g. p_list_of_workspaces_to_recover = ['Workspace1','Workspace2'] 
p_list_of_workspaces_to_recover = [] #['Prod1','Prod2'] #to specify exact workspaces
# Specify an exact list of workspaces to ignore e.g. p_ws_ignore_list = ['Microsoft Fabric Capacity Metrics 26/02/2024 16:15:42','AdminInsights']
p_ws_ignore_list = [] # add workspaces to this list to ignore them from the metadata extract process including git details
# Specify a list with wildcards using % e.g. to ignore anything with _DEV and _TEST as a suffix p_ws_ignore_like_list = ['%_DEV%','%_TEST%']  
p_ws_ignore_like_list  = [] #['%_DEV%','%_TEST%','%CLONE%']   #eg to specify exact ignore list ['BCDR']

# Boolean parameter to specify verbose informational messages. 
# Only set to True if additional logging information required, otherwise notebook may generate significant (print) messages.
p_logging_verbose = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Check default lakehouse

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

# ###### Check target capacity

# CELL ********************

# Verify target capacity and status
if target_capacity != '':
    if _is_valid_uuid(target_capacity):
        target_capacity_id = target_capacity.lower()
    else: 
        # fetch the capacity ID
        target_capacity_id = labs.resolve_capacity_id(target_capacity)
else: # if not set then fetch capacity of this workspace
    target_capacity_id = labs.resolve_capacity_id()

# check capacity status
cap_status = get_capacity_status(target_capacity_id)
if cap_status == 'Inactive':
    raise ValueError(f"Status of capacity {target_capacity} is {cap_status}. Please resume the capacity and retry")
else:
    print(f"Status of capacity {target_capacity} is {cap_status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Stage 4: Recover metadata tables from the BCDR workspace
# 
# In order for the recovery process to begin, it needs the metadata of the primary environment (created by running the Store DR Metadata notebook) such as workspaces. This data will be persisted as tables in the default lakehouse of this notebook.
# <div>
# <img src="https://github.com/hurtn/images/blob/main/starting_reco_stage2v3.png?raw=true" width="800"/>
# </div>


# CELL ********************

# Gathers the list of recovers tables and source paths to be copied into the lakehouse associated with this notebook 

src_path = f'abfss://{p_bcdr_workspace_src}@onelake.dfs.fabric.microsoft.com/{p_bcdr_lakehouse_src}.Lakehouse'

table_list = get_lh_object_list(src_path)
print('The following tables will attempt to be recovered and persisted as tables in the default lakehouse of this notebook...')
display(table_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Restoring recovery tables...')
res = copy_lh_objects(table_list[table_list['type']=='table'],p_bcdr_workspace_src,p_bcdr_workspace_tgt,
                      p_bcdr_lakehouse_src,p_bcdr_lakehouse_tgt,p_recovered_object_suffix,False)
print('Done.')
display(res)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exact GUIDs from your BCDR_Primary URL
primary_ws_id = "9aaf3e2e-0243-4209-84ea-0339a1083d61"
primary_lh_id = "e1a48c79-97e1-4e90-b7f3-377bb0da39e4"

tables_to_restore = ['capacities', 'workspaces', 'items']

for tbl in tables_to_restore:
    try:
        # Based on your URL and screenshot, the tables are in the 'dbo' folder
        # Path structure: Tables / SchemaName / TableName
        source_table_path = f"abfss://{primary_ws_id}@onelake.dfs.fabric.microsoft.com/{primary_lh_id}/Tables/dbo/{tbl}"
        
        # Read the delta data
        df_meta = spark.read.format("delta").load(source_table_path)
        
        # Write to your DR lakehouse (creates workspaces_recovered, items_recovered, etc.)
        target_table_name = f"{tbl}{p_recovered_object_suffix}"
        df_meta.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
        
        print(f"Successfully restored {tbl} to {target_table_name}")
    except Exception as e:
        print(f"Failed to restore {tbl} from path: {source_table_path}\nError: {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Stage 5: Recreate workspaces
# Recover the workspaces that used to exist in the primary. The suffix parameter will be appended to the workspace name
# 
# <div>
# <img src="https://github.com/hurtn/images/blob/main/Stage5_workspaces_created.png?raw=true" width="800"/>
# </div>

# CELL ********************

thisWsId = notebookutils.runtime.context['currentWorkspaceId'] #obtaining this so we don't accidently delete this workspace!

recovered_ws_sql = "SELECT distinct ID,Type,Name,Capacity_Id FROM workspaces" + p_recovered_object_suffix + " where id != '"+thisWsId+"' and name != '"+p_bcdr_workspace_src+"'"

if len(p_list_of_workspaces_to_recover)>0:
  recovered_ws_sql = recovered_ws_sql+" and Name in ('" +  "', '".join(p_list_of_workspaces_to_recover)+ "') "

if len(p_ws_ignore_list)>0:
   recovered_ws_sql = recovered_ws_sql+ " and Name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        recovered_ws_sql  = recovered_ws_sql + " and name not like '" + notlike + "'"

print('Recreating workspaces with suffix of '+ p_secondary_ws_suffix + '...')
#print(recovered_ws_sql)
df = spark.sql(recovered_ws_sql).collect()
for i in df:
    #print(i['ID'])
    if i['Type'] == 'Workspace':
      try:
        if p_logging_verbose:
          print("Creating workspace: " + i['Name']+p_secondary_ws_suffix + " in target capacity "+ target_capacity +"...")
        response = fabric.create_workspace(i['Name']+p_secondary_ws_suffix,target_capacity_id)
        if p_logging_verbose:
          print("Created workspace with ID: " + response)
      except Exception as error:
          errmsg =  "Failed to recreate workspace " + i['Name'] +p_secondary_ws_suffix + " with capacity ID ("+ i['Capacity_Id'] + ") due to: "+str(error)
          print(errmsg)
print('Now reloading workspace metadata table...')
# Now popupate the workspace metadata table
saveWorkspaceMeta()
print('Done.')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Stage 6: Connect Git and Initialise
# 
# 
# Iterate through workspaces and connect them to the same branch they were connected to in primary.
# Then intialise and update from git to start synchronising items from the repo.
# <div>
# <img src="https://github.com/hurtn/images/blob/main/Stage6_gitconnected.png?raw=true" width="800"/>
# </div>

# CELL ********************

gitsql = "select gt.gitConnectionState,gt.gitProviderDetails, wks.name Workspace_Name, wks.id Workspace_ID from gitconnections_recovered gt " \
         "inner join workspaces wks on gt.Workspace_Name = replace(wks.name,'" + p_secondary_ws_suffix+ "','') " \
         "where gt.gitConnectionState = 'ConnectedAndInitialized' and wks.name like '%"+p_secondary_ws_suffix+"' and wks.id != '"+thisWsId+"'" 

if len(p_list_of_workspaces_to_recover)>0:
    gitsql = gitsql+" and gt.Workspace_Name in ('""" +  "', '".join(p_list_of_workspaces_to_recover)+ "') "

if len(p_ws_ignore_list)>0:
    gitsql = gitsql+" and gt.Workspace_Name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        gitsql  = gitsql + " and name not like '" + notlike + "'"

print('Reconnecting workspaces to Git...')
df = spark.sql(gitsql).collect()

for idx,i in enumerate(df):
    if i['gitConnectionState'] == 'ConnectedAndInitialized':
        
        url = "/v1/workspaces/" + i['Workspace_ID'] + "/git/connect"
        payload = '{"gitProviderDetails": ' + i['gitProviderDetails'] + '}'
        #print(str(payload))

        try:
            if p_logging_verbose:
                print('Attempting to connect workspace '+ i['Workspace_Name'])
            response = client.post(url,json= json.loads(payload))
            if p_logging_verbose:
                print(str(response.status_code) + response.text) 
            success = True
            
        except Exception as error:
            error_string = str(error)
            error_index = error_string.find("Error:")

            # Extract the substring after "Error:"
            error_message = error_string[error_index + len("Error:"):].strip()
            headers_index = error_message.find("Headers:")

            # Extract the substring before "Headers:"
            error_message = error_message[:headers_index].strip()
            error_data = json.loads(error_message)
            # Extract the error message
            error_message = error_data.get("message", "")

            errmsg =  "Couldn't connect git to workspace " + i['Workspace_Name'] + "("+ i['Workspace_ID'] + "). Error: "+str(error_message)
            print(str(errmsg))
            success = False
        # If connection successful then try to initialise    
        if (success):
            url = "/v1/workspaces/" + i['Workspace_ID'] + "/git/initializeConnection"
            payload = {"initializationStrategy":"PreferRemote"}
            try:
                if p_logging_verbose:
                    print('Attempting to initialize git connection for workspace '+ i['Workspace_Name'])
                response = client.post(url,json= payload)
                #print(str(response.status_code) + response.text) 
                commithash = response.json()['remoteCommitHash']
                if p_logging_verbose:
                    print('Successfully initialized. Updating with commithash '+commithash)
                if commithash!='':
                    url = "/v1/workspaces/" + i['Workspace_ID'] + "/git/updateFromGit"
                    payload = '{"remoteCommitHash": "' + commithash + '","conflictResolution": {"conflictResolutionType": "Workspace","conflictResolutionPolicy": "PreferWorkspace"},"options": {"allowOverrideItems": true}}'
                    response = client.post(url,json= json.loads(payload))
                    if response.status_code==202:
                        print('Successfully started sync, LRO in progress...')
                        location_url = response.headers.get("Location")
                        print(f"Polling URL to track operation status is {location_url}")
                        time.sleep(15)
                        response = long_running_operation_polling(location_url, 15)

            except Exception as error:
                success = False
                error_string = str(error)
                error_index = error_string.find("Error:")

                # Extract the substring after "Error:"
                error_message = error_string[error_index + len("Error:"):].strip()
                headers_index = error_message.find("Headers:")

                # Extract the substring before "Headers:"
                error_message = error_message[:headers_index].strip()
                error_data = json.loads(error_message)
                # Extract the error message
                error_message = error_data.get("message", "")
                errmsg =  "Couldn't initialize git for workspace " + i['Workspace_Name'] + "("+ i['Workspace_ID'] + "). Error: "+str(error_message)
                print(str(errmsg))

                        
if success:        
    print('Done')
else:
    print('Completed with errors.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### After git sync completes then capture environment metadata (items and reports)  
# #TODO: Need to write a method to check the sync is complete before attempting to extract the items. For now you will need to wait until the sync has complete and all workspaces have been updated from git. For now adding a sleep into the process to make sure it waits a minute for the sync to complete.

# CELL ********************

import time
print('Gathering metadata about reports and items... ')
saveItemMeta(verbose_logging=p_logging_verbose, ws_ignore_list=p_ws_ignore_list,ws_ignore_like_list=p_ws_ignore_like_list,list_of_workspaces_to_recover=p_list_of_workspaces_to_recover)
#saveReportMeta(verbose_logging=p_logging_verbose,only_secondary=True,ws_ignore_list=p_ws_ignore_list,ws_ignore_like_list=p_ws_ignore_like_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Stage 7: Recover lakehouse data (files and tables) to corresponding recovered workspaces
# 
# Copy lakehouse data (files and tables) from source abfs path to target lakehouse abfs path
# 
# <div>
# <img src="https://github.com/hurtn/images/blob/main/lh_reocvery.png?raw=true" width="800"/>
# </div>

# CELL ********************

thisWsId = notebookutils.runtime.context['currentWorkspaceId'] #failsafe: obtaining this workspace id so we don't accidently overwrite objects in this workspace!

data_recovery_sql = """select wr.Name primary_ws_name, ir.type primary_type, ir.DisplayName primary_name,  
wr.id primary_ws_id, ir.id primary_id,wks.id secondary_ws_id, wks.Name secondary_ws_name, it.id secondary_id, 
it.DisplayName secondary_name, cr.display_name capacity_name
from items_recovered ir 
    inner join workspaces_recovered wr on wr.Id = ir.WorkspaceId 
    inner join capacities_recovered cr on wr.capacity_id = cr.id
    inner join workspaces wks on wr.Name = replace(wks.name,'""" + p_secondary_ws_suffix+ """','')
    inner join items it on ir.DisplayName = it.DisplayName and it.WorkspaceId = wks.Id 
where ir.type in ('Lakehouse','Warehouse') and it.type = ir.type """

if len(p_list_of_workspaces_to_recover):
  data_recovery_sql = data_recovery_sql + """ and wr.Name in ('""" +  "', '".join(p_list_of_workspaces_to_recover)+ """')"""

data_recovery_sql = data_recovery_sql + """and wks.name like '%"""+p_secondary_ws_suffix+"""'  and wks.id != '"""+thisWsId+"""' order by ir.type,primary_ws_name"""

#print(data_recovery_sql)
df_recovery_items=spark.sql(data_recovery_sql)
if df_recovery_items.count()>0:
    print("The subsequent notebook cells attempt to recover lakehouse and warehouse data for the following items...")
else:
    print("No lakehouses and warehouses found to recover.")
# populate dataframes for lakehouse metadata and warehouse metadata respectively 
lakehousedf = df_recovery_items.filter("primary_type='Lakehouse'").collect()
warehousedf = df_recovery_items.filter("primary_type='Warehouse'").collect()

display(df_recovery_items.select("primary_ws_name","primary_type","primary_name","secondary_ws_name","secondary_name", "secondary_ws_id", "capacity_name"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Traceback (most recent call last) Cell In[37], 
# line 30     
# 28 table_list = get_wh_object_list(schema_list['name'],src_path)     
# 29 #recover tables---> 
# 30 copy_lh_objects(table_list[table_list['type']=='table'],i['primary_ws_id'], i['secondary_ws_id'] ,i['primary_id'],i['secondary_id'],p_recovered_object_suffix,False,True)     
# 31 table_list = get_lh_object_list(src_path,['Files'])     
# 32 #recover files File ~/cluster-env/trident_env/lib/python3.11/site-packages/pandas/core/frame.py:3893, in DataFrame.__getitem__(self, key)   
# 3891 if self.columns.nlevels > 1:   3892     return self._getitem_multilevel(key)-> 3893 indexer = self.columns.get_loc(key)   3894 if is_integer(indexer):   3895     indexer = [indexer] File ~/cluster-env/trident_env/lib/python3.11/site-packages/pandas/core/indexes/range.py:418, in RangeIndex.get_loc(self, key)    416         raise KeyError(key) from err    417 if isinstance(key, Hashable):--> 418     raise KeyError(key)    419 self._check_indexing_error(key)    420 raise KeyError(key) KeyError: 'type' 


# CELL ********************

def isSchemaEnabledLakehouse(p_workspace, p_lakehouse):
    if _is_valid_uuid(p_workspace):
        p_workspace_id = p_workspace
    else:
        p_workspace_id = fabric.resolve_workspace_id(p_workspace)

    if _is_valid_uuid(p_lakehouse):
        p_lakehouse_id = p_lakehouse
    else:
        p_lakehouse_id = fabric.resolve_item_id(p_lakehouse,'Lakehouse',p_workspace)
    jres = client.get(f'v1/workspaces/{p_workspace_id}/lakehouses/{p_lakehouse_id}').json()
    if "defaultSchema" in jres['properties']:
        return True
    else:
        return False

print('The following lakehouse(s) will attempt to be recovered... ')
display(lakehousedf)

for idx,i in enumerate(lakehousedf):
    # Define the full abfss source path of the primary BCDR workspace and lakehouse 
    src_path = f'abfss://'+i['primary_ws_id']+'@onelake.dfs.fabric.microsoft.com/'+i['primary_id']
    if p_logging_verbose:
        print('Attempting to recover items for workspace: ' + i['primary_ws_name'] + ', lakehouse: ' + i['primary_name'] + ' into target workspace ' + i['secondary_ws_name'] + ' lakehouse ' + i['secondary_name'])
    if isSchemaEnabledLakehouse(i['primary_ws_id'],i['primary_id']):
        schema_list = get_lh_object_list(src_path,['Tables'])
        table_list = get_wh_object_list(schema_list['name'],src_path)
        if len(table_list)>0:         #check there are tables present in the lakehouse before attempt to copy
            #recover tables
            copy_lh_objects(table_list[table_list['type']=='table'],i['primary_ws_id'], i['secondary_ws_id'] ,i['primary_id'],i['secondary_id'],p_recovered_object_suffix,False,True)
            table_list = get_lh_object_list(src_path,['Files'])
            #recover files
            copy_lh_objects(table_list[table_list['type']=='file'],i['primary_ws_id'], i['secondary_ws_id'] ,i['primary_id'],i['secondary_id'],p_recovered_object_suffix,False,True)
        else:
            print(f'No tables found in lakehouse {i["primary_name"]}')
    else:
        table_list = get_lh_object_list(src_path)
        copy_lh_objects(table_list[table_list['type']=='file'],i['primary_ws_id'], i['secondary_ws_id'] ,i['primary_id'],i['secondary_id'],p_recovered_object_suffix,False,True)
        #recover tables
        copy_lh_objects(table_list[table_list['type']=='table'],i['primary_ws_id'], i['secondary_ws_id'] ,i['primary_id'],i['secondary_id'],p_recovered_object_suffix,False,True)

print('Done')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Stage 8: Prepare warehouse recovery
# ###### Important: 
# This process creates a staging lakehouse to store shortcuts which point back to the DR copy of the tables (actually delta folders) - in testing this will point back to folders in the primary region but in a true DR scenario where Microsoft has failed over the OneLake endpoints, they will point back to folders in the secondary (paired) storage region.
# A parameterised pipeline is injected into each target workspace which will load target tables from these shortcuts.
# This staging lakehouse and pipeline can be deleted manually after the datawarehouse tables have been successfully recovered.
# <div>
# <img src="https://github.com/hurtn/images/blob/main/wh_recovery.png?raw=true" width="1000"/>
# </div>

# CELL ********************

def get_token(audience="pbi"):
    return notebookutils.credentials.getToken(audience)

storageclient = fabric.FabricRestClient(token_provider=get_token)

print('The following warehouse(s) will attempt to be recovered... ')
display(warehousedf)
print('\nPreparing for recovery...\n')
# interate through all the data warehouses to recover
for idx,i in enumerate(warehousedf):
    if p_logging_verbose:
        print('Setting up for recovery of warehouse '+i['primary_ws_name'] + '.'+i['primary_name'] + ' into ' + i['secondary_ws_name'] + '.'+i['secondary_name'] )

    src_path = f'abfss://'+i['primary_ws_id']+'@onelake.dfs.fabric.microsoft.com/'+i['primary_id']
    tgt_path = f'abfss://'+i['secondary_ws_id']+'@onelake.dfs.fabric.microsoft.com/'+i['secondary_id']

    # extract the list of schemas per data 
    schema_list = get_lh_object_list(src_path,['Tables'])
    # extract a list of warehouse objects per schema and store in a list
    table_list = get_wh_object_list(schema_list['name'],src_path)
  
    # create a temporary staging lakehouse per warehouse to create shortcuts into, 
    # which point back to original warehouse data currently in the DR storage account
    lhname = 'temp_rlh_' + i['primary_ws_name']+'_'+i['primary_name']
    # check if it exists before attempting create
    if p_logging_verbose:
        print('Checking whether the temporary lakehouse "'+ lhname +'" exists in workspace '+i['secondary_ws_name']+'...')
    temp_lh_id = getItemId(i['secondary_ws_id'],lhname,'Lakehouse')
    if temp_lh_id == 'NotExists':
        lhname = 'temp_rlh_' + i['primary_ws_name']+'_'+i['primary_name'][:256] # lakehouse name should not exceed 256 characters
        payload = payload = '{"displayName": "' + lhname + '",' \
        + '"description":  "Interim staging lakehouse for primary warehouse recovery: ' \
        + i['primary_ws_name']+'_'+i['primary_name'] + 'into workspace '+ i['secondary_ws_name'] + '(' + i['secondary_ws_id'] +')"}'
        try:
            lhurl = "v1/workspaces/" + i['secondary_ws_id'] + "/lakehouses"
            lhresponse = client.post(lhurl,json= json.loads(payload))
            temp_lh_id = lhresponse.json()['id']
            if p_logging_verbose:
                print('Temporary lakehouse "'+ lhname +'" created with Id ' + temp_lh_id + ': ' + str(lhresponse.status_code) + ' ' + str(lhresponse.text))
        except Exception as error:
            print(error.errorCode)
    else:
        if p_logging_verbose:
            print('Temporary lakehouse '+lhname+' (' + temp_lh_id + ') already exists.')
        

    # Create shortcuts for every table in the format of schema_table under the tables folder
    for index,itable in table_list.iterrows():
        shortcutExists=False
        # Check if shortcut exists
        try:
            url = "v1/workspaces/" + i['secondary_ws_id'] + "/items/" + temp_lh_id + "/shortcuts/Tables/"+itable['schema']+'_'+itable['name']
            tlhresponse = storageclient.get(url)
            shortcutExists = True
            if p_logging_verbose:
                print('Shortcut '+itable['schema']+'_'+itable['name'] +' already exists')
        except Exception as error:
            shortcutExists = False    

        if not shortcutExists: 
            # Create shortcuts - one per table per schema
            url = "v1/workspaces/" + i['secondary_ws_id'] + "/items/" + temp_lh_id + "/shortcuts"
            scpayload = '{' \
            '"path": "Tables/",' \
            '"name": "'+itable['schema']+'_'+itable['name']+'",' \
            '"target": {' \
            '"oneLake": {' \
                '"workspaceId": "' + i['primary_ws_id'] + '",' \
                '"itemId": "'+ i['primary_id'] +'",' \
                '"path": "/Tables/' + itable['schema']+'/'+itable['name'] + '"' \
                '}}}' 
            try:
                #print(scpayload)                
                shctresponse = storageclient.post(url,json= json.loads(scpayload))
                if p_logging_verbose:
                    print('Shortcut '+itable['schema']+'_'+itable['name'] + ' created.' )

            except Exception as error:
                print('Error creating shortcut '+itable['schema']+'_'+itable['name']+' due to '+str(error) + ':' + shctresponse.text)
    
    recovery_pipeline_prefix= 'plRecover_WH6'       
    # recovery pipeline name should not exceed 256 characters
    recovery_pipeline = recovery_pipeline_prefix+'_'+i['primary_ws_name'] + '_'+i['primary_name'][:256]
    if p_logging_verbose:
        print('Attempting to deploy a copy pipeline in the target workspace to load the target warehouse tables from the shortcuts created above... ')
    # Create the pipeline in the target workspace that loads the target warehouse from shortcuts created above 
    plid = getItemId( i['secondary_ws_id'],recovery_pipeline,'DataPipeline')
    #print(plid)
    if plid == 'NotExists':
      plid = createDWrecoverypl(i['secondary_ws_id'],recovery_pipeline_prefix+'_'+i['primary_ws_name'] + '_'+i['primary_name'])
      if p_logging_verbose:
          print('Recovery pipeline ' + recovery_pipeline + ' created with Id '+plid)
    else:
      if p_logging_verbose:
          print('Datawarehouse recovery pipeline "' + recovery_pipeline + '" ('+plid+') already exist in workspace "'+i['secondary_ws_name'] + '" ('+i['secondary_ws_id']+')')  
          print('\n')
print('Done')     



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# wait for sql endpooint sync to reflect the new shortcuts that have just been created
time.sleep(60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Stage 9: Recover warehouse data by running copy pipelines

# CELL ********************

print('Starting warehouse recovery pipelines...')
# interate through all the data warehouses to recover
for idx,i in enumerate(warehousedf):
    if p_logging_verbose:
        print('Invoking pipeline to copy warehouse data from  '+i['primary_ws_name'] + '.'+i['primary_name'] + ' into ' + i['secondary_ws_name'] + '.'+i['secondary_name'] )

    src_path = f'abfss://'+i['primary_ws_id']+'@onelake.dfs.fabric.microsoft.com/'+i['primary_id']
    #tgt_path = f'abfss://'+i['secondary_ws_id']+'@onelake.dfs.fabric.microsoft.com/'+i['secondary_id']

    # extract the list of schemas per data 
    schema_list = get_lh_object_list(src_path,['Tables'])
    #display(schema_list)
    # extract a list of warehouse objects per schema and store in a list
    table_list = get_wh_object_list(schema_list['name'].to_list(),src_path)

    tablesToCopyParam = table_list[['schema','name']].to_json( orient='records')
    # ensure the temporary lakehouse exists
    lhname = 'temp_rlh_' + i['primary_ws_name']+'_'+i['primary_name']
    temp_lh_id = getItemId(i['secondary_ws_id'],lhname,'Lakehouse')
    #temp_lh_id ='0f0f6b7c-1761-41e6-896e-30014f16ff6d'
    
    # obtain the connection string for the lakehouse to pass to the copy pipeline
    whurl  = "v1/workspaces/" + i['secondary_ws_id'] + "/lakehouses/" + temp_lh_id
    whresponse = client.get(whurl)
    lhconnStr = whresponse.json()['properties']['sqlEndpointProperties']['connectionString']

    # get the SQLEndpoint ID of the lakehouse to pass to the copy pipeline
    items = fabric.list_items(workspace=i['secondary_ws_id'])
    temp_lh_sqle_id = items[(items['Type'] == 'SQLEndpoint') & (items['Display Name']==lhname)]['Id'].values[0]


    # obtain the connection string for the warehouse to pass to the copy pipeline    
    whurl  = "v1/workspaces/" + i['secondary_ws_id'] + "/warehouses/" + i['secondary_id']
    whresponse = client.get(whurl)
    whconnStr = whresponse.json()['properties']['connectionInfo']

    recovery_pipeline = recovery_pipeline_prefix+'_'+i['primary_ws_name'] + '_'+i['primary_name'][:256]
    # obtain the pipeline id created to recover this warehouse
    plid = getItemId( i['secondary_ws_id'],recovery_pipeline,'DataPipeline')
    if plid == 'NotExists':
        print('Error: Could not execute pipeline '+recovery_pipeline+ ' as the ID could not be obtained ')
    else:
        # pipeline url including pipeline Id unique to each warehouse
        plurl = 'v1/workspaces/'+i['secondary_ws_id'] +'/items/'+plid+'/jobs/instances?jobType=Pipeline'
        #print(plurl)

        payload_data = '{' \
            '"executionData": {' \
                '"parameters": {' \
                    '"lakehouseId": "' + temp_lh_sqle_id + '",' \
                    '"tablesToCopy": ' + tablesToCopyParam + ',' \
                    '"workspaceId": "' + i['secondary_ws_id'] +'",' \
                    '"warehouseId": "' + i['secondary_id'] + '",' \
                    '"lakehouseConnStr": "' + lhconnStr + '",' \
                    '"warehouseConnStr": "' + whconnStr + '"' \
                    '}}}'
        print(payload_data)
        plresponse = client.post(plurl, json=json.loads(payload_data))
        if p_logging_verbose:
            print(str(plresponse.status_code))      
print('Done')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Stage 10: Add workspace roles assignments to the new workspaces

# CELL ********************

if p_add_ws_role_assignments:
    ws_role_sql = "SELECT wks.ID new_workspace_id, wks.name new_workspace, rar.* FROM wsroleassignments_recovered rar inner join workspaces wks on rar.workspaceName = replace(wks.Name,'" + p_secondary_ws_suffix+ "','')" \
                "where wks.name like '%"+p_secondary_ws_suffix+"' and wks.id != '"+thisWsId+"'" 

    # Only apply roles to the (new) workspaces based the list of workspaces defined in the parameter section at the top of this notebook. 
    # Note that the list is based on the workspace name defined in the primary but will be translated to the associated (new) workspace recently created in the secondary.
    if len(p_list_of_workspaces_to_recover)>0:
        ws_role_sql = ws_role_sql+" and rar.workspaceName in ('" +  "', '".join(p_list_of_workspaces_to_recover)+ "') "

    # Ingore workspaces based on the ignore list defined in the parameter section at the top of this notebook
    if len(p_ws_ignore_list)>0:
        ws_role_sql = ws_role_sql+ " and rar.workspaceName not in ('" + "', '".join(p_ws_ignore_list)+ "') "

    if len(p_ws_ignore_like_list)>0:
        for notlike in p_ws_ignore_like_list:
            ws_role_sql  = ws_role_sql + " and name not like '" + notlike + "'"
    
    print('Adding workspace role assignments...')

    #print(ws_role_sql)
    dfwsrole = spark.sql(ws_role_sql).collect()
    for idx,i in enumerate(dfwsrole):
        wsroleurl = "/v1/workspaces/" + i['new_workspace_id'] + "/roleAssignments"
        wsrolepayload = '{"principal": {"id": "'+i['principalId']+'", "type": "'+i['principalType']+'"},"role": "'+i['role']+'"}'
        #print(str(wsrolepayload))
        
        try:
            if p_logging_verbose:
                print("Attempting to add role assignments " + i['role'] + " for " +  i['principalType'] + " princpal " +i['displayName'] + " (" +i['principalId'] + ") to workspace " + i['new_workspace'] + "("+ i['new_workspace_id'] + ")...")

            response = client.post(wsroleurl,json= json.loads(wsrolepayload))

            success = True
        except Exception as error:
            error_string = str(error)
            error_index = error_string.find("Error:")

            # Extract the substring after "Error:"
            error_message = error_string[error_index + len("Error:"):].strip()
            headers_index = error_message.find("Headers:")

            # Extract the substring before "Headers:"
            error_message = error_message[:headers_index].strip()
            error_data = json.loads(error_message)
            # Extract the error message
            error_message = error_data.get("message", "")
            if error_message is not None:
                errmsg =  "Couldn't add role assignment " + i['role'] + " for princpal " +i['displayName'] + " to workspace " + i['workspaceName'] + "("+ i['workspaceId'] + "). Error: "+error_message
            else:
                errmsg =  "Couldn't add role assignment " + i['role'] + " for princpal " +i['displayName'] + " to workspace " + i['workspaceName'] + "("+ i['workspaceId'] + ")."
            print(str(errmsg))
            success = False
print('Done')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Update default lakehouse for notebooks
# 
# Supports both standard and T-SQL notebooks

# CELL ********************

## Load lookup dataframe for primary to DR workspace conversion
dfoldws = spark.sql("""SELECT wr.Id old_ws_id, wr.Name old_ws_name, wks.Id new_ws_id, wks.Name new_ws_name 
                       FROM workspaces_recovered wr inner join workspaces wks on wr.Name = replace(wks.Name,'""" + p_secondary_ws_suffix+ """','')
                       where wks.name like '%"""+p_secondary_ws_suffix+"""' and wks.id != '"""+thisWsId+"""'
                        AND wks.name != '""" + p_bcdr_workspace_src + """' 
                       """ )
#display(dfoldws)

# Load lookup dataframe for primary item details such as workspace 
dfolditms = spark.sql("""SELECT ir.id old_item_id, ir.displayName old_item_name, wr.Id old_ws_id, wr.Name old_ws_name, 
                                wks.Id new_ws_id, wks.Name new_ws_name 
                       FROM items_recovered ir INNER JOIN workspaces_recovered wr on ir.workspaceId = wr.Id 
                       INNER JOIN workspaces wks on wr.Name = replace(wks.Name,'""" + p_secondary_ws_suffix+ """','')
                        AND wks.name != '""" + p_bcdr_workspace_src + """'  
                       where wks.name like '%"""+p_secondary_ws_suffix+"""' and wks.id != '"""+thisWsId+"""'""" )


ws_nb_sql = "SELECT wks.ID new_workspace_id, wks.name new_workspace_name, itm.* " + \
                "FROM items itm inner join workspaces wks on itm.workspaceId = wks.Id and wks.name != '" + p_bcdr_workspace_src + "'  " + \
                "AND itm.type = 'Notebook' and wks.name like '%"+p_secondary_ws_suffix+"' and wks.id != '"+thisWsId+"'"


# Ingore workspaces based on the ignore list defined in the parameter section at the top of this notebook
if len(p_ws_ignore_list)>0:
    ws_nb_sql = ws_nb_sql+ " and wks.name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        ws_nb_sql  = ws_nb_sql + " and wks.name not like '%" + notlike + "%'"

print('Looping through notebooks...')

#print(ws_nb_sql)
dfwsnb = spark.sql(ws_nb_sql).collect()
#display(dfwsnb)
for idx,i in enumerate(dfwsnb):
    print(f"Updating notebook {i['displayName']}")
    # Get the current notebook definition
    notebook_def = notebookutils.notebook.getDefinition(i['displayName'],workspaceId=i['new_workspace_id'])

    json_payload = json.loads(notebook_def)
    # update default lakehouse if exists
    if 'dependencies' in json_payload['metadata'] \
        and 'lakehouse' in json_payload['metadata']['dependencies'] \
        and json_payload['metadata']["dependencies"]["lakehouse"] is not None:
        # Remove all lakehouses
        current_lakehouse = json_payload['metadata']['dependencies']['lakehouse']

        if 'default_lakehouse_name' in current_lakehouse:
            # look up corresponding lakehouse workspace details
            old_lakehouse_id = current_lakehouse['default_lakehouse']
            #old_lakehouse_ws_name = dfoldws.filter(f"old_ws_id=='{old_lakehouse_ws_id}'").collect()[0][1]
            #new_lakehouse_ws_id = dfoldws.filter(f"old_ws_id=='{old_lakehouse_ws_id}'").collect()[0][2]
            #new_lakehouse_ws_name = dfoldws.filter(f"old_ws_id=='{old_lakehouse_ws_id}'").collect()[0][3]

            old_lakehouse_ws_id,old_lakehouse_ws_name, new_lakehouse_ws_id, new_lakehouse_ws_name = dfolditms.filter(f"old_item_id=='{old_lakehouse_id}'").collect()[0][2:6]

            print('Converting notebook ' + i['displayName'] + ' in workspace ' + i['new_workspace_name'] + '('+ i['new_workspace_id'] + ') with default lakehouse ' + 
            current_lakehouse['default_lakehouse_name']+ ' in previous workspace '+old_lakehouse_ws_name + '('+ old_lakehouse_ws_id + ') to ' + 
            new_lakehouse_ws_name + '(' + new_lakehouse_ws_id +')')

            json_payload['metadata']['dependencies']['lakehouse'] = {}
            
            #Update new notebook definition after removing existing lakehouses and with new default lakehouseId
            (notebookutils.notebook.updateDefinition(
                        name = i['displayName'],
                        content  = json.dumps(json_payload),  
                        defaultLakehouse = current_lakehouse['default_lakehouse_name'],
                        defaultLakehouseWorkspace = new_lakehouse_ws_id,
                        workspaceId = new_lakehouse_ws_id
                        )
                )
            print(f"Updated notebook {i['displayName']}")

        else:
            print(f"No default lakehouse was found in the source notebook {i['displayName']}")


    # update default warehouse if existss
    elif 'dependencies' in json_payload['metadata'] \
        and 'warehouse' in json_payload['metadata']['dependencies'] \
        and json_payload['metadata']["dependencies"]["warehouse"] is not None:

        # Fetch existing details
        current_warehouse = json_payload['metadata']['dependencies']['warehouse']
        old_warehouse_id = current_warehouse['default_warehouse']
        old_wh_name,old_warehouse_ws_id,old_warehouse_ws_name, new_warehouse_ws_id, new_warehouse_ws_name = dfolditms.filter(f"old_item_id=='{old_warehouse_id}'").collect()[0][1:6]

        target_wh_id = fabric.resolve_item_id(item_name = old_wh_name,type='Warehouse',workspace=new_warehouse_ws_id)

        if 'default_warehouse' in current_warehouse:
            print('Converting notebook ' + i['displayName'] + ' in workspace ' + i['new_workspace_name'] + '('+ i['new_workspace_id'] + ') with default warehouse ' + 
            old_wh_name + ' in previous workspace '+old_warehouse_ws_name + '('+ old_warehouse_ws_id + ') to ' + 
            old_wh_name + ' in new workspace '+new_warehouse_ws_name + '(' + new_warehouse_ws_id +')')

        
            #Update new notebook definition after removing existing lakehouses and with new default lakehouseId
            json_payload['metadata']['dependencies']['warehouse']['default_warehouse'] = target_wh_id
            for warehouse in json_payload['metadata']['dependencies']['warehouse']['known_warehouses']:
                if warehouse['id'] == old_warehouse_id:
                    warehouse['id'] = target_wh_id
            #print(json.dumps(json_payload, indent=4))
            (notebookutils.notebook.updateDefinition(
                    name = i['displayName'],
                    content  = json.dumps(json_payload),
                    workspaceId = i['new_workspace_id']
                    )
            )
            print(f"Updated notebook {i['displayName']}")

        else:
            print(f"No default warehouse was found in the source notebook {i['displayName']}")

    else:
        print(f"No default lakehouse/warehouse set for notebook {i['displayName']}, ignoring.")
print('Done')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Update direct lake model lakehouse/warehouse connection
# 
# This only converts the connection for non-default semantic models
# 
# https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.directlake.html#sempy_labs.directlake.update_direct_lake_model_lakehouse_connection
#     

# CELL ********************

ws_sm_sql = "SELECT wks.ID new_workspace_id, wks.name new_workspace_name, itm.* " + \
                "FROM items itm inner join workspaces wks on itm.workspaceId = wks.Id " + \
                "AND itm.type = 'SemanticModel' and wks.name like '%"+p_secondary_ws_suffix+"' " \
                "AND wks.id != '"+thisWsId+"'"

# Ingore workspaces based on the ignore list defined in the parameter section at the top of this notebook
if len(p_ws_ignore_list)>0:
    ws_sm_sql = ws_sm_sql+ " and wks.name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        ws_sm_sql  = ws_sm_sql + " and wks.name not like '%" + notlike + "%'"

#print(ws_sm_sql)
dfwssm = spark.sql(ws_sm_sql).collect()
#display(dfwssm)

# Iterate over each dataset in the dataframe
for idx,row in enumerate(dfwssm):

    # Check if the dataset is not the default semantic model
    if not labs.is_default_semantic_model(row['displayName'], row['new_workspace_id']):
        print('Updating semantic model connection ' + row['displayName'] + ' in workspace '+ row['new_workspace_name'])
        old_ws_name =row['new_workspace_name'].replace(p_secondary_ws_suffix,'')
        labs.directlake.update_direct_lake_model_connection(dataset=row['displayName'], 
                                                                        workspace= row['new_workspace_name'],
                                                                        source=get_direct_lake_source(row['displayName'], workspace= old_ws_name)[1], 
                                                                        source_type=get_direct_lake_source(row['displayName'], workspace= old_ws_name)[0], 
                                                                        source_workspace=row['new_workspace_name'])
        labs.refresh_semantic_model(dataset=row['displayName'], workspace= row['new_workspace_name'])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Update data pipeline source & sink connections
# 
# Support changes lakehouses, warehouses, notebooks and connections from source to target. <br>
# Connections changes should be expressed as an array of tuples [(from_1:to_1),)from_N:to_N)]
# 
# Example usage:
#     p_new_json = swap_pipeline_connection(pipeline_json,dfwsitms,
#             ['Warehouse','Lakehouse','Notebook'],
#             [('CONNECTION_ID_FROM','CONNECTION_ID_TO'),('CONNECTION_ID_FROM','CONNECTION_NAME_TO'),('CONNECTION_NAME_FROM','CONNECTION_ID_TO')]) 


# CELL ********************

connections_from_to = [('1b478585-f04d-49bf-b31d-cd57c3d8ca25','11dd2898-018a-4d52-843a-3cac828984c4')]

ws_itms_sql = """select itm.type,itm_rec.Id old_item_id, itm.Id new_item_id, itm.displayName itm_name,
 wks_rec.Id old_ws_id, wks_rec.Name old_ws_name, wks.Id new_ws_id, wks.Name new_ws_name 
from items itm
inner join workspaces wks on itm.workspaceId = wks.Id
inner join items_recovered itm_rec on itm.displayName = itm_rec.displayName and itm.type = itm_rec.type 
inner join workspaces_recovered wks_rec on wks_rec.Name = replace(wks.Name,'""" + p_secondary_ws_suffix+ """','') 
and wks.id != '"""+thisWsId+"""' and wks_rec.Id = itm_rec.workspaceId
where itm_rec.Id != itm.Id and wks.Name != wks_rec.Name"""

# Ingore workspaces based on the ignore list defined in the parameter section at the top of this notebook
if len(p_ws_ignore_list)>0:
    ws_itms_sql = ws_itms_sql+ " and wks.name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        ws_itms_sql  = ws_itms_sql + " and wks.name not like '%" + notlike + "%'"

dfwsitms = spark.sql(ws_itms_sql)
dfwspl =  dfwsitms.filter(f"type =='DataPipeline'").collect()
print('Looping through pipelines...')
#display(dfwsitms)

for idx,i in enumerate(dfwspl):

    #if i['itm_name']=='plRecover_WH6_Prod2_Warehouse2_fixed':
    pipeline_json = json.loads(labs.get_data_pipeline_definition(i['itm_name'],i['new_ws_name']))
    pipeline_json_bak = json.dumps(pipeline_json)
    p_new_json = swap_pipeline_connection(pipeline_json,dfwsitms,
            ['Warehouse','Lakehouse','Notebook'],
            connections_from_to) 
    #print(json.dumps(pipeline_json, indent=4))
    if pipeline_json_bak != json.dumps(p_new_json): #only updae if there were changes
        update_data_pipeline_definition(name=i['itm_name'],pipeline_content=pipeline_json, workspace=i['new_ws_name'])
    else:
        print(f"No changes made for pipeline {i['itm_name']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Rebind reports in new DR workspace
# 
# https://semantic-link-labs.readthedocs.io/en/latest/sempy_labs.report.html#sempy_labs.report.report_rebind

# CELL ********************

# Load lookup dataframe for primary item details such as workspace 
dfolditms = spark.sql("""SELECT ir.id old_item_id, ir.displayName old_item_name, wr.Id old_ws_id, wr.Name old_ws_name, 
                                wks.Id new_ws_id, wks.Name new_ws_name 
                       FROM items_recovered ir INNER JOIN workspaces_recovered wr on ir.workspaceId = wr.Id 
                       INNER JOIN workspaces wks on wr.Name = replace(wks.Name,'""" + p_secondary_ws_suffix+ """','')
                       where wks.name like '%"""+p_secondary_ws_suffix+"""' and wks.id != '"""+thisWsId+"""'""" )

dfnewitms = spark.sql("""SELECT itm.id new_item_id, itm.displayName new_item_name, wks.Id new_ws_id, wks.Name new_ws_name
                       FROM items itm INNER JOIN workspaces wks on itm.workspaceId = wks.Id 
                       where wks.name like '%"""+p_secondary_ws_suffix+"""' and wks.id != '"""+thisWsId+"""'""" )
                       
ws_sm_sql = "SELECT wks.ID new_workspace_id, wks.name new_workspace_name, itm.* " + \
                "FROM items itm inner join workspaces wks on itm.workspaceId = wks.Id " + \
                "AND itm.type = 'Report' and wks.name like '%"+p_secondary_ws_suffix+"' " \
                "AND wks.id != '"+thisWsId+"' order by 1"

ws_sm_sql = """select rep.Name, rep.Dataset_Id old_dataset_id, rep_rec.Id old_rep_id, wks_rec.Id old_ws_id, wks_rec.Name old_ws_name, wks.Id new_ws_id, wks.Name new_ws_name 
from reports rep inner join items itm on rep.Id = itm.Id 
inner join workspaces wks on itm.workspaceId = wks.Id
inner join reports_recovered rep_rec on rep.Name = rep_rec.Name 
inner join items_recovered itm_rec on itm_rec.Id = rep_rec.Id
inner join workspaces_recovered wks_rec on wks_rec.Name = replace(wks.Name,'""" + p_secondary_ws_suffix+ """','') 
and wks_rec.Id = itm_rec.workspaceId
where rep_rec.Id != rep.Id and wks.Name != wks_rec.Name"""

# Ingore workspaces based on the ignore list defined in the parameter section at the top of this notebook
if len(p_ws_ignore_list)>0:
    ws_sm_sql = ws_sm_sql+ " and wks.name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        ws_sm_sql  = ws_sm_sql + " and wks.name not like '%" + notlike + "%'"

#print(ws_sm_sql)
dfwssm = spark.sql(ws_sm_sql).collect()
#display(dfwssm)
current_workspace_id =''

# Iterate over each dataset in the dataframe
for idx,row in enumerate(dfwssm):
    if not dfolditms.filter(f"old_item_id=='{row['old_dataset_id']}'").isEmpty(): #check to see whether the report dataset is in a workspace from primary
        old_dataset_name,old_dataset_ws_id,old_dataset_ws_name, new_dataset_ws_id, new_dataset_ws_name = dfolditms.filter(f"old_item_id=='{row['old_dataset_id']}'").collect()[0][1:6]
        #old_report_name,old_report_ws_id,old_report_ws_name, new_report_ws_id, new_report_ws_name = dfolditms.filter(f"old_item_id=='{row['Id']}").collect()[0][1:6]

        print(f"Rebinding report {row['Name']} in workspace {row['new_ws_name']} with dataset name {old_dataset_name} in target workspace {new_dataset_ws_name}")
        labs.report.report_rebind(report=row['Name'],dataset=old_dataset_name, report_workspace=row['new_ws_name'], dataset_workspace=new_dataset_ws_name)
    else:
        new_item_name,new_ws_id, new_ws_name = dfnewitms.filter(f"new_item_id=='{row['old_dataset_id']}'").collect()[0][1:4]
        print(f"Report {row['Name']} in workspace {row['new_ws_name']} has a dataset {new_item_name} in a workspace {new_ws_name} ({new_ws_id})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Clean up - deletes recovered workspaces!!
# <div style="display: flex; align-items: flex-end;"><img style="float: left; margin-right: 10px;" src="https://github.com/hurtn/images/blob/main/stop.png?raw=true" width="50"><span><h6>Only run the cell below if you are re-testing this process  or do not wish to keep the recovered workspaces in the secondary.<br>Keeping the cell frozen ensures it is not run when the Run all button is used.</span></div>

# CELL ********************

# Please ensure you have run the workspaceutils command at the top of this notebook before running this cell to ensure all necessary imports and variables are loaded.

print('Refreshing the workspaces metadata table...')
# Refresh the list of current workspaces
saveWorkspaceMeta()

thisWsId = notebookutils.runtime.context['currentWorkspaceId'] #obtaining this so we don't accidently delete this workspace!

delete_ws_sql = "SELECT distinct ID,Type,Name FROM workspaces where Name like '%"+p_secondary_ws_suffix+"' and id != '"+thisWsId+"' and Name != '" +p_bcdr_workspace_src+"'" 

# Ingore workspaces based on the ignore list defined in the parameter section at the top of this notebook
if len(p_ws_ignore_list)>0:
    delete_ws_sql = delete_ws_sql+ " and name not in ('" + "', '".join(p_ws_ignore_list)+ "') "

if len(p_ws_ignore_like_list)>0:
    for notlike in p_ws_ignore_like_list:
        delete_ws_sql  = delete_ws_sql + " and name not like '" + notlike + "'"


print('Deleting workspaces...')
# Get all workspaces created with the prefix set in the parameters at the top so that they can be deleted, except for this workspace of course!
df = spark.sql(delete_ws_sql).collect()
for i in df:
    #print(i['ID'])
    if i['Type'] == 'Workspace':
      workspaceId = i['ID']
      if p_logging_verbose:
        print("Deleting workspace "+i['Name'] + '(' + i['ID'] + ')')
      response = client.delete("/v1/workspaces/"+workspaceId)
      if p_logging_verbose and response.status_code ==200:
        print('Successfully deleted')

print('Refreshing the workspaces metadata table after deleting recovered workspaces...')
# now refresh workspaces
saveWorkspaceMeta()
print('Done')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
