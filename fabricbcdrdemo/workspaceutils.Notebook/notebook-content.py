# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils
import pandas as pd
import datetime, time
import re,json
import sempy
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,current_timestamp,lit
import sempy_labs as labs
from sempy_labs import migration, directlake
from sempy_labs import lakehouse as lake
from sempy_labs import report as rep
from sempy_labs.tom import connect_semantic_model


# workspaceutils - common utilities

# instantiate the Fabric rest client
client = fabric.FabricRestClient()
pbiclient = fabric.PowerBIRestClient()

# loading a dataframe of connections to perform an ID lookup if required 
df_conns = labs.list_connections()

# get the current workspace ID based on the context of where this notebook is run from
thisWsId = notebookutils.runtime.context['currentWorkspaceId'] 

# Gets the status of a capacity
def get_capacity_status(p_target_cap):
    dfC = fabric.list_capacities()
    dfC_filt = dfC[dfC["Id"] == p_target_cap]
    return dfC_filt['State'].iloc[0]

# this function attempts to save a dataframe which can be either a pandas dataframe or spark dataframe
def saveTable(pdf,table_name, mode='overwrite'):
    if mode=='append' and not any(table.name == table_name for table in spark.catalog.listTables()):
            mode = 'overwrite'

    if (isinstance(pdf, pd.DataFrame) and pdf.empty) or \
       (isinstance(pdf, DataFrame) and pdf.isEmpty()):
        return('No ' + table_name + ' found, nothing to save (Dataframe is empty)')
    if not isinstance(pdf, DataFrame):
        pdf = spark.createDataFrame(pdf)

    df = pdf.select([col(c).alias(
            c.replace( '(', '')
            .replace( ')', '')
            .replace( ',', '')
            .replace( ';', '')
            .replace( '{', '')
            .replace( '}', '')
            .replace( '\n', '')
            .replace( '\t', '')
            .replace( ' ', '_')
            .replace( '.', '_')
        ) for c in pdf.columns])
    #display(df)
    df.withColumn("metaTimestamp",current_timestamp()).write.mode(mode) \
      .option("mergeSchema", "true").saveAsTable(table_name)
    return(str(df.count()) +' records saved to the '+table_name + ' table.')

def saveCapacityMeta():
    spark.sql("drop table if exists capacitiess")
    df = fabric.list_capacities()
    print(saveTable(df,"capacities"))

def saveWorkspaceMeta(suppress_output=False):
    spark.sql("drop table if exists workspaces")
    df = fabric.list_workspaces()
    #display(df)
    if not suppress_output:
        print(saveTable(df,"workspaces"))
    else:
        saveTable(df,"workspaces")

def saveItemMeta(verbose_logging,ws_ignore_list, ws_ignore_like_list, list_of_workspaces_to_recover):
    all_items = []
    spark.sql("drop table if exists items")
    saveResults = saveWorkspaceMeta()

    wsitemssql = "SELECT distinct ID,Type,Name FROM workspaces where 1=1"

    if len(ws_ignore_like_list)>0:
        for notlike in ws_ignore_like_list:
            wsitemssql  = wsitemssql + " and name not like '" + notlike + "'"

    if len(ws_ignore_list)>0:
        wsitemssql  = wsitemssql + " and name not in ('" + "', '".join(ws_ignore_list)+ "') "

    if len(list_of_workspaces_to_recover)>0:
        wsitemssql = wsitemssql+" and Name like '%_DR'" #in ('" +  "', '".join(list_of_workspaces_to_recover)+ "') "

    df = spark.sql(wsitemssql).collect()

    for i in df:
        if verbose_logging:
            print('Getting items for workspace ' + i['Name'] + '...')
        if i['Type'] == 'Workspace':
            url = "/v1/workspaces/" + i['ID'] + "/items"
        try:
            itmresponse = client.get(url)
            #print(itmresponse.json()) 
            all_items.extend(itmresponse.json()['value']) 
        except Exception as error:
            errmsg =  "Couldn't get list of items for workspace " + i['Name'] + "("+ i['ID'] + ")."
            if (verbose):
                 errmsg = errmsg + "Error: "+str(error)
            print(str(errmsg))
    itmdf=spark.read.json(sc.parallelize(all_items))
    print(saveTable(itmdf,'items'))

def saveReportMeta(verbose_logging, only_secondary=False,secondary_ws_suffix='', ws_ignore_list=[], ws_ignore_like_list=[]):
    all_report_data = []
    table_name = 'reports'
    spark.sql("Drop table if exists "+ table_name)
    reportsql = "SELECT distinct ID,Type,Name FROM workspaces where Type!='AdminInsights'"
    
    if len(ws_ignore_like_list)>0:
        for notlike in ws_ignore_like_list:
            reportsql  = reportsql + " and name not like '" + notlike + "'"
    if len(ws_ignore_list)>0:
        reportsql  = reportsql + " and name not in ('" + "', '".join(ws_ignore_list)+ "') "

    if only_secondary:
        reportsql = reportsql + " and Name like '%"+secondary_ws_suffix+"'" 
    reportdf = spark.sql(reportsql).collect()

    for idx,i in enumerate(reportdf):
        if i['Type'] == 'Workspace':
            try:
                if verbose_logging:
                    print('Retreiving reports for workspace '+ i['Name'] + '...')
                dfwsreports = fabric.list_reports(i['ID'] )   
                if idx == 0:
                        dfallreports = dfwsreports
                else:
                        dfallreports = pd.concat([dfallreports, dfwsreports], ignore_index=True, sort=False)
            except WorkspaceNotFoundException as e:
                print("WorkspaceNotFoundException:", e)
            except FabricHTTPException as e:
                print("Caught a FabricHTTPException. Check the API endpoint, authentication.")

            except Exception as error:
                errmsg =  "Couldn't retreive report details for workspace " + i['Name'] + "("+ i['ID'] + "). Error: "+str(error)
                print(str(errmsg))

    dfallreports = dfallreports.drop('Subscriptions', axis=1)
    dfallreports = dfallreports.drop('Users', axis=1)
    print(saveTable(dfallreports,'reports'))

def get_lh_object_list(base_path,data_types = ['Tables', 'Files'])->pd.DataFrame:

    '''
    Function to get a list of tables for a lakehouse
    adapted from https://fabric.guru/getting-a-list-of-folders-and-delta-tables-in-the-fabric-lakehouse
    This function will return a pandas dataframe containing names and abfss paths of each folder for Files and Tables
    '''
    #data_types = ['Tables', 'Files'] #for if you want a list of files and tables
    #data_types = ['Tables'] #for if you want a list of tables

    df = pd.concat([
        pd.DataFrame({
            'name': [item.name for item in notebookutils.fs.ls(f'{base_path}/{data_type}/')],
            'type': data_type[:-1].lower() , 
            'src_path': [item.path for item in notebookutils.fs.ls(f'{base_path}/{data_type}/')],
        }) for data_type in data_types], ignore_index=True)

    return df

def get_wh_object_list(schema_list,base_path)->pd.DataFrame:

    '''
    Function to get a list of tables for a warehouse by schema
    '''
    data_type = 'Tables'
    dfs = []

    for schema_prefix in schema_list:
        if notebookutils.fs.exists(f'{base_path}/{data_type}/{schema_prefix}/'):
            items = notebookutils.fs.ls(f'{base_path}/{data_type}/{schema_prefix}/')
            if items:  # Check if the list is not empty
                df = pd.DataFrame({
                    'schema': schema_prefix,
                    'name': [item.name for item in items],
                    'type': data_type[:-1].lower(),
                    'src_path': [item.path for item in items],
                })
                dfs.append(df)

    if dfs:  # Check if the list of dataframes is not empty
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = pd.DataFrame()  # Return an empty dataframe if no dataframes were created

    return df

def copy_lh_objects(table_list,workspace_src,workspace_tgt,lakehouse_src,lakehouse_tgt,recovered_object_suffix,fastcopy=True,usingIDs=False)->pd.DataFrame:
    # declare an array to keep the instrumentation
    cpresult = []
    # loop through all the tables to extract the source path 
    for table in table_list.src_path:
        source = table
        destination = source.replace(f'abfss://{workspace_src}', f'abfss://{workspace_tgt}')
        if usingIDs:
            destination = destination.replace(f'{lakehouse_src}', f'{lakehouse_tgt}')
        else:
            destination = destination.replace(f'{lakehouse_src}.Lakehouse', f'{lakehouse_tgt}.Lakehouse') + recovered_object_suffix
        start_time =  datetime.datetime.now()
        if notebookutils.fs.exists(destination):
             notebookutils.fs.rm(destination, True)
        if fastcopy:
            # use fastcopy util which is a python wrapper to azcopy
            notebookutils.fs.fastcp(source+'/*', destination+'/', True)
        else:
            notebookutils.fs.cp(source, destination, True)

        # recording the timing and add it to the results list
        end_time = datetime.datetime.now()
        copyreslist = [source, destination, start_time.strftime("%Y-%m-%d %H:%M:%S"),  end_time.strftime("%Y-%m-%d %H:%M:%S"), str((end_time - start_time).total_seconds())]
        cpresult.append(copyreslist)
    return pd.DataFrame(cpresult,columns =['source--------------------------------------','target--------------------------------------','start------------','end_time------------','elapsed seconds----'])

def getItemId(wks_id,itm_name,itm_type):
    df = fabric.list_items(type=None,workspace=wks_id)
    #print(df)
    if df.empty:
        #print('No items found in workspace '+i['Name']+' (Dataframe is empty)')
        return 'NotExists'
    else:
        #display(df)
        #print(df.query('"Display Name"="'+itm_name+'"'))
        if itm_type != '':
          newdf= df.loc[(df['Display Name'] == itm_name) & (df['Type'] == itm_type)]['Id']
        else:
          newdf= df.loc[(df['Display Name'] == itm_name)]['Id']  
        if newdf.empty:
          return 'NotExists'
        else:
          return newdf.iloc[0]

###### Function to create DW recovery pipeline
def createDWrecoverypl(ws_id,pl_name = 'Recover_Warehouse_Data_From_DR'):
  client = fabric.FabricRestClient()

  dfurl= "v1/workspaces/"+ ws_id + "/items"
  payload = { 
  "displayName": pl_name, 
  "type": "DataPipeline", 
  "definition": { 
    "parts": [ 
      { 
        "path": "pipeline-content.json", 
        "payload":  "ewogICAgInByb3BlcnRpZXMiOiB7CiAgICAgICAgImFjdGl2aXRpZXMiOiBbCiAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICJuYW1lIjogIkl0ZXJhdGVTY2hlbWFUYWJsZXMiLAogICAgICAgICAgICAgICAgInR5cGUiOiAiRm9yRWFjaCIsCiAgICAgICAgICAgICAgICAiZGVwZW5kc09uIjogW10sCiAgICAgICAgICAgICAgICAidHlwZVByb3BlcnRpZXMiOiB7CiAgICAgICAgICAgICAgICAgICAgIml0ZW1zIjogewogICAgICAgICAgICAgICAgICAgICAgICAidmFsdWUiOiAiQHBpcGVsaW5lKCkucGFyYW1ldGVycy50YWJsZXNUb0NvcHkiLAogICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJFeHByZXNzaW9uIgogICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgImJhdGNoQ291bnQiOiAyMCwKICAgICAgICAgICAgICAgICAgICAiYWN0aXZpdGllcyI6IFsKICAgICAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiAiQ29weVdhcmVob3VzZVRhYmxlcyIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJDb3B5IiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJkZXBlbmRzT24iOiBbCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiYWN0aXZpdHkiOiAiU2V0IHRhYmxlIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImRlcGVuZGVuY3lDb25kaXRpb25zIjogWwogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIlN1Y2NlZWRlZCIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgXQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgIF0sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAicG9saWN5IjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0aW1lb3V0IjogIjAuMTI6MDA6MDAiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJyZXRyeSI6IDIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInJldHJ5SW50ZXJ2YWxJblNlY29uZHMiOiAzMCwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAic2VjdXJlT3V0cHV0IjogZmFsc2UsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInNlY3VyZUlucHV0IjogZmFsc2UKICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZVByb3BlcnRpZXMiOiB7CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInNvdXJjZSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGUiOiAiRGF0YVdhcmVob3VzZVNvdXJjZSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJxdWVyeVRpbWVvdXQiOiAiMDI6MDA6MDAiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAicGFydGl0aW9uT3B0aW9uIjogIk5vbmUiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiZGF0YXNldFNldHRpbmdzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImFubm90YXRpb25zIjogW10sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAibGlua2VkU2VydmljZSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAibmFtZSI6ICIwN2EwMzAwNl9kMWI2XzRhMzlfYmViMV8wYmJhMmFhZjVmZjciLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJwcm9wZXJ0aWVzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiYW5ub3RhdGlvbnMiOiBbXSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGUiOiAiRGF0YVdhcmVob3VzZSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlUHJvcGVydGllcyI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJlbmRwb2ludCI6ICJAcGlwZWxpbmUoKS5wYXJhbWV0ZXJzLmxha2Vob3VzZUNvbm5TdHIiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImFydGlmYWN0SWQiOiAiQHBpcGVsaW5lKCkucGFyYW1ldGVycy5sYWtlaG91c2VJZCIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAid29ya3NwYWNlSWQiOiAiQHBpcGVsaW5lKCkucGFyYW1ldGVycy53b3Jrc3BhY2VJZCIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJEYXRhV2FyZWhvdXNlVGFibGUiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInNjaGVtYSI6IFtdLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGVQcm9wZXJ0aWVzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJzY2hlbWEiOiAiZGJvIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidGFibGUiOiB7CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ2YWx1ZSI6ICJAY29uY2F0KGNvbmNhdChpdGVtKCkuc2NoZW1hLCdfJyksaXRlbSgpLm5hbWUpIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGUiOiAiRXhwcmVzc2lvbiIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJzaW5rIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJEYXRhV2FyZWhvdXNlU2luayIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJhbGxvd0NvcHlDb21tYW5kIjogdHJ1ZSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInRhYmxlT3B0aW9uIjogImF1dG9DcmVhdGUiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiZGF0YXNldFNldHRpbmdzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImFubm90YXRpb25zIjogW10sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAibGlua2VkU2VydmljZSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAibmFtZSI6ICIwYzAzMTIzYV9kMzEyXzQ2YzRfYThlN181YjRjYWQ4ZjEyZDciLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJwcm9wZXJ0aWVzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiYW5ub3RhdGlvbnMiOiBbXSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGUiOiAiRGF0YVdhcmVob3VzZSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlUHJvcGVydGllcyI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJlbmRwb2ludCI6ICJAcGlwZWxpbmUoKS5wYXJhbWV0ZXJzLndhcmVob3VzZUNvbm5TdHIiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImFydGlmYWN0SWQiOiAiQHBpcGVsaW5lKCkucGFyYW1ldGVycy53YXJlaG91c2VJZCIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAid29ya3NwYWNlSWQiOiAiQHBpcGVsaW5lKCkucGFyYW1ldGVycy53b3Jrc3BhY2VJZCIKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJEYXRhV2FyZWhvdXNlVGFibGUiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInNjaGVtYSI6IFtdLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGVQcm9wZXJ0aWVzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJzY2hlbWEiOiAiZGJvIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidGFibGUiOiB7CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ2YWx1ZSI6ICJAaXRlbSgpLm5hbWUiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJFeHByZXNzaW9uIgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImVuYWJsZVN0YWdpbmciOiB0cnVlLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0cmFuc2xhdG9yIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJUYWJ1bGFyVHJhbnNsYXRvciIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlQ29udmVyc2lvbiI6IHRydWUsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlQ29udmVyc2lvblNldHRpbmdzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgImFsbG93RGF0YVRydW5jYXRpb24iOiB0cnVlLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInRyZWF0Qm9vbGVhbkFzTnVtYmVyIjogZmFsc2UKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgfSwKICAgICAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiAiU2V0IHRhYmxlIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlIjogIlNldFZhcmlhYmxlIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJkZXBlbmRzT24iOiBbCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAiYWN0aXZpdHkiOiAiU2V0IHNjaGVtYSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJkZXBlbmRlbmN5Q29uZGl0aW9ucyI6IFsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJTdWNjZWVkZWQiCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIF0KICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBdLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgInBvbGljeSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAic2VjdXJlT3V0cHV0IjogZmFsc2UsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInNlY3VyZUlucHV0IjogZmFsc2UKICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZVByb3BlcnRpZXMiOiB7CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInZhcmlhYmxlTmFtZSI6ICJUYWJsZW5hbWUiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ2YWx1ZSI6IHsKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInZhbHVlIjogIkBpdGVtKCkubmFtZSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlIjogIkV4cHJlc3Npb24iCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAibmFtZSI6ICJTZXQgc2NoZW1hIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ0eXBlIjogIlNldFZhcmlhYmxlIiwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJkZXBlbmRzT24iOiBbXSwKICAgICAgICAgICAgICAgICAgICAgICAgICAgICJwb2xpY3kiOiB7CiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInNlY3VyZU91dHB1dCI6IGZhbHNlLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJzZWN1cmVJbnB1dCI6IGZhbHNlCiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgICAgICAgICAgICAgInR5cGVQcm9wZXJ0aWVzIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICJ2YXJpYWJsZU5hbWUiOiAiU2NoZW1hbmFtZSIsCiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgInZhbHVlIjogewogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidmFsdWUiOiAiQGl0ZW0oKS5zY2hlbWEiLAogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAidHlwZSI6ICJFeHByZXNzaW9uIgogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgICAgICAgICAgICAgfQogICAgICAgICAgICAgICAgICAgIF0KICAgICAgICAgICAgICAgIH0KICAgICAgICAgICAgfQogICAgICAgIF0sCiAgICAgICAgInBhcmFtZXRlcnMiOiB7CiAgICAgICAgICAgICJsYWtlaG91c2VJZCI6IHsKICAgICAgICAgICAgICAgICJ0eXBlIjogInN0cmluZyIsCiAgICAgICAgICAgICAgICAiZGVmYXVsdFZhbHVlIjogIjBmMGY2YjdjLTE3NjEtNDFlNi04OTZlLTMwMDE0ZjE2ZmY2ZCIKICAgICAgICAgICAgfSwKICAgICAgICAgICAgInRhYmxlc1RvQ29weSI6IHsKICAgICAgICAgICAgICAgICJ0eXBlIjogImFycmF5IiwKICAgICAgICAgICAgICAgICJkZWZhdWx0VmFsdWUiOiBbCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAic2NoZW1hIjogImRibyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogIkRhdGUiCiAgICAgICAgICAgICAgICAgICAgfSwKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJzY2hlbWEiOiAiZGJvIiwKICAgICAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiAiR2VvZ3JhcGh5IgogICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAic2NoZW1hIjogImRibyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogIkhhY2tuZXlMaWNlbnNlIgogICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAic2NoZW1hIjogImRibyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogIk1lZGFsbGlvbiIKICAgICAgICAgICAgICAgICAgICB9LAogICAgICAgICAgICAgICAgICAgIHsKICAgICAgICAgICAgICAgICAgICAgICAgInNjaGVtYSI6ICJkYm8iLAogICAgICAgICAgICAgICAgICAgICAgICAibmFtZSI6ICJUaW1lIgogICAgICAgICAgICAgICAgICAgIH0sCiAgICAgICAgICAgICAgICAgICAgewogICAgICAgICAgICAgICAgICAgICAgICAic2NoZW1hIjogImRibyIsCiAgICAgICAgICAgICAgICAgICAgICAgICJuYW1lIjogIlRyaXAiCiAgICAgICAgICAgICAgICAgICAgfSwKICAgICAgICAgICAgICAgICAgICB7CiAgICAgICAgICAgICAgICAgICAgICAgICJzY2hlbWEiOiAiZGJvIiwKICAgICAgICAgICAgICAgICAgICAgICAgIm5hbWUiOiAiV2VhdGhlciIKICAgICAgICAgICAgICAgICAgICB9CiAgICAgICAgICAgICAgICBdCiAgICAgICAgICAgIH0sCiAgICAgICAgICAgICJ3b3Jrc3BhY2VJZCI6IHsKICAgICAgICAgICAgICAgICJ0eXBlIjogInN0cmluZyIsCiAgICAgICAgICAgICAgICAiZGVmYXVsdFZhbHVlIjogIjE1MDExNDNjLTI3MmYtNGEyZi05NzZhLTdlNTU5NzFlNGMyYiIKICAgICAgICAgICAgfSwKICAgICAgICAgICAgIndhcmVob3VzZUlkIjogewogICAgICAgICAgICAgICAgInR5cGUiOiAic3RyaW5nIiwKICAgICAgICAgICAgICAgICJkZWZhdWx0VmFsdWUiOiAiNGQxYmQ5NTEtOTlkZS00YmQ3LWI3YmMtNzFjOGY1NmRiNDExIgogICAgICAgICAgICB9LAogICAgICAgICAgICAid2FyZWhvdXNlQ29ublN0ciI6IHsKICAgICAgICAgICAgICAgICJ0eXBlIjogInN0cmluZyIsCiAgICAgICAgICAgICAgICAiZGVmYXVsdFZhbHVlIjogIjcyd3diaXZpMnViZWpicnRtdGFobzMyYjR5LWhxa2FjZmpwZTR4dXZmM2twemt6b2hzbWZtLmRhdGF3YXJlaG91c2UuZmFicmljLm1pY3Jvc29mdC5jb20iCiAgICAgICAgICAgIH0sCiAgICAgICAgICAgICJsYWtlaG91c2VDb25uU3RyIjogewogICAgICAgICAgICAgICAgInR5cGUiOiAic3RyaW5nIiwKICAgICAgICAgICAgICAgICJkZWZhdWx0VmFsdWUiOiAiNzJ3d2JpdmkydWJlamJydG10YWhvMzJiNHktaHFrYWNmanBlNHh1dmYza3B6a3pvaHNtZm0uZGF0YXdhcmVob3VzZS5mYWJyaWMubWljcm9zb2Z0LmNvbSIKICAgICAgICAgICAgfQogICAgICAgIH0sCiAgICAgICAgInZhcmlhYmxlcyI6IHsKICAgICAgICAgICAgIlRhYmxlbmFtZSI6IHsKICAgICAgICAgICAgICAgICJ0eXBlIjogIlN0cmluZyIKICAgICAgICAgICAgfSwKICAgICAgICAgICAgIlNjaGVtYW5hbWUiOiB7CiAgICAgICAgICAgICAgICAidHlwZSI6ICJTdHJpbmciCiAgICAgICAgICAgIH0KICAgICAgICB9LAogICAgICAgICJsYXN0TW9kaWZpZWRCeU9iamVjdElkIjogIjRhYTIwYWY3LTk0YmQtNDM0OC1iZWY4LWY4Y2JjZDg0MGQ1MSIsCiAgICAgICAgImxhc3RQdWJsaXNoVGltZSI6ICIyMDI0LTExLTEzVDE1OjUyOjUyWiIKICAgIH0KfQ==", 
        "payloadType": "InlineBase64" 
      } 
    ] 
  } 
}   
  
  response = json.loads(client.post(dfurl,json= payload).content)
  return response['id']

def getWorkspaceRolesAssignments(pWorkspaceId):
    url = "/v1/workspaces/" +pWorkspaceId + "/roleAssignments"
    try:
        print('Attempting to connect workspace '+ i['Workspace_Name'])
        response = client.post(url,json= json.loads(payload))
        print(str(response.status_code) + response.text) 
        success = True
    except Exception as error:
        errmsg =  "Couldn't connect git to workspace " + i['Workspace_Name'] + "("+ i['Workspace_ID'] + "). Error: "+str(error)
        print(str(errmsg))


def long_running_operation_polling(uri,retry_after):
    try:
        print(f"Polling long running operation ID {uri} has been started with a retry-after time of {retry_after} seconds.")
        while True:
            response = client.get(uri)
            operation_state = response.json()
            print('operation state = '+str(operation_state))
            print(f"Long running operation status: {operation_state['status']}")
            if operation_state['status'] in ["NotStarted", "Running"]:
                time.sleep(retry_after)
            else:
                break
        if operation_state['status'] == "Failed":
            print(f"The long running operation has been completed with failure. Error response: {json.dumps(operation_state['Error'])}")
        else:
            print("The long running operation has been successfully completed.")
            #response = client.get(uri+'/result')
            return operation_state['status']
    except Exception as e:
        print(f"The long running operation has been completed with failure. Error response: {e}")

class noDefaultLakehouseException(Exception):
    pass



from typing import Optional
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    lro,
    _decode_b64,
)
import sempy_labs._icons as icons

import base64
from typing import Optional, Tuple, List
from uuid import UUID


def update_data_pipeline_definition(
    name: str, pipeline_content: dict, workspace: Optional[str] = None
):
    """
    Updates an existing data pipeline with a new definition.

    Parameters
    ----------
    name : str
        The name of the data pipeline.
    pipeline_content : dict
        The data pipeline content (not in Base64 format).
    workspace : str, default=None
        The name of the workspace.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    (workspace, workspace_id) = resolve_workspace_name_and_id(workspace)
    client = fabric.FabricRestClient()
    pipeline_payload = base64.b64encode(json.dumps(pipeline_content).encode('utf-8')).decode('utf-8')
    pipeline_id = fabric.resolve_item_id(
        item_name=name, type="DataPipeline", workspace=workspace
    )

    request_body = {
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": pipeline_payload,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }


    response = client.post(
        f"v1/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition",
        json=request_body,
    )

    lro(client, response, return_status_code=True)

    print(
        f"{icons.green_dot} The '{name}' pipeline was updated within the '{workspace}' workspace."
    )

def _is_valid_uuid(
    guid: str,
):
    """
    Validates if a string is a valid GUID in version 4

    Parameters
    ----------
    guid : str
        GUID to be validated.

    Returns
    -------
    bool
        Boolean that indicates if the string is a GUID or not.
    """

    try:
        UUID(str(guid), version=4)
        return True
    except ValueError:
        return False

import json
from jsonpath_ng import jsonpath, parse
from typing import Optional, Tuple, List
from uuid import UUID


# Swaps the connection properties of an activity belonging to the specified item type(s)
def swap_pipeline_connection(pl_json: dict, p_lookup_df: DataFrame,
                                p_item_type: List =['Warehouse','Lakehouse','Notebook'], 
                                p_conn_from_to: Optional[List[Tuple[str,str]]]=[]):

    def lookupItem(p_itm_id) -> Tuple[str]:
        #print(p_itm_id)
        if not p_lookup_df.filter(f"old_item_id=='{p_itm_id}'").isEmpty():
            return p_lookup_df.filter(f"old_item_id=='{p_itm_id}'").collect()[0][1:8]
        else:
            return['','','','','','','']

    source_nb_id=''
    source_id, target_id, itm_name, source_ws_id, source_ws_name, target_ws_id, target_ws_name = lookupItem('0000-0000-0000-0000')
  
    if 'Warehouse' in p_item_type or 'Lakehouse' in p_item_type:
        ls_expr = parse('$..linkedService')
        for endpoint_match in ls_expr.find(pl_json):
            if endpoint_match.value['properties']['type'] == 'DataWarehouse' and 'Warehouse' in p_item_type:
                # only update the warehouse if it was located in the source workspace i.e. we will update the properties to the target workspace if the warehouse resided in the same workspace as the pipeline
                #print(endpoint_match.value)
                warehouse_id = endpoint_match.value['properties']['typeProperties']['artifactId']
                if _is_valid_uuid(warehouse_id): #only swap if valid uuid otherwise most likely it is parameterised 
                    print(f"warehouse id = {warehouse_id}")
                    warehouse_endpoint = endpoint_match.value['properties']['typeProperties']['endpoint']
                    #print(warehouse_endpoint)
                    source_id, target_id, itm_name, source_ws_id, source_ws_name, target_ws_id, target_ws_name = lookupItem(warehouse_id)
                    if source_id !='': # check whether any corresponding values were returned form the lookup
                        print(f"changing warehouse {itm_name} (source_id={source_id} target_id={target_id}) from source workspace {source_ws_name} ({source_ws_id}) to target workspace {target_ws_name} ({target_ws_id})")
                        # look up the connection string for the warehouse in the target workspace
                        whurl  = f"v1/workspaces/{target_ws_id}/warehouses/{target_id}"
                        whresponse = client.get(whurl)
                        lhconnStr = whresponse.json()['properties']['connectionString']
                        endpoint_match.value['properties']['typeProperties']['artifactId'] = target_id
                        endpoint_match.value['properties']['typeProperties']['workspaceId'] = target_ws_id
                        endpoint_match.value['properties']['typeProperties']['endpoint'] = lhconnStr
                        #print(endpoint_match.value)
                        ls_expr.update(endpoint_match,endpoint_match.value)
                    else:
                        print(f"Could not find associated IDs for warehouse {warehouse_id}")
                else:
                    print(f"Lakehouse was not a valid UUID and was parameterised with {source_nb_id} therefore ignoring")

            if endpoint_match.value['properties']['type'] == 'Lakehouse' and 'Lakehouse' in p_item_type:
                #print(endpoint_match.value)
                lakehouse_id = endpoint_match.value['properties']['typeProperties']['artifactId']
                if _is_valid_uuid(lakehouse_id):  #only swap if valid uuid otherwise most likely it is parameterised 
                    print(f"lakehouse id = {lakehouse_id}")
                    source_id, target_id, itm_name, source_ws_id, source_ws_name, target_ws_id, target_ws_name = lookupItem(lakehouse_id)
                    if source_id != '':
                        print(f"changing lakehouse {itm_name} (source_id={source_id} target_id={target_id}) from source workspace {source_ws_name} ({source_ws_id}) to target workspace {target_ws_name} ({target_ws_id})")
                        # find the lakehouse id of the lakehouse with the same name in the target workspace
                        endpoint_match.value['properties']['typeProperties']['artifactId'] = target_id
                        endpoint_match.value['properties']['typeProperties']['workspaceId'] = target_ws_id
                        ls_expr.update(endpoint_match,endpoint_match.value)
                        #    print(endpoint_match.value)
                    else:
                        print(f"Could not find associated IDs for lakehouse {lakehouse_id}")
                else:
                    print(f"Lakehouse was not a valid UUID and was parameterised with {source_nb_id} therefore ignoring")


    if 'Notebook' in p_item_type: 
        ls_expr = parse('$..activities')

        for endpoint_match in ls_expr.find(pl_json):
            for activity in endpoint_match.value:
                #print(activity['type'])
                if activity['type']=='TridentNotebook' and 'Notebook' in p_item_type: #only update if the notebook was in the same workspace as the pipeline
                    source_nb_id = activity['typeProperties']['notebookId']
                    if _is_valid_uuid(source_nb_id):  
                        source_id, target_id, itm_name, source_ws_id, source_ws_name, target_ws_id, target_ws_name = lookupItem(source_nb_id)
                        if source_id != '':
                            print(f"changing notebook {itm_name} (source_id={source_id} target_id={target_id}) from source workspace {source_ws_name} ({source_ws_id}) to target workspace {target_ws_name} ({target_ws_id})")
                            activity['typeProperties']['notebookId']=target_id
                            activity['typeProperties']['workspaceId']=target_ws_id
                            #ls_expr.update(endpoint_match,endpoint_match.value)
                        else:
                            print(f"Could not find associated IDs for notebook {source_nb_id}")
                    else:
                        print(f"Notebook activity was not a valid UUID and was parameterised with {source_nb_id} therefore ignoring")
    if p_conn_from_to:
        for ti_conn_from_to in p_conn_from_to:
            if not _is_valid_uuid(ti_conn_from_to[0]):
                #print('Connection from is string '+ str(ti_conn_from_to[0]))
                dfC_filt = df_conns[df_conns["Connection Name"] == ti_conn_from_to[0]]       
                connId_from = dfC_filt['Connection Id'].iloc[0]     
            else:
                connId_from = ti_conn_from_to[0]

            if not _is_valid_uuid(ti_conn_from_to[1]):
                #print('Connection from is string '+ str(ti_conn_from_to[1]))
                dfC_filt = df_conns[df_conns["Connection Name"] == ti_conn_from_to[1]]       
                connId_to = dfC_filt['Connection Id'].iloc[0]     
            else:
                connId_to = ti_conn_from_to[1]

            ls_expr = parse('$..externalReferences')
            for externalRef in ls_expr.find(pl_json):
                if externalRef.value['connection']==connId_from:
                    print('Changing connection from '+str(connId_from))
                    externalRef.value['connection']=connId_to
                    ls_expr.update(externalRef,externalRef.value)
                    print('to '+str(connId_to))

    return pl_json



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# temporary function to fix a bug raised with SLL. To be removed once PR #787 is live.
from sempy_labs._helper_functions import get_direct_lake_sql_endpoint

def get_direct_lake_source(
    dataset: str | UUID, workspace: Optional[str | UUID] = None
) -> Tuple[str, str, UUID, UUID]:
    """
    Obtains the source information for a direct lake semantic model (if the source is located in the same workspace as the semantic model).

    Parameters
    ----------
    dataset : str | uuid.UUID
        The name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[str, str, UUID, UUID]
        If the source of the direct lake semantic model is a lakehouse this will return: 'Lakehouse', Lakehouse Name, SQL Endpoint Id, Workspace Id
        If the source of the direct lake semantic model is a warehouse this will return: 'Warehouse', Warehouse Name, Warehouse Id, Workspace Id
        If the semantic model is not a Direct Lake semantic model, it will return None, None, None.
    """

    from sempy_labs._helper_functions import get_direct_lake_sql_endpoint

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    sql_endpoint_id = get_direct_lake_sql_endpoint(dataset=dataset, workspace=workspace)
    dfI = fabric.list_items(workspace=workspace)
    dfI_filt = dfI[(dfI["Id"] == sql_endpoint_id) & (dfI["Type"].isin(["SQLEndpoint","Warehouse"]))]

    artifact_type, artifact_name, artifact_id = None, None, None

    if not dfI_filt.empty:
        artifact_name = dfI_filt["Display Name"].iloc[0]
        artifact_id = dfI[
            (dfI["Display Name"] == artifact_name)
            & (dfI["Type"].isin(["Lakehouse", "Warehouse"]))
        ]["Id"].iloc[0]
        artifact_type = dfI[
            (dfI["Display Name"] == artifact_name)
            & (dfI["Type"].isin(["Lakehouse", "Warehouse"]))
        ]["Type"].iloc[0]

    return artifact_type, artifact_name, artifact_id, workspace_id


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
