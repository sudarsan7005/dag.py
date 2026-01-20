import os
import logging
import traceback
 
from time import sleep
from pathlib import Path
import threading
from google.cloud import bigquery
import pandas as pd
from dag_utils import (getRequestedDate,publish_api_endpoint)
from check_common_table import getCountriesByRegion
import copy
import ast
import datetime
import re
import csv
from tabulate import tabulate
from concurrent.futures import ThreadPoolExecutor, as_completed


# Dictionary to store DataFrames for each orchestratorid
data_frames = {}
data_frame_locks = {}
threading_execute_query_Failure = False

def data_patrol_access_logic(**kwargs):
    global threading_execute_query_Failure
    threading_execute_query_Failure = False

    config = kwargs["project_config"]
    orchestratorid = config['dag0_id']  # Use orchestratorid as the unique key
    
    if kwargs.get("task"):
        parentTaskGroup = kwargs["task"].__dict__["task_id"].split('.')[0]
    elif kwargs.get("task_id"):
        parentTaskGroup = kwargs["task_id"].split('.')[0]
    else:
        logging.error("Unable to find parameter 'task' or 'task_id'")
        raise ValueError("Unable to find parameter 'task' or 'task_id'")

    uniqueDataFrameID = orchestratorid + "_" + parentTaskGroup  # Use orchestratorid and task_id combination as the uniquedataframe key

    # Initialize the DataFrame and lock for this orchestratorid
    if uniqueDataFrameID not in data_frames:
        data_frames[uniqueDataFrameID] = pd.DataFrame()
        data_frame_locks[uniqueDataFrameID] = threading.Lock()
    
    logging.info(f"Using uniqueDataFrameID: {uniqueDataFrameID}")

    if config["gcp_environment"] == "False":
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/volume/jsonkey.json"
    bqClient = bigquery.Client()

    stage = parentTaskGroup.split('_')[-1]
    regionkey = config['requested_region']
    data_source = config['requested_datasource']
    component = kwargs["component"]
    sDateToExec = getRequestedDate(config, data_source)
    insight_dataset = config['insights']['dataset']
    table_datapatrol_validation_log = config['insights']['tables']['current']['datapatrol_validation_log']
    table_global_ingestion_processes = config['landed']['tables']['global_ingestion_processes']

    # Fetching the list of all the objects to be validated
    sCommaSepCountries = getCountriesByRegion(config)

    ingestion_datasources_query = f"SELECT STRING_AGG(DISTINCT LOWER(Process),',') FROM {table_global_ingestion_processes}"
    ingestion_datasources_result = bq_exec(ingestion_datasources_query)
    ingestion_datasources = ingestion_datasources_result.split(",") if ingestion_datasources_result else []

    # Getting the list of all the fields for datapatrol_validation_log table
    query_sCommaSepFields = f'SELECT STRING_AGG(column_name, ", ") FROM `{insight_dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name = SPLIT("{table_datapatrol_validation_log}", ".")[OFFSET(2)]'
    sCommaSepFields = bq_exec(query_sCommaSepFields)

    # Call the ingestion datapatrol movement if the datasource is an ingestion datasource
    if data_source.lower() in ingestion_datasources:
        if stage == 'dag02':
            move_ingestion_error_log(sCommaSepCountries, sCommaSepFields, copy.copy(kwargs))
        else:
            logging.info(f"Data movement for ingestion datapatrol stage skipped for stage {stage}")
    else:
        logging.info(f"Data movement for ingestion datapatrol stage skipped for {data_source}")

    component_condition = f' AND sourcecomponent = "{component}"' if component else ''
    aTablesValidated = getConfigRecords(config, stage, sCommaSepCountries, component)

    if not aTablesValidated.empty:
        ti = kwargs["ti"]
        xcom_taskids = f'trigger_{data_source}_dag02.threshold_check'
        xcom_data = ti.xcom_pull(task_ids=xcom_taskids, key="data_access_threshold_check")
        if xcom_data:
            xcom_df = pd.DataFrame(xcom_data)
            logging.info(tabulate(xcom_df, headers='keys', tablefmt='psql'))

            # Get valid combinations of Country and Component which don't exceed the threshold
            valid_combinations = {
                (row["Country"].upper(), row["Component"]) if row.get("Component") else (row["Country"].upper())
                for row in xcom_data if row.get("Is_Threshold_Exceeded") == "N"
            }

            logging.info(f"Valid combinations: {valid_combinations}")

            # filtering records from aTablesValidated which doesn't exceed threshold.
            # further data patrol process is done only for records which doesnt exceed threshold at dag02 level.
            if config["dp_threshold_override_flag"].upper() != 'TRUE':
                if component:
                    valid_df = pd.DataFrame(list(valid_combinations), columns=["Country", "Component"])
                    aTablesValidated["Country"] = aTablesValidated["Country"].str.upper()
                    aTablesValidated["Component"] = aTablesValidated["Component"]
                    valid_df["Country"] = valid_df["Country"].str.upper()
                    valid_df["Component"] = valid_df["Component"]
                    aTablesValidated = aTablesValidated.merge(valid_df, on=["Country", "Component"])
                else:
                    valid_df = pd.DataFrame(list(valid_combinations), columns=["Country"])
                    aTablesValidated["Country"] = aTablesValidated["Country"].str.upper()
                    valid_df["Country"] = valid_df["Country"].str.upper()
                    aTablesValidated = aTablesValidated.merge(valid_df, on=["Country"])
                    logging.info(f"aActiveTables {aTablesValidated}")
                if aTablesValidated.empty:
                    logging.info("aActiveTables is empty after filtering threshold exceeded records.")
                    return None

        # Deleting the records from validation_log table before inserting
        query_delete = f'DELETE FROM `{table_datapatrol_validation_log}` WHERE createddttm ="{sDateToExec}" AND Source = "{data_source}" AND Region ="{regionkey}" AND Stage = "{stage}" AND OrchestratorID = "{orchestratorid}"' + component_condition
        if sCommaSepCountries:
            query_delete += ' AND country IN (' + sCommaSepCountries + ')'
        logging.info(f'Delete Query: {query_delete}')
        exec_bq_job(bqClient, query_delete)

        # Dynamically fetch column names from the data_validation_log table
        table_schema = bqClient.get_table(table_datapatrol_validation_log).schema
        columns = [field.name for field in table_schema]

        # Limit to 20 concurrent threads
        max_threads = 20
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [
                executor.submit(execute_query, table, sDateToExec, sCommaSepFields, stage, columns, copy.copy(kwargs), uniqueDataFrameID)
                for _, table in aTablesValidated.iterrows()
            ]

        for future in as_completed(futures):
            try:
                future.result()  # This will raise any exceptions from the thread
            except Exception as e:
                logging.error(f"Thread execution failed: {traceback.format_exc()}")
                threading_execute_query_Failure = True

        # After the data inserted for all threads in unique data frame
        if not data_frames[uniqueDataFrameID].empty:
            logging.info(f"Inserting accumulated data into the target table. DataFrame contents for {uniqueDataFrameID}:\n{data_frames[uniqueDataFrameID]}")
            create_table_df(data_frames[uniqueDataFrameID], table_datapatrol_validation_log)
            logging.info("Data insertion completed.")
        
        # Clean up after processing
        del data_frames[uniqueDataFrameID]
        del data_frame_locks[uniqueDataFrameID]

        if threading_execute_query_Failure:
            raise Exception("FAILED - threading_execute_query_Failure")

threading_execute_query_Failure_lock = threading.Lock()

def execute_query(table, sDateToExec, sCommaSepFields, stage, columns, kwargs, uniqueDataFrameID):
    global threading_execute_query_Failure

    try:
        config = kwargs["project_config"]
        orchestratorid = config['dag0_id']
        regionkey = config['requested_region']
        data_source = config['requested_datasource']
        insight_dataset = config['insights']['dataset']
        
        # Unpacking data for the columns for each record
        sdatapatrolcountry, sObjectType, sObjectName, sObjectField, sOperator, sObjectFieldValue, sErrorCode,sRecommendation, sErrorDescription, sErrorSeverity, sType, sAdditionalCondition, sAdditionalFields, sCountryField, sFileIngestionCondition, sVendor, sComponent = table['Country'], table['ObjectType'], table['ObjectName'], table['ObjectField'], table['Operator'], table['ObjectFieldValue'], table['ErrorCode'], table['Recommendation'],table['ErrorDescription'], table['ErrorSeverity'], table['Type'], table['AdditionalCondition'], table['AdditionalFields'], table['CountryKeyField'], table['FileIngestionCondition'], table['Vendor'], table['Component']

        # Replace placeholders in the object name and additional condition
        sObjectName = sObjectName.replace("<COUNTRY_PLACEHOLDER>", sdatapatrolcountry.lower()).replace("<REGION_PLACEHOLDER>", regionkey.lower()).replace("<VENDOR_PLACEHOLDER>", sVendor.lower())
        sAdditionalCondition = sAdditionalCondition.replace("sDateToExec", sDateToExec)

        # For views, add the date-specific suffix
        if sObjectType.lower() == 'view':
            sObjectName = sObjectName + "_" + sDateToExec.replace('-', '_')

        # Select the dataset based on the object name
        sObjectName_dataset = '.'.join(sObjectName.split('.')[:-1])

        # Extract fields from sAdditionalFields or all fields for the sObjectName
        if sAdditionalFields != '':
            query_stablefields = f"""SELECT RTRIM((SELECT STRING_AGG(CONCAT("'",Field,":',","REPLACE(IFNULL(SAFE_CAST(",Field," AS STRING),'null'),',',''),'|'")) FROM UNNEST(SPLIT('{sAdditionalFields}',',')) AS Field),",'|'")"""
        else:
            query_stablefields = f"""SELECT RTRIM((SELECT STRING_AGG(CONCAT("'",column_name,":',","REPLACE(IFNULL(SAFE_CAST(",column_name," AS STRING),'null'),',',''),'|'")) FROM `{sObjectName_dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name = SPLIT('{sObjectName}', '.')[OFFSET(2)]),",'|'")"""

        stablefields = bq_exec(query_stablefields)

        # Prepare the SELECT query to fetch data
        query_select = f"SELECT '{orchestratorid}' AS orchestratorid, '{stage}' AS stage, '{regionkey}' AS region, '{sdatapatrolcountry}' AS country, '{data_source}' AS source, '{sComponent}' AS sourcecomponent, '{sObjectName}' AS objectname, '{sErrorSeverity}' AS errorseverity, '{sType}' AS type, CONCAT('{sErrorDescription}', ';', {stablefields}) AS errordescription, '{sObjectField}' AS erroridentifier, DATE('{sDateToExec}') AS createddttm, CURRENT_TIMESTAMP() AS recordtimestamp,'{sRecommendation}' as recommendation FROM {sObjectName} WHERE {sAdditionalCondition}"

        # Add additional filters for tables
        if sObjectType.lower() == 'table':
            if sObjectName.endswith("history"):
                query_select += f' AND createddttm = "{sDateToExec}"'
            else:
                query_select += f' AND rawingestiondttm = (SELECT MAX(rawingestiondttm) FROM {sObjectName} WHERE {sFileIngestionCondition}'
                query_select += f' AND `{insight_dataset}`.GetValidCountryKey({sCountryField}) IN ("{sdatapatrolcountry}"))'

            # Country field validation
            if sCountryField:
                if sErrorCode == 'E004':  # Country Error Code
                    query_select += f' AND IFNULL(`{insight_dataset}`.GetValidCountryKey({sCountryField}),"") NOT IN ("{sdatapatrolcountry}")'
                else:
                    query_select += f' AND `{insight_dataset}`.GetValidCountryKey({sCountryField}) IN ("{sdatapatrolcountry}")'

        query_select=query_select.replace('<COUNTRY_PLACEHOLDER>',sdatapatrolcountry)
        query_select=query_select.replace('<REGION_PLACEHOLDER>',regionkey)
        logging.info(f'Select Query: {query_select}')

        # Fetch data using the SELECT query
        raw_data = create_data_frame(query_select)
        query_result_df = pd.DataFrame(raw_data,columns=columns)
        logging.info(f"Fetched {query_result_df.shape[0]} records for {sObjectName} with query: {query_select}")
        
        # Append the fetched data to the data frame dictionary
        with data_frame_locks[uniqueDataFrameID]:
            frames_to_concat = [
                df for df in [data_frames[uniqueDataFrameID], query_result_df] if not df.empty and not df.isna().all(axis=None)
            ]
            if frames_to_concat:
                data_frames[uniqueDataFrameID] = pd.concat(frames_to_concat, ignore_index=True)

    except Exception as exception:
        with threading_execute_query_Failure_lock:
            threading_execute_query_Failure = True
        logging.error(traceback.format_exc())
        raise ValueError(f"ERROR in execute_query for {uniqueDataFrameID}, {exception}")

def data_patrol_send(**kwargs):

    config = kwargs["project_config"]
    isGlobalApproach = kwargs.get("global_approach") or False

    if config["gcp_environment"] == "False":
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/volume/jsonkey.json"
    
    bqClient = bigquery.Client()
    data_source = kwargs["data_source"]
    sDateToExec = getRequestedDate(config, data_source)
    if not isGlobalApproach:
        parentTaskGroup = kwargs["task"].__dict__["task_id"].split('.')[0]
        fullTaskId = parentTaskGroup + '.' + kwargs["task_to_pullfrom"]
        trueTask = parentTaskGroup + '.' + kwargs["true_task"]
        logging.info(f"trueTask: {trueTask}")
        falseTask = parentTaskGroup + '.' + kwargs["false_task"]
        logging.info(f"falseTask: {falseTask}")
        tables = kwargs['task_instance'].xcom_pull(task_ids=fullTaskId)
        tableName = tables[kwargs["key_to_retrieve"]]
        timestamp = tableName.split('_temp_')[-1]
    else:
        parentTaskGroup = kwargs["task_id"].split('.')[0]
        timestamp = sDateToExec.replace("-","_")+'_'+kwargs["timestamp"]
    stage = parentTaskGroup.split('_')[-1]
    region = config["requested_region"]
    country = kwargs["country"]
    insight_dataset = config['insights']['dataset']
    source_bucket = config["common"]["gcs_bucket_bq_output"]
    source_object = config["out_folder_name"].strip().replace('_','') + kwargs["source_object"]
    destination_path = kwargs["destination_path"]
    destination_object = config["out_folder_name"].strip().replace('_','') + kwargs["destination_object"]
    log_path = kwargs["log_path"]
    log_object = config["out_folder_name"].strip().replace('_','') + kwargs["log_object"]
    source_path = kwargs["source_path"]
    table_datapatrol_validation_log = config['insights']['tables']['current']['datapatrol_validation_log']
    component = kwargs["component"]
    table_datapatrol_validation_log_summary= config['insights']['tables']['current']['datapatrol_validation_log_summary']
    table_datapatrol_stage = config['landed']['tables']['datapatrol_stage']
    table_currencykey = config['landed']['tables']['currencykey']
    table_vendor_region_mapping = config['landed']['tables']['vendor_region_mapping']
    orchestratorid = config['dag0_id']
    doc_format = "csv"
    views=kwargs["views"]
    threshold = config['threshold'][region][data_source][country.lower()]
    dp_threshold_override_flag =config['dp_threshold_override_flag']

    if threshold !='':
        threshold_value=float(threshold)
    else:
        threshold_value=0

    logging.info(f"threshold_value: {threshold_value}")

    if not isGlobalApproach:
        source_file_path = 'gs://'+source_bucket+"/"+source_path+source_object+'_'+timestamp+'.'+doc_format
        destination_file_path = 'gs://'+source_bucket+"/"+destination_path + destination_object+'_'+timestamp+'.'+doc_format
    else:
        source_file_path = 'gs://'+source_bucket+"/"+source_path+source_object+'.'+doc_format
        destination_file_path = 'gs://'+source_bucket+"/"+destination_path + destination_object +'.'+doc_format
    log_file_location = 'gs://'+source_bucket+"/"+log_path+log_object+'_'+timestamp+'.csv'

    logging.info(f"source_file_path: {source_file_path}")
    logging.info(f"destination_file_path: {destination_file_path}")
    logging.info(f"log_file_location: {log_file_location}")

    logging.info("Comparing g2 count with view count")
    g2_vs_view_count_failure=g2count_vs_viewcount(source_file_path,views,sDateToExec)

    # Deleting the records from validation_log table before inserting to keep the records consistent for that particular dag_orchestration_id
    query_delete = f'DELETE FROM `{table_datapatrol_validation_log}` WHERE createddttm ="{sDateToExec}" AND Source = "{data_source}" AND sourcecomponent = "{component}" AND Region ="{region}" AND Stage = "{stage}" AND OrchestratorID = "{orchestratorid}" AND country="{country}"'
    logging.info(f'Delete Query: {query_delete}')
    exec_bq_job(bqClient,query_delete)
    
    # Get Vendor from sourcepath
    vendor = source_path.split('/')[0]
    logging.info(f"vendor: {vendor}")

    # get the entries from the datapatrol configuration table for the given stage,region and country
    dp_config_df = getConfigRecords(config,stage,country,component)
    
    # filter the entries based on the source file path and name
    dp_config_df['ObjectName'] = f"{vendor}/{region}/"+dp_config_df['ObjectName']

    # replace the placeholders in the objectname
    dp_config_df['ObjectName'] = dp_config_df['ObjectName'].str.replace("<COUNTRY_PLACEHOLDER>",country.lower()).str.replace("<REGION_PLACEHOLDER>",region.lower()).str.replace("<VENDOR_PLACEHOLDER>",vendor.lower())
    if isGlobalApproach:
        dp_config_df['ObjectName'] = dp_config_df["ObjectName"].apply(lambda x: modify_last_path_component(x, kwargs["source_object"]))
    dp_config_df=dp_config_df[dp_config_df['ObjectName'].str.lower()==(source_path+source_object).lower()]
    # read the data from the source file
    file_data_df=read_csv(source_file_path)

    # get the baseline count from the dp_config_df       
    baseline_count=int(get_baseline_count(dp_config_df,sDateToExec,country,region,vendor))

    # get the wagetypes from the dp_config_df
    wagetypes=get_wagetypes(dp_config_df,region,country,component)
    infotypes=get_infotypes(dp_config_df,region,country,component)

    # get the currencykey from the currencykey table
    currencykey=getCurrencykey(country,table_currencykey)

    #validate the g2 file with the datapatrol configurations
    error_logs,error_records,file_data = check_g2_file(file_data_df,dp_config_df,wagetypes,infotypes,country,currencykey,insight_dataset,data_source)

    # create a dataframe for the error records and log the errors from send in the datapatrol validation log table
    if len(error_logs)>0:
        error_df=pd.DataFrame(error_logs, columns=['errorseverity','type','errordescription','erroridentifier','recommendation'])

        #logging.info(f"error_logs: {error_logs}")

        error_df = error_df.assign(
                orchestratorid=orchestratorid,
                stage=stage,
                region=region,
                country=country,
                source=data_source,
                sourcecomponent=component,
                objectname=source_file_path,
                createddttm=datetime.datetime.strptime(sDateToExec, "%Y-%m-%d").date(),
                recordtimestamp=datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                )
        
        logging.info(f"error_df: {error_df}")
        
        # Reorder the columns
        error_df = error_df[['orchestratorid','stage','region','country','source','sourcecomponent','objectname','errorseverity','type','errordescription','erroridentifier','recommendation','createddttm','recordtimestamp']]
        
        # inserting the dag06 error logs to datapatrol_validation_log table
        job = create_table_df(error_df, table_datapatrol_validation_log)

    # remove the error records from the g2 file and create a new g2 file
    remove_error_records_from_g2_file(error_records,file_data,destination_file_path)
    
    # create an error log file for the records in the datapatrol validation log table for the given orchestrator id, createddttm and country
    dp_log_query=(
                 f'SELECT dp_log.orchestratorid, '
                 f'dp_log.stage, '
                 f'dp_stage.StageDescription, '
                 f'dp_log.region, '
                 f'dp_log.country, '
                 f'dp_log.source, '
                 f'dp_log.sourcecomponent, '
                 f'dp_log.objectname, '
                 f'dp_log.errorseverity, '
                 f'dp_log.type, '
                 f'dp_log.erroridentifier, '
                 f'dp_log.createddttm, '
                 f'dp_log.recordtimestamp, '
                 f'dp_log.errordescription , '
                 f'dp_log.recommendation '
                 f'FROM '
                 f'{table_datapatrol_validation_log} AS dp_log '
                 f'INNER JOIN '
                 f'{table_datapatrol_stage} AS dp_stage '
                 f'ON dp_log.Stage=dp_stage.Stage '
                 f'WHERE orchestratorid="{orchestratorid}" '
                 f'AND createddttm= DATE("{sDateToExec}") '
                 f'AND country="{country}" '
                 f'AND region="{region}" '
                 f'AND source="{data_source}" '
                 f'AND sourcecomponent="{component}" '
                )
    
    logging.info(f"dp_log_query: {dp_log_query}")
    
    # obtaining the dataframes from the queries
    df_error_log=create_data_frame(dp_log_query)
    if not df_error_log.empty:
        df_error_log['recordtimestamp'] = df_error_log['recordtimestamp'].dt.tz_localize(None)
    #logging.info(f"df_error_log: {df_error_log}")

    # get the  failure count for all the stages where the Type='Error'
    df_type_error=df_error_log.query('type=="Error"')
    stagefailurecount=df_type_error.shape[0]
    totalfailurecount=int(stagefailurecount+g2_vs_view_count_failure)
    
    error_percentage,threshold_check_status =threshold_check(baseline_count,totalfailurecount,threshold_value)
    
    logtimestamp = datetime.datetime.now()
    executiondate = datetime.datetime.strptime(sDateToExec, "%Y-%m-%d").date()

    info_df = pd.DataFrame(['Timestamp : '+str(logtimestamp),'ExecutionDate : '+sDateToExec, 'Region : '+region, 'Country : '+country, 'DataSource : '+data_source, 'DataSource Component : '+component, 'Stage Failure : '+str(stagefailurecount),'G2 vs View Count Failure : '+str(g2_vs_view_count_failure),'Total Base Record : '+str(baseline_count),'Total Failure : '+str(totalfailurecount) ,'Threshold(CutOff%) : '+str(threshold_value), 'Error(Calculated%) : '+str(error_percentage), 'Status : '+threshold_check_status])
    logging.info(f"info_df: {info_df}")

    # loading the summary entries in the datapatrol_validation_log_summary table
    dp_summary_df =pd.DataFrame([[logtimestamp,executiondate,orchestratorid,region,country,data_source,component,stagefailurecount,g2_vs_view_count_failure,totalfailurecount,threshold_value,error_percentage,threshold_check_status]], columns=['logtimestamp','executiondate','orchestratorid','region','country','source','sourcecomponent','stagefailure','g2vsviewcountfailure','totalfailure','thresholdpercentage','errorpercentage','status'])
    
    # Deleting the records from validation_log_summary table before inserting to keep the records consistent for that particular dag_orchestration_id
    query_delete = f'DELETE FROM `{table_datapatrol_validation_log_summary}` WHERE Source = "{data_source}" AND sourcecomponent = "{component}" AND Region ="{region}" AND OrchestratorID = "{orchestratorid}" AND country= "{country}" AND executiondate = DATE("{sDateToExec}")'
    logging.info(f'Delete Query: {query_delete}')
    exec_bq_job(bqClient,query_delete)    
    
    # insert the summary entries in the datapatrol_validation_log_summary table
    insert_job = create_table_df(dp_summary_df, table_datapatrol_validation_log_summary)
    create_error_log_file(info_df,log_file_location,df_error_log)
    
    logging.info(f"Calling publish_api_endpoint to send message from the project {config['gcp_project']} to the topic {config['pub_sub_api_endpoint_topic']}")
    publish_api_endpoint(
        task_id="success_log",
        project_id=config["gcp_project"],
        topic=config["pub_sub_api_endpoint_topic"],
        app_name="human_machine",
        key="Data Patrol Transformation"
    ) 
    logging.info("publish_api_endpoint call completed")

    if not isGlobalApproach:
        #Conditional task ID assignments based on threshold status.
        if threshold_check_status != 'Failure':
            logging.info(f"in true task: {trueTask}")
            return trueTask
        else:
            if dp_threshold_override_flag.upper()=='TRUE':
                '''This is an override flag to override the threshold check incase of Failure Scenario and generate the g2 file always.
                The value of this flag will be "TRUE" in DEV environment and "FALSE" in other environments to avoid AT UT validation failure without modifying threshold value'''
                logging.info(f"in true task {trueTask} as threshold check is overriden")
                return trueTask
            else:
                logging.info(f"in false task: {falseTask}")
                return falseTask
    else:
        return threshold_check_status, dp_threshold_override_flag


def create_error_log_file(info_df,log_file_location,df_error_log):
    
    if df_error_log.shape[0]>0:
        #If a string has a ',' or is empty, it quotes it
        df_error_log = df_error_log.apply(lambda x: x.map(lambda y: f'"{y}"' if ',' in str(y) or str(y) == ' ' else y))
        df_error_log.columns = range(len(df_error_log.columns))
        column_list=[['orchestratorid,','stage,','stagedescription,','region,','country,','source,','sourcecomponent,','objectname,','errorseverity,','type,','erroridentifier,','createddttm,','recordtimestamp,','errordescription,','recommendation']]
        df_col = pd.DataFrame(column_list)
        #Adds a , at the end of each log field
        for i in range(len(df_error_log.columns)-1):
            df_error_log[i] = df_error_log[i].astype(str) + ','
        list_of_dfs=[info_df,df_col,df_error_log]
        
    else:
        list_of_dfs=[info_df]
    
    df_data = pd.concat(list_of_dfs, ignore_index=True)
    #Creates the csv based of the concatenation of the three dataframes with ' ' as separator
    df_data.to_csv(log_file_location, header=False, index=False,sep=' ',quoting=csv.QUOTE_NONE, escapechar=' ', encoding='utf-8-sig')
    logging.info("file created successfully created at "+log_file_location) 


        

        
def remove_error_records_from_g2_file(error_records,file_data,destination_file_path):
    filtered_list=[]
    if (len(error_records) > 0):
        logging.info("removing error records from G2 file!!!")
        for val in file_data:
            if val not in error_records:
                filtered_list.append(val)
        data_list =filtered_list
    else:
        logging.info("No error records!!!")
        data_list =file_data
    result = write_g2_file(data_list,destination_file_path)
    logging.info(f"Error records removal result :{result}")

def check_g2_file(file_data_df,dp_config_df,wagetypes,infotypes,country,currencykey,insight_dataset,data_source):
        
        file_data=[]  # list that holds g2 file records
        temp_list=[]  # list that holds the records of each row in the g2 file
        status=""     # status of the record whether it is success or failure
        error_logs=[]  # list that holds the error details logged from the send into the datapatrol_validation_log table
        error_records=[]  # g2 file error records list which need to be removed from the g2 file
        
        for index, rows in file_data_df.iterrows():
            file_data.append(rows[0])

        for val in file_data:
            temp_list=val.split('|"')
            description=""
            status=""
            for i in range(1,len(temp_list)):
                # Get all validation rules for current field position
                filtered_df = dp_config_df[(dp_config_df['ObjectField'] == str(i)) & (dp_config_df['AdditionalCondition']==temp_list[0]) & (dp_config_df['Operator']!='BASELINEQUERY')]
                #check at what index wagetypecheck operator is present in items
                wagetype_config_df = dp_config_df[(dp_config_df['AdditionalCondition']==temp_list[0]) & (dp_config_df['Operator'].str.upper()=='WAGETYPECHECK')]
                if wagetype_config_df.shape[0]>0:
                    wagetype_field_index = int(wagetype_config_df['ObjectField'].values[0])
                    row_wagetype = temp_list[wagetype_field_index] if wagetype_field_index >= 0 and wagetype_field_index < len(temp_list) else ''
                if filtered_df.shape[0]>0:
                    for index,rows in filtered_df.iterrows():
                        # converts the string to dictionary
                        mandatoryfields=ast.literal_eval(rows['AdditionalFields'])

                        if rows['Operator'].upper()=='IN':
                            if temp_list[i] in ast.literal_eval(rows['ObjectFieldValue']):
                                status="Failure"
                                # for the mandatory fields indexes , concat the temp list values seperated by '|'
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])

                        elif rows['Operator'].upper()=='NOTIN':
                            if temp_list[i] not in ast.literal_eval(rows['ObjectFieldValue']):
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])

                        elif rows['Operator'].upper()=='DATEFORMAT':
                            regex_mapping = {
                                r'^\d{4}\d{2}\d{2}$': '%Y%m%d'
                            }
                            logging.info(f"ObjectFieldValue: {rows['ObjectFieldValue']}")
                            pattern = re.compile(rows['ObjectFieldValue'])
                            match = pattern.match(temp_list[i])
                            if not match:
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])
                            else:
                                try:
                                   date= datetime.datetime.strptime(temp_list[i], regex_mapping[rows['ObjectFieldValue']])
                                except ValueError:
                                    status="Failure"
                                    description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                    error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])
                                
                        elif rows['Operator'].upper()=='AMOUNTFORMAT':
                            if temp_list[i] not in [None,''] and not (re.compile(rows['ObjectFieldValue']).match(temp_list[i])):
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])

                        elif rows['Operator'].upper()=='INFOTYPECOMBINATIONFORMAT' and infotypes!={}:
                            if temp_list[i] not in infotypes.get(temp_list[0]) or temp_list[i] in ['',None]:
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i]],rows['Recommendation'])

                        elif rows['Operator'].upper()=='HOURSFORMAT':
                            if temp_list[i] not in [None,''] and not (re.compile(rows['ObjectFieldValue']).match(temp_list[i])):
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])        

                        elif rows['Operator'].upper()=='WAGETYPECHECK' and wagetypes!={}:
                            if temp_list[i] not in wagetypes.get(temp_list[0]) or temp_list[i] in ['',None]:
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])

                        elif rows['Operator'].upper()=='COUNTRYCHECK':
                            if temp_list[i] not in [country]:
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])

                        elif rows['Operator'].upper()=='CURRENCYCHECK':
                            if temp_list[i] not in [currencykey]:
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])
                        
                        elif rows['Operator'].upper()=='WAGETYPECURRENCYCHECK':
                            if temp_list[i] not in [get_wagetypecurrencykey(insight_dataset,country,row_wagetype,data_source)]:
                                status="Failure"
                                description=rows['ErrorDescription']+";"+"|".join(f'{value}:{temp_list[key]}' for key, value in zip(mandatoryfields.keys(), mandatoryfields.values()))
                                error_logs.append([rows['ErrorSeverity'],rows['Type'],description,mandatoryfields[i],rows['Recommendation']])
                        else:
                            logging.info(f"Invalid operator {rows['Operator']}")                     
                else:
                    continue
            if status=="Failure":
                error_records.append(val)
        return (error_logs,error_records,file_data)

def exec_bq_job(bqClient, sqlQuery, job_config=None):
    logging.info("Calling={}".format(sqlQuery))
    if job_config :
        query_job = bqClient.query(sqlQuery, job_config=job_config)
    else :    
        query_job = bqClient.query(sqlQuery)    
    return query_job.result()

def read_csv(source_file_path):
    try:
        df=pd.read_csv(source_file_path,header=None)
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame([])

def create_data_frame(sql):
    bqClient = bigquery.Client()
    df = bqClient.query(sql).to_dataframe()
    return df

def bq_exec(query):
    bqClient = bigquery.Client()
    query_exec = bqClient.query(query)
    query_result = query_exec.result()
    result_val =[ row[0] for row in query_result][0]
    return result_val

def g2count_vs_viewcount(source_file_path,views,date_exec):
    # returns the difference between the count of the G2 file and the count of the views used in the G2 file creation
    g2_count=get_g2_count(source_file_path)
    views_count=get_views_count(views,date_exec)
    return abs(g2_count-views_count)

def get_views_count(views,date_exec):
    # function to get the count of all the views used in the G2 file creation
    try:
        view_count_query=""
        for view in views.split(','):
            view_count_query=view_count_query+"SELECT '"+view+"' AS Viewname, COUNT(*) AS Count FROM "+view+"_"+date_exec.replace('-','_')+" UNION ALL "
        view_count_query=view_count_query[:-10]
        views_count_df=create_data_frame(view_count_query)
        views_count=views_count_df['Count'].sum()
        return views_count
    except Exception as err:
        return str(err)
            
def get_g2_count(source_file_path):
    # function to get the count of the G2 file
    df_g2_file = read_csv(source_file_path)
    g2_count=df_g2_file.shape[0]
    return g2_count 

def create_table_df(df,table):
    bqClient = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",)
    job = bqClient.load_table_from_dataframe(df, table,job_config=job_config)   
    job.result()
    return True

def write_g2_file(data_list,destination_file_path):
    try:
        to_csv(destination_file_path,pd.DataFrame(data_list))
    except pd.errors.EmptyDataError:
        to_csv(destination_file_path,pd.DataFrame([]))
    return True

def to_csv(destination_file_path,df):
    df.to_csv(destination_file_path,header=None, index=False,quoting=csv.QUOTE_NONE)

def threshold_check(baseline_count,failure_count,threshold_value):
    try:
        logging.info("in threshold function")
        logging.info(f"baseline_count: {baseline_count}")
        
        if baseline_count==0 or failure_count==0:
            error_perentage=0
        else:
            error_perentage = round(((failure_count/(baseline_count))*100),2)
        logging.info(f"error_perentage: {error_perentage}")
        
        if error_perentage > threshold_value  and error_perentage!=0:
            status = 'Failure'
        elif error_perentage <= threshold_value  and error_perentage!=0:
            status = 'Partial'
        elif error_perentage==0:
            status = 'Success'
        else:
            status = 'Success'
        logging.info(f"status: {status}")
        return (error_perentage,status)
    except ZeroDivisionError:
        return(0,'Success')
    
def get_wagetypes(dp_config_df,region,country,component):
    try:
        if not dp_config_df[dp_config_df['Operator']=='WAGETYPECHECK'].empty:
            wagetype_query =""
            # get all the unique wagetype queries from the dp_config_df and execute the corresponding query the distinct wagetypes
            queries_configured=dp_config_df[dp_config_df['Operator']=='WAGETYPECHECK']['ObjectFieldValue'].drop_duplicates().values
            # create one query by combining all the queries
            for query in queries_configured:
                logging.info(f"query: {query}")
                wagetype_query+=query+" UNION ALL "
            # replace the placeholders in the wagetype query 
            wagetype_query=wagetype_query.replace("<COUNTRY_PLACEHOLDER>",country.lower()).replace("<REGION_PLACEHOLDER>",region.lower()).replace("<COMPONENT_PLACEHOLDER>",component.lower()) 
            wagetype_query=wagetype_query[:-10] # remove the last UNION ALL
            logging.info(f"wagetype_query: {wagetype_query}")
            wagetypes_result=create_data_frame(wagetype_query)
            if not wagetypes_result.empty:
                wagetypes_result.columns =['Wagetype','Infotype']
                wagetypes_result.drop_duplicates(inplace=True)
                # convert the wagetpes_result to dictionary where infotype is the key and corresonding wagetype list is the value
                wagetypes = wagetypes_result.groupby('Infotype')['Wagetype'].apply(list).to_dict()
                return wagetypes
            else:
                logging.info("No wagetypes found in the wagetype query")
                return {}
        else:
            logging.info("No WAGETYPECHECK configured in the datapatrol configuration table")
            return {} # if no check is configured in the datapatrol configuration table we return an empty dictionary as the wagetypes validation is not performed
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in get wagetype, {}'.format(exception))
    
def get_infotypes(dp_config_df,region,country,component):
    try:
        if not dp_config_df[dp_config_df['Operator']=='INFOTYPECOMBINATIONFORMAT'].empty:
            infotype_query =""
            # get all the unique wagetype queries from the dp_config_df and execute the corresponding query the distinct wagetypes
            queries_configured=dp_config_df[dp_config_df['Operator']=='INFOTYPECOMBINATIONFORMAT']['ObjectFieldValue'].drop_duplicates().values
            # create one query by combining all the queries
            for query in queries_configured:
                logging.info(f"query: {query}")
                infotype_query+=query+" UNION ALL "
            # replace the placeholders in the wagetype query 
            infotype_query=infotype_query.replace("<COUNTRY_PLACEHOLDER>",country.lower()).replace("<REGION_PLACEHOLDER>",region.lower()).replace("<COMPONENT_PLACEHOLDER>",component.lower()) 
            infotype_query=infotype_query[:-10] # remove the last UNION ALL
            logging.info(f"infotype_query: {infotype_query}")
            infotype_result=create_data_frame(infotype_query)
            if not infotype_result.empty:
                infotype_result.columns =['Country','Infotype','Wagetype']
                infotype_result.drop_duplicates(inplace=True)
                infotypes = infotype_result.to_dict('records')
                return infotypes
            else:
                logging.info("No infotypes found in the infotypes query")
                return {}
        else:
            logging.info("No INFOTYPECOMBINATIONFORMAT configured in the datapatrol configuration table")
            return {} # if no check is configured in the datapatrol configuration table we return an empty dictionary as the wagetypes validation is not performed
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in get infotype, {}'.format(exception))
 


def get_baseline_count(dp_config_df,sDateToExec,country,region,vendor):  
    try: 
        '''get the baseline query from the dp_config_df and execute the corresponding query which fetches count from the baseline count config.Finally sum up the counts to get the total baseline count.
           Sample query incase of single table: SELECT COUNT(*) FROM table1
           Sample query incase of multiple tables: SELECT COUNT(*) FROM table1 UNION ALL SELECT COUNT(*) FROM table2'''
        baseline_query = dp_config_df[dp_config_df['Operator']=='BASELINEQUERY']['ObjectFieldValue'].values[0]
        datasource_suffix = sDateToExec.replace("-","_")
        # replace the sDateToExec and datasource_suffix in the baseline query
        baseline_query=baseline_query.replace("sDateToExec",sDateToExec).replace("datasource_suffix",datasource_suffix).replace("<COUNTRY_PLACEHOLDER>",country.lower()).replace("<REGION_PLACEHOLDER>",region.lower()).replace("<VENDOR_PLACEHOLDER>",vendor.lower())
        logging.info(f"baseline_query: {baseline_query}")
        baseline_result_df = create_data_frame(baseline_query)
        baseline_result_df.columns =['Count']
        baseline_count=baseline_result_df['Count'].sum()
        return baseline_count
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in get baseline count, {}'.format(exception))

def move_ingestion_error_log(sCommaSepCountries,sCommaSepFields,kwargs):
    try:
        config = kwargs["project_config"]
        data_source = config['requested_datasource']
        logging.info(f'Data movement for ingestion datapatrol stage started for {data_source}\n\n')

        bqClient = bigquery.Client()
        if config["gcp_environment"] == "False":
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/volume/jsonkey.json"
        orchestratorid = config['dag0_id']
        ingestion_error_log=config['insights']['tables']['current']['datapatrol_ingestion_error_log']
        table_datapatrol_validation_log=config['insights']['tables']['current']['datapatrol_validation_log']
        sRegionKey = config['requested_region']
        sDataSourceKey = config['requested_datasource']
        sComponent = config['requested_datasource_component']
        sDateToExec= getRequestedDate(config, sDataSourceKey)


        if sComponent!="" and sComponent is not None :
            component_condition=" AND sourcecomponent ='"+sComponent+"'"
        else:
            component_condition="" 

        # Deleting the records from validation_log table before inserting to keep the records consistent for that particular dag_orchestration_id
        query_delete = f'DELETE FROM `{table_datapatrol_validation_log}` WHERE createddttm ="{sDateToExec}" AND Source = "{data_source}"  AND Region ="{sRegionKey}" AND Stage = "Ingestion" AND OrchestratorID = "{orchestratorid}"'+component_condition
        if sCommaSepCountries:
                query_delete += ' AND country IN (' + sCommaSepCountries + ')' 

        logging.info(f'Delete Query for ingestion stage: {query_delete}')
        exec_bq_job(bqClient,query_delete)

        # Inserting the records from ingestion_error_log to datapatrol_validation_log table
        query_insert = f'INSERT `{table_datapatrol_validation_log}` ({sCommaSepFields}) SELECT "{orchestratorid}", stage, region, country, source, sourcecomponent, objectname, errorseverity, type, errordescription,"", ' \
                    f'DATE(\'{sDateToExec}\') AS createddttm, CURRENT_TIMESTAMP() AS recordtimestamp,"" FROM `{ingestion_error_log}` AS T WHERE ' \
                    f'source=\'{sDataSourceKey}\' AND region = \'{sRegionKey}\' AND country IN ( {sCommaSepCountries} )'+component_condition

        logging.info(query_insert)

        exec_bq_job(bqClient,query_insert)
        logging.info(f'Records inserted into {table_datapatrol_validation_log} for {sDateToExec}')    
        logging.info(f'Data movement for ingestion datapatrol stage completed successfully for {data_source}\n\n')
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in move_ingestion_error_log, {}'.format(exception)) 

    
def getCurrencykey(country,table_currencykey):
    try:
        currencykey_query = f'SELECT CurrencyKey FROM `{table_currencykey}` WHERE CountryKey = "{country}"'
        currencykey = bq_exec(currencykey_query)
        return currencykey
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in getCurrencykey, {}'.format(exception))
    
def getConfigRecords(config,stage,countries,component):
    try:
        # prepare the query
        table_datapatrol_configuration = config['landed']['tables']['datapatrol_configuration']
        table_datapatrol_error_mapping = config['landed']['tables']['datapatrol_error_mapping']
        table_datapatrol_error_severity_mapping = config['landed']['tables']['datapatrol_error_severity_mapping']
        table_datapatrol_log_fields_mapping = config['landed']['tables']['datapatrol_log_fields_mapping']
        table_vendor_region_mapping = config['landed']['tables']['vendor_region_mapping']
        data_source = config['requested_datasource']
        regionkey = config['requested_region']

        if component:
            component_condition = f'AND dp_config.Component = "{component}" '
        else:
            component_condition = ''

        dp_config_query = (
                                f'SELECT '
                                f'dp_config.Country AS Country, '
                                f'dp_config.Stage AS Stage, '
                                f'dp_config.ObjectType AS ObjectType, '
                                f'dp_config.ObjectName AS ObjectName, '
                                f'dp_config.ObjectField AS ObjectField, '
                                f'dp_config.Operator AS Operator, '
                                f'dp_config.ObjectFieldValue AS ObjectFieldValue, '
                                f'dp_config.AdditionalCondition AS AdditionalCondition, '
                                f'dp_config.ErrorCode AS ErrorCode, '
                                f'dp_config.Recommendation AS Recommendation, '
                                f'dp_err_map.Description AS ErrorDescription, '
                                f'dp_sev_map.SeverityDescription AS ErrorSeverity, '
                                f'dp_err_map.Type AS Type, '
                                f'dp_fields_map.LogFields AS AdditionalFields, '
                                f'dp_config.CountryKeyField AS CountryKeyField, '
                                f'dp_config.FileIngestionCondition AS FileIngestionCondition, '
                                f'dp_config.Vendor AS Vendor, '
                                f'dp_config.Component AS Component '
                                f'FROM ' 
                                f'`{table_datapatrol_configuration}` AS dp_config '
                                f'INNER JOIN '
                                f'`{table_datapatrol_error_mapping}` AS dp_err_map '
                                f'ON '
                                f'dp_config.ErrorCode=dp_err_map.code '
                                f'INNER JOIN '
                                f'`{table_datapatrol_error_severity_mapping}` AS dp_sev_map '
                                f'ON '
                                f'dp_config.SeverityID=dp_sev_map.SeverityID '
                                f'INNER JOIN '
                                f'`{table_datapatrol_log_fields_mapping}` AS dp_fields_map '
                                f'ON dp_config.FieldsCode=dp_fields_map.FieldsCode '
                                f'INNER JOIN '
                                f'`{table_vendor_region_mapping}` AS vendor_region_map '
                                f' ON dp_config.Vendor = vendor_region_map.Vendor '
                                f'WHERE dp_config.SOURCE = "{data_source}" '
                                f'AND vendor_region_map.Region = "{regionkey}" '
                                f'AND dp_config.Stage = "{stage}" '
                                f'{component_condition}'
                                f'AND dp_config.IsActive = True '
                            )
        if stage == 'dag06':  
            # add below query to the existing query to get the baseline query records
            baseline_query = (
                              f'UNION ALL '
                              f'SELECT '
                              f'dp_config.Country AS Country, '
                              f'dp_config.Stage AS Stage, '
                              f'dp_config.ObjectType AS ObjectType, '
                              f'dp_config.ObjectName AS ObjectName, '
                              f'dp_config.ObjectField AS ObjectField, '
                              f'dp_config.Operator AS Operator, '
                              f'dp_config.ObjectFieldValue AS ObjectFieldValue, '
                              f'dp_config.AdditionalCondition AS AdditionalCondition, '
                              f'dp_config.ErrorCode AS ErrorCode, '
                              f'dp_config.Recommendation AS Recommendation, '
                              f'"" AS ErrorDescription, '
                              f'"" AS ErrorSeverity, '
                              f'"" AS Type, '
                              f'"" AS AdditionalFields, '
                              f'"" AS CountryKeyField, '
                              f'"" AS FileIngestionCondition, '
                              f'dp_config.Vendor AS Vendor, '
                              f'dp_config.Component AS Component '
                              f'FROM '
                              f'`{table_datapatrol_configuration}` AS dp_config '
                              f'INNER JOIN '
                              f'`{table_vendor_region_mapping}` AS vendor_region_map '
                              f'ON dp_config.Vendor = vendor_region_map.Vendor '
                              f'WHERE dp_config.SOURCE = "{data_source}" '
                              f'AND vendor_region_map.Region = "{regionkey}" '
                              f'AND dp_config.Stage = "{stage}" '
                              f'AND dp_config.Operator="BASELINEQUERY" '
                              f'{component_condition}'
                              f'AND dp_config.IsActive = True '
                            )

            dp_config_query = dp_config_query + baseline_query
        
            logging.info(dp_config_query)
        
        df=create_data_frame(dp_config_query)
    
        if not df.empty:
            df['Country'] = df['Country'].str.split(',')
            df = df.explode(['Country']).reset_index(drop=True)
            df = df[df['Country'].isin(countries.replace("'", "").split(','))]

        else:
            logging.info(f"Datapatrol skipped for {stage} for datasource {data_source} as no datapatrol rules are configured") 

        return df
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in getConfigRecords, {}'.format(exception))

def modify_last_path_component(path, new_value):
    path_components = path.split('/')
    path_components[-1] = new_value

    return '/'.join(path_components)

def get_wagetypecurrencykey(insight_dataset,country,row_wagetype,data_source):
    try:
        print(f"insight_dataset: {insight_dataset}, country: {country}, row_wagetype: {row_wagetype}, data_source: {data_source}")
        # add function call to {insights.dataset}`.GetCurrencyByWageType(inCountryKey STRING, inWageType STRING, inDataSource STRING) 
        wagetypecurrency_query = f"SELECT `{insight_dataset}`.GetCurrencyByWageType('{country}', '{row_wagetype}','{data_source}')"
        wagetypecurrencykey = bq_exec(wagetypecurrency_query)
        if wagetypecurrencykey is None:
            currency_sql = f"SELECT `{insight_dataset}`.GetCurrencyKey('{country}')"
            wagetypecurrencykey = bq_exec(currency_sql)
        
        return wagetypecurrencykey
    except Exception as exception:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in get_wagetypecurrencykey, {}'.format(exception))
