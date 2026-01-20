"""dag core"""
import operator
import datetime
import logging
import sys
import os
import json



PTH = os.path.abspath(os.getcwd())
sys.path.append(PTH)
PTH1 = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PTH1)
datasourcetemplatepath = ""
# import airflow classes for initializing class
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Python Callable tasks
from dag_lib.dag_jobs import (
    job_exec
)

from dag_lib.dag_checkconditiontrigger_jobs import (
    check_state_locations_calculate_import_needed
)
from dag_lib.task_class import TaskConfig

import dag_lib.audit.audit_callable as audit_callable
import dag_lib.dag_config as dag_config
import dag_lib.dag_utils as dag_utils
import dag_lib.create_output_tables as create_output_tables
import dag_lib.check_common_table as check_common_table
import dag_lib.check_table_switch as check_table_switch
import dag_lib.check_output_file as check_output_file
import dag_lib.create_adp_format_file as create_adp_format_file
import dag_lib.adp_myte_file_replace_date as adp_myte_file_replace_date
import dag_lib.create_reverse_file as create_reverse_file
import dag_lib.adp_deduct_banked_hours as adp_deduct_banked_hours
import dag_lib.adp_pr_voidorder_log as adp_pr_voidorder_log
import dag_lib.data_patrol as data_patrol
import dag_lib.global_data_movement as global_data_movement
import dag_lib.threshold_check as threshold_check


from dag_lib.operators_class import Operators
from dag_lib.config_class import JSONConfig
from dag_lib.custom_operators.extended_bigquery_executejob_operator import ExtendedBigQueryExecuteJobOperator
from dag_lib.custom_operators.extended_bigquery_gcsexport_operator import ExtendedBigQueryGcsExportOperator
from dag_lib.custom_operators.extended_bigquery_gcsexport_singledate_operator import ExtendedBigQueryGcsExportSingleDateOperator
from dag_lib.custom_operators.extended_bigquery_gcsfiletransform_operator import ExtendedGCSFileTransformOperator
from dag_lib.custom_operators.extended_bigquery_gcsfiletransform_singledate_operator import ExtendedGCSFileTransformSingleDateOperator
from dag_lib.custom_operators.extended_gcs_to_bigquery_schemafields_operator import ExtendedGCSToBigQuerySchemaFieldsOperator
from dag_lib.custom_operators.extended_gcstogcs_operator import ExtendedGCSToGCSOperator
from dag_lib.custom_operators.extended_gcstogcs_compensatory_operator import ExtendedGCSToGCSCompensatoryOperator
from dag_lib.custom_operators.extended_adp_gcstogcs_operator import ExtendedADPGCSToGCSOperator
from dag_lib.custom_operators.extended_afs_gcstogcs_operator import ExtendedAFSGCSToGCSOperator
from dag_lib.custom_operators.extended_gcs_files_merge_operator import GCSFilesMergeOperator
from dag_lib.custom_operators.dp_common_operator import DataPatrolCommonOperator
from dag_lib.custom_operators.dp_myte_operator import DataPatrolMyteOperator
from dag_lib.custom_operators.dp_ot_operator import DataPatrolOTOperator
from dag_lib.custom_operators.dp_performance_recognition_operator import DataPatrolPerformanceRecognitionOperator
from dag_lib.custom_operators.dp_snow_operator import DataPatrolSnowOperator
from dag_lib.custom_operators.dp_equity_operator import DataPatrolEquityOperator
from dag_lib.custom_operators.state_locations_gcsfiletransform_operator import GCSFileTransformStateLocationsMultipleOperator
from dag_lib.custom_operators.state_locations_gcstobq_operator import GCSFileToBigQueryStateLocationsMultipleOperator
from dag_lib.custom_operators.create_empty_file import CreateEmptyFile
from dag_lib.custom_operators.testdata_import_operator import TestDataImportOperator
from dag_lib.custom_operators.datasource_testdataimport_operator import DataSourceTestDataImportOperator
from dag_lib.custom_operators.global_logic_template_operator import GlobalLogicTemplateOperator
from dag_lib.custom_operators.global_logic_data_patrol_operator import GlobalLogicDataPatrolOperator
from dag_lib.custom_operators.global_logic_processors_operator import GlobalLogicProcessorsOperator
from dag_lib.custom_operators.global_generate_file_operator import GlobalGenerateFileOperator
from dag_lib.custom_operators.global_run_python_or_sql_file_operator import GlobalRunPythonOrSQLFileOperator

def define_dag_with_subdags(dml_path, project_config_path, dag_id, requested_datasource):
    """docs"""

    if requested_datasource == "":
        #This is for generic Dag0 not to be created. Only dags from at-test should via -refreshDags flag.
        logging.info(f"{dag_id} cannot be run as it does not have its settings properly set.")
        return

    project_config = dag_config.get_projlist_config(project_config_path)  
    os.environ['FEATURE_CONFIG'] = str(project_config.config["feature_flags"])
    tags = get_Tags(project_config, requested_datasource)
    default_dag_args = get_default_dag_args()
    getdatasourcetemplatepath(project_config_path)
    
    with models.DAG(
        dag_id,
        schedule="@once",
        tags=tags,
        default_args=default_dag_args
    ) as dag:
        build_DAG(dag_id, dml_path, project_config, requested_datasource, dag)        

    return dag

def build_DAG(dag_id, dml_path, project_config, requested_datasource, dag):
                
        task_list = dag_config.get_task_list(dml_path, project_config, dag_id, False, requested_datasource)
        task_list = dict(sorted(task_list.items(), key=operator.itemgetter(0)))
    
        logging.info(dag_id)
        
        nextdag = ""
        gscopr = []

        for item_index, task in task_list.items():

            #task = detail

            logging.info("----")
            logging.info('%s - %s', str(item_index), task.task_id)

            if task.task_id == "success_log":
                #These success_log and failed_log tasks are from DAG0 Dag (NOT TASKGROUPS)
                task.tasklist = (',').join([dagTask.task_id for dagTask in dag.tasks if 'success_log' in dagTask.task_id])
                task.nextdag = nextdag
            elif task.task_id == "failed_log":
                task.tasklist = (',').join([dagTask.task_id for dagTask in dag.tasks])
                task.nextdag = nextdag
            else:
                task.tasklist = task.task_id
                task.nextdag = ""

            if  task.task_id == "start_log" or task.task_id == "success_log" or task.task_id == "failed_log":
                if task.task_id == "start_log":
                    gscopr.append(
                        PythonOperator(
                            task_id=task.task_id,
                            dag=dag,
                            python_callable=job_exec,
                            trigger_rule=task.trigger_rule,
                            op_kwargs=task.__dict__.items(),
                            pool_slots=3,
                            retries=3,  # Number of retries
                            retry_delay=datetime.timedelta(minutes=10)  # Delay between retries
                        )
                    )
                else:
                    gscopr.append(
                        PythonOperator(
                            task_id=task.task_id,
                            dag=dag,
                            python_callable=job_exec,
                            trigger_rule=task.trigger_rule,
                            op_kwargs=task.__dict__.items(),
                            pool_slots = 3
                        )
                    )

            else:
                task_list_dml_path = get_dml_path_dag(task.trigger_dag_id)
                taskgroup_dag_id = dag_id + "." + task.task_id
                taskgroup_datasource = dag_utils.get_subdag_datasource(task.trigger_dag_id)
                taskgroup_task_list = dag_config.get_task_list(task_list_dml_path, project_config, taskgroup_dag_id, True, taskgroup_datasource)
                taskgroup_task_list = dict(sorted(taskgroup_task_list.items(), key=operator.itemgetter(0)))

                with TaskGroup(task.task_id, tooltip=f"Tasks for {task.task_id}") as tg1:
                    defineTasks(taskgroup_task_list, dag, project_config, task_list_dml_path, task.task_id)

                gscopr.append(tg1)

            if item_index not in [0]:
                if task.parent_task_id == "":
                    gscopr[item_index - 1] >> gscopr[item_index]

                else:
                    
                    parent_task_list = []

                    common_final_task = 'common_final_empty'
                    requested_data_source = requested_datasource

                    if common_final_task in task.task_id:
                        parent_task_list = [itemList for itemList in task.parent_task_id.split(",") if requested_data_source in itemList or 'common' in itemList or 'start_log' in itemList]
                    else:
                        parent_task_list = task.parent_task_id.split(",")

                    parent_task_list = [x.replace("\n", "") for x in parent_task_list]
                
                    for parent in parent_task_list:
                        parent_index = 0

                        for index, item in task_list.items():                        
                            if item.task_id == parent:
                                parent_index = index
                                break

                        gscopr[parent_index] >> gscopr[item_index]


def get_default_dag_args() :

    today = datetime.datetime.today() # 2021-04-15 14:48:02.564700
    timedelta = datetime.timedelta(1) # 1 day, 0:00:00
    time = datetime.datetime.min.time() # 00:00:00
    yesterday =  datetime.datetime.combine(today-timedelta,time) #2021-04-14 00:00:00

    return {"start_date": yesterday, "retries": 5, "retry_delay": datetime.timedelta(seconds=120)}
    

def get_Tags(project_config: JSONConfig, requested_datasource):

    region = project_config.config['requested_region']
    country = project_config.config['requested_country']
    isDagDependencyFrom = project_config.config['isDagDependencyFrom']
    isRerun = project_config.config['requested_rerun'].upper() == "TRUE"
    component = project_config.config['requested_datasource_component']
    requested_rawsource = project_config.config['requested_rawsource']
    hasVendorTransfer = project_config.config['requested_vendor_transfer'].upper() == "TRUE"
    execDate = project_config.config['requested_date']
    isBatchd02 = project_config.config['requested_batchd02'].upper() == "TRUE"

    dagTags = []

    if region != "": dagTags.append(region)
    if country != "": 
        for country_ in country.split(","):
            dagTags.append(country_)
    if execDate != "": dagTags.append(execDate)
    if requested_datasource != "": dagTags.append(requested_datasource)
    if isDagDependencyFrom != "None" and isDagDependencyFrom != "": dagTags.append(f"{isDagDependencyFrom}_dependency")
    if isRerun: dagTags.append("Rerun")
    dagTags.append(component) if component != "" else dagTags.append("*")
    if requested_rawsource != "": dagTags.append(f"{requested_rawsource}")
    if hasVendorTransfer: dagTags.append(f"Transfer")
    if isBatchd02: dagTags.append("Batchd02")

    return dagTags
 
def get_dag_id(project_config_path, dag_id_prop):
    """docs"""
    proj_config_map = dag_config.get_projlist_config(project_config_path)
    return proj_config_map.vars[dag_id_prop]

def get_dml_path_dag(dag_id): 
    PTH1 = os.path.dirname(os.path.abspath(__file__))

    dag_index = dag_id.find("dag_")
    generic_dag_name = dag_id[dag_index:len(dag_id)] 

    config_path = '/dml'
    relative_path = '/../'    

    dag_01 = 'dag_01_init'
    dag_02 = 'dag_02_raw_history'
    dag_03 = 'dag_03_curate_to_in'
    dag_04 = 'dag_04_in_history'
    dag_05 = 'dag_05_views'
    dag_06 = 'dag_06_files'

    switcher = {
        dag_01: PTH1 + relative_path + dag_01 + config_path,
        dag_02: PTH1 + relative_path + dag_02 + config_path,
        dag_03: PTH1 + relative_path + dag_03 + config_path,
        dag_04: PTH1 + relative_path + dag_04 + config_path,
        dag_05: PTH1 + relative_path + dag_05 + config_path,
        dag_06: PTH1 + relative_path + dag_06 + config_path
    }

    return switcher.get(generic_dag_name, '0')

def defineTasks(task_list, dag, project_config, task_list_dml_path, taskGroupId):
    tasklist = ""
    nextdag = ""
    nextdagid = 0

    gscopr = []
    for item_index, detail in task_list.items():

        task: TaskConfig = detail

        logging.info("----")
        logging.info('%s - %s', str(item_index), task.task_id)
        
        if task.operator.lower() == Operators.triggerdagrunoperator:
            varnextdag = (
                str(nextdagid)
                + ":{'id':'"
                + str(item_index)
                + "','taskid':'"
                + task.task_id
                + "','trigger_dag_id':'"
                + task.trigger_dag_id
                + "'}"
            )
            nextdag = nextdag + "," + varnextdag if nextdag != "" else varnextdag
            nextdagid = nextdagid + 1

        #taskgroups have only success log to report its status
        if task.task_id == "success_log":
            #Getting tasks from respective taskgroup
            taskGroupTasks = ([filteredTaskId for filteredTaskId in dag.task_group.__dict__['used_group_ids'] 
                if filteredTaskId is not None and not filteredTaskId.__contains__("None") and not filteredTaskId.__contains__("join") \
                    and filteredTaskId.__contains__('.') and filteredTaskId.__contains__(taskGroupId)]
            )
            detail.tasklist =  ','.join(taskGroupTasks)
            detail.nextdag = nextdag
        else:
            tasklist = (
                tasklist + "," + task.task_id
                if tasklist != ""
                else task.task_id
            )
            detail.tasklist = task.task_id
            detail.nextdag = ""

        if task.operator.lower() == Operators.extendedbigquerygcsexportoperator:
            gscopr.append(
                ExtendedBigQueryGcsExportOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_project_dataset_table=project_config.config["insights"]["dataset"] + "." + task.source_project_dataset_table,
                    destination_cloud_storage_uris="gs://" + task.destination_cloud_storage_uris,
                    export_format=task.export_format,
                    print_header=bool(False),       
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,             
                    labels={
                        #"dag_id": dag_id,
                        "task_id": task.task_id,
                        "air_id": task.air_id
                    },
                )
            )
        elif task.operator.lower() == Operators.extendedbigquerygcsexportsingledateoperator:
            gscopr.append(
                ExtendedBigQueryGcsExportSingleDateOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_project_dataset_table=project_config.config["insights"]["dataset"] + "." + task.source_project_dataset_table,
                    destination_cloud_storage_uris="gs://" + task.destination_cloud_storage_uris,
                    export_format=task.export_format,
                    print_header=bool(False),       
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,             
                    labels={
                        #"dag_id": dag_id,
                        "task_id": task.task_id,
                        "air_id": task.air_id
                    },
                )
            )         
        elif task.operator.lower() == Operators.extendedbigquerygcsfiletransformoperator:
            gscopr.append(
                ExtendedGCSFileTransformOperator(
                    task_id=task.task_id,
                    dag=dag,                   
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    destination_bucket= project_config.config["common"]["gcs_bucket_bq_output"],
                    destination_object= task.destination_path + project_config.config["out_folder_name"].strip().replace('_','') + task.destination_object, 
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    export_format=task.export_format,
                    transform_script = ["python", task_list_dml_path + task.py_script],
                )
            )
        elif task.operator.lower() == Operators.extendedbigquerygcsfiletransformsingledateoperator:
            gscopr.append(
                ExtendedGCSFileTransformSingleDateOperator(
                    task_id=task.task_id,
                    dag=dag,                   
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    destination_bucket= project_config.config["common"]["gcs_bucket_bq_output"],
                    destination_object= task.destination_path + project_config.config["out_folder_name"].strip().replace('_','') + task.destination_object, 
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    export_format=task.export_format,
                    transform_script = ["python", task_list_dml_path + task.py_script],
                )
            )
        elif task.operator.lower() == Operators.gcsfilesmergeoperator:
            gscopr.append(
                GCSFilesMergeOperator(
                    task_id=task.task_id,
                    dag=dag,                   
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object_path=task.source_object_path,
                    out_put_folder_name=project_config.config["out_folder_name"].strip().replace('_',''),
                    source_object=task.source_object,
                    destination_bucket= project_config.config["common"]["gcs_bucket_bq_output"],
                    destination_object= task.destination_path + project_config.config["out_folder_name"].strip().replace('_','') + task.destination_object, 
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    export_format=task.export_format,
                    gcp_environment=project_config.config["gcp_environment"]
                )
            )
        elif task.operator.lower() == Operators.gcsfiletransformstatelocationsmultipleoperator:
            gscopr.append(
                GCSFileTransformStateLocationsMultipleOperator(
                    task_id=task.task_id,
                    dag=dag,                   
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_development_import"],                    
                    source_object=task.source_object,
                    destination_bucket= project_config.config["common"]["gcs_bucket_bq_development_import"],
                    destination_object= task.destination_path + task.destination_object, 
                    export_format=task.export_format,
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,             
                    transform_script = ["python", task_list_dml_path + task.py_script]
                )
            )
        elif task.operator.lower() == Operators.gcstobqoperatormultiple:
            gscopr.append(
                GCSFileToBigQueryStateLocationsMultipleOperator(
                    task_id=task.task_id,
                    dag=dag,
                    bucket=project_config.config["common"]["gcs_bucket_bq_development_import"],
                    source_objects=[task.source_path],
                    schema_object=task.schema_object,
                    destination_project_dataset_table=task.destination_table_variable,
                    skip_leading_rows=1,
                    write_disposition=task.write_disposition,
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    gcp_environment = project_config.config["gcp_environment"]
                )
            )
        elif task.operator.lower() == Operators.bqcreatetablesforexport:
            gscopr.append(
                PythonOperator(
                    task_id=task.task_id,
                    dag=dag,
                    python_callable=create_output_tables.create_output_tables,
                    op_kwargs={**detail.__dict__,
                               "temporal_table_mapping":task.ds_region}
                )
            )
        elif task.operator.lower() == Operators.createemptyfileoperator:
            gscopr.append(
                CreateEmptyFile(
                    task_id=task.task_id,
                    dag=dag,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    format=task.format,
                    destination=task.destination,
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    gcp_environment=project_config.config["gcp_environment"],
                    apply=task.apply,
                    empty_file_required=project_config.config["feature_flags"]["670943-1_AIR140704"]
                )
            )
        elif task.operator.lower() == Operators.bigqueryinsertjoboperator:
            sql = task.sql


            # Within the sql code there may be literals that are replaced in some environment. 
            # To do this we use the key replacement in the .task file.
            # Example: replacement=twin,{raw.dataset},{raw.dataset_replaced}
            if task.replacement:  
                replaces = task.replacement.split(";")
                for replace in replaces :  
                    r = replace.split(",")
                    if r[0].upper() ==  project_config.config["release_environment"].upper():  
                        sql = sql.replace(r[1],r[2])

            gscopr.append(
                BigQueryInsertJobOperator(
                    task_id=task.task_id,
                    dag=dag,
                    configuration={
                        "query": {
                            "query": sql,
                            "useLegacySql": False,
                        }
                    }
                )
            )
        elif task.operator.lower() == Operators.globalbigqueryinsertjoboperator:
            sql = task.sql
            country = task.country                        
            sql = sql.replace('@placeholder_countrykey',country)   
            sql = sql.replace('@placeholder_featureflag',str(task.feature_flag))
            sql = key_replacement(sql,task.key_replacement)

            gscopr.append(
                BigQueryInsertJobOperator(
                    task_id=task.task_id,
                    dag=dag,
                    configuration={
                        "query": {
                            "query": sql,
                            "useLegacySql": False,
                        }
                    }
                )
            )
        elif task.operator.lower() == Operators.globallogictemplateoperator:
            gscopr.append(
                GlobalLogicTemplateOperator(
                    task_id=task.task_id,
                    dag=dag,
                    config = project_config.config,
                    trigger_rule=task.trigger_rule
                )
            )
        elif task.operator.lower() == Operators.globallogicdatapatroloperator:
            gscopr.append(
                GlobalLogicDataPatrolOperator(
                    task_id=task.task_id,
                    dag=dag,
                    config=project_config.config,
                    trigger_rule=task.trigger_rule
                )
            )
        elif task.operator.lower() == Operators.globallogicprocessoroperator:
            gscopr.append(
                GlobalLogicProcessorsOperator(
                    task_id=task.task_id,
                    dag=dag,
                    config=project_config.config,
                    processor_type=task.processor_type
                )
            )    
        elif task.operator.lower() == Operators.checktasktriggercondition:
            pythonCallable = None
            if task.python_callable == 'check_state_locations_calculate_import_needed':
                pythonCallable = check_state_locations_calculate_import_needed
            gscopr.append(
                BranchPythonOperator(
                    task_id=task.task_id,
                    dag=dag,
                    python_callable=pythonCallable,
                    trigger_rule=task.trigger_rule,
                    op_kwargs=detail.__dict__.items()
                )
            )
        elif task.operator.lower() == Operators.extendedbigqueryexecutejoboperator:
            gscopr.append(
                ExtendedBigQueryExecuteJobOperator(
                    task_id=task.task_id,
                    dag=dag,
                    sql=task.sql,
                    use_legacy_sql=task.use_legacy_sql,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    config = project_config.config
                )
            )
        elif task.operator.lower() == Operators.globalbigqueryexecutejoboperator:
            query = task.sql
            query = key_replacement(query,task.key_replacement)
            gscopr.append(
                ExtendedBigQueryExecuteJobOperator(
                    task_id=task.task_id,
                    dag=dag,
                    sql=query,
                    use_legacy_sql=task.use_legacy_sql,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    config = project_config.config
                )
            ) 
        elif task.operator.lower() == Operators.datapatrolmyteoperator:
            gscopr.append(
                DataPatrolMyteOperator(
                    task_id=task.task_id,
                    dag=dag, 
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    source_path=task.source_path,
                    destination_object=task.destination_object,
                    destination_path=task.destination_path,
                    log_object=task.log_object,
                    log_path=task.log_path,
                    data_source = task.data_source,
                    region = project_config.config["requested_region"],
                    threshold = project_config.config["threshold"],
                    country = task.country,
                    component = project_config.config["requested_datasource_component"],
                    true_task = task.true_task,
                    false_task = task.false_task,
                    output_folder_name=project_config.config["out_folder_name"].strip().replace('_',''),
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    gcp_environment=project_config.config["gcp_environment"]
                    
                )
            )
        elif task.operator.lower() == Operators.datapatrolotoperator:
            gscopr.append(
                DataPatrolOTOperator(
                    task_id=task.task_id,
                    dag=dag, 
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    source_path=task.source_path,
                    destination_object=task.destination_object,
                    destination_path=task.destination_path,
                    log_object=task.log_object,
                    log_path=task.log_path,
                    data_source = task.data_source,
                    region = project_config.config["requested_region"],
                    threshold = project_config.config["threshold"],
                    country = task.country,
                    component = project_config.config["requested_datasource_component"],
                    true_task = task.true_task,
                    false_task = task.false_task,
                    output_folder_name=project_config.config["out_folder_name"].strip().replace('_',''),
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    gcp_environment=project_config.config["gcp_environment"]
                    
                )
            )
        elif task.operator.lower() == Operators.datapatrolperformancerecognitionoperator:
            gscopr.append(
                DataPatrolPerformanceRecognitionOperator(
                    task_id=task.task_id,
                    dag=dag, 
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    source_path=task.source_path,
                    destination_object=task.destination_object,
                    destination_path=task.destination_path,
                    log_object=task.log_object,
                    log_path=task.log_path,
                    data_source = task.data_source,
                    region = project_config.config["requested_region"],
                    threshold = project_config.config["threshold"],
                    country = task.country,
                    component = project_config.config["requested_datasource_component"],
                    true_task = task.true_task,
                    false_task = task.false_task,
                    output_folder_name=project_config.config["out_folder_name"].strip().replace('_',''),
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    gcp_environment=project_config.config["gcp_environment"]    
                                   
                )
            )
        elif task.operator.lower() == Operators.datapatrolsnowoperator:
            gscopr.append(
                DataPatrolSnowOperator(
                    task_id=task.task_id,
                    dag=dag, 
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    source_path=task.source_path,
                    destination_object=task.destination_object,
                    destination_path=task.destination_path,
                    log_object=task.log_object,
                    log_path=task.log_path,
                    data_source = task.data_source,
                    region = project_config.config["requested_region"],
                    threshold = project_config.config["threshold"],
                    country = task.country,
                    component = task.component,
                    true_task = task.true_task,
                    false_task = task.false_task,
                    output_folder_name=project_config.config["out_folder_name"].strip().replace('_',''),
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    gcp_environment=project_config.config["gcp_environment"]
                    
                )
            )
        elif task.operator.lower() == Operators.datapatrolequityoperator:
            gscopr.append(
                DataPatrolEquityOperator(
                    task_id=task.task_id,
                    dag=dag, 
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    source_object=task.source_object,
                    source_path=task.source_path,
                    destination_object=task.destination_object,
                    destination_path=task.destination_path,
                    log_object=task.log_object,
                    log_path=task.log_path,
                    data_source = task.data_source,
                    region = project_config.config["requested_region"],
                    threshold = project_config.config["threshold"],
                    country = task.country,
                    component = task.component,
                    true_task = task.true_task,
                    false_task = task.false_task,
                    output_folder_name=project_config.config["out_folder_name"].strip().replace('_',''),
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    gcp_environment=project_config.config["gcp_environment"]
                    
                )
            )
        elif task.operator.lower() == Operators.checkthresholdtriggerconditionoperator:
                pythonCallable = None
                if task.python_callable == 'data_patrol_send':
                    pythonCallable = data_patrol.data_patrol_send
                gscopr.append(
                    BranchPythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=pythonCallable,
                        trigger_rule=task.trigger_rule,
                        op_kwargs=detail.__dict__.items(),      
                    )
                )
        elif task.operator.lower() == Operators.pythonoperator:
            gscopr.append(
                PythonOperator(
                    task_id=task.task_id,
                    dag=dag,
                    python_callable=job_exec,
                    trigger_rule=task.trigger_rule,
                    op_kwargs=detail.__dict__.items()
                )
            )
        elif task.operator.lower() == Operators.globalrunpythonorsqlfileoperator:
            gscopr.append(
                GlobalRunPythonOrSQLFileOperator(
                    dag=dag,
                    config = project_config.config,
                    param_config = project_config,
                    replacements = getattr(task, "replacement", None),  # <-- Pass replacements if present
                    trigger_rule=task.trigger_rule,
                    **dict(detail.__dict__)
                )
            )
        elif task.operator.lower() == Operators.dagpythonoperator:
            if task.python_callable == 'check_common_table':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=check_table_switch.check_common_table,
                        op_kwargs=detail.__dict__.items(),
                    )
                )
            elif task.python_callable == 'check_datasource_tables':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=check_common_table.check_schema,
                        op_kwargs=detail.__dict__.items(),
                    )
                )   
            elif task.python_callable == 'check_data_moved_between_tables':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=check_common_table.check_data_moving_between_tables,
                        op_kwargs=detail.__dict__.items(),
                    )
                )    
            elif task.python_callable == 'check_data_dependencies':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=check_common_table.check_data_dependencies,
                        op_kwargs=detail.__dict__.items(),
                    )
                )
            elif task.python_callable == 'check_global_data_dependencies':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=check_common_table.check_global_data_dependencies,
                        op_kwargs=detail.__dict__.items(),
                    )
                )
            elif task.python_callable == 'create_adp_format_file':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=create_adp_format_file.create_adp_format_file,
                        op_kwargs=detail.__dict__.items(),      
                    )
                ) 
            elif task.python_callable == 'adp_myte_file_replace_date':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=adp_myte_file_replace_date.adp_myte_file_replace_date,
                        op_kwargs=detail.__dict__.items(),      
                    )
                ) 
            elif task.python_callable == 'adp_deduct_banked_hours':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=adp_deduct_banked_hours.adp_deduct_banked_hours,
                        op_kwargs=detail.__dict__.items(),      
                    )
                ) 
            elif task.python_callable == 'create_reverse_file':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=create_reverse_file.create_reverse_file,
                        op_kwargs=detail.__dict__.items(),      
                    )
                )     
            elif task.python_callable == 'check_output_file':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=check_output_file.check_output_file,
                        op_kwargs=detail.__dict__.items(),
                    )
                ) 
            elif task.python_callable == 'audit_callable':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=audit_callable.audit_callable,
                        op_kwargs=detail.__dict__.items(),
                    )
                )
            elif task.python_callable == 'adp_pr_voidorder_log':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=adp_pr_voidorder_log.adp_pr_voidorder_log,
                        op_kwargs=detail.__dict__.items(),      
                    )
                )
            elif task.python_callable == 'data_patrol_access_logic':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=data_patrol.data_patrol_access_logic,
                        op_kwargs=detail.__dict__.items(),      
                    )
                )
            elif task.python_callable == 'threshold_check':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=threshold_check.data_access_threshold_check,
                        op_kwargs=detail.__dict__.items(),
                    )
                )    
            elif task.python_callable == 'global_data_movement':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=global_data_movement.main_move_data,
                        op_kwargs={**detail.__dict__,
                               "datasourcetemplatepath":datasourcetemplatepath}
                    )
            )
            elif task.python_callable == 'delete_in_insights':
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=global_data_movement.delete_in_insights,
                        op_kwargs=detail.__dict__.items(),
                    )
            )
            else:
                gscopr.append(
                    PythonOperator(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=task.python_callable,
                        op_kwargs=detail.__dict__.items(),
                    )
                )   
        elif task.operator.lower() == Operators.dynamictaskpythonperator:
            if task.python_callable == 'process_row':
                # Get reference of global_create_views task to retrieve its output
                global_create_views_task = dag.get_task(f'{taskGroupId}.global_create_views')

                gscopr.append(
                    PythonOperator.partial(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=global_create_views_task.process_row,
                        map_index_template="{{ OutputViewName.split('.', 2)[2] }}"
                    ).expand(op_kwargs=global_create_views_task.output)
                )
            elif task.python_callable == 'generate_file':
                # Get reference of global_files_creator task to retrieve its output
                global_files_creator_task = dag.get_task(f'{taskGroupId}.global_files_creator')

                gscopr.append(
                    PythonOperator.partial(
                        task_id=task.task_id,
                        dag=dag,
                        python_callable=global_files_creator_task.generate_file,
                        map_index_template="{{ Region }}_{{ Country }}_{{ DataSource }}_{{ Component }}"
                    ).expand(op_kwargs=global_files_creator_task.output)
                )
        elif task.operator.lower() == Operators.triggerdagrunoperator:
            gscopr.append(
                TriggerDagRunOperator(
                    task_id=task.task_id,
                    dag=dag,
                    trigger_dag_id=task.trigger_dag_id,
                    wait_for_completion=True
                )
            )
        elif task.operator.lower() == Operators.gcstobqoperator:
            gscopr.append(
                ExtendedGCSToBigQuerySchemaFieldsOperator(
                    task_id=task.task_id,
                    dag=dag,
                    bucket=project_config.config["common"]["gcs_bucket_bq_development_import"],
                    source_objects=[task.source_path],
                    schema_object=task.schema_object,
                    destination_project_dataset_table=task.destination_table_variable,
                    skip_leading_rows=1,
                    write_disposition=task.write_disposition,
                    gcp_environment = project_config.config["gcp_environment"]                   

                )
            )
        elif task.operator.lower() == Operators.gcstobqoperatorappend:
            gscopr.append(
                ExtendedGCSToBigQuerySchemaFieldsOperator(
                    task_id=task.task_id,
                    dag=dag,
                    bucket=project_config.config["common"]["gcs_bucket_bq_development_import"],
                    source_objects=[task.source_path],
                    schema_object=task.schema_object,
                    destination_project_dataset_table=task.destination_table_variable,
                    skip_leading_rows=1,
                    write_disposition='WRITE_APPEND',
                    gcp_environment = project_config.config["gcp_environment"]                   
                )
            )        
        elif task.operator.lower() == Operators.datasourcetestdataimportoperator:
            gscopr.append(
                DataSourceTestDataImportOperator(
                    task_id=task.task_id,
                    dag=dag,
                    importBucket=project_config.config["common"]["gcs_bucket_bq_development_import"],
                    task_list_dml_path=task_list_dml_path,
                    table_keys=task.table_keys,
                    table_names=task.table_names,
                    pool_slots = 5,
                    out_folder_name = project_config.config["out_folder_name"],
                    write_disposition = task.write_disposition
                )
            )
        elif task.operator.lower() == Operators.filetogcsoperator:
            gscopr.append(
                LocalFilesystemToGCSOperator(
                    task_id=task.task_id,
                    dag=dag,
                    src= task_list_dml_path + task.source_path,
                    dst= task.destination_path,
                    bucket=project_config.config["common"]["gcs_bucket_bq_development_import"]
                )
            )
        elif task.operator.lower() == Operators.gcstogcsoperator:
            gscopr.append(
                GCSToGCSOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    source_object=task.source_path + task.source_object,
                    destination_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    destination_object=task.destination_path + task.destination_object
                )
            )
        elif task.operator.lower() == Operators.extendedgcstogcsoperator:
            gscopr.append(
                ExtendedGCSToGCSOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    source_object=project_config.config["out_folder_name"].strip().replace('_','') + task.source_object,
                    destination_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    destination_object=task.destination_object,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    source_path=task.source_path,
                    destination_path=task.destination_path,
                    move=project_config.config["requested_vendor_transfer"]
                )
        )
        elif task.operator.lower() == Operators.extendedgcstogcscompensatoryoperator:
            gscopr.append(
                ExtendedGCSToGCSCompensatoryOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    source_object=project_config.config["out_folder_name"].strip().replace('_','') + task.source_object,
                    destination_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    destination_object=task.destination_object,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    source_path=task.source_path,
                    destination_path=task.destination_path,
                    move=project_config.config["requested_vendor_transfer"],
                    requested_date=project_config.config['requested_date']
                )
            )
        elif task.operator.lower() == Operators.extendedadpgcstogcsoperator:
            gscopr.append(
                ExtendedADPGCSToGCSOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    source_object=task.source_object,
                    destination_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    destination_object=task.destination_object,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    source_path=task.source_path,
                    destination_path=task.destination_path,
                    move=project_config.config["requested_vendor_transfer"],
                    file_mapping=task.file_mapping,
                    region=task.region,
                    country=task.country,
                    data_source=task.data_source,
                    release_environment = project_config.config["release_environment"],
                    dataset_id = project_config.config["insights"]["dataset"],
                    output_folder= project_config.config["out_folder_name"].strip().replace('_', ''),
                    gcp_environment = project_config.config["gcp_environment"],
                    requested_date = dag_utils.getRequestedDate(project_config.config,project_config.config["requested_datasource"]),
                    datalake_mocked_flag=project_config.config["datalake_mocked_flag"]
                )
        )
        elif task.operator.lower() == Operators.extendedafsgcstogcsoperator:
            gscopr.append(
                ExtendedAFSGCSToGCSOperator(
                    task_id=task.task_id,
                    dag=dag,
                    source_bucket=project_config.config["common"]["gcs_bucket_bq_output"],
                    source_object=project_config.config["out_folder_name"].strip().replace('_','') + task.source_object,
                    destination_bucket=project_config.config["common"]["gcs_bucket_bq_output"],                    
                    destination_object=task.destination_object,                    
                    task_to_pullfrom=task.task_to_pullfrom,
                    key_to_retrieve=task.key_to_retrieve,
                    source_path=task.source_path,
                    destination_path=task.destination_path,
                    move=project_config.config["requested_vendor_transfer"]
                )
        )
        elif task.operator.lower() == Operators.globalgeneratefileoperator:
            gscopr.append(
                 GlobalGenerateFileOperator(
                    task_id=task.task_id,
                    dag=dag,
                    config = project_config.config
                )
        )
        elif task.operator.lower() == Operators.emptyoperator:
            gscopr.append(
                EmptyOperator(
                    task_id=task.task_id,
                    dag=dag,
                    trigger_rule=task.trigger_rule
                )
            )        

        if item_index not in [0]:
            #If no parent, lets set the dependency right before the last existing item
            if len(task.parent_task_id_list) == 0:
                gscopr[item_index - 1] >> gscopr[item_index]
            else:
                for parent in task.parent_task_id_list:
                    parent_index = 0

                    for index, item in task_list.items():                        
                        if item.task_id == parent:
                            parent_index = index
                            break
                    try:
                        gscopr[parent_index] >> gscopr[item_index]
                    except IndexError:
                        raise Exception(f"There was an issue loading the hierarchy for task [{task.task_id}]. Check that it's parent task is loaded first.")

def key_replacement(sql, key_replacement):
    logging.info("sql before key replacement: %s",sql)
    logging.info("key_replacement keys : %s",key_replacement)
    if key_replacement:  
        replaces = key_replacement.split(";")
        logging.info(replaces)
                
        for replace in replaces :  
            r = replace.split(",")
            sql = sql.replace(r[0],r[1])
    logging.info("sql after key replacement: %s",sql)
    return sql

def getdatasourcetemplatepath(project_config_path):
    global datasourcetemplatepath
    datasourcetemplatepath = str(project_config_path).replace("projectlist.json", "datasourcetemplate.json")
    return None
