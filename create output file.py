import os
import logging
import traceback
import json

import datetime, pytz
from google.cloud import bigquery

from dag_utils import getRequestedDate
from datetime import timedelta
from google.cloud.exceptions import NotFound
import logging
import time

def create_output_tables(**kwargs):
    try:
        config = kwargs["project_config"]
        # The temporal_table_mapping data is coming from the create_table.task file 
        # and passed as kwarg while executing from the dag_core
        config['temporal_table_mapping'] =kwargs['temporal_table_mapping']

        if config["gcp_environment"] == "False":
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/volume/jsonkey.json"

        bqClient = bigquery.Client(project=config['gcp_project'])

        nowUnformatted = datetime.datetime.now(datetime.timezone.utc)
        expirationTime = config["tablesOutputExpirationTimeDays"]
        suffix = getRequestedDate(config, config["requested_datasource"]).replace("-","_") + "_" + nowUnformatted.strftime('%Y%m%d%H%M%S%f')

        tablesToCreate = tables_to_create(config)
             
        objToReturn = json.loads('{}')
        
        for table in tablesToCreate:
            temp_table_name = f"{table}_temp_{suffix}"
            print('Creating Table ' + temp_table_name)
            tmp_table_def = bigquery.Table(temp_table_name)
            
            tmp_table_def.expires = nowUnformatted + timedelta(days=int(expirationTime))
            bqClient.create_table(tmp_table_def, exists_ok=True)
            objToReturn.update({table: temp_table_name})

        #check the table creation status
        wait_until_all_tables_exist(objToReturn)

        return objToReturn

    except:
        logging.error(traceback.format_exc())
        raise ValueError('ERROR in create_tables')

def tables_to_create(config):
    requested_datasource = config["requested_datasource"]
    requested_region = config["requested_region"]
    temporal_table_mapping = config['temporal_table_mapping'].replace('\n','').replace('\t','').replace(' ','').strip().split('|')
    temporal_table_dict = {}
    
    for temporal_table in temporal_table_mapping:
        table_key = str(temporal_table).split(':')[0]
        table_value =temporal_table.split(':')[1].split(',')
        temporal_table_dict[table_key]=table_value
    
    tables_for_myte_uki = temporal_table_dict['myte_uki']
    tables_for_myte_na = temporal_table_dict['myte_na']
    tables_for_myte_sea = temporal_table_dict['myte_sea']
    tables_for_myte_anz = temporal_table_dict['myte_anz']
    tables_for_myte_latam = temporal_table_dict['myte_latam']
    tables_for_myte_saf = temporal_table_dict['myte_saf']
    tables_for_payment_elections = temporal_table_dict['payment_elections_na']
    tables_for_equity_na = temporal_table_dict['equity_na']
    tables_for_equity_uki = temporal_table_dict['equity_uki']
    tables_for_otaccural = temporal_table_dict['otaccural_uki']
    tables_for_pmi = temporal_table_dict['pmi_uki']
    tables_for_performance_recognition_uki = temporal_table_dict['performance_recognition_uki']
    tables_for_hr_misc_uki = temporal_table_dict['hr_misc_uki']
    config_tables = extract_config_tables(config,requested_region)
    tablesToCreate = []
    if requested_datasource == 'myte' and requested_region == 'uki' and len(tables_for_myte_uki) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_myte_uki))
    elif requested_datasource == 'myte' and requested_region == 'na' and len(tables_for_myte_na) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_myte_na))
    elif requested_datasource == 'myte' and requested_region == 'latam' and len(tables_for_myte_latam) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_myte_latam))   
    elif requested_datasource == 'myte' and requested_region == 'saf' and len(tables_for_myte_saf) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_myte_saf))
    elif requested_datasource == 'myte' and requested_region == 'sea' and len(tables_for_myte_sea) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_myte_sea))   
    elif requested_datasource == 'myte' and requested_region == 'anz' and len(tables_for_myte_anz) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_myte_anz))   
    elif requested_datasource == 'payment_elections' and len(tables_for_payment_elections) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_payment_elections))
    elif requested_datasource == 'equity' and requested_region == 'na' and len(tables_for_equity_na) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_equity_na))
    elif requested_datasource == 'equity' and requested_region == 'uki' and len(tables_for_equity_uki) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_equity_uki))
    elif requested_datasource == 'otaccrual' and len(tables_for_otaccural) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_otaccural))
    elif requested_datasource == 'pmi' and len(tables_for_pmi) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_pmi))
    elif requested_datasource == 'performance_recognition' and requested_region == 'uki' and len(tables_for_performance_recognition_uki) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_performance_recognition_uki))
    elif requested_datasource == 'hr_misc' and requested_region == 'uki' and len(tables_for_hr_misc_uki) != 0:
        tablesToCreate = append_tableToCreate(config_tables,tablesToCreate,_apply_feature_flag(config,tables_for_hr_misc_uki))
    # apply feature flags validation here
    return tablesToCreate

def append_tableToCreate(config_tables,tableToCreate,tableToAppend):
    for each_table in tableToAppend:
        tableToCreate.append(config_tables[each_table])
    return tableToCreate

def extract_config_tables(config,requested_region):
    if requested_region == 'na': 
        return config["temporal"]["tables"]["current"]["ceridian"]["na"]
    elif requested_region == 'uki':
        return config["temporal"]["tables"]["current"]["adp"]["uki"]
    elif requested_region == 'sea':
        return config["temporal"]["tables"]["current"]["adp"]["sea"]
    elif requested_region == 'latam':
        return config["temporal"]["tables"]["current"]["adp"]["latam"]
    elif requested_region == 'anz':
        return config["temporal"]["tables"]["current"]["adp"]["anz"]
    elif requested_region == 'saf':
        return config["temporal"]["tables"]["current"]["adp"]["saf"]

def _apply_feature_flag(config,tables_list):
    # add condition for each feature flag here as per the interface implementation
    # if not config['feature_flags']['701663-1_AIR140704']:
    #     try:
    #         tables_list.remove('table_bq_output_adp_hr_misc_avanade_overtime_uk')
    #     except:
    #         print("Not applicable for 'table_bq_output_adp_hr_misc_avanade_overtime_uk' since it belongs to different datasource")
    return tables_list

def wait_until_all_tables_exist(objToReturn):
    logging.info("Checking whether all the tables are created or not...")
    missing_tables = list(objToReturn.items())
    bqclient = bigquery.Client()   
    timeout = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
    
    while missing_tables  and datetime.datetime.now(datetime.timezone.utc)< timeout:
        still_missing_tables = []
        
        for logical_name, table_name in missing_tables:
            try:
                bqclient.get_table(table_name)
                logging.info(f"Table {table_name} is created successfully.")
            except NotFound:
                logging.info(f"Table {table_name} is not yet created.")
                still_missing_tables.append((logical_name, table_name))

        missing_tables = still_missing_tables  # continue until empty
        if missing_tables:
            time.sleep(1 * 60) # Wait for 1 minute before checking again
           
    
    if missing_tables:
        raise ValueError(f"Some tables are still missing after 10 minutes: {missing_tables}")

    logging.info("All tables are created successfully.")
