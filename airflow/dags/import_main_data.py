
from datetime import timedelta, datetime
import pytz
import os
import pandas as pd
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator




AIRFLOW_CONN_ECG_DW = os.getenv('AIRFLOW_CONN_ECG_DW')
POSTGRES_HOOK = PostgresHook('ecg_dw')
ENGINE = POSTGRES_HOOK.get_sqlalchemy_engine()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    'import_main_data_hd_pls',
    default_args=default_args,
    description='Import Main Transactions Files',
    schedule_interval='@daily',
    start_date=days_ago(1),
    is_paused_upon_creation=False
)

wait_for_init = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',
    execution_date_fn = lambda x: datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),
    timeout=1,
    dag=dag
)

def get_validated(filetype):

    with ENGINE.connect() as con:

        result = con.execute(f"""SELECT Filename FROM ops.FlatFileLoadRegistry where validated=True and extension='{filetype}' """)

        return set(row.values()[0] for row in result)


def get_processed(filetype):

    with ENGINE.connect() as con:

        result = con.execute(f"""SELECT Filename FROM ops.FlatFileLoadRegistry where processed=True and extension='{filetype}' """)

        return set(row.values()[0] for row in result)


def update_flatfile_registry(file_data):

    command = f"""
    INSERT INTO ops.FlatFileLoadRegistry(Filename, Extension, LoadDate, Processed, Validated)
    VALUES('{file_data['filename']}','{file_data['extension']}','{file_data['loaddate']}',{file_data['processed']}, {file_data['validated']} ) 
    ON CONFLICT (Filename) 
    DO UPDATE SET processed={file_data['processed']}, validated={file_data['validated']}, loaddate='{file_data['loaddate']}';
    """

    with ENGINE.connect() as con:

        con.execute(command)

def convert_to_pg_array(array_string):
    return array_string.replace('[', '{').replace(']', '}').replace("'", '')

def preprocess_csv():

    IMPORT_PATH = '/import/csv/raw/'
    EXPORT_PATH = '/import/csv/processed/'
    PROCESSED = get_processed('csv')
    IGNORED = '.keep'
    
    for file in sorted(os.listdir(IMPORT_PATH)):

        if file not in PROCESSED and file != IGNORED and file !='.DS_Store':
            extension = file.split('.')[-1]

            df = pd.read_csv(IMPORT_PATH+file, encoding='utf-8',dtype={'study_id': str, 'subject_id': str, 'ed_stay_id': str, 'ed_hadm_id': str, 'hosp_hadm_id': str})

            df['ed_diag_ed'] = df['ed_diag_ed'].apply(convert_to_pg_array)
            df['ed_diag_hosp'] = df['ed_diag_hosp'].apply(convert_to_pg_array)
            df['hosp_diag_hosp'] = df['hosp_diag_hosp'].apply(convert_to_pg_array)
            df['all_diag_hosp'] = df['all_diag_hosp'].apply(convert_to_pg_array)
            df['all_diag_all'] = df['all_diag_all'].apply(convert_to_pg_array)
            df['file_name'] = df['file_name'].str.split('/')
            df['filename_1'] = df['file_name'].apply(lambda x: x[0].split('-')[-1])
            df['filename_2'] = df['file_name'].apply(lambda x: x[2])
            df['filename_3'] = df['file_name'].apply(lambda x: x[3])
            df['filename_4'] = df['file_name'].apply(lambda x: x[4])
            df['filename_5'] = df['file_name'].apply(lambda x: x[5])
            df = df.drop('file_name', axis=1)
            df['ecg_time'] = pd.to_datetime(df['ecg_time'])
            df['dod'] = pd.to_datetime(df['dod'])
            df['ecg_taken_in_ed'] = df['ecg_taken_in_ed'].astype(bool)
            df['ecg_taken_in_hosp'] = df['ecg_taken_in_hosp'].astype(bool)
            df['ecg_taken_in_ed_or_hosp'] = df['ecg_taken_in_ed_or_hosp'].astype(bool)
            df['age'] = df['age'].fillna(-1).astype(int)
            df['anchor_year'] = df['anchor_year'].fillna(-1).astype(int)
            df['anchor_age'] = df['anchor_age'].fillna(-1).astype(int)
            df['ecg_no_within_stay'] = df['ecg_no_within_stay'].fillna(0).astype(int)
            df['Imported_File'] = file

            df.to_csv(EXPORT_PATH+file,index=False)

            file_data = {'filename': file, 'extension': extension, 'loaddate': datetime.now() ,'processed':True, 'validated': False}
            
            update_flatfile_registry(file_data)
            
            print(f'Processed {file}')


def import_csv():
    PATH = '/import/csv/processed'
    VALIDATED = get_validated('csv')
    IGNORED = '.keep'
    for file in sorted(os.listdir(PATH)):
        if file not in VALIDATED and file != IGNORED:
            extension = file.split('.')[-1]
            SQL_STATEMENT = """
            COPY import.EcgCSV(record_id, study_id, subject_id, ecg_time, ed_stay_id, ed_hadm_id, hosp_hadm_id, ed_diag_ed, ed_diag_hosp, hosp_diag_hosp, all_diag_hosp, all_diag_all, gender, age, anchor_age, anchor_year, dod, ecg_no_within_stay, ecg_taken_in_ed, ecg_taken_in_hosp, ecg_taken_in_ed_or_hosp, fold, strat_fold, file_name_1, file_name_2, file_name_3, file_name_4, file_name_5, Imported_File) FROM STDIN DELIMITER ',' CSV HEADER;
            """
            conn = POSTGRES_HOOK.get_conn()
            cur = conn.cursor()

            try:
                with open(os.path.join(PATH, file), mode='r', encoding='utf-8', errors='replace') as f:
                    cur.copy_expert(SQL_STATEMENT, f)
                conn.commit()

                file_data = {
                    'filename': file,
                    'extension': extension,
                    'loaddate': datetime.now(),
                    'processed': True,
                    'validated': True
                }
                update_flatfile_registry(file_data)
                print(f'Imported {file}')
            except UnicodeDecodeError as e:
                print(f"Unicode decode error in file {file}: {e}")
            except Exception as e:
                print(f"Unexpected error with file {file}: {e}")
            finally:
                cur.close()
                conn.close()


preprocess_csv_task = PythonOperator(
    task_id='preprocess_csv',
    python_callable=preprocess_csv,
    dag=dag
)

import_csv_task = PythonOperator(
    task_id='import_csv',
    python_callable=import_csv,
    dag=dag
)

create_csv_destination_task = PostgresOperator(
    task_id='create_csv_destination',
    sql = """
CREATE TABLE IF NOT EXISTS import.EcgCSV (

   record_id varchar(255) ,
   file_name_1 varchar(255),
   file_name_2 varchar(255),
   file_name_3 varchar(255),
   file_name_4 varchar(255), 
   file_name_5 varchar(255),              
   study_id varchar(255),              
   subject_id varchar(255),            
   ecg_time timestamp,             
   ed_stay_id varchar(255),            
   ed_hadm_id varchar(255),            
   hosp_hadm_id varchar(255),          
   ed_diag_ed TEXT[],            
   ed_diag_hosp TEXT[],          
   hosp_diag_hosp TEXT[],        
   all_diag_hosp TEXT[],        
   all_diag_all TEXT[],          
   gender varchar(10),                 
   age int,                            
   anchor_year int,                    
   anchor_age int,                     
   dod date,                           
   ecg_no_within_stay int,             
   ecg_taken_in_ed boolean,            
   ecg_taken_in_hosp boolean,          
   ecg_taken_in_ed_or_hosp boolean,    
   fold int,                           
   strat_fold int,
   Imported_File   varchar(255),
   Loaded_Timestamp timestamp not null default now()

   );
    """,
    dag=dag,
    postgres_conn_id = 'ecg_dw',
    autocommit = True
)



wait_for_init >> create_csv_destination_task >> preprocess_csv_task >> import_csv_task 