from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import sys
from datetime import datetime, timedelta
import subprocess
from subprocess import call

default_args = {
    'owner':'nsf_data_ingestion',
    'depends_on_past':'False',
    'start_date': datetime.now(),
}

dag = DAG('nsf_data_ingestion', default_args = default_args, schedule_interval=timedelta(hours=3), catchup=False)

GitClone = BashOperator(
    task_id='GitClone',
    bash_command='date',
    dag=dag,
)

TFDIF_Model = BashOperator(
    task_id='TFDIF_Model',
    bash_command='date',
    dag=dag,
)

Medline = BashOperator(
    task_id='Medline',
    bash_command='date',
    dag=dag,
)

Pubmed = BashOperator(
    task_id='Pubmed',
    bash_command='date',
    dag=dag,
)

Federal_Reporter = BashOperator(
    task_id='Federal_Reporter',
    bash_command='date',
    dag=dag,
)

Medline.set_upstream(GitClone)
Pubmed.set_upstream(GitClone)
Federal_Reporter.set_upstream(GitClone)

Medline_Download = BashOperator(
    task_id='Medline_Download',
    bash_command='sleep 5',
    dag=dag,
)

Medline_Persist = BashOperator(
    task_id='Medline_Persist',
    bash_command='date',
    dag=dag,
)

Medline_Parquet = BashOperator(
    task_id='Medline_Parquet',
    bash_command='date',
    dag=dag,
)

Medline_Download.set_upstream(Medline)
Medline_Persist.set_upstream(Medline_Download)
Medline_Parquet.set_upstream(Medline_Persist)

Pubmed_Download = BashOperator(
    task_id='Pubmed_Download',
    bash_command='sleep 5',
    dag=dag,
)

Pubmed_Untar = BashOperator(
    task_id='Pubmed_Untar',
    bash_command='date',
    dag=dag,
)

Pubmed_Chunk = BashOperator(
    task_id='Pubmed_Chunk',
    bash_command='date',
    dag=dag,
)

Pubmed_Zip = BashOperator(
    task_id='Pubmed_Zip',
    bash_command='sleep 5',
    dag=dag,
)

Pubmed_Persist = BashOperator(
    task_id='Pubmed_Persist',
    bash_command='date',
    dag=dag,
)

Pubmed_Parquet = BashOperator(
    task_id='Pubmed_Parquet',
    bash_command='date',
    dag=dag,
)

Pubmed_Download.set_upstream(Pubmed)
Pubmed_Untar.set_upstream(Pubmed_Download)
Pubmed_Chunk.set_upstream(Pubmed_Untar)
Pubmed_Zip.set_upstream(Pubmed_Chunk)
Pubmed_Persist.set_upstream(Pubmed_Zip)
Pubmed_Parquet.set_upstream(Pubmed_Persist)

Federal_Download = BashOperator(
    task_id='Federal_Download',
    bash_command='sleep 5',
    dag=dag,
)

Federal_Persist = BashOperator(
    task_id='Federal_Persist',
    bash_command='date',
    dag=dag,
)

Federal_Parquet = BashOperator(
    task_id='Federal_Parquet',
    bash_command='date',
    dag=dag,
)

Federal_Download.set_upstream(Federal_Reporter)
Federal_Persist.set_upstream(Federal_Download)
Federal_Parquet.set_upstream(Federal_Persist)

TFDIF_Model.set_upstream(Medline_Parquet)
TFDIF_Model.set_upstream(Pubmed_Parquet)
TFDIF_Model.set_upstream(Federal_Parquet)
