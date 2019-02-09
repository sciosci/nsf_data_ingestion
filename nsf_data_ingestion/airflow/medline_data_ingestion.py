from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import sys
from datetime import datetime, timedelta
import subprocess
from subprocess import call
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/pubmed_open_access')
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/medline')
from download_medline import download_pub_data
from download_medline import persist
from process_medline_xml import spark_session_process

medleasebaseline = '/nlmdata/.medleasebaseline/gz/'
medlease = '/nlmdata/.medlease/gz/'
hdfs_path = '/user/ananth/pub'
directory_path_data = '/home/ananth/data/'
xml_path = '/user/ananth/pub/data'
parquet_path = '/user/ananth/parquet/'


default_args = {
    'owner':'medline_data_ingestion',
    'depends_on_past':'False',
    'start_date': datetime(2019,2,8),
    #'retries':0,
    'schedule_interva': '@daily',
    'dagrun_timeout': timedelta(minutes=50),
    'retry_delay':timedelta(minutes=10),
}

dag = DAG('medline_data_ingestion', default_args = default_args, schedule_interval='*/1 * * * *')

def pull():
    os.chdir('/home/ananth/nsf_data_ingestion/')
    output = subprocess.check_output(["git", "pull", "origin", "airflow_model"])
    print(os.curdir)

GitClone = PythonOperator(
         task_id = 'GitClone',
         python_callable = pull,
         dag = dag)
        
Download_Medleasebaseline = PythonOperator(
         task_id = 'Download-Medleasebaseline',
         python_callable = download_pub_data,
         op_kwargs={'ftp_path': medleasebaseline},
         dag = dag)

Download_Medlease =  PythonOperator(
         task_id = 'Download-Medlease',
         python_callable = download_pub_data,
         op_kwargs={'ftp_path': medlease},
         dag = dag)
        
HDFS_Persist =  PythonOperator(
          task_id = 'HDFS-Persist',
          python_callable = persist,
          op_kwargs={'hdfs_path': hdfs_path, 'directory_path_data': directory_path_data},
          dag = dag)

Spark_Process = PythonOperator(
          task_id = 'Spark-Process',
          python_callable = spark_session_process,
          op_kwargs={'xml_path': xml_path, 'parquet_path': parquet_path},
          dag = dag)

Download_Medleasebaseline.set_upstream(GitClone)
Download_Medlease.set_upstream(Download_Medleasebaseline)
HDFS_Persist.set_upstream(Download_Medlease)
Spark_Process.set_upstream(HDFS_Persist)
