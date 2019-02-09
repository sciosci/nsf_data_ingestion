from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import sys
from datetime import datetime, timedelta
import subprocess
from subprocess import call
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/medline')
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/pubmed_open_access')
from nsf_download import download_pubmed_data
from nsf_download import untar_file
from nsf_download import chunk_data
from nsf_download import zip_data
from nsf_download import put_files_in_hadoop

chunk_size = 500
directory_path_data = '/home/ananth/airflow/data/'
directory_untar_data = directory_path_data + 'untar_data/'
directory_path_processed = directory_path_data + 'chunk_data'
directory_path_compressed = directory_path_data + 'compressed_pubmed_data'


default_args = {
    'owner':'nsf_data_ingestion',
    'depends_on_past':'False',
    'start_date': datetime(2019,1,15),
    #'retries':0,
    'schedule_interva': '*/5 * * * *',
    'dagrun_timeout': timedelta(seconds=5),
    #'retry_delay':timedelta(minutes=5),
}

dag = DAG('nsf_data_ingestion', default_args = default_args, schedule_interval=timedelta(1))

def pull():
    os.chdir('/home/ananth/nsf_data_ingestion/')
    output = subprocess.check_output(["git", "pull", "origin", "airflow_model"])
    print(os.curdir)

GitClone = PythonOperator(
         task_id = 'GitClone',
         python_callable = pull,
         dag = dag)
        
Download = PythonOperator(
         task_id = 'Download',
         python_callable = download_pubmed_data,
         op_kwargs={'directory_path_data': directory_path_data},
         dag = dag)

Untar =  PythonOperator(
         task_id = 'Untar',
         python_callable = untar_file,
         op_kwargs={'directory_path_data': directory_path_data, 'directory_untar_data': directory_untar_data},
         dag = dag)
    
Chunk =  PythonOperator(
         task_id = 'Chunking',
         python_callable = chunk_data,
         op_kwargs={'directory_path_processed': directory_path_processed, 'directory_path_data': directory_path_data, 'chunk_size':   chunk_size},
         dag = dag)
    
Zip =     PythonOperator(
          task_id = 'Zipping',
          python_callable = zip_data,
          op_kwargs={'directory_path_data': directory_path_data, 'directory_path_processed': directory_path_processed},
          dag = dag)
        
HDFS_Persist =  PythonOperator(
          task_id = 'HDFS-Persist',
          python_callable = put_files_in_hadoop,
          op_kwargs={'directory_path_compressed': directory_path_compressed},
          dag = dag)

Download.set_upstream(GitClone)
Untar.set_upstream(Download)
Chunk.set_upstream(Untar)
Zip.set_upstream(Chunk)
HDFS_Persist.set_upstream(Zip)
