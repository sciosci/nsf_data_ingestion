import sys
import os
sys.path.append('/home/ananth/airflow/dags/')
import config
from config import *

default_args = {
    'owner':'nsf_data_ingestion',
    'depends_on_past':'False',
    'start_date': datetime.now(),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('nsf_data_ingestion', default_args = default_args, schedule_interval=timedelta(hours=3), catchup=False)

GitClone = PythonOperator(
         task_id = 'GitClone',
         python_callable = pull,
         dag = dag)
        
Download = PythonOperator(
         task_id = 'Download',
         python_callable = download_pubmed_data,
         op_kwargs={'directory_path_data': config.pub_directory_path_data},
         dag = dag)

Untar =  PythonOperator(
         task_id = 'Untar',
         python_callable = untar_file,
         op_kwargs={'directory_path_data': config.pub_directory_path_data, 'directory_untar_data': config.pub_directory_untar_data},
         dag = dag)
    
Chunk =  PythonOperator(
         task_id = 'Chunking',
         python_callable = chunk_data,
         op_kwargs={'directory_path_processed': config.pub_directory_path_processed, 'directory_path_data': config.pub_directory_path_data, 'chunk_size': config.pub_chunk_size},
         dag = dag)
    
Zip =     PythonOperator(
          task_id = 'Zipping',
          python_callable = zip_data,
          op_kwargs={'directory_path_data': config.pub_directory_path_data, 'directory_path_processed': config.pub_directory_path_processed, 'directory_path_compressed': config.pub_directory_path_compressed},
          dag = dag)
        
HDFS_Persist =  PythonOperator(
          task_id = 'HDFS-Persist',
          python_callable = put_files_in_hadoop,
          op_kwargs={'directory_path_compressed': config.pub_directory_path_compressed, 'hadoop_directory': config.pub_hadoop_directory},
          dag = dag)

Download.set_upstream(GitClone)
Untar.set_upstream(Download)
Chunk.set_upstream(Untar)
Zip.set_upstream(Chunk)
HDFS_Persist.set_upstream(Zip)
