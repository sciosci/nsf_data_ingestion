import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
sys.path.append('PycharmProjects/nsf_data_ingestion')
from nsf_data_ingestion.main.utils.utils_functions import pull
from nsf_data_ingestion.main.download.dowload_data import download
from nsf_data_ingestion.main.download.dowload_data import untar
from nsf_data_ingestion.main.download.dowload_data import chunking
from nsf_data_ingestion.main.download.dowload_data import zipping
from nsf_data_ingestion.main.persist_hdfs.persist_data import persist_hdfs

default_args = {
    'owner':'nsf_data_ingestion',
    'depends_on_past':'False',
    'start_date': datetime.now(),
}

dag = DAG('medline_data_ingestion', default_args = default_args, schedule_interval=timedelta(hours=3), catchup=False)


GitClone = PythonOperator(
         task_id = 'GitClone',
         python_callable = pull,
         dag = dag)

        
Download_Medleasebaseline = PythonOperator(
         task_id = 'Download-Medleasebaseline',
         python_callable = download,
         op_kwargs={'data_source_name': 'medline'},
         dag = dag)

Download_Medlease =  PythonOperator(
         task_id = 'Download-Medlease',
         python_callable=download,
         op_kwargs={'data_source_name': 'medline'},
         dag = dag)
        
HDFS_Persist =  PythonOperator(
          task_id = 'HDFS-Persist',
          python_callable = persist_hdfs,
          op_kwargs={'data_source_name': 'medline'},
          dag = dag)

Download_Medleasebaseline.set_upstream(GitClone)
Download_Medlease.set_upstream(Download_Medleasebaseline)
HDFS_Persist.set_upstream(Download_Medlease)
