import sys
sys.path.append('/home/ananth/airflow/dags/')
import config
from config import *

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
         python_callable = download_med_data,
         op_kwargs={'ftp_path': config.medline_medleasebaseline,
			'medline_ftp_server': config.medline_ftp_server,
			'medline_directory_path_data': config.medline_directory_path_data,
			'timestamp_file': config.timestamp_file},
        dag = dag)

Download_Medlease =  PythonOperator(
         task_id = 'Download-Medlease',
         python_callable = download_med_data,
         op_kwargs={'ftp_path': config.medline_medlease,
			'medline_ftp_server': config.medline_ftp_server, 
			'medline_directory_path_data': config.medline_directory_path_data,
			'timestamp_file': config.timestamp_file},
         dag = dag)
        
HDFS_Persist =  PythonOperator(
          task_id = 'HDFS-Persist',
          python_callable = persist,
          op_kwargs={'hdfs_path': config.medline_hdfs_path, 'directory_path_data': config.medline_directory_path_data},
          dag = dag)

#Spark_Process = PythonOperator(
#          task_id = 'Spark-Process',
#          python_callable = spark_session_process,
#          op_kwargs={'xml_path': xml_path, 'parquet_path': parquet_path},
#          dag = dag)

Download_Medleasebaseline.set_upstream(GitClone)
Download_Medlease.set_upstream(Download_Medleasebaseline)
HDFS_Persist.set_upstream(Download_Medlease)
#Spark_Process.set_upstream(HDFS_Persist)
