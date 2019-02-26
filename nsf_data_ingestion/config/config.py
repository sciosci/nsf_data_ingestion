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

from medline_download import download_med_data
from medline_download import persist

libraries_list = ['/home/ananth/nsf_data_ingestion/libraries/pubmed_parser_lib.zip', '/home/ananth/nsf_data_ingestion/libraries/unidecode_lib.zip']
source_path = '/home/ananth/nsf_data_ingestion/'
spark_path = '/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/'

#PUBMED VARIABLES
pub_chunk_size = 500
pub_directory_path_data = '/home/ananth/airflow/nsf_data/'
pub_directory_untar_data = pub_directory_path_data + 'untar_data/'
pub_directory_path_processed = pub_directory_path_data + 'chunk_data'
pub_directory_path_compressed = pub_directory_path_data + 'compressed_pubmed_data'
pub_hadoop_directory = '/user/ananth/pub/'

medline_directory_path_data = '/home/ananth/airflow/medline_data/'
timestamp_file = 'time_stamp.txt'
medline_ftp_server = 'ftp.nlm.nih.gov'
medline_medleasebaseline = '/nlmdata/.medleasebaseline/gz/'
medline_medlease = '/nlmdata/.medlease/gz/'
medline_hdfs_path = '/user/ananth/medline/'
medline_xml_path = '/user/ananth/medline/data'
medline_parquet_path = '/user/ananth/medline/parquet/'







def pull():
    os.chdir(source_path)
    output = subprocess.check_output(["git", "pull", "origin", "airflow_model"])
    print(os.curdir)
