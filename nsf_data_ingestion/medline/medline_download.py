import sys
## sys.path.append('/home/ananth/nsf_data_ingestion/')
sys.path.append('/home/eileen/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_last_load
import os
from shutil import copyfile
from shutil import rmtree
import tarfile
import shutil
import zipfile
from ftplib import FTP
import logging
import calendar
import time
from subprocess import call
logging.getLogger().setLevel(logging.INFO)
import reconnecting_ftp

def download_med_data(param_list):
    medline_ftp_server = param_list.get('ftp_server')
    medline_directory_path_data = param_list.get('directory_path')
    timestamp_file = param_list.get('timestamp_file')
    medline_medlease_urls = param_list.get('medline_medlease_urls')

#     medline_ftp_server = param_list.get('ftp_server')
#     medline_directory_path_data = param_list.get('directory_path')
#     ftp_path = param_list.get('medleasebaseline_url')
#     timestamp_file = param_list.get('timestamp_file')
#     medline_medlease_urls = param_list.get('medline_medlease_urls')
    
    last_load = get_last_load(medline_directory_path_data, timestamp_file)
    
    if last_load >= 604800:
        logging.info('Old Data. Downloading Updated Data')
        rmtree(medline_directory_path_data)
        os.makedirs(medline_directory_path_data)
        
        for medline_url in medline_medlease_urls:
            with reconnecting_ftp.Client(hostname=medline_ftp_server,port=21,user="",password="") as ftp:
                ftp.cwd(medline_url)
                files = ftp.nlst()
                for file in files:
                    if file.endswith('.xml.gz'):
                        with open(medline_directory_path_data + file, 'wb') as localfile:
                            ftp.retrbinary("RETR " + file, localfile.write)
                        logging.info('Downloading file - ' + file + ', from ' + medline_url + '. Pleae Wait.................')

            
            logging.info('Updating TimeStamp')
            f = open(medline_directory_path_data + "time_stamp.txt", "a")
            cur_time = calendar.timegm(time.gmtime())
            f.write(str(cur_time))
            f.close()
        
def persist(param_list):
    medline_hdfs_path = param_list.get('hdfs_path')
    medline_directory_path_data = param_list.get('directory_path')
    logging.info('Persisting data to HDFS')
    if not call(["hdfs", "dfs", "-test", "-d", medline_hdfs_path]):
        call(["hdfs", "dfs", "-rm", "-r", "-f", medline_hdfs_path])

    call(["hdfs", "dfs", "-mkdir", medline_hdfs_path])
    call(["hdfs", "dfs", "-put", medline_directory_path_data, medline_hdfs_path])
    logging.info('Files Persisted to - %s', medline_hdfs_path)

def download(data_source_name):
    download_med_data(data_source_params.mapping.get(data_source_name))
                                                     
def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))
