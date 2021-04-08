import sys
from shutil import rmtree
import os
from shutil import copyfile
from shutil import rmtree
import tarfile
import shutil
import zipfile
from ftplib import FTP
import glob, os, os.path
import logging
import calendar
import time
from subprocess import call
sys.path.append('/home/eileen/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_last_load

def download_fed_data(param_list):
    logging.info("the list of parameters are ", param_list)
    federal_directory_path_data = param_list.get('directory_path')
    timestamp_file = param_list.get('timestamp_file')
    directory_path_data ='/home/eileen/federal_data/'
    timestamp_file = 'time_stamp.txt'
    last_load = get_last_load(federal_directory_path_data, timestamp_file)
#     if last_load >= 604800:
#     if last_load >= 8400000:
#     if os.path.exists(federal_directory_path_data):
#         rmtree(federal_directory_path_data)

#     os.makedirs(federal_directory_path_data)
    for i in range(2004, 2021):
        logging.info('Downloading Fed Data.......')
        os.system('wget ' + 'https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJ_X_FY' +str(i)+ '.zip -nv -P ' + federal_directory_path_data)
        os.system('wget ' + 'https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJABS_X_FY' +str(i)+ '.zip -nv -P ' + federal_directory_path_data)
        logging.info('Downloading Complete.........')
        logging.info('Updating TimeStamp........')
        f = open(federal_directory_path_data + "time_stamp.txt", "a")
        cur_time = calendar.timegm(time.gmtime())
        f.write(str(cur_time))
        f.close()
        logging.info('Download Complete........')
        
#     else:
#         logging.info('Data Intact......!!!!!')
        

def persist1(param_list):
    logging.info("the list of parameters are ", param_list)
    federal_directory_path_data = param_list.get('directory_path')
    federal_xml_path= param_list.get('xml_path')
    logging.info("data path eileen " , federal_directory_path_data)
    filelist = glob.glob(os.path.join(federal_directory_path_data, "*.xml"))
#     for f in filelist:
#         os.remove(f)
    logging.info('Persisting data to HDFS', call(["hdfs", "dfs", "-test", "-d", federal_directory_path_data]))
    logging.info('the data path is persists : ',federal_directory_path_data)
    logging.info('Persisting FedRePORTER_PRJ..............')
    call('unzip -p "'+federal_directory_path_data+'*FedRePORTER_PRJ_X_FY*.zip"'+' | hdfs dfs -put - '+federal_directory_path_data+'projects.xml', shell = True)
    logging.info('Persisting FedRePORTER_PRJABS..............')
    call('unzip -p "'+federal_directory_path_data+'*FedRePORTER_PRJABS_X_FY*.zip"'+' | hdfs dfs -put - '+federal_directory_path_data+'abstracts.xml', shell = True)
    logging.info('Data Persisted..............')
    logging.info('Persisting data to HDFS')
    if not call(["hdfs", "dfs", "-test", "-d", federal_xml_path]):
        call(["hdfs", "dfs", "-rm", "-r", "-f", federal_xml_path])
        
    call(["hdfs", "dfs", "-mkdir", federal_xml_path])
    logging.info('Persisting FedRePORTER_PRJ..............')
    call(["hdfs", "dfs", "-put", federal_directory_path_data+'projects.xml', federal_xml_path])
    
    logging.info('Persisting FedRePORTER_PRJABS..............')
    call(["hdfs", "dfs", "-put", federal_directory_path_data+'abstracts.xml', federal_xml_path])
    logging.info('Data Persisted..............')
    
    
def persist(param_list):
    federal_directory_path_data = param_list.get('directory_path')
    federal_xml_path= param_list.get('xml_path')
    logging.info('Persisting data to HDFS', call(["hdfs", "dfs", "-test", "-d", federal_xml_path]))
        
    if not call(["hdfs", "dfs", "-test", "-d", federal_xml_path]):
         call(["hdfs", "dfs", "-rm","-r","-f", federal_xml_path])
    call(["hdfs", "dfs", "-mkdir", federal_xml_path])
    call('unzip -p "'+federal_directory_path_data+'DownloadFile?fileToDownload=FedRePORTER_PRJ_X*.zip"'+' | hdfs dfs -put -   '+federal_xml_path+'projects.xml', shell = True)
    call('unzip -p "'+federal_directory_path_data+'DownloadFile?fileToDownload=FedRePORTER_PRJABS_X_FY*.zip"'+' | hdfs dfs -put -   '+federal_xml_path+'abstracts.xml', shell = True)


def download(data_source_name):
    download_fed_data(data_source_params.mapping.get(data_source_name))

def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))