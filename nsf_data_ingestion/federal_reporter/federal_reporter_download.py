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
sys.path.append('/home/eileen/nsf_data_ingestion/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_last_load

#  FEDERAL DOWNLOAD FUNCTIONS
#####################################################################################################################################################################
def download_fed_data(param_list):
    logging.info("the list of parameters are ", param_list)
#     directory_path_data = param_list.get('directory_path')
#     timestamp_file = param_list.get('timestamp_file')
    directory_path_data ='/home/eileen/federal_data/'
    timestamp_file = 'time_stamp.txt'
    last_load = get_last_load(directory_path_data, timestamp_file)
#     if last_load >= 604800:
    if last_load >= 8400000:
        if os.path.exists(directory_path_data):
            rmtree(directory_path_data)

        os.makedirs(directory_path_data)
        for i in range(2004, 2017):
            logging.info('Downloading Fed Data.......')
            os.system(
                'wget ' + param_list.get('FedRePORTER_PRJ_url') +str(i)+ '.zip -nv -P ' + directory_path_data)
            os.system(
                'wget ' + param_list.get('FedRePORTER_PRJABS_url') +str(i)+ '.zip -nv -P ' + directory_path_data)
        logging.info('Downloading Complete.........')
        
        logging.info('Updating TimeStamp........')
        f = open(directory_path_data + "time_stamp.txt", "a")
        cur_time = calendar.timegm(time.gmtime())
        f.write(str(cur_time))
        f.close()
        logging.info('Download Complete........')
        
    else:
        logging.info('Data Intact......!!!!!')
        

def persist1(param_list):
    logging.info("the list of parameters are ", param_list)
#     data_path = param_list.get('directory_path')
#     hdfs_path = param_list.get('hdfs_path')
    data_path ='/home/eileen/federal_data/'
    hdfs_path = '/user/eileen/federal/xml/'
    logging.info("data path sghosh " , data_path)
    filelist = glob.glob(os.path.join(data_path, "*.xml"))
#     for f in filelist:
#         os.remove(f)
    logging.info('Persisting data to HDFS', call(["hdfs", "dfs", "-test", "-d", data_path]))
    logging.info('the data path is persists : ',data_path)
#     os.system('jar -cvf' + data_path + '"*FedRePORTER_PRJ_X_FY*.zip" > '+ data_path+ 'projects.xml')
# #     os.system('unzip -p ' + data_path + '"*FedRePORTER_PRJABS_X_FY*.zip" >'+ data_path+'abstracts.xml')
# #     logging.info('Persisting data to HDFS')
# #     if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
# #         call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])
        
# #     call(["hdfs", "dfs", "-mkdir", hdfs_path])
    logging.info('Persisting FedRePORTER_PRJ..............')
    call('unzip -p "'+data_path+'*FedRePORTER_PRJ_X_FY*.zip"'+' | hdfs dfs -put - '+data_path+'projects.xml', shell = True)
    logging.info('Persisting FedRePORTER_PRJABS..............')
    call('unzip -p "'+data_path+'*FedRePORTER_PRJABS_X_FY*.zip"'+' | hdfs dfs -put - '+data_path+'abstracts.xml', shell = True)
    logging.info('Data Persisted..............')
    logging.info('Persisting data to HDFS')
    if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
        call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])
        
    call(["hdfs", "dfs", "-mkdir", hdfs_path])
    logging.info('Persisting FedRePORTER_PRJ..............')
    call(["hdfs", "dfs", "-put", data_path+'projects.xml', hdfs_path])
    
    logging.info('Persisting FedRePORTER_PRJABS..............')
    call(["hdfs", "dfs", "-put", data_path+'abstracts.xml', hdfs_path])
    logging.info('Data Persisted..............')
    
    
def persist(param_list):
    data_path = '/home/eileen/federal_data/'
    grants_gov_xml_path = '/user/eileen/federal/xml/'
    logging.info('Persisting data to HDFS', call(["hdfs", "dfs", "-test", "-d", grants_gov_xml_path]))
        
    if not call(["hdfs", "dfs", "-test", "-d", grants_gov_xml_path]):
         call(["hdfs", "dfs", "-rm","-r","-f", grants_gov_xml_path])
    call(["hdfs", "dfs", "-mkdir", grants_gov_xml_path])
    call('unzip -p "'+data_path+'DownloadFile?fileToDownload=FedRePORTER_PRJ_X*.zip"'+' | hdfs dfs -put -   '+grants_gov_xml_path+'projects.xml', shell = True)
    call('unzip -p "'+data_path+'DownloadFile?fileToDownload=FedRePORTER_PRJABS_X_FY*.zip"'+' | hdfs dfs -put -   '+grants_gov_xml_path+'abstracts.xml', shell = True)


def download(data_source_name):
    download_fed_data(data_source_params.mapping.get(data_source_name))

def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))