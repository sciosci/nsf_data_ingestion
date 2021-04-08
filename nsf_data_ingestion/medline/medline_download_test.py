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
logging.basicConfig(filename='test_download.log')
import datetime
import reconnecting_ftp


def download_med_data():
    medline_ftp_server = 'ftp.ncbi.nlm.nih.gov'
    #medline_ftp_server = param_list.get('ftp_server')
    medline_directory_path_data = '/home/eileen/medline_data/'
    #medline_directory_path_data = param_list.get('directory_path')
    #medline_medleasebaseline = '/nlmdata/.medleasebaseline/gz/'
    #medline_medleasebaseline = param_list.get('medleasebaseline_url')
    timestamp_file = 'time_stamp.txt'
    #timestamp_file = param_list.get('timestamp_file')
    medline_medlease_urls = ['/pubmed/baseline','/pubmed/updatefiles']
    #medline_medlease_urls = param_list.get('medlease_url')

#     medline_ftp_server = param_list.get('ftp_server')
#     medline_directory_path_data = param_list.get('directory_path')
#     ftp_path = param_list.get('medleasebaseline_url')
#     timestamp_file = param_list.get('timestamp_file')
#     medline_medlease_urls = param_list.get('medline_medlease_urls')
    
    last_load = get_last_load(medline_directory_path_data, timestamp_file)
    
    if 1:
        logging.info('Old Data. Downloading Updated Data')
        rmtree(medline_directory_path_data)
        os.makedirs(medline_directory_path_data)
        
        for medline_url in medline_medlease_urls:
            with reconnecting_ftp.Client(hostname=medline_ftp_server,port=21,user="",password="") as ftp:
                ftp.cwd(medline_url)
                files = ftp.nlst()
                for file in files:
                    if file.endswith('.xml.gz'):
                        with open(medline_directory_path_data + file, 'wb') as f:
                            ftp.retrbinary("RETR " + file, f.write)
                        logging.info(str(datetime.datetime.now()) + ' || Downloading file - ' + file + ', from ' + medline_url + '. Pleae Wait.................')

            
            logging.info('Updating TimeStamp')
            f = open(medline_directory_path_data + "time_stamp.txt", "a")
            cur_time = calendar.timegm(time.gmtime())
            f.write(str(cur_time))
            f.close()

if __name__ == '__main__':
    download_med_data()
