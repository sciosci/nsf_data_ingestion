import os
import subprocess
import sys
# sys.path.append('/home/ananth/nsf_data_ingestion/')
sys.path.append('/home/eileen/nsf_data_ingestion/nsf_data_ingestion/')
## from nsf_data_ingestion.config import nsf_config
# from nsf_data_ingestion.objects import data_source_params
from shutil import copyfile
from shutil import rmtree
import tarfile
import shutil
import zipfile
from ftplib import FTP
import logging
import calendar
import time
logging.getLogger().setLevel(logging.INFO)


def get_archive_file_list():
    list1 = ['comm_use.A-B.xml.tar.gz','comm_use.C-H.xml.tar.gz','comm_use.I-N.xml.tar.gz','comm_use.O-     Z.xml.tar.gz','non_comm_use.A-B.xml.tar.gz','non_comm_use.C-H.xml.tar.gz','non_comm_use.I-N.xml.tar.gz','non_comm_use.O-Z.xml.tar.gz']
    list = ['non_comm_use.O-Z.xml.tar.gz']
    return list



def pull():
#     os.chdir(nsf_config.source_path)
    os.chdir("/home/eileen/nsf_data_ingestion/nsf_data_ingestion")
    output = subprocess.check_output(["git", "pull", "origin", "origin/feature/nsf_grants_merged"])
    logging.info('Pulled Out Branch.....')
    print(os.curdir)


def get_last_load(directory_path_data, timestamp_file):
    if os.path.exists(directory_path_data):
        logging.info('Directory Path Exists')
        if os.path.exists(directory_path_data + timestamp_file):
            logging.info('Timestamp Exists')
            f = open(directory_path_data + timestamp_file, "r")
            old_time_stamp = int(f.read())
            current_time_stamp = calendar.timegm(time.gmtime())
            f.close()
            timediff = current_time_stamp - old_time_stamp
            return timediff
        else:
            logging.info('No Timestamp....Creating New TimeStamp')
            f = open(directory_path_data + timestamp_file, "w")
            cur_time = calendar.timegm(time.gmtime())
            one_day = cur_time - 86400
            f.write(str(one_day))
            f.close()
            return one_day
    else:
        logging.info('Directory Path Doesnot Exist....Creating New')
        os.makedirs(directory_path_data)
        get_last_load(directory_path_data, timestamp_file)
        
if __name__ == "__main__":
    pull()
