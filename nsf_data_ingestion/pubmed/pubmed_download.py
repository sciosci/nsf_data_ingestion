import sys
sys.path.append('/home/ananth/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_archive_file_list
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

def download_pubmed_data(param_list):
    directory_path_data = param_list.get('download_path')
    timestamp_file = param_list.get('timestamp_file')
    
    last_load = get_last_load(directory_path_data, timestamp_file)
    
    if last_load >= 86400:
        logging.info('Old Data. Downloading Updated Data')
        if os.path.exists(directory_path_data):
            rmtree(directory_path_data)

        os.makedirs(directory_path_data)
        ftp = FTP('ftp.ncbi.nlm.nih.gov')
        ftp.login(user='', passwd = '')
        ftp.cwd("/pub/pmc/oa_bulk/")
        archive_file_list = get_archive_file_list()
        print(archive_file_list)
        for i, val in enumerate(archive_file_list):
            localfile = open(directory_path_data+val, 'wb')
            ftp.retrbinary("RETR " + val ,localfile.write)
            logging.info('Downloading Pleae Wait.................')
        ftp.quit()
        localfile.close()
        logging.info('Download Complete..........................')
        
        logging.info('Updating TimeStamp........')
        f = open(directory_path_data + "time_stamp.txt", "a")
        cur_time = calendar.timegm(time.gmtime())
        f.write(str(cur_time))
        f.close()

    else:
        logging.info('Data Intact......!!!!!')
    
# untar .tar.gz files from Pubmed Open Acccess
def untar_file(param_list):

        directory_path_data = param_list.get('download_path')
        directory_untar_data = param_list.get('unzip_path')

        logging.info('Untarring please wait.................')
        if os.path.exists(directory_untar_data):
            rmtree(directory_untar_data)
        os.makedirs(directory_untar_data)

        for file in os.listdir(directory_path_data):
            if file.endswith('.xml.tar.gz'):
                tar = tarfile.open(directory_path_data+file)
                tar.extractall(directory_untar_data)
                tar.close()

        logging.info('Untar Completed................')

    #chunk_data(directory_path_data, directory_untar_data)

# Method to chunk .xml files in directory of count
# passed as command line arguement to variable 'chunk_size'


def chunk_data(param_list):

    directory_path_processed = param_list.get('chunked_path')
    directory_path_data = param_list.get('download_path')
    chunk_size = param_list.get('chunk_size')
    count = 0
    ncount = 0

    if os.path.exists(directory_path_processed):
        rmtree(directory_path_processed)
    os.makedirs(directory_path_processed)

    new_directory_path = directory_path_processed + '/' + str(count)
    if not os.path.exists(new_directory_path):
        os.makedirs(new_directory_path)
    count = count + 1

    for subdir, dirs, files in os.walk(directory_path_data):
        for file in files:
            if file.endswith('.nxml'):
                if count%chunk_size == 0:
                    ncount  += 1
                    count += 1
                    new_directory_path = directory_path_processed + '/' + str(ncount)
                    if not os.path.exists(new_directory_path):
                        logging.info('Creating Chunk  %s ................', ncount)
                        os.makedirs(new_directory_path)
                else:
                    copyfile(subdir + '/' +file, new_directory_path + '/' + file)
                    count += 1
    logging.info('Chunking Completed..........................')
    #zip_data(directory_path_data, directory_path_processed)

# Method to zip all the folders in the chunk data directory


def zip_data(param_list):

    directory_path_processed = param_list.get('chunked_path')
    directory_path_compressed = param_list.get('directory_path')

    print (directory_path_processed)
    logging.info('zipping files in directory - %s', directory_path_processed)

    for folder in os.listdir(directory_path_processed):
        zipf = zipfile.ZipFile('{0}.zip'.format(os.path.join(directory_path_processed, folder)), 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(os.path.join(directory_path_processed, folder)):
            for filename in files:
                zipf.write(os.path.abspath(os.path.join(root, filename)), arcname=filename)
        zipf.close()

    logging.info('Compressing zipped files to folder - %s', directory_path_compressed)
    if os.path.exists(directory_path_compressed):
        rmtree(directory_path_compressed)
    os.makedirs(directory_path_compressed)

    for subdir, dirs, files in os.walk(directory_path_processed):
        for file in files:
            if file.endswith('.zip'):
                shutil.move(subdir + '/' + file, directory_path_compressed + '/' + file)

    logging.info('Data is zipped....................')


        
def persist(param_list):

    hdfs_path = param_list.get('hdfs_path')
    directory_path = param_list.get('directory_path')
    logging.info('Persisting data to HDFS')
    if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
        call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])

    call(["hdfs", "dfs", "-mkdir", hdfs_path])
    call(["hdfs", "dfs", "-put", directory_path, hdfs_path])
    logging.info('Files Persisted to - %s', hdfs_path)


def download(data_source_name):
    download_pubmed_data(data_source_params.mapping.get(data_source_name))
    
def untar(data_source_name):
    untar_file(data_source_params.mapping.get(data_source_name))

def chunking(data_source_name):
    chunk_data(data_source_params.mapping.get(data_source_name))

def zipping(data_source_name):
    zip_data(data_source_params.mapping.get(data_source_name))
                 
def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))
