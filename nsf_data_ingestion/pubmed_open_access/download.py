# Script to download the Data from Pubmed Open Access
import os
import tarfile,sys
from shutil import copyfile
from shutil import rmtree
import sys
import tarfile
import shutil
import zipfile
from ftplib import FTP
from subprocess import call
import logging

# Method to download Pubmed Open Access Data
# The files are stored in directory passed as parameter
# by command line arguement to variable 'directory_path_data'
def download_pubmed_data(directory_path_data):
    
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
    #untar_file(directory_path_data)

# untar .tar.gz files from Pubmed Open Acccess
def untar_file(directory_path_data, directory_untar_data):
        
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
def chunk_data(directory_path_processed, directory_path_data, chunk_size):

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
def zip_data(directory_path_data, directory_path_processed):
                 
    
    print (directory_path_processed)
    logging.info('zipping files in directory - %s', directory_path_processed)
                 
    for folder in os.listdir(directory_path_processed):
        zipf = zipfile.ZipFile('{0}.zip'.format(os.path.join(directory_path_processed, folder)), 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(os.path.join(directory_path_processed, folder)):
            for filename in files:
                zipf.write(os.path.abspath(os.path.join(root, filename)), arcname=filename)
        zipf.close()

    directory_path_compressed = directory_path_data + 'compressed_pubmed_data'
    
    logging.info('Compressing zipped files to folder - %s', directory_path_compressed)
    if os.path.exists(directory_path_compressed):
        rmtree(directory_path_compressed)
    os.makedirs(directory_path_compressed)

    for subdir, dirs, files in os.walk(directory_path_processed):
        for file in files:
            if file.endswith('.zip'):
                shutil.move(subdir + '/' + file, directory_path_compressed + '/' + file)

    logging.info('Data is zipped....................')
    #put_files_in_hadoop(directory_path_compressed)

def put_files_in_hadoop(directory_path_compressed):
    
    logging.info('Persisting data to HDFS')
    
    hadoop_directory = '/user/kanagre/'
    call(["hdfs","dfs", "-rm", "-r", "-f",  hadoop_directory+'compressed_pubmed_data'])
    call(["hdfs","dfs", "-put", directory_path_compressed,hadoop_directory])
    
    logging.info('Files Persisted to - %s', hadoop_directory)


def get_archive_file_list():
    list1 = ['comm_use.A-B.xml.tar.gz','comm_use.C-H.xml.tar.gz','comm_use.I-N.xml.tar.gz','comm_use.O-     Z.xml.tar.gz','non_comm_use.A-B.xml.tar.gz','non_comm_use.C-H.xml.tar.gz','non_comm_use.I-N.xml.tar.gz','non_comm_use.O-Z.xml.tar.gz']
    list = ['non_comm_use.O-Z.xml.tar.gz']
    return list
