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

# Method to download Pubmed Open Access Data
# The files are stored in directory passed as parameter
# by command line arguement to variable 'directory_path_data'
def download_pubmed_data():
    directory_path_data = sys.argv[1:][0]

    ftp = FTP('ftp.ncbi.nlm.nih.gov')
    ftp.login(user='', passwd = '')
    ftp.cwd("/pub/pmc/oa_bulk/")
    archive_file_list = get_archive_file_list()
    for i, val in enumerate(archive_file_list):
        localfile = open(directory_path_data+val, 'wb')
        ftp.retrbinary("RETR " + val ,localfile.write)
    ftp.quit()
    localfile1.close()
    untar_file(directory_path_data)

# untar .tar.gz files from Pubmed Open Acccess
def untar_file(directory_path_data):

    directory_untar_data = directory_path_data + 'untar_data/'
    if os.path.exists(directory_untar_data):
        rmtree(directory_untar_data)
    os.makedirs(directory_untar_data)

    for file in os.listdir(directory_path_data):
        if file.endswith('.xml.tar.gz'):
            tar = tarfile.open(directory_path_data+file)
            tar.extractall(directory_untar_data)
            tar.close()

    chunk_data(directory_path_data, directory_untar_data)

# Method to chunk .xml files in directory of count
# passed as command line arguement to variable 'chunk_size'
def chunk_data(directory_path_data, directory_untar_data):

    count = 0
    ncount = 0

    directory_path_processed = directory_path_data + 'chunk_data'
    if os.path.exists(directory_path_processed):
        rmtree(directory_path_processed)
    os.makedirs(directory_path_processed)

    chunk_size = int(sys.argv[1:][1])

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
                        os.makedirs(new_directory_path)
                else:
                    copyfile(subdir + '/' +file, new_directory_path + '/' + file)
                    count += 1
    zip_data(directory_path_processed)

# Method to zip all the folders in the chunk data directory
def zip_data(directory_path_processed):
    print (directory_path_processed)
    for folder in os.listdir(directory_path_processed):
        zipf = zipfile.ZipFile('{0}.zip'.format(os.path.join(directory_path_processed, folder)), 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(os.path.join(directory_path_processed, folder)):
            for filename in files:
                zipf.write(os.path.abspath(os.path.join(root, filename)), arcname=filename)
        zipf.close()

def get_archive_file_list():
    list = ['comm_use.A-B.xml.tar.gz','comm_use.C-H.xml.tar.gz','comm_use.I-N.xml.tar.gz','comm_use.O-Z.xml.tar.gz','non_comm_use.A-B.xml.tar.gz','non_comm_use.C-H.xml.tar.gz','non_comm_use.I-N.xml.tar.gz','non_comm_use.O-Z.xml.tar.gz']
    #list = ['comm_use.A-B.xml.tar.gz']
    return list

download_pubmed_data()
