# Script to download the Data from Pubmed Open Access
import urllib
import os
import tarfile,sys
from shutil import copyfile
from shutil import rmtree
import sys
import tarfile
import shutil

# Method to download Pubmed Open Access Data
# The files are stored in directory passed as parameter
# by command line arguement to variable 'directory_path_data'
def download_pubmed_data():
    directory_path_data = sys.argv[1:][0]
    urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.A-B.xml.tar.gz", filename=directory_path_data+"comm_use.A-B.xml.tar.gz")
    urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.C-H.xml.tar.gz", filename=directory_path_data+"comm_use.C-H.xml.tar.gz")
    #urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.I-N.xml.tar.gz", filename=directory_path_data+"comm_use.I-N.xml.tar.gz")
    #urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.O-Z.xml.tar.gz", filename=directory_path_data+"comm_use.O-Z.xml.tar.gz")
    #urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.A-B.xml.tar.gz", filename=directory_path_data+"non_comm_use.A-B.xml.tar.gz")
    #urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.C-H.xml.tar.gz", filename=directory_path_data+"non_comm_use.C-H.xml.tar.gz")
    #urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.I-N.xml.tar.gz", filename=directory_path_data+"non_comm_use.I-N.xml.tar.gz")
    #urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.O-Z.xml.tar.gz", filename=directory_path_data+"non_comm_use.O-Z.xml.tar.gz")
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

    with open('location.yaml', 'r') as file:
        location = yaml.load(file)

    directory_path_processed = directory_path_data + '/chunk_data'
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

download_pubmed_data()
