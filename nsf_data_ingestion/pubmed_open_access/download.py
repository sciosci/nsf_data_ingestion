# Script to download the Data from Pubmed Open Access
import urllib
import os
import tarfile,sys
from shutil import copyfile
from shutil import rmtree
import yaml
import sys

#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.A-B.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.A-B.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.C-H.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.C-H.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.I-N.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.I-N.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/comm_use.O-Z.xml.tar.gz", filename="/users/kanagre/pubmed_data/comm_use.O-Z.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.A-B.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.A-B.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.C-H.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.C-H.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.I-N.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.I-N.xml.tar.gz")
#urllib.urlretrieve("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/non_comm_use.O-Z.xml.tar.gz", filename="/users/kanagre/pubmed_data/non_comm_use.O-Z.xml.tar.gz")
def chunk_data():

    count = 0
    ncount = 0

    with open('location.yaml', 'r') as file:
        location = yaml.load(file)

    #directory_path_data = location["path"]["input_data"]
    directory_path_data = sys.argv[1:][0]
    #directory_path_processed = location["path"]["chunk_data"]
    directory_path_processed = sys.argv[1:][1]
    chunk_size = int(sys.argv[1:][2])

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
                        os.makedirs(new_directory_path)
                else:
                    copyfile(subdir + '/' +file, new_directory_path + '/' + file)
                    count += 1

chunk_data()
