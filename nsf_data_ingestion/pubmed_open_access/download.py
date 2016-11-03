# Script to download the Data from Pubmed Open Access
import urllib
import os
import tarfile,sys
from shutil import copyfile
from shutil import rmtree

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

    directory_path_data = '/Users/kartik/Documents/NSF_GRANT_PROJECT/bulk_data/'
    directory_path = '/Users/kartik/Documents/NSF_GRANT_PROJECT/processed_data/'
    #rmtree(directory_path)
    #sh.rm(sh.glob('/Users/kartik/Documents/NSF_GRANT_PROJECT/processed_data/*'))

    new_directory_path = directory_path + '/' + str(count)
    if not os.path.exists(new_directory_path):
        os.makedirs(new_directory_path)
    count = count + 1

    for subdir, dirs, files in os.walk(directory_path_data):
        for file in files:
            if file.endswith('.nxml'):
                if count%2000 == 0:
                    print ("Multiple")
                    ncount  = ncount + 1
                    count = count + 1
                    new_directory_path = directory_path + '/' + str(ncount)
                    print ncount
                    if not os.path.exists(new_directory_path):
                        os.makedirs(new_directory_path)
                else:
                    copyfile(subdir + '/' +file, new_directory_path + '/' + file)
                    print("Not multiple")
                    count = count + 1
                    print count

chunk_data()
