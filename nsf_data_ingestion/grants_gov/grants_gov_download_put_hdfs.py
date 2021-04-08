import datetime
import sys
from shutil import rmtree
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
logging.getLogger().setLevel(logging.INFO)


def download_grants_data():
    print("started")
    directory_path_data = '/home/eileen/grants_gov/'
    hdfs_path = '/user/eileen/grants/'
    loopstart=1
    today = datetime.date.today()
    daystart = int(today.strftime("%d"))
    dayend = daystart - 6
    while(loopstart <=7):
        loopstart = loopstart+1
        daystr = today.strftime("%Y%m%d")
        logging.info("Processing file for the date : ", daystr)
        filenamezip = "GrantsDBExtract" + daystr + "v2.zip"
        print(str(os.path.exists(directory_path_data+filenamezip)))
        if(os.path.exists(directory_path_data+filenamezip)):
            print("file persists")
            os.remove(directory_path_data+filenamezip)        
        url = "https://www.grants.gov/web/grants/xml-extract.html?download=" + filenamezip
        os.system('wget ' + url + ' -O ' + directory_path_data + filenamezip + ' -nv')
        print("Successfully extracted file from grants gov url ",filenamezip)
        today = today - datetime.timedelta(days=1)
        daystart = int(today.strftime("%d"))

def hdfs_put_grants_data():
    directory_path_data = '/home/eileen/grants_gov/'
    grants_gov_xml_path = '/user/eileen/grants/data/'
    logging.info('Persisting data to HDFS', call(["hdfs", "dfs", "-test", "-d", grants_gov_xml_path]))
        
    if not call(["hdfs", "dfs", "-test", "-d", grants_gov_xml_path]):
         call(["hdfs", "dfs", "-rm","-r","-f", grants_gov_xml_path])
    call(["hdfs", "dfs", "-mkdir", grants_gov_xml_path])
    call('unzip -p "'+directory_path_data+'Grants*.zip"'+' | hdfs dfs -put - '+grants_gov_xml_path+'grants.xml', shell = True)
    logging.info(directory_path_data)