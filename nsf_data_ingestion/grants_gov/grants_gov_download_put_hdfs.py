import datetime

import findspark
import io, os, time
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
from pyspark.sql import SparkSession

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
sys.path.append('/home/sghosh08/nsf_new/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_last_load

def download_grants_data():
    
    #directory_path_data = param_list.get('directory_path')
    directory_path_data = '/home/sghosh08/grants/'
    hdfs_path = '/user/sghosh08/grants/data/raw/'
    print(directory_path_data)
   
    
    today = datetime.date.today()
    daystart = int(today.strftime("%d"))
    dayend = daystart - 7
    while(dayend<=daystart):        
        print(today)
        daystr = today.strftime("%Y%m%d")
        logging.info("Processing file for the date : ", daystr)
        filenamezip = "GrantsDBExtract" + daystr + "v2.zip"
        url = "https://www.grants.gov/web/grants/xml-extract.html?download=" + filenamezip
#downloading last seven days files        
        os.system('wget ' + url + ' -O ' + directory_path_data + filenamezip + ' -nv')
        print("Successfully extracted file from grants gov url ",filenamezip)
#checking file persist and put in hdfs       
        filelist = glob.glob(os.path.join(directory_path_data, "*.xml"))
        for f in filelist:
            os.remove(f)
        logging.info('Persisting data to HDFS')
        if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
            call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])
        
        call(["hdfs", "dfs", "-mkdir", hdfs_path])
 
            
        logging.info(directory_path_data)
        call('unzip -p "'+directory_path_data+'Grants*.zip"'+' | hdfs dfs -put - '+hdfs_path+'grants.xml', shell = True)

        
        
        today = today - datetime.timedelta(days=1)
        daystart = int(today.strftime("%d"))
    
   

#     spark = SparkSession.builder.\
#             config('spark.jars', '/home/ananth/nsf_data_ingestion/libraries/spark-xml_2.11-0.5.0.jar').\
#             getOrCreate()
#     #project_folder = sys.argv[1]
#     download_grants_data()