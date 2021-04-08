# import datetime

# import findspark
# import io, os, time
# findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
# from pyspark.sql import SparkSession

# import sys
# from shutil import rmtree
# import os
# from shutil import copyfile
# from shutil import rmtree
# import tarfile
# import shutil
# import zipfile
# from ftplib import FTP
# import glob, os, os.path
# import logging
# import calendar
# import time
# from subprocess import call
# sys.path.append('/home/eileen/nsf_data_ingestion/')
# from nsf_data_ingestion.config import nsf_config
# from nsf_data_ingestion.objects import data_source_params
# from nsf_data_ingestion.utils.utils_functions import get_last_load

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
logging.getLogger().setLevel(logging.INFO)

def download_grants_data(param_list):
    print("started")
    logging.info("the list of parameters are ", param_list)
    grants_gov_path_data = param_list.get('directory_path')
    grants_gov_hdfs_path = param_list.get('hdfs_path')
    #directory_path_data = '/home/eileen/grants_gov/'
    #hdfs_path = '/user/eileen/grants/data/raw/'
    print(grants_gov_path_data)
    
    loopstart=1
    today = datetime.date.today()
    daystart = int(today.strftime("%d"))
    dayend = daystart - 6
    while(loopstart <=7):
        loopstart = loopstart+1
        daystr = today.strftime("%Y%m%d")
        logging.info("Processing file for the date : ", daystr)
        filenamezip = "GrantsDBExtract" + daystr + "v2.zip"
        print(str(os.path.exists(grants_gov_path_data+filenamezip)))
        if(os.path.exists(grants_gov_path_data+filenamezip)):
            print("file persists")
            os.remove(grants_gov_path_data+filenamezip)
#             
            
        url = "https://www.grants.gov/web/grants/xml-extract.html?download=" + filenamezip
# #downloading last seven days files 
        
        os.system('wget ' + url + ' -O ' + grants_gov_path_data + filenamezip + ' -nv')
        print("Successfully extracted file from grants gov url ",filenamezip)
        today = today - datetime.timedelta(days=1)
        daystart = int(today.strftime("%d"))

def hdfs_put_grants_data(param_list):
    grants_gov_path_data = param_list.get('directory_path')
    #directory_path_data = '/home/eileen/grants_gov/
    grants_gov_xml_path = param_list.get('xml_path')
    #'/user/eileen/grants/data/xml/'
    logging.info('Persisting data to HDFS', call(["hdfs", "dfs", "-test", "-d", grants_gov_xml_path]))
        
    if not call(["hdfs", "dfs", "-test", "-d", grants_gov_xml_path]):
         call(["hdfs", "dfs", "-rm","-r","-f", grants_gov_xml_path])
    call(["hdfs", "dfs", "-mkdir", grants_gov_xml_path])
    call('unzip -p "'+grants_gov_path_data+'Grants*.zip"'+' | hdfs dfs -put - '+grants_gov_xml_path+'grants.xml', shell = True)
    logging.info(grants_gov_path_data)
