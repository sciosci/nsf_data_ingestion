import sys
import os
import calendar
import time
sys.path.append('/home/eileen/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_last_load
import logging
logging.getLogger().setLevel(logging.INFO)
from datetime import datetime
import urllib.request
import time
from shutil import copyfile
from shutil import rmtree
from subprocess import call
import tarfile
import shutil
import zipfile
from ftplib import FTP
import calendar

retry_time = 45

def get_raw_data(param_list):
    logging.info("the list of parameters are ", param_list)
    path = '/home/eileen/airflow/arxiv_data/'
    logging.info("checking directory path " , type(path),path)
    raw_url = param_list.get('arxiv_raw_url')
    timestamp_file = param_list.get('timestamp_file')
    
    last_load = get_last_load(path, timestamp_file)
    if 1:     
        logging.info('Old Data. Downloading Updated Data')
        rmtree(path)
        os.makedirs(path)
        logging.info('Downloading Data.........to - ' + path)
        start = time.clock()
        logging.info("request: %s" % (raw_url))
        request = urllib.request.Request(raw_url)
        response = urllib.request.urlopen(request).read().decode('utf-8')
        rawfile = open(path + 'papers_0.xml', 'w')
        rawfile.write(response)
        rawfile.close()
        end = time.clock()
        logging.info("takes: %f s" % (end - start))

        pos_start = response.rfind('<resumptionToken')
        pos_end = response.rfind('</resumptionToken')
        if pos_end > 0 and pos_end > pos_start:
            pos = response.rfind('>', pos_start, pos_end)
            resume_token = response[pos + 1:pos_end]
            logging.info("request_resume: %s" % (resume_token))
            get_resume(resume_token, path, param_list)
        f = open(path + "time_stamp.txt", "a")
        cur_time = calendar.timegm(time.gmtime())
        f.write(str(cur_time))
        f.close()
        logging.info('Data Downloaded......!!!!!')

    else:
        logging.info('Data Intact......!!!!!')
        
        
def get_resume(token, path, param_list):
    #path = '/home/eileen/airflow/arxiv_data/'
    flag = True  
    arxiv_raw_url = param_list.get('arxiv_raw_url')
    repeat = 0
    time.sleep(retry_time)
    start = time.clock()
    filen = 'papers_%s.xml' % (token.replace('|', '_'))
    
    if os.path.exists(path + filen):
        flag = False
    
    if flag:
        rawfile = open(path + filen, 'w')
        try:
            query = "http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken=%s" % (token)
            request = urllib.request.Request(query)
            response = urllib.request.urlopen(request).read().decode('utf-8')
            rawfile.write(response)
            rawfile.close()


            end = time.clock()
            logging.info("takes: %f s" % (end - start))

            pos_start = response.rfind('<resumptionToken')
            pos_end = response.rfind('</resumptionToken')
            if pos_end > 0 and pos_end > pos_start and repeat < 20:
                pos = response.rfind('>', pos_start, pos_end)
                resume_token = response[pos + 1:pos_end]
                logging.info("request_resume: %s" % (resume_token))
                get_resume(resume_token, path, param_list)
                repeat += 1
        except Exception as err:
            logging.info(err)
            logging.info("retry resume_token: %s" % (token))
            time.sleep(30)
            get_resume(token, path, param_list)


def persist(param_list):
    hdfs_path = '/user/eileen/arxiv_data/'
    directory_path = '/home/eileen/airflow/arxiv_data/'
    if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
        logging.info('Parquet Files Exist !........Deleting')
        call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])
                     
    logging.info('Persisting data to HDFS') 
    call(["hdfs", "dfs", "-mkdir", hdfs_path])
    call(["hdfs", "dfs", "-put", directory_path, hdfs_path])
    logging.info('Files Persisted to - %s', hdfs_path)


def download(data_source_name):
    get_raw_data(data_source_params.mapping.get(data_source_name))
    
def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))