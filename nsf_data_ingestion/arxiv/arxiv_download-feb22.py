import sys
import os
import calendar
import time
# sys.path.append('/home/ananth/nsf_data_ingestion/')
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


# save_path = ""

def get_raw_data(param_list):
#     path = param_list.get('download_path')
    logging.info("the list of parameters are ", param_list)
    #arxiv_directory_path_data = param_list.get('download_path')
    path = '/home/eileen/airflow/arxiv_data/'
    logging.info("checking directory path " , type(path),path)
    #arxiv_raw_url = param_list.get('arxiv_raw_url')
    raw_url = param_list.get('arxiv_raw_url')
    timestamp_file = param_list.get('timestamp_file')
    
    last_load = get_last_load(path, timestamp_file)
    #last_load = get_last_load(path, timestamp_file)
    
#     if last_load >= 604800:
    if 1:     
        logging.info('Old Data. Downloading Updated Data')
        #rmtree(arxiv_directory_path_data)
        rmtree(path)
        #os.makedirs(arxiv_directory_path_data)
        os.makedirs(path)
    
        #logging.info('Downloading Data.........to - ' + arxiv_directory_path_data)
        logging.info('Downloading Data.........to - ' + path)
        start = time.clock()
        #logging.info("request: %s" % (arxiv_raw_url))
        logging.info("request: %s" % (raw_url))
        #request = urllib.request.Request(arxiv_raw_url)
        request = urllib.request.Request(raw_url)
        response = urllib.request.urlopen(request).read().decode('utf-8')
        #rawfile = open(arxiv_directory_path_data + 'papers_0.xml', 'w')
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
            #get_resume(resume_token, arxiv_directory_path_data, param_list)
            get_resume(resume_token, path, param_list)
        
        #f = open(arxiv_directory_path_data + "time_stamp.txt", "a")
        f = open(path + "time_stamp.txt", "a")
        cur_time = calendar.timegm(time.gmtime())
        f.write(str(cur_time))
        f.close()
        logging.info('Data Downloaded......!!!!!')

    else:
        logging.info('Data Intact......!!!!!')
        
        
#def get_resume(token, arxiv_directory_path_data, param_list):
def get_resume(token, path, param_list):
    path = '/home/eileen/airflow/arxiv_data/'
    #arxiv_directory_path_data = param_list.get('download_path')
    flag = True
    
    #arxiv_raw_url = param_list.get('arxiv_raw_url')
    resume_url = param_list.get('arxiv_raw_url')
    repeat = 0
    time.sleep(retry_time)
    start = time.clock()
    filen = 'papers_%s.xml' % (token.replace('|', '_'))
    
    #if os.path.exists(arxiv_directory_path_data + filen):
    if os.path.exists(path + filen):
        flag = False
    
    if flag:
        #rawfile = open(arxiv_directory_path_data + filen, 'w')
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
                #get_resume(resume_token, arxiv_directory_path_data, param_list)
                get_resume(resume_token, path, param_list)
                repeat += 1
        except Exception as err:
            logging.info(err)
            logging.info("retry resume_token: %s" % (token))
            time.sleep(30)
            #get_resume(token, arxiv_directory_path_data, param_list)
            get_resume(token, path, param_list)


def persist_hdfs(param_list):

    #hdfs_path = param_list.get('hdfs_path')
    #directory_path = param_list.get('directory_path')
    #arxiv_hadoop_directory = param_list.get('hdfs_path')
    #arxiv_directory_path_data = param_list.get('download_path')
    hdfs_path = '/user/eileen/arxiv_data/'
    #arxiv_directory_path_data = param_list.get('download_path')
    directory_path = '/home/eileen/airflow/arxiv_data/'
    if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
    #if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
        logging.info('Parquet Files Exist !........Deleting')
        #call(["hdfs", "dfs", "-rm", "-r", "-f", arxiv_hadoop_directory])
        call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])
                     
    logging.info('Persisting data to HDFS') 
    #call(["hdfs", "dfs", "-mkdir", arxiv_hadoop_directory])
    call(["hdfs", "dfs", "-mkdir", hdfs_path])
    #call(["hdfs", "dfs", "-put", arxiv_directory_path_data, arxiv_hadoop_directory])
    call(["hdfs", "dfs", "-put", directory_path, hdfs_path])
    #logging.info('Files Persisted to - %s', arxiv_hadoop_directory)
    logging.info('Files Persisted to - %s', hdfs_path)


def download(data_source_name):
    get_raw_data(data_source_params.mapping.get(data_source_name))
    
def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))