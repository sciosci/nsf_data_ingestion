import sys
import os
sys.path.append('/home/ananth/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils.utils_functions import get_last_load
import logging
logging.getLogger().setLevel(logging.INFO)
from datetime import datetime
import urllib.request
import time
from subprocess import call

retry_time = 45


# save_path = ""

def get_raw_data(param_list):
    path = param_list.get('directory_path')
    raw_url = param_list.get('arxiv_raw_url')
    timestamp_file = param_list.get('timestamp_file')
    
    last_load = get_last_load(directory_path_data, timestamp_file)
    
    if last_load >= 86400:
    
        logging.info('Downloading Data.........to - ' + path)
        start = time.clock()
        logging.info("request: %s" % (raw_url))
        request = urllib.request.Request(raw_url)
        response = urllib.request.urlopen(request).read().decode('utf-8')
        rawfile = open(path + 'papers_0.xml', 'w')
        rawfile.write(response)
        rawfile.close()

        #save_to_hdfs("papers_0.xml", path)

        end = time.clock()
        logging.info("takes: %f s" % (end - start))

        pos_start = response.rfind('<resumptionToken')
        pos_end = response.rfind('</resumptionToken')
        if pos_end > 0 and pos_end > pos_start:
            pos = response.rfind('>', pos_start, pos_end)
            resume_token = response[pos + 1:pos_end]
            logging.info("request_resume: %s" % (resume_token))
            get_resume(resume_token, path, hdfs_path, param_list)
        
        f = open(path + "time_stamp.txt", "a")
        cur_time = calendar.timegm(time.gmtime())
        f.write(str(cur_time))
        f.close()

    else:
        logging.info('Data Intact......!!!!!')
def get_resume(token, path, hdfs_path, param_list):
    
    resume_url = param_list.get('arxiv_raw_url')
    repeat = 0
    time.sleep(retry_time)
    start = time.clock()
    filen = 'papers_%s.xml' % (token.replace('|', '_'))
    rawfile = open(path + filen, 'w')
    try:
        request = urllib.request.Request(resume_url)
        response = urllib.request.urlopen(request).read().decode('utf-8')
        rawfile.write(response)
        rawfile.close()
        #save_to_hdfs(filen, hdfs_path)

        end = time.clock()
        logging.info("takes: %f s" % (end - start))

        pos_start = response.rfind('<resumptionToken')
        pos_end = response.rfind('</resumptionToken')
        if pos_end > 0 and pos_end > pos_start and repeat < 20:
            pos = response.rfind('>', pos_start, pos_end)
            resume_token = response[pos + 1:pos_end]
            logging.info("request_resume: %s" % (resume_token))
            get_resume(resume_token, path, hdfs_path, param_list)
            repeat += 1
    except Exception as err:
        logging.info(err)
        logging.info("retry resume_token: %s" % (token))
        time.sleep(30)
        get_resume(token)


def persist(param_list):

    hdfs_path = param_list.get('hdfs_path')
    directory_path = param_list.get('directory_path')
    logging.info('Persisting data to HDFS')
    if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
        call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])

    call(["hdfs", "dfs", "-mkdir", hdfs_path])
    call(["hdfs", "dfs", "-put", directory_path, hdfs_path])
    logging.info('Files Persisted to - %s', hdfs_path)


def download(data_source_name):
    get_raw_data(data_source_params.mapping.get(data_source_name))
    
def persist_hdfs(data_source_name):
persist(data_source_params.mapping.get(data_source_name))