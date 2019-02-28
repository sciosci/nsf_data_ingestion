from subprocess import call
import logging
from nsf_data_ingestion.main.objects import data_source_params

def persist(param_list):

    hdfs_path = param_list.get('hdfs_path')
    directory_path = param_list.get('directory_path')
    logging.info('Persisting data to HDFS')
    if not call(["hdfs", "dfs", "-test", "-d", hdfs_path]):
        call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path])

    call(["hdfs", "dfs", "-mkdir", hdfs_path])
    call(["hdfs", "dfs", "-put", directory_path, hdfs_path])
    logging.info('Files Persisted to - %s', hdfs_path)



def persist_hdfs(data_source_name):
    persist(data_source_params.mapping.get(data_source_name))
