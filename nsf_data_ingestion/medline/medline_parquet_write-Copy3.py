import findspark
findspark.init('/opt/cloudera/parcels/SPARK2/lib/spark2/')
import sys
sys.path.append('/home/eileen/nsf_data_ingestion/')
import zipfile
import zipimport
import io
import logging
logging.getLogger().setLevel(logging.INFO)
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import rank, max, sum, desc
import zlib
import importlib
import os
from os import path
from shutil import copyfile
from shutil import rmtree
from subprocess import call

#sys.path.append('/home/ananth/nsf_data_ingestion/')

from nsf_data_ingestion.config import spark_config
from nsf_data_ingestion.objects import data_source_params


def create_session():
    spark = SparkSession.builder.\
        config('spark.dynamicAllocation.enabled','true').\
        config('spark.dynamicAllocation.maxExecutors','100').\
        config('spark.dynamicAllocation.executorIdleTimeout','30s').\
        config('spark.driver.maxResultSize', '8g').\
        config('spark.yarn.maxAppAttempts', '3').\
        config('spark.driver.memory', '10g').\
        config('spark.executor.memory', '10g').\
        config('spark.task.maxFailures', '10').\
        config('spark.yarn.am.memory', '8g').\
        config('spark.yarn.max.executor.failures', '3').\
        config('spark.kryoserializer.buffer.max','2000m').\
        appName('medline').\
        getOrCreate()
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/pubmed_parser-0.1.0-py3.6.egg')
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/Unidecode-1.1.1-py3.6.egg')
        
    return spark

def parse_gzip_medline_str(gzip_str):
    import pubmed_parser as pp
    import unidecode
    print(pp.__file__)
    print(unidecode.__file__)
    
    filepath = gzip_str[0]
    gzip_content = gzip_str[1]
    _, file_name = path.split(filepath)
    # decompress gzip

    xml_string = gzip_content.split("\n", 3)[3];
    articles = pp.parse_medline_xml(xml_string)
    return [Row(file_name=file_name, **article_dict)
            for article_dict in articles]


if __name__ == '__main__':
    data_source_name = 'medline'
    params_list = data_source_params.mapping.get(data_source_name)
    medline_xml_path = '/user/eileen/medline/medline_data_new/'
    #medline_xml_path= params_list.get('xml_path')
    print(medline_xml_path)
    medline_parquet_path = '/user/eileen/medline/parquet/'
    #medline_parquet_path= params_list.get('parquet_path')
    #libraries_list = spark_config.libraries_list
    
    print("Reading from {} and writing to {}.".format(medline_xml_path, medline_parquet_path))
    
    spark = create_session()
    hdfs_data = spark.sparkContext.wholeTextFiles(os.path.join(medline_xml_path, '*.xml.gz'), minPartitions=10000)
    preprocess = hdfs_data.flatMap(parse_gzip_medline_str)
    medline_df = preprocess.toDF()
    
    window = Window.partitionBy(['pmid']).orderBy(desc('file_name'))
    ##only get the last version of documents
    last_medline_df = medline_df.select(
        max('delete').over(window).alias('is_deleted'),
        rank().over(window).alias('pos'), '*').\
        where('is_deleted = False and pos = 1').\
        drop('is_deleted').drop('pos').drop('delete')
    
    if not call(["hdfs", "dfs", "-test", "-d", medline_parquet_path]):
            logging.info('Parquet Files Exist Deleting .......')
            call(["hdfs", "dfs", "-rm", "-r", "-f", medline_parquet_path])
            
    logging.info('Writing New parquet Files .......')
    medline_df.write.parquet(medline_parquet_path)
    #medline_df.head(10)
    
    spark.stop()