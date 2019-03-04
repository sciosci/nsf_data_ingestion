import sys
import zipfile
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

libraries_list = ['/home/ananth/nsf_data_ingestion/libraries/pubmed_parser_lib.zip', '/home/ananth/nsf_data_ingestion/libraries/unidecode_lib.zip']

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


def cluster_run():    
    project_folder = '/user/ananth/federal/'
    logging.info('Creating Spark Session....')
    data_path = '/user/ananth/medline/medline_data/'
    parquet_path = '/user/ananth/medline/parquet/'
    print("Reading from {} and writing to {}.".format(data_path, parquet_path))
    
    spark = SparkSession.builder.getOrCreate()
    for library in libraries_list:
        logging.info('Adding Libraries' + str(library))
        spark.sparkContext.addPyFile(library)    # adding libraries
        spark.sparkContext.addPyFile(library)
        
    hdfs_data = spark.sparkContext.wholeTextFiles(os.path.join(data_path, '*.xml.gz'), minPartitions=10000)
    preprocess = hdfs_data.flatMap(parse_gzip_medline_str)
    medline_df = preprocess.toDF()
    medline_df.show()
    

    