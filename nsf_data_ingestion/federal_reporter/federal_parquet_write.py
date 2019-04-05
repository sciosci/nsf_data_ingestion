import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
import sys
import zipfile
import io
import logging
logging.getLogger().setLevel(logging.INFO)
import os
from os import path
from shutil import copyfile
from shutil import rmtree
from subprocess import call
from pyspark.sql import SparkSession

def convert_to_parquet(spark, project_folder):
    file_list = ['projects', 'abstracts']
    
    for filename in file_list:
        location = project_folder + filename
        df = spark.read.\
            format('com.databricks.spark.xml').\
            options(rowTag='ROW').\
            load(location + '.xml')
        
        if not call(["hdfs", "dfs", "-test", "-d", location +'.parquet']):
            logging.info('Parquet Files Exist Deleting .......')
            call(["hdfs", "dfs", "-rm", "-r", "-f", location +'.parquet'])
            
        logging.info('Writing New parquet Files .......')
        df.write.parquet(location + '.parquet')

def main(data_source_name):    
    project_folder = '/user/ananth/federal/'
    logging.info('Creating Spark Session....')
    spark = SparkSession.builder.config('spark.jars', '/home/ananth/nsf_data_ingestion/libraries/spark-xml_2.11-0.5.0.jar').config("spark.executor.instances", '3').config("spark.executor.memory", '30g').config('spark.executor.cores', '5').config('spark.cores.max', '5').appName(data_source_name).getOrCreate()
    logging.info('Writing to Parquet.....')
    convert_to_parquet(spark, project_folder)
    logging.info('Parquet Write Complete.....')
    spark.stop()