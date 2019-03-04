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
    spark = SparkSession.builder.config('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.4.1').getOrCreate()
    logging.info('Writing to Parquet.....')
    convert_to_parquet(spark, project_folder)
    logging.info('Parquet Write Complete.....')