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
    file_list = ['synopsis', 'forecast']
    
    for filename in file_list:
            location = project_folder + filename
            
        
            if not call(["hdfs", "dfs", "-test", "-d", location +'.parquet']):
                logging.info('Parquet Files Exist Deleting .......')
                call(["hdfs", "dfs", "-rm", "-r", "-f", location +'.parquet'])
    synopsis = spark.read.format('com.databricks.spark.xml').\
    options(rowTag='OpportunitySynopsisDetail_1_0').\
    load(project_folder + 'grants.xml')
    forecast = spark.read.format('com.databricks.spark.xml'). \
    options(rowTag='OpportunityForecastDetail_1_0'). \
    load(project_folder + 'grants.xml')
    synopsis.write.parquet(project_folder+"synopsis.parquet")
    forecast.write.parquet(project_folder+"forecast.parquet")   
    logging.info('Writing New parquet Files .......')
         
def main():    
    project_folder = '/user/eileen/grants/data/xml'
    logging.info('Creating Spark Session....')
    spark = SparkSession.builder.config('spark.jars', '/home/eileen/nsf_data_ingestion/libraries/spark-xml_2.11-0.5.0.jar').config("spark.executor.instances", '3').config("spark.executor.memory", '10g').config('spark.executor.cores', '3').config('spark.cores.max', '3').appName('write_grants_parquet').getOrCreate()
    logging.info('Writing to Parquet for grants.....')
    convert_to_parquet(spark, project_folder)
    logging.info('Parquet Write Complete grants.....')
    spark.stop()