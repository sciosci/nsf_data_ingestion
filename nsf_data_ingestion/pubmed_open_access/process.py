# Script to Scrapp the data Pubmed Open Access in to .csv files

import csv
import xml.etree.ElementTree as ET
import os
import pubmed_parser as pp
import random
import string
import json
import uuid
import glob
import yaml
from shutil import rmtree
#import findspark
#import pyspark
#from pyspark.sql import SQLContext, HiveContext
import pandas as pd
import sys

with open('location.yaml', 'r') as file:
    location = yaml.load(file)
#directory_path_chunk = location["path"]["chunk_data"]
directory_path_chunk = sys.argv[1:][0]
#directory_path_csv = location["path"]["csv_data"]
directory_path_csv = sys.argv[1:][1]
#directory_path_parquet = location["path"]["csv_data"]
directory_path_parquet = sys.argv[1:][2]
rmtree(directory_path_csv)
os.makedirs(directory_path_csv)

def main():
    print "This is main"

    for subdir, dirs, files in os.walk(directory_path_chunk):
        for file in files:
            if file.endswith('.nxml'):
                print(file)
                filename = os.path.join(subdir, file)
                dict_out = pp.parse_pubmed_xml(filename)
                xml_json = json.dumps(dict_out, ensure_ascii=False)
                document_info = parse_document(dict_out)
                parse_scientist(dict_out, document_info)

def parse_document(dict_out):
    document_csv = open(directory_path_csv+"document.csv", 'a')
    if os.stat(directory_path_csv+"document.csv").st_size == 0:
        writer = csv.writer(document_csv)
        writer.writerow(["id", "title", "summary", "year", "pubmed_id", "journal", "pubmed_central_id"])

    id = uuid.uuid1()
    summary = dict_out['abstract'].encode('utf-8').strip()
    title = dict_out['full_title'].encode('utf-8').strip()
    year = dict_out['publication_year'].encode('utf-8').strip()
    pubmed_id = dict_out['pmid'].encode('utf-8').strip()
    journal = dict_out['journal'].encode('utf-8').strip()
    pubmed_central_id = dict_out['pmc'].encode('utf-8').strip()

    writer = csv.writer(document_csv)
    writer.writerow([id, title, summary, year, pubmed_id, journal, pubmed_central_id])
    document_csv.close()
    document_info = {"document_id": id, "document_pubmed_id": pubmed_id, "document_pubmed_central_id": pubmed_central_id}
    return document_info

def parse_scientist(dict_out, document_info):
    document_id = document_info['document_id']
    document_pubmed_id = document_info['document_pubmed_id']
    document_pubmed_central_id = document_info['document_pubmed_central_id']
    affiliation_array = []

    scientist_csv = open(directory_path_csv+"scientist.csv", 'a')
    organization_csv = open(directory_path_csv+"organization.csv", 'a')
    scientist_organization_csv = open(directory_path_csv+"scientist_organization.csv", 'a')

    if os.stat(directory_path_csv+"scientist.csv").st_size == 0:
        scientist_writer = csv.writer(scientist_csv)
        scientist_writer.writerow(["id", "first_name", "last_name", "document_id", "document_pubmed_id", "document_pubmed_central_id"])

    if os.stat(directory_path_csv+"organization.csv").st_size == 0:
        organization_writer = csv.writer(organization_csv)
        organization_writer.writerow(["id", "name"])

    if os.stat(directory_path_csv+"scientist_organization.csv").st_size == 0:
        scientist_organization_writer = csv.writer(scientist_organization_csv)
        scientist_organization_writer.writerow(["scientist_id", "organization_id"])

    affiliation_list = dict_out['affiliation_list']
    for affiliation in affiliation_list:
        if affiliation[0] is not None:
            affiliation_name = affiliation[0].encode('utf-8').strip()
        if affiliation[1] is not None:
            affiliation_organization = affiliation[1].encode('utf-8').strip()
        affiliation_organization_id = uuid.uuid1()
        organization_writer = csv.writer(organization_csv)
        organization_writer.writerow([affiliation_organization_id, affiliation_organization])
        affiliation_object = {"organization_id": affiliation_organization_id, "affiliation_name": affiliation_name, "organization": affiliation_organization}
        affiliation_array.append(affiliation_object)

    affiliation_list = dict_out['affiliation_list']
    author_list = dict_out['author_list']

    for author in dict_out['author_list']:
        scientist_writer = csv.writer(scientist_csv)
        scientist_id = uuid.uuid1()

        if author[1] is not None:
            first_name = author[1].encode('utf-8').strip()
        else:
            first_name = ""
        if author[0] is not None:
            last_name = author[0].encode('utf-8').strip()
        else:
            last_name = ""
        if author[2] is not None:
            author_affiliation = author[2]
        else:
            author_affiliation = ""
        scientist_writer.writerow([scientist_id, first_name, last_name, document_id, document_pubmed_id, document_pubmed_central_id])

        for affiliation in affiliation_array:
            if affiliation['affiliation_name'] is not None:
                if affiliation['affiliation_name'] == author_affiliation:
                    scientist_organization_writer = csv.writer(scientist_organization_csv)
                    scientist_organization_writer.writerow([scientist_id, affiliation['organization_id']])

    scientist_csv.close()
    organization_csv.close()
    scientist_organization_csv.close()

    def process_hadoop():
        os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark"
        findspark.init()

        conf = pyspark.SparkConf().\
        setAppName('test_app').\
        set('spark.yarn.appMasterEnv.PYSPARK_PYTHON', '/home/deacuna/anaconda3/bin/python').\
        set('spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON', '/home/deacuna/anaconda3/bin/python').\
        setMaster('yarn-client').\
        set('executor.memory', '1g').\
        set('spark.yarn.executor.memoryOverhead', '4098').\
        set('spark.sql.codegen', 'true').\
        set('spark.yarn.executor.memory', '500m').\
        set('yarn.scheduler.minimum-allocation-mb', '500m').\
        set('spark.dynamicAllocation.maxExecutors', '3').\
        set('jars', 'hdfs://eggs/graphframes-0.1.0-spark1.6.jar').\
        set('spark.driver.maxResultSize', '4g')

        sc = pyspark.SparkContext(conf=conf)
        sqlContext = HiveContext(sc)

        # document_csv
        document_csv = pd.read_csv("document.csv")
        document_df = pd.DataFrame(document_csv)
        document_df.id = document_df.id.astype(str)
        document_df.title = document_df.title.astype(str)
        document_df.summary = document_df.summary.astype(str)
        document_df.year = document_df.year.astype(str)
        document_df.pubmed_id = document_df.pubmed_id.astype(str)
        document_df.journal = document_df.journal.astype(str)
        document_df.pubmed_central_id = document_df.pubmed_central_id.astype(str)
        document_spark_df = sqlContext.createDataFrame(document_df)
        document_spark_df.write.parquet('document.parquet')

        # scientist_organization_csv
        scientist_organization_csv = pd.read_csv("scientist_organization.csv")
        scientist_organization_df = pd.DataFrame(scientist_organization_csv)
        scientist_organization_df.scientist_id = scientist_organization_df.scientist_id.astype(str)
        scientist_organization_df.organization_id = scientist_organization_df.organization_id.astype(str)
        scientist_organization_spark_df = sqlContext.createDataFrame(scientist_organization_df)
        scientist_organization_spark_df.write.parquet('scientist_organization.parquet')

        # scientist_csv
        scientist_csv = pd.read_csv("scientist.csv")
        scientist_df = pd.DataFrame(scientist_csv)
        scientist_df.id = scientist_df.id.astype(str)
        scientist_df.first_name = scientist_df.first_name.astype(str)
        scientist_df.last_name = scientist_df.last_name.astype(str)
        scientist_df.document_id = scientist_df.document_id.astype(str)
        scientist_df.document_pubmed_id = scientist_df.document_pubmed_id.astype(str)
        scientist_df.document_pubmed_central_id = scientist_df.document_pubmed_central_id.astype(str)
        scientist_spark_df = sqlContext.createDataFrame(scientist_df)
        scientist_spark_df.write.parquet('scientist.parquet')

        # organization_csv
        organization_csv = pd.read_csv("organization.csv")
        organization_df = pd.DataFrame(organization_csv)
        organization_df.id = organization_df.id.astype(str)
        organization_df.name = organization_df.name.astype(str)
        organization_spark_df = sqlContext.createDataFrame(organization_df)
        organization_spark_df.write.parquet('organization.parquet')

main()
