import findspark
findspark.init('/opt/cloudera/parcels/SPARK2/lib/spark2/')
import xml.etree.ElementTree as ET
import findspark
import os
import sys
from subprocess import call
findspark.init()
from pyspark.sql import SparkSession
import logging
logging.getLogger().setLevel(logging.INFO)


def process(file):
    """
    Process an Arvix XML file and generates a dictionary with its contents
    :param file: XML file
    :return: list of dictionaries with contents
    """
    prefix = "{http://www.openarchives.org/OAI/2.0/}"
    prefix2 = "{http://purl.org/dc/elements/1.1/}"

    content = file[1].decode("utf-8")
    data = []
    try:
        root = ET.fromstring(content)
        records = root.find(prefix + "ListRecords")
        for record in records.findall(prefix + "record"):
            identifier = record[0][0].text
            datastamp = record[0][1].text
            subjects = []
            for subject in record[0].findall(prefix + "setSpec"):
                subjects.append(subject.text)
            if len(subjects) == 0:
                subjects.append("none")

            dc = record[1][0]
            title = dc[0].text
            authors = []
            for author in dc.findall(prefix2 + "creator"):
                authors.append(author.text)
            abstract = dc.find(prefix2 + "description").text

            d = {"id": identifier, "datastamp": datastamp, "title": title, "subjects": subjects, "authors": authors,
                 "abstract": abstract}
            data.append(d)
        return data
    except:
        return data

if __name__ == '__main__':
##     xmlpath = '/user/ananth/arxiv_data/arxiv_data/'
#     parpath = '/user/ananth/arxiv/parquet/'
    xmlpath='/user/eileen/arxiv_data/arxiv_data/'
    parpath = '/user/eileen/arxiv_data/parquet/'
    #parpath = '/user/eileen/arxiv/parquet/'
    spark = SparkSession.builder.config('spark.jars', '/home/eileen/nsf_data_ingestion/libraries/spark-xml_2.11-0.5.0.jar').config("spark.executor.instances", '3').config("spark.executor.memory", '10g').config('spark.executor.cores', '3').config('spark.cores.max', '3').appName('arxiv').getOrCreate()
    #spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    filep = os.path.join(xmlpath, "papers_*.xml")
    xmlfiles = sc.binaryFiles(filep)
    xmldf = xmlfiles.flatMap(process).toDF()
    xmldf.count()
    
    if not call(["hdfs", "dfs", "-test", "-d", parpath]):
            logging.info('Parquet Files Exist Deleting .......')
            call(["hdfs", "dfs", "-rm", "-r", "-f", parpath])
            
    logging.info('Writing New parquet Files .......')
    xmldf.write.parquet(parpath)
    spark.stop()
    
    
# def main(data_source_name): 
#     project_folder = '/user/eileen/federal/xml/'
#     logging.info('Creating Spark Session....')
#     spark = SparkSession.builder.config('spark.jars', '/home/eileen/nsf_data_ingestion/libraries/spark-xml_2.11-0.5.0.jar').config("spark.executor.instances", '3').config("spark.executor.memory", '10g').config('spark.executor.cores', '3').config('spark.cores.max', '3').appName(data_source_name).getOrCreate()
#     logging.info('Writing to Parquet.....')
#     convert_to_parquet(spark, project_folder)
#     logging.info('Parquet Write Complete.....')
#     spark.stop()