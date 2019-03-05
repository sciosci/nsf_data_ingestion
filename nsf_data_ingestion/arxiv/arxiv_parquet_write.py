import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
import xml.etree.ElementTree as ET
import os
import sys
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
    root = ET.fromstring(content)
    records = root.find(prefix + "ListRecords")
    data = []
    try:
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
    xmlpath = '/user/ananth/arxiv_data/arxiv_data/'
    parpath = '/user/ananth/arxiv/parquet/'
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    filep = os.path.join(xmlpath, "papers_*.xml")
    xmlfiles = sc.binaryFiles(filep)
    xmldf = xmlfiles.flatMap(process).toDF()
    logging.info('Writing to Parquet.....')
    xmldf.write.parquet(parpath)
    logging.info('Parquet Write Complete.....')
    spark.stop()