import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
import pyspark
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import rank, max, sum, desc
import zlib
import os
from os import path



def createSession(libraries_list):
    spark = SparkSession.builder.config("spark.executor.memory", '3g').config('spark.executor.cores', '2').getOrCreate()
    for library in libraries_list:
        spark.sparkContext.addPyFile(library)    # adding libraries
        import library
        print(library.__file__)
    return spark



def parse_gzip_medline_str(gzip_str):
    filepath = gzip_str[0]
    gzip_content = gzip_str[1]
    _, file_name = path.split(filepath)

    xml_string = gzip_content.split("\n", 3)[3];  # remove unicode declaration
    articles = pp.parse_medline_xml(xml_string)
    return [Row(file_name=file_name, **article_dict)
            for article_dict in articles]



def process(data_path, parquet_path, libraries_list):
    spark = createSession(libraries_list)
    pub_data_rdd = spark.sparkContext.wholeTextFiles(os.path.join(data_path, '*.xml.gz'))
    preprocess = pub_data_rdd.flatMap(parse_gzip_medline_str)
    medline_df = preprocess.toDF(preprocess)
    
    window = Window.partitionBy(['pmid']).orderBy(desc('file_name'))
    #only get the last version of documents
    last_medline_df = medline_df.select(
        max('delete').over(window).alias('is_deleted'),
        rank().over(window).alias('pos'), '*').\
        where('is_deleted = False and pos = 1').\
        drop('is_deleted').drop('pos').drop('delete')
    
    medline_df.write.parquet(parquet_path)
    spark.stop()


