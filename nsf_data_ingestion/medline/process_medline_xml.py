import sys
import os
import findspark
findspark.init()
import pyspark
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.functions import rank, max, sum, desc
from os import path
import zlib


def create_spark_session(name):
    spark = SparkSession.builder.\
        appName(name).\
        enableHiveSupport().\
        config('spark.yarn.executor.memoryOverhead', '3g').\
        getOrCreate()
    spark.sparkContext.addPyFile(os.environ['PUBMEDPARSER_PATH'])
    return spark

def parse_gzip_medline_str(gzip_str):
    filepath, gzip_content = gzip_str
    _, file_name = path.split(filepath)
    # decompress gzip
    contents = zlib.decompress(gzip_content, 16+zlib.MAX_WBITS)
    articles = pp.parse_medline_xml(contents)
    return [Row(file_name=file_name, **article_dict)
            for article_dict in articles]


if __name__ == '__main__':
    spark = create_spark_session('PUBMED dump processing')
    import pubmed_parser as pp
    # path to read xml in hdfs
    xml_path = sys.argv[1]
    # path to save parquet in hdfs
    parquet_path = sys.argv[2]
    print("Reading from {} and writing to {}.".format(xml_path, parquet_path))

    medline_gzip_rdd = \
        spark.sparkContext.binaryFiles(os.path.join(xml_path, '*.xml.gz'),
                                       minPartitions=10000)
    preprocess = medline_gzip_rdd.flatMap(parse_gzip_medline_str)
    medline_df = preprocess.toDF()
    medline_df.cache()
    medline_df.count()
    window = Window.partitionBy(['pmid']).orderBy(desc('file_name'))
    # only get the last version of documents
    last_medline_df = medline_df.select(
        max('delete').over(window).alias('is_deleted'),
        rank().over(window).alias('pos'), '*').\
        where('is_deleted = False and pos = 1').\
        drop('is_deleted').drop('pos')
    last_medline_df.write.parquet(parquet_path)
    spark.stop()
