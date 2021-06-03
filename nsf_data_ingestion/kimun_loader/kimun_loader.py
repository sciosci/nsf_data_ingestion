#!/usr/bin/env python
# coding: utf-8

import os, findspark

# temporarily changing PYSPARK_PYTHON to avoid rdd error.
os.environ['PYSPARK_PYTHON'] = '/home/tozeng/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/hzhuang/anaconda3/bin/python'

findspark.init('/opt/cloudera/parcels/SPARK2/lib/spark2/')

import pyspark
import scipy.sparse, logging, json
import numpy as np
from pyspark import SparkContext
from pyspark.sql import functions, SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, unix_timestamp, from_unixtime, coalesce, to_date
from pyspark.ml.linalg import Vectors, _convert_to_vector, VectorUDT


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
        config('spark.jars', '/home/eileen/nsf_data_ingestion/libraries/elasticsearch-hadoop-7.10.1.jar').\
        appName('kimun_loader').\
        getOrCreate()
    
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/dist/nsf_data_ingestion-0.0.1-py3.7.egg')
    return spark

def parse(tup):
    d = {}
    d['id']=tup['id']
    d['city']=tup['city']
    d['country'] = tup['country']
    d['date'] = tup['date']
    d['documentType'] = tup['type']
    d['endDate'] = tup['end_date']
    d['organizations'] = tup['organizations']
    d['otherID'] = tup['other_id']
    d['scientists'] = tup['scientists']
    d['sourceID'] = tup['source_id']
    d['source'] = tup['source']
    d['summary'] = tup['abstract']
    d['text'] = tup['content']
    d['title'] = tup['title']
    d['venue']=tup['venue']
    d['topicNorm'] = list(tup['topic'])
    return (d['id'], json.dumps(d))


def dense_to_sparse(vector):
    sparse = _convert_to_vector(scipy.sparse.csc_matrix(vector.toArray()).T)
    #matrix = np.array(sparse.toArray()).as_matrix().reshape(-1,1)
    return sparse

##### elastic push function update pending
# def elastic_push(result):
#     es_write_conf = {
#             "es.nodes" : "128.230.247.186",
#             "es.port" : "9201",
#             "es.resource" : 'kimun/documents',
#             "es.input.json": "yes",
#             "es.mapping.id": "id",
#             "es.batch.size.entries": "5000",
#             "es.batch.write.retry.wait": "3000"
#         }

#     result.saveAsNewAPIHadoopFile(
#             path='-',
#             outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable",
#             valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
#             conf=es_write_conf)

##### function to be updated
# def kimun_load():
#     spark = create_session()
#     sqlContext = SQLContext(spark.sparkContext)
#     topic_df = sqlContext.read.parquet('/user/sghosh08/tfidf_topic/')
#     topic_rdd = topic_df.rdd
#     result = topic_rdd.map(parse)
#     elastic_push(result)


def to_date_(col, formats=("MM/dd/yyyy", "yyyy")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])

def kimun_load():
    logging.info("running kimun_load")
    spark = create_session()
    sqlContext = SQLContext(spark.sparkContext)

    #topic_df = sqlContext.read.parquet('/user/hzhan212/eileen/svd.sample') # for test purpose
    topic_df = sqlContext.read.parquet('/user/eileen/topic_svd/')

    #new date formatting
    topic_df.date = topic_df.select('date', from_unixtime(unix_timestamp('date', 'yyy')).alias('date'))
    split_col = pyspark.sql.functions.split(topic_df['date'], '-')
    topic_df = topic_df.withColumn('date', split_col.getItem(0))
    topic_df = topic_df.withColumn("formatted_date", to_date_("date"))
    topic_df.formatted_date = topic_df.select('formatted_date',from_unixtime(unix_timestamp('formatted_date','yyy')).alias('formatted_date'))
    split_col = pyspark.sql.functions.split(topic_df['formatted_date'], '-')
    topic_df = topic_df.withColumn('formatted_date', split_col.getItem(0))
    topic_df.date = topic_df.select(topic_df.formatted_date).alias('date')
    columns = topic_df.columns
    topic_df = topic_df.drop('date')
    topic_df = topic_df.withColumnRenamed('formatted_date', 'date')
    topic_df = topic_df.withColumn("title", F.regexp_replace(F.regexp_replace(F.regexp_replace("title", "\\]\\[", ""), "\\[",""),"\\]",""))
    topic_df = topic_df.withColumn("abstract", F.regexp_replace(F.regexp_replace(F.regexp_replace("abstract", "\\]\\[", ""), "\\[", ""), "\\]", ""))
    columns = topic_df.columns
    val2 = topic_df.select(columns).groupBy(['title', 'scientists', 'venue']).agg(F.min('date'))
    val2 = val2.withColumnRenamed('min(date)', 'date')
    val3 = val2.join(topic_df, ['title', 'scientists', 'venue', 'date'])
    val3 = val3.withColumn("date", val3["date"].cast("int"))
    val3 = val3.withColumn("date", val3["date"].cast("string"))
    #date formatting end
    sc = spark.sparkContext
    print("before conf ")
    es_write_conf = {
            "es.nodes" : "128.230.247.186",\
            "es.port" : "9201",\
            # TODO: check if the name of index is consistent with the current one
            "es.resource" : 'kimun_jim2/documents',\
            "es.input.json": "yes",\
            "es.mapping.id": "id",\
            "es.batch.size.entries": "5000",\
            "es.batch.write.retry.wait": "3000"
        }

    rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat", "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_write_conf)
    topic_rdd = val3.rdd
    result = topic_rdd.map(parse)
    result = result.repartition(1)
    print("after repartition")

    result.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf)

    logging.info("Data Loaded in Elastic Search check kibana index count")
    spark.stop()
