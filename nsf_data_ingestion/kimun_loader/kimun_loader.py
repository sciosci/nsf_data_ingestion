import os
import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
import pyspark
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
import scipy.sparse
from pyspark.ml.linalg import Vectors, _convert_to_vector, VectorUDT
from pyspark.sql.functions import udf, col
import numpy as np
import json

def create_session():
    spark = SparkSession.builder.config("spark.executor.memory", '40g')\
        .config('spark.executor.cores', '9')\
        .config('spark.cores.max', '9')\
        .config('spark.jars', '/home/ananth/tmp/ElasticSearch/elasticsearch-hadoop-6.7.1.jar')\
        .getOrCreate()
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

def elastic_push(result):
    es_write_conf = {
            "es.nodes" : "128.230.247.186",
            "es.port" : "9201",
            "es.resource" : 'kimun_example/documents',
            "es.input.json": "yes",
            "es.mapping.id": "id"
        }

    result.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=es_write_conf)

def kimun_load():
    spark = create_session()
    sqlContext = SQLContext(spark.sparkContext)
    topic_df = sqlContext.read.parquet('/user/ananth/tfidf_topic')
    topic_rdd = topic_df.rdd
    result = topic_rdd.map(parse)
    elastic_push(result)