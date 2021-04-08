# #!/usr/bin/env python
# # coding: utf-8

# # In[1]:


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

def kimun_load():
    print("kimun success")

# # In[2]:

# def create_session():
#     spark = SparkSession.builder.config("spark.executor.memory", '40g')\
#         .config('spark.executor.cores', '9')\
#         .config('spark.cores.max', '9')\
#         .config('spark.jars', '/home/ananth/tmp/ElasticSearch/elasticsearch-hadoop-6.7.1.jar')\
#         .getOrCreate()
#     return spark

# def create_session():
#     spark = SparkSession.builder.config("spark.executor.instances", '5')\
#         .config("spark.executor.memory", '40g')\
#         .config('spark.executor.cores', '9')\
#         .config('spark.cores.max', '9')\
#         .config('spark.jars', '/home/sghosh08/nsf_new/nsf_data_ingestion/libraries/elasticsearch-hadoop-6.7.1.jar')\
#         .appName('kimun_loader')\
#         .getOrCreate()
#     return spark


# # In[3]:

# def parse(tup):
#     d = {}
#     d['id']=tup['id']
#     d['city']=tup['city']
#     d['country'] = tup['country']
#     d['date'] = tup['date']
#     d['documentType'] = tup['type']
#     d['endDate'] = tup['end_date']
#     d['organizations'] = tup['organizations']
#     d['otherID'] = tup['other_id']
#     d['scientists'] = tup['scientists']
#     d['sourceID'] = tup['source_id']
#     d['summary'] = tup['abstract']
#     d['text'] = tup['content']
#     d['title'] = tup['title']
#     d['venue']=tup['venue']
#     return (d['id'], json.dumps(d))

# # In[4]:

# def dense_to_sparse(vector):
#     sparse = _convert_to_vector(scipy.sparse.csc_matrix(vector.toArray()).T)
#     #matrix = np.array(sparse.toArray()).as_matrix().reshape(-1,1)
#     return sparse


# # In[5]:
# def elastic_push(result):
#     es_write_conf = {
#             "es.nodes" : "128.230.247.186",
#             "es.port" : "9201",
#             "es.resource" : 'kimun_version5/documents',
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

# # In[6]:


# def kimun_load():
#     spark = create_session()
#     sqlContext = SQLContext(spark.sparkContext)
#     topic_df = sqlContext.read.parquet('/user/sghosh08/tfidf_topic/')
#     topic_rdd = topic_df.rdd
#     result = topic_rdd.map(parse)
#     elastic_push(result)


# # In[7]:

# import os
# import findspark
# findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
# import pyspark
# from pyspark.sql import functions
# from pyspark.sql import SparkSession
# from pyspark import SparkContext
# from pyspark.sql import SQLContext
# import scipy.sparse
# from pyspark.sql.functions import unix_timestamp
# from pyspark.sql.functions import from_unixtime
# from pyspark.sql.functions import coalesce, to_date
# from pyspark.ml.linalg import Vectors, _convert_to_vector, VectorUDT
# from pyspark.sql.functions import udf, col
# from pyspark.sql import functions as F
# from pyspark.sql.functions import coalesce, to_date
# import numpy as np
# import json


# # In[8]:

# def to_date_(col, formats=("MM/dd/yyyy", "yyyy")):
#     # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
#     return coalesce(*[to_date(col, f) for f in formats])



# # In[9]:

# spark = create_session()
# sqlContext = SQLContext(spark.sparkContext)
# topic_df = sqlContext.read.parquet('/user/eileen/tfidf.parquet/')
# #new date formatting
# topic_df.date = topic_df.select('date', from_unixtime(unix_timestamp('date', 'yyy')).alias('date'))
# split_col = pyspark.sql.functions.split(topic_df['date'], '-')
# topic_df = topic_df.withColumn('date', split_col.getItem(0))
# topic_df = topic_df.withColumn("formatted_date", to_date_("date"))
# topic_df.formatted_date =                          topic_df.select('formatted_date',from_unixtime(unix_timestamp('formatted_date','yyy')).alias('formatted_date'))
# split_col = pyspark.sql.functions.split(topic_df['formatted_date'], '-')
# topic_df = topic_df.withColumn('formatted_date', split_col.getItem(0))
# topic_df.date = topic_df.select(topic_df.formatted_date).alias('date')
# columns = topic_df.columns
# topic_df = topic_df.drop('date')
# topic_df = topic_df.withColumnRenamed('formatted_date', 'date')
# topic_df =             topic_df             .withColumn("title", F.regexp_replace(F.regexp_replace(F.regexp_replace("title", "\\]\\[", ""), "\\[",""),"\\]",""))
# topic_df = topic_df.withColumn("abstract", F.regexp_replace(F.regexp_replace(F.regexp_replace                                                                          ("abstract", "\\]\\[", ""), "\\[", ""), "\\]", ""))
# columns = topic_df.columns
# val2 = topic_df.select(columns).groupBy(['title', 'scientists', 'venue']).agg(F.min('date'))
# val2 = val2.withColumnRenamed('min(date)', 'date')
# val3 = val2.join(topic_df, ['title', 'scientists', 'venue', 'date'])
# val3 = val3.withColumn("date", val3["date"].cast("int"))
# val3 = val3.withColumn("date", val3["date"].cast("string"))
# #date formatting end
# sc = spark.sparkContext
# print("before conf ")
# es_write_conf = {
#         "es.nodes" : "128.230.247.186",\
#         "es.port" : "9201",\
#         "es.resource" : 'kimun_version5/documents',\
#         "es.input.json": "yes",\
#         "es.mapping.id": "id",\
#         "es.batch.size.entries": "5000",\
#         "es.batch.write.retry.wait": "3000"
#     }


# # In[10]:

# val3.limit(5).toPandas()


# # In[11]:


# topic_df.limit(5).toPandas()


# # In[13]:

# # from pyspark import SparkContext
# # sc =SparkContext()


# rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat", "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=es_write_conf)
# topic_rdd = val3.rdd
# result = topic_rdd.map(parse)
# result = result.repartition(1)
# print("after repartition")


# # In[ ]:


# result.saveAsNewAPIHadoopFile(        path='-',        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", keyClass="org.apache.hadoop.io.NullWritable",        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",        conf=es_write_conf)


# # In[ ]:





# # In[8]:


# spark.stop()

