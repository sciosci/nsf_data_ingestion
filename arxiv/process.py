
# coding: utf-8

# In[93]:

import os
os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark"


# In[94]:

import findspark
findspark.init()


# In[95]:

import pyspark
conf = pyspark.SparkConf().    setAppName('test_app').    set('spark.yarn.appMasterEnv.PYSPARK_PYTHON', '/home/deacuna/anaconda3/bin/python').    set('spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON', '/home/deacuna/anaconda3/bin/python').    setMaster('yarn-client').    set('executor.memory', '1g').    set('spark.yarn.executor.memoryOverhead', '4098').    set('spark.sql.codegen', 'true').    set('spark.yarn.executor.memory', '500m').    set('yarn.scheduler.minimum-allocation-mb', '500m').    set('spark.dynamicAllocation.maxExecutors', '3').    set('jars', 'hdfs://eggs/graphframes-0.1.0-spark1.6.jar').    set('spark.driver.maxResultSize', '4g')


# In[96]:

from pyspark.sql import SQLContext, HiveContext
sc = pyspark.SparkContext(conf=conf)
sqlContext = HiveContext(sc)


# In[97]:

import pandas as pd


# In[5]:

from sklearn.datasets import load_diabetes


# In[98]:

df


# In[99]:

df = pd.read_csv('df.csv')
df_authors = pd.read_csv('df_authors.csv')


# In[100]:

df = df.drop(df.columns[[0]], axis=1)
df_authors = df_authors.drop(df_authors.columns[[0]], axis=1)


# In[101]:

print (type(df_authors['authors']))


# In[102]:

df_spark = sqlContext.createDataFrame(df)
df_authors_spark = sqlContext.createDataFrame(df_authors)


# In[103]:

df.count()


# In[104]:

df_spark.show()


# In[105]:

df_spark.write.parquet('papers.parquet')



# In[106]:

df_authors_spark.write.parquet('author_names.parquet')


# In[ ]:




# In[ ]:




# In[107]:

sc.stop()

