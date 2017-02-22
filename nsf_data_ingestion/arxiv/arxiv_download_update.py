
# coding: utf-8

# In[219]:

#import os
import sys
import os
os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark"
#os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/SPARK2/lib/spark2"
import findspark
findspark.init()


# In[220]:

import pyspark
conf = pyspark.SparkConf().    setAppName('test_app').    set('spark.yarn.appMasterEnv.PYSPARK_PYTHON', '/home/deacuna/anaconda3/bin/python').    set('spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON', '/home/deacuna/anaconda3/bin/python').    setMaster('yarn-client').    set('executor.memory', '1g').    set('spark.yarn.executor.memoryOverhead', '4098').    set('spark.sql.codegen', 'true').    set('spark.yarn.executor.memory', '500m').    set('yarn.scheduler.minimum-allocation-mb', '500m').    set('spark.dynamicAllocation.maxExecutors', '3').    set('jars', 'hdfs://eggs/graphframes-0.1.0-spark1.6.jar').    set('spark.driver.maxResultSize', '4g')


# In[221]:

from pyspark.sql import SQLContext, HiveContext
sc = pyspark.SparkContext(conf=conf)
sqlContext = HiveContext(sc)


# In[227]:

sc


# In[223]:

import pandas as pd
from sklearn.datasets import load_diabetes
sc.addPyFile("hdfs:///user/deacuna/eggs/arxivpy-0.1.dev0-py3.5.egg")
import arxivpy as ax


# In[224]:




# In[225]:




# In[205]:

categories = ['stat.AP','stat.CO','stat.ML','stat.ME','stat.TH','q-bio.BM','q-bio.CB','q-bio.GN','q-bio.MN','q-bio.NC','q-bio.OT','q-bio.PE','q-bio.QM','q-bio.SC','cs.AR','cs.AI','cs.CL','cs.CC','cs.CE','cs.CG','cs.GT','cs.CV','cs.CY','cs.CR','cs.DS','cs.DB','cs.DL','cs.DM','cs.DC','cs.GL','cs.GR','cs.HC','cs.IR','cs.IT','cs.LG','cs.LO','cs.MS','cs.MA','cs.MM','cs.NI','cs.NE','cs.NA','cs.OS','cs.OH','cs.PF','cs.PL','cs.RO','cs.SE','cs.SD','cs.SC','nlin.AO','nlin.CG','nlin.CD','nlin.SI','nlin.PS','math.AG','math.AT','math.AP','math.CT','math.CA','math.CO','math.AC','math.CV','math.DG','math.DS','math.FA','math.GM','math.GN','math.GT','math.GR','math.HO','math.IT','math.KT','math.LO','math.MP','math.MG','math.NT','math.NA','math.OA','math.OC','math.PR','math.QA','math.RT','math.RA','math.SP','math.ST','math.SG','astro-ph','cond-mat.dis-nn','cond-mat.mes-hall','cond-mat.mtrl-sci','cond-mat.other','cond-mat.soft','cond-mat.stat-mech','cond-mat.str-el','cond-mat.supr-con','gr-qc','hep-ex','hep-lat','hep-ph','hep-th','math-ph','nucl-ex','nucl-th','physics.acc-ph','physics.ao-ph','physics.atom-ph','physics.atm-clus','physics.bio-ph','physics.chem-ph','physics.class-ph','physics.comp-ph','physics.data-an','physics.flu-dyn','physics.gen-ph','physics.geo-ph','physics.hist-ph','physics.ins-det','physics.med-ph','physics.optics',	'physics.ed-ph','physics.soc-ph','physics.plasm-ph','physics.pop-ph','physics.space-ph','quant-ph']


# In[ ]:




# In[62]:

# First we need to download data:
x = ax.query(search_query = categories,start_index=0, max_index=10) #Download 100k rows of data
df = pd.DataFrame(x) #Convert it into a dataframe
df_authors = df[['authors','id']] #Create an authors table
#Cleaning the author table
df_authors = pd.DataFrame(df_authors.authors.str.split(',').tolist(), index=df_authors.id).stack()
df_authors = df_authors.reset_index()[[0, 'id']] # id variable is currently labeled 0
df_authors.columns = ['authors', 'id'] # renaming var1
df.rename(columns={'Unnamed: 0': 'arxiv_id'}, inplace=True)
df_authors.rename(columns={'Unnamed: 0': 'temp_id'}, inplace=True)


# In[69]:

#Converting to parquet:

df_spark = sqlContext.createDataFrame(df)
df_authors_spark = sqlContext.createDataFrame(df_authors)
#basepath = sys.argv[1]
#basepath
#abc = os.path.join(basepath, "papers.parquet")
#df_spark.write.parquet(abc)
df_spark.write.parquet('papers.parquet')
df_authors_spark.write.parquet('author_names.parquet')
#sc.stop()


# In[228]:

parquetPapers = sqlContext.read.parquet("papers.parquet") #Read Parquet file 


# In[229]:

parquetPapers


# In[230]:

sqlContext.registerDataFrameAsTable(parquetPapers, "table1")


# In[233]:

result = sqlContext.sql("SELECT MAX(arxiv_id) AS start_index FROM table1").collect()


# In[235]:

(result)


# In[236]:

result[0]


# In[ ]:




# In[ ]:

#Copy the current parquet file into the backup folder
parquetPapers = sqlContext.read.parquet("papers.parquet") #Read Parquet file 

sqlContext.registerDataFrameAsTable(parquetPapers, "table1")
result = sqlContext.sql("SELECT MAX(arxiv_id) AS Start_Index FROM table1").collect()

# Now, we need to download additional data:
x = ax.query(search_query = categories,
             start_index = result[0].Start_Index,
             max_index=(result[0].Start_Index) + 100) #Download 100k rows of data

df = pd.DataFrame(x) #Convert it into a dataframe
df_authors = df[['authors','id']] #Create an authors table
#Cleaning the author table
df_authors = pd.DataFrame(df_authors.authors.str.split(',').tolist(), index=df_authors.id).stack()
df_authors = df_authors.reset_index()[[0, 'id']] # id variable is currently labeled 0
df_authors.columns = ['authors', 'id'] # renaming var1
df.rename(columns={'Unnamed: 0': 'arxiv_id'}, inplace=True)
df_authors.rename(columns={'Unnamed: 0': 'temp_id'}, inplace=True)


# Delete the existing parquet first:
get_ipython().system('hdfs dfs -rm -r /user/skatchhi/papers.parquet')
get_ipython().system('hdfs dfs -rm -r /user/skatchhi/author_names.parquet')


#Converting to parquet:
df_spark = sqlContext.createDataFrame(df)
df_authors_spark = sqlContext.createDataFrame(df_authors)
#basepath = sys.argv[1]
#basepath
#abc = os.path.join(basepath, "papers.parquet")
#df_spark.write.parquet(abc)
df_spark.write.parquet('papers.parquet')
df_authors_spark.write.parquet('author_names.parquet')
#sc.stop()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



