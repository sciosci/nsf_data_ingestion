import os
import sys
import os
#os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark"
os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/SPARK2/lib/spark2"
import findspark
findspark.init()

import pyspark
conf = pyspark.SparkConf().\
	setAppName('test_app').\
    set('spark.yarn.appMasterEnv.PYSPARK_PYTHON', '/home/deacuna/anaconda3/bin/python').\
    set('spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON', '/home/deacuna/anaconda3/bin/python').\
    setMaster('yarn-client').\
    set('executor.memory', '6g').\
    set('spark.yarn.executor.memoryOverhead', '4098').\
    set('spark.sql.codegen', 'true').\
    set('spark.yarn.executor.memory', '500m').\
    set('yarn.scheduler.minimum-allocation-mb', '500m').\
    set('spark.dynamicAllocation.maxExecutors', '3').\
    set('jars', 'hdfs://eggs/graphframes-0.1.0-spark1.6.jar').\
    set('spark.driver.maxResultSize', '16g').\
    set('spark.port.maxRetries', 60)
    #set('spark.driver.memory', '16g')
    
from pyspark.sql import SQLContext, HiveContext
sc = pyspark.SparkContext(conf=conf)
sqlContext = HiveContext(sc)

import time
import urllib
import datetime
#from itertools import ifilter
from collections import Counter, defaultdict
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import matplotlib.pylab as plt
import pandas as pd
import numpy as np
#import bibtexparser
from datetime import date
pd.set_option('mode.chained_assignment','warn')
from lxml import etree
from subprocess import call


# # Download Latest Data

def data_download_date():
    OAI = "{http://www.openarchives.org/OAI/2.0/}"
    ARXIV = "{http://arxiv.org/OAI/arXiv/}"
    global df2
    global df
    df = pd.DataFrame(columns=("title","abstract","date","categories", "created", "id", "doi","resumptionToken"))
    df2 = pd.DataFrame()


    from datetime import date
    try:
        parquetPapers = sqlContext.read.parquet("papers.parquet") #Read Parquet file
        #parquetPapers = sqlContext.read.parquet("sys.argv[1]") #Read Parquet file
        #parquetPapers = sqlContext.read.parquet("papers_11_14.parquet") #Read Parquet file
        sqlContext.registerDataFrameAsTable(parquetPapers, "table1")
        result = sqlContext.sql("SELECT MAX(date) AS date FROM table1").collect()
        x = result[0].date/1000000000
    except:
        x = 0.
    latest_date = date.fromtimestamp(x)
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d")
    
    
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    #url = (base_url +
    #       "from=1900-01-01&until=2016-12-31&" +
    #       "metadataPrefix=arXiv")
    #url = (base_url + "resumptionToken="resumptionToken)
    #url = base_url + "resumptionToken=%s"%(resumptionToken)
    
    url = base_url+"from=%s"%(str(latest_date))+"&until=%s"%(str(now))+"&metadataPrefix=arXiv"
    
    while True:
        print ("fetching", url)
        try:
            response = urllib.request.urlopen(url)
        except urllib.error.HTTPError as e:
            if e.code == 503:
                to = int(e.hdrs.get("retry-after", 30))
                print ("Got 503. Retrying after {0:d} seconds.".format(to))
                
                time.sleep(to)
                continue
            
            else:
                raise
        
        xml = response.read()
        root = ET.fromstring(xml)
        
        for record in root.find(OAI+'ListRecords').findall(OAI+"record"):
            arxiv_id = record.find(OAI+'header').find(OAI+'identifier')
            meta = record.find(OAI+'metadata')
            info = meta.find(ARXIV+"arXiv")
            date = record.find(OAI+'header').find(OAI+'datestamp').text
            date = datetime.datetime.strptime(date, "%Y-%m-%d")
            created = info.find(ARXIV+"created").text
            created = datetime.datetime.strptime(created, "%Y-%m-%d")
            categories = info.find(ARXIV+"categories").text
            
            doi = info.find(ARXIV+"doi")
            if doi is not None:
                doi = doi.text.split()[0]
            
            aFname = info.find(ARXIV+"authors").find(ARXIV+"author").find(ARXIV+"keyname").text
            
            for authors in info.findall(ARXIV+"authors"):
                id = info.find(ARXIV+"id").text[0:]
                for author in authors.findall(ARXIV+"author"):
                    author_fname = {'author_fname':author.find(ARXIV+"keyname").text,
                                     'id': id}
                    df2 = df2.append(author_fname,ignore_index=True)
                    
            
            
            token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
            
            contents = {'title': info.find(ARXIV+"title").text,
                        'abstract': info.find(ARXIV+"abstract").text.strip(),
                        'date': date,
                        'categories': categories.split(),
                        'created': created,
                        'id': info.find(ARXIV+"id").text,#arxiv_id.text[4:],
                        'doi': doi,
                        'resumptionToken': token.text,
                        }
            
            df = df.append(contents, ignore_index=True)
            
        
        token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
        if token is None or token.text is None:
            break
            
        else:
            url = base_url + "resumptionToken=%s"%(token.text)
    #return df,df2
    df_spark = sqlContext.createDataFrame(df)
    #!hdfs dfs -rm -r /user/skatchhi/latest_papers.parquet # Delete the existing parquet first:
    # Cannot delete the file using '!', it is a ipython trick. Use "from subprocess import call" instead
    call(['hdfs','dfs','-rm','-r','latest_papers.parquet'])# originally sys arg was 0
    df_spark.write.parquet('latest_papers.parquet')
    df_author_spark = sqlContext.createDataFrame(df2)
    #!hdfs dfs -rm -r /user/skatchhi/latest_authors.parquet # Delete the existing parquet first:
    call(['hdfs','dfs','-rm','-r','latest_authors.parquet'])
    df_author_spark.write.parquet('latest_authors.parquet')
    
    parquetPapers = sqlContext.read.option("mergeSchema", "true").parquet("papers.parquet")
    parquetPapers = parquetPapers.toPandas()
    df = sqlContext.read.option("mergeSchema", "true").parquet("latest_papers.parquet")
    df = df.toPandas()
    frames = [parquetPapers,df]
    parquetPapers = pd.concat(frames)
    parquetPapers = pd.DataFrame(parquetPapers)
    papers_to_parquet(parquetPapers)
    
    parquetAuthors = sqlContext.read.option("mergeSchema", "true").parquet("authors.parquet")
    parquetAuthors = parquetAuthors.toPandas()
    df2 = sqlContext.read.option("mergeSchema", "true").parquet("latest_authors.parquet")
    df2 = df2.toPandas()
    frames1 = [parquetAuthors,df2]
    parquetAuthors = pd.concat(frames1)
    frames1 = pd.DataFrame(frames1)
    authors_to_parquet(parquetAuthors)
    
    


data_download_date()


# # Function: Converting to Parquet


#sys.argv[1] = "users/skatchhi/"


def papers_to_parquet(df):
    df_spark = sqlContext.createDataFrame(df)
    basepath = sys.argv[1]
    abc = os.path.join(basepath, "papers.parquet")
    #df_spark.write.parquet(abc)
    #!hdfs dfs -rm -r /user/skatchhi/papers_11_14.parquet # Delete the existing parquet first:
    call(['hdfs','dfs','-rm','-r','sys.arg[1]','papers.parquet'])
    df_spark.write.parquet(abc)
    #sc.stop()


def authors_to_parquet(df2):
    df2_spark = sqlContext.createDataFrame(df2)
    parquet_path = sys.argv[1]
    xyz = os.path.join(parquet_path, "authors.parquet")
    #df_spark.write.parquet(abc)
    #!hdfs dfs -rm -r /user/skatchhi/authors_11_14.parquet # Delete the existing parquet first:
    call(['hdfs','dfs','-rm','-r','sys.arg[1]','authors.parquet'])
    df2_spark.write.parquet(xyz)
    #df_spark.write.parquet(parquet_path)
