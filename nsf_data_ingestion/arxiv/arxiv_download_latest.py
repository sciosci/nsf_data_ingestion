import sys
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import functions as fn

import time
import urllib
import datetime
import xml.etree.ElementTree as ET

import pandas as pd
pd.set_option('mode.chained_assignment','warn')


def data_download_date(paper_path, author_path):
    conf = pyspark.SparkConf()
    sc = pyspark.SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    OAI = "{http://www.openarchives.org/OAI/2.0/}"
    ARXIV = "{http://arxiv.org/OAI/arXiv/}"
    df = pd.DataFrame(columns=("title","abstract","date","categories", "created", "id", "doi","resumptionToken"))
    df2 = pd.DataFrame()


    try:
        parquetPapers = sqlContext.read.parquet(paper_path)
        max_date = parquetPapers.select(fn.max('date').alias('max_date')).first().max_date
        x = max_date/1000000000
    except:
        x = 0.

    latest_date = datetime.date.fromtimestamp(x)
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d")

    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    
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

    df_paper = sqlContext.createDataFrame(df)
    df_paper.write.parquet(paper_path, mode='append')
    df_author = sqlContext.createDataFrame(df2)
    df_author.write.parquet(author_path, mode='append')


if __name__ == '__main__':
    paper_path = sys.argv[1]
    author_path = sys.argv[2]
    data_download_date(paper_path, author_path)
