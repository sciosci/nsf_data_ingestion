
# coding: utf-8

# In[1]:

import time
import urllib2
import datetime
from itertools import ifilter
from collections import Counter, defaultdict
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import matplotlib.pylab as plt
import pandas as pd
import numpy as np
#import bibtexparser

pd.set_option('mode.chained_assignment','warn')


# In[2]:

from lxml import etree


# In[3]:

get_ipython().magic(u'matplotlib inline')


# In[77]:

OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"


# In[78]:

df2 = pd.DataFrame()
df = pd.DataFrame(columns=("title","abstract","date","categories", "created", "id", "doi","resumptionToken"))


# In[79]:

global df2
global df


# In[60]:

def data_download():
   
    global df2
    global df
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    url = (base_url +
           "from=1900-01-01&until=2016-12-31&" +
           "metadataPrefix=arXiv")
    
    while True:
        print "fetching", url
        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            if e.code == 503:
                to = int(e.hdrs.get("retry-after", 30))
                print "Got 503. Retrying after {0:d} seconds.".format(to)
                
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
    
    return df,df2


# In[80]:

def data_download_token(resumptionToken):
    
    global df2
    global df
    base_url = "http://export.arxiv.org/oai2?verb=ListRecords&"
    #url = (base_url +
    #       "from=1900-01-01&until=2016-12-31&" +
    #       "metadataPrefix=arXiv")
    #url = (base_url + "resumptionToken="resumptionToken)
    url = base_url + "resumptionToken=%s"%(resumptionToken)
    
    while True:
        print "fetching", url
        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            if e.code == 503:
                to = int(e.hdrs.get("retry-after", 30))
                print "Got 503. Retrying after {0:d} seconds.".format(to)
                
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
    return df,df2


# In[94]:

arxiv_papers, arxiv_authors = data_download()


# In[ ]:

df,df2 = data_download_token("1426111|208001")


# In[96]:

frames = [arxiv_papers,df]
arxiv_papers = pd.concat(frames)
frames1 = [arxiv_authors,df2]
arxiv_authors = pd.concat(frames1)


# In[113]:

arxiv_papers.to_pickle('arxiv_papers')


# In[114]:

arxiv_authors.to_pickle('arxiv_authors')

