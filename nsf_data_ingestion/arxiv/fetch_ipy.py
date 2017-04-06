
# coding: utf-8

# In[117]:

import os
import time
import pickle
import random
import argparse
import urllib.request
import feedparser
import sys
import findspark
os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/SPARK2/lib/spark2"
findspark.init()
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import functions as fn


from utils import Config, safe_pickle_dump


# In[118]:

conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
sqlContext = SQLContext(sc)


# In[119]:

def encode_feedparser_dict(d):
  """ 
  helper function to get rid of feedparser bs with a deep copy. 
  I hate when libs wrap simple things in their own classes.
  """
  if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
    j = {}
    for k in d.keys():
      j[k] = encode_feedparser_dict(d[k])
    return j
  elif isinstance(d, list):
    l = []
    for k in d:
      l.append(encode_feedparser_dict(k))
    return l
  else:
    return d


# In[120]:

def parse_arxiv_url(url):
  """ 
  examples is http://arxiv.org/abs/1512.08756v2
  we want to extract the raw id and the version
  """
  ix = url.rfind('/')
  idversion = j['id'][ix+1:] # extract just the id (and the version)
  parts = idversion.split('v')
  assert len(parts) == 2, 'error parsing url ' + url
  return parts[0], int(parts[1])


# In[116]:

if __name__ == "__main__":
    
    
  paper_path = sys.argv[1]
  # parse input arguments
  parser = argparse.ArgumentParser()
  parser.add_argument('--search-query', type=str,
                      default = cat_default,
                      #default='cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML',
                      help='query used for arxiv API. See http://arxiv.org/help/api/user-manual#detailed_examples')
  parser.add_argument('--start-index', type=int, default=0, help='0 = most recent API result')
  parser.add_argument('--max-index', type=int, default=50000, help='upper bound on paper index we will fetch')
  parser.add_argument('--results-per-iteration', type=int, default=500,help='passed to arxiv API')
  parser.add_argument('--wait-time', type=float, default=5.0, help='lets be gentle to arxiv API (in number of seconds)')
  parser.add_argument('--break-on-no-added', type=int, default=1, help='break out early if all returned query papers are already in db? 1=yes, 0=no')
  args = parser.parse_args([])

  # misc hardcoded variables
  base_url = 'http://export.arxiv.org/api/query?' # base api query url
  print('Searching arXiv for %s' % (args.search_query, ))
  
    
  # lets load the existing database to memory
  try:
    db = pickle.load(open(Config.db_path, 'rb'))
  except Exception as e:
    print('error loading existing database:')
    print(e)
    print('starting from an empty database')
    db = {}

  # -----------------------------------------------------------------------------
  # main loop where we fetch the new results
  print('database has %d entries at start' % (len(db), ))
  num_added_total = 0
  for i in range(args.start_index, args.max_index, args.results_per_iteration):

    print("Results %i - %i" % (i,i+args.results_per_iteration))
    query = 'search_query=%s&sortBy=lastUpdatedDate&start=%i&max_results=%i' % (args.search_query,
                                                         i, args.results_per_iteration)
    with urllib.request.urlopen(base_url+query) as url:
      response = url.read()
    parse = feedparser.parse(response)
    num_added = 0
    num_skipped = 0
    for e in parse.entries:

      j = encode_feedparser_dict(e)

      # extract just the raw arxiv id and version for this paper
      rawid, version = parse_arxiv_url(j['id'])
      j['_rawid'] = rawid
      j['_version'] = version

      # add to our database if we didn't have it before, or if this is a new version
      if not rawid in db or j['_version'] > db[rawid]['_version']:
        db[rawid] = j
        print('Updated %s added %s' % (j['updated'].encode('utf-8'), j['title'].encode('utf-8')))
        num_added += 1
        num_added_total += 1
      else:
        num_skipped += 1

    # print some information
    print('Added %d papers, already had %d.' % (num_added, num_skipped))

    if len(parse.entries) == 0:
      print('Received no results from arxiv. Rate limiting? Exiting. Restart later maybe.')
      print(response)
      break

    if num_added == 0 and args.break_on_no_added == 1:
      print('No new papers were added. Assuming no new papers exist. Exiting.')
      break

    print('Sleeping for %i seconds' % (args.wait_time , ))
    time.sleep(args.wait_time + random.uniform(0, 3))

  # save the database before we quit, if we found anything new
  if num_added_total > 0:
    print('Saving database with %d papers to %s' % (len(db), Config.db_path))
    safe_pickle_dump(db, Config.db_path)


# In[ ]:




# In[106]:

with open('db.p', 'rb') as pickle_file:
    content = pickle.load(pickle_file)
    content = pd.DataFrame(content)
    content = content.T

#df_paper = sqlContext.createDataFrame(content)
#df_paper.write.parquet(paper_path, mode='append')


# In[107]:

#temp_data = content
content


# In[ ]:




# In[71]:

#cat_default = 'stat.AP+stat.CO+stat.ML+stat.ME+stat.TH+q-bio.BM+q-bio.CB+q-bio.GN+q-bio.MN+q-bio.NC+q-bio.OT+q-bio.PE+q-bio.QM+q-bio.SC+q-bio.TO+cs.AR+cs.AI+cs.CL+cs.CC+cs.CE+cs.CG+cs.GT+cs.CV+cs.CY+cs.CR+cs.DS+cs.DB+cs.DL+cs.DM+cs.DC+cs.GL+cs.GR+cs.HC+cs.IR+cs.IT+cs.LG+cs.LO+cs.MS+cs.MA+cs.MM+cs.NI+cs.NE+cs.NA+cs.OS+cs.OH+cs.PF+cs.PL+cs.RO+cs.SE+cs.SD+cs.SC+nlin.AO+nlin.CG+nlin.CD+nlin.SI+nlin.PS+math.AG+math.AT+math.AP+math.CT+math.CA+math.CO+math.AC+math.CV+math.DG+math.DS+math.FA+math.GM+math.GN+math.GT+math.GR+math.HO+math.IT+math.KT+math.LO+math.MP+math.MG+math.NT+math.NA+math.OA+math.OC+math.PR+math.QA+math.RT+math.RA+math.SP+math.ST+math.SG+astro-ph+cond-mat.dis-nn+cond-mat.mes-hall+cond-mat.mtrl-sci+cond-mat.other+cond-mat.soft+cond-mat.stat-mech+cond-mat.str-el+cond-mat.supr-con+gr-qc+hep-ex+hep-lat+hep-ph+hep-th+math-ph+nucl-ex+nucl-th+physi+cs.acc-ph+physi+cs.ao-ph+physi+cs.atom-ph+physi+cs.atm-clus+physi+cs.bio-ph+physi+cs.chem-ph+physi+cs.class-ph+physi+cs.comp-ph+physi+cs.data-an+physi+cs.flu-dyn+physi+cs.gen-ph+physi+cs.geo-ph+physi+cs.hist-ph+physi+cs.ins-det+physi+cs.med-ph+physi+cs.optics+physi+cs.ed-ph+physi+cs.soc-ph+physi+cs.plasm-ph+physi+cs.pop-ph+physi+cs.space-ph+quant-ph'


# In[75]:

cat_default = 'cat:stat.AP+OR+cat:stat.CO+OR+cat:stat.ML+OR+cat:stat.ME+OR+cat:stat.TH+OR+cat:q-bio.BM+OR+cat:q-bio.CB+OR+cat:q-bio.GN+OR+cat:q-bio.MN+OR+cat:q-bio.NC+OR+cat:q-bio.OT+OR+cat:q-bio.PE+OR+cat:q-bio.QM+OR+cat:q-bio.SC+OR+cat:q-bio.TO+OR+cat:cs.AR+OR+cat:cs.AI+OR+cat:cs.CL+OR+cat:cs.CC+OR+cat:cs.CE+OR+cat:cs.CG+OR+cat:cs.GT+OR+cat:cs.CV+OR+cat:cs.CY+OR+cat:cs.CR+OR+cat:cs.DS+OR+cat:cs.DB+OR+cat:cs.DL+OR+cat:cs.DM+OR+cat:cs.DC+OR+cat:cs.GL+OR+cat:cs.GR+OR+cat:cs.HC+OR+cat:cs.IR+OR+cat:cs.IT+OR+cat:cs.LG+OR+cat:cs.LO+OR+cat:cs.MS+OR+cat:cs.MA+OR+cat:cs.MM+OR+cat:cs.NI+OR+cat:cs.NE+OR+cat:cs.NA+OR+cat:cs.OS+OR+cat:cs.OH+OR+cat:cs.PF+OR+cat:cs.PL+OR+cat:cs.RO+OR+cat:cs.SE+OR+cat:cs.SD+OR+cat:cs.SC+OR+cat:nlin.AO+OR+cat:nlin.CG+OR+cat:nlin.CD+OR+cat:nlin.SI+OR+cat:nlin.PS+OR+cat:math.AG+OR+cat:math.AT+OR+cat:math.AP+OR+cat:math.CT+OR+cat:math.CA+OR+cat:math.CO+OR+cat:math.AC+OR+cat:math.CV+OR+cat:math.DG+OR+cat:math.DS+OR+cat:math.FA+OR+cat:math.GM+OR+cat:math.GN+OR+cat:math.GT+OR+cat:math.GR+OR+cat:math.HO+OR+cat:math.IT+OR+cat:math.KT+OR+cat:math.LO+OR+cat:math.MP+OR+cat:math.MG+OR+cat:math.NT+OR+cat:math.NA+OR+cat:math.OA+OR+cat:math.OC+OR+cat:math.PR+OR+cat:math.QA+OR+cat:math.RT+OR+cat:math.RA+OR+cat:math.SP+OR+cat:math.ST+OR+cat:math.SG+OR+cat:astro-ph+OR+cat:cond-mat.dis-nn+OR+cat:cond-mat.mes-hall+OR+cat:cond-mat.mtrl-sci+OR+cat:cond-mat.other+OR+cat:cond-mat.soft+OR+cat:cond-mat.stat-mech+OR+cat:cond-mat.str-el+OR+cat:cond-mat.supr-con+OR+cat:gr-qc+OR+cat:hep-ex+OR+cat:hep-lat+OR+cat:hep-ph+OR+cat:hep-th+OR+cat:math-ph+OR+cat:nucl-ex+OR+cat:nucl-th+OR+cat:physi+OR+cat:cs.acc-ph+OR+cat:physi+OR+cat:cs.ao-ph+OR+cat:physi+OR+cat:cs.atom-ph+OR+cat:physi+OR+cat:cs.atm-clus+OR+cat:physi+OR+cat:cs.bio-ph+OR+cat:physi+OR+cat:cs.chem-ph+OR+cat:physi+OR+cat:cs.class-ph+OR+cat:physi+OR+cat:cs.comp-ph+OR+cat:physi+OR+cat:cs.data-an+OR+cat:physi+OR+cat:cs.flu-dyn+OR+cat:physi+OR+cat:cs.gen-ph+OR+cat:physi+OR+cat:cs.geo-ph+OR+cat:physi+OR+cat:cs.hist-ph+OR+cat:physi+OR+cat:cs.ins-det+OR+cat:physi+OR+cat:cs.med-ph+OR+cat:physi+OR+cat:cs.optics+OR+cat:physi+OR+cat:cs.ed-ph+OR+cat:physi+OR+cat:cs.soc-ph+OR+cat:physi+OR+cat:cs.plasm-ph+OR+cat:physi+OR+cat:cs.pop-ph+OR+cat:physi+OR+cat:cs.space-ph+OR+cat:quant-ph'


# In[88]:


cat_cs = 'cat:cs.AR+OR+cat:cs.AI+OR+cat:cs.CL+OR+cat:cs.CC+OR+cat:cs.CE+OR+cat:cs.CG+OR+cat:cs.GT+OR+cat:cs.CV+OR+cat:cs.CY+OR+cat:cs.CR+OR+cat:cs.DS+OR+cat:cs.DB+OR+cat:cs.DL+OR+cat:cs.DM+OR+cat:cs.DC+OR+cat:cs.GL+OR+cat:cs.GR+OR+cat:cs.HC+OR+cat:cs.IR+OR+cat:cs.IT+OR+cat:cs.LG+OR+cat:cs.LO+OR+cat:cs.MS+OR+cat:cs.MA+OR+cat:cs.MM+OR+cat:cs.NI+OR+cat:cs.NE+OR+cat:cs.NA+OR+cat:cs.OS+OR+cat:cs.OH+OR+cat:cs.PF+OR+cat:cs.PL+OR+cat:cs.RO+OR+cat:cs.SE+OR+cat:cs.SD+OR+cat:cs.SC'


# In[ ]:




# In[ ]:




# In[27]:

import pandas as pd


# In[ ]:




# In[95]:

with open('db.p', 'rb') as pickle_file:
    content = pickle.load(pickle_file)
    content = pd.DataFrame(content)
    content = content.T


# In[96]:

content


# In[42]:




# In[44]:




# In[ ]:



