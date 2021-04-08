# #!/usr/bin/env python
# # coding: utf-8

# # In[17]:


# import subprocess
# import os
# import logging
# import pandas as pd
# import json
# # In[39]:
# if __name__ == '__main__':
#     #string = pd.read_csv("kimun_index_structure.txt")
#     #command = string
#     #command= {'settings':}
#     header = "Content-Type:application/json"
#     #command= "{'settings': {'analysis': {'filter': {'alphabets_char_filter': {'pattern': '[^a-zA-Z]','type': 'pattern_replace',              'replacement': ''            },            'word_joiner': {              'catenate_all': 'true',              'type': 'word_delimiter'            },            'autocomplete_flter': {              'type': 'edge_ngram',              'min_gram': '1',              'max_gram': '150'            },            'edge_ngram': {              'token_chars': [                'letter',                'digit'              ],              'min_gram': '1',              'type': 'edgeNGram',              'max_gram': '50'            },            'english_poss_stemmer': {              'name': 'possessive_english',              'type': 'stemmer'            },            'filter_stop':{             'type':'stop',             'stopwords':  '_english_'          },            'filter_shingle':{             'type':'shingle',             'max_shingle_size':4,             'min_shingle_size':2,             'output_unigrams':'true'          }          },          'analyzer': {            'keyword_analyzer': {              'filter': [                'lowercase',                'alphabets_char_filter',                'trim',                'english_poss_stemmer'              ],              'tokenizer': 'standard'            },            'autocomplete': {              'filter': [                'lowercase',                'word_joiner',                'autocomplete_flter'              ],              'type': 'custom',              'tokenizer': 'my_tokenizer'            },            'edge_ngram_analyzer': {              'filter': [                'lowercase',                'alphabets_char_filter',                'trim',                'english_poss_stemmer',                'edge_ngram'              ],              'tokenizer': 'standard'            },            'analyzer_shingle':{              'tokenizer':'standard',               'filter':['standard', 'lowercase', 'stop', 'filter_shingle']            },            'english_analyzer': {              'type': 'standard',              'stopwords': '_english_'            }          },          'tokenizer': {            'my_tokenizer': {              'pattern': ';',              'type': 'pattern'            }          }        }  },    'mappings': {      'documents': {        'properties': {          'date': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'documentType': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'id': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'organizations': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'scientists': {            'type': 'text',            'fields': {              'scientists_search': {                'type' : 'text',                'search_analyzer': 'keyword_analyzer',                'analyzer': 'edge_ngram_analyzer'              },              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            },            'search_analyzer': 'keyword_analyzer',            'analyzer': 'edge_ngram_analyzer'          },          'source': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'sourceID': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'summary': {            'type': 'text',            'fields': {                'summary_search' : {                  'type' : 'text',                  'analyzer': 'analyzer_shingle',                  'search_analyzer':'analyzer_shingle'                },                'summary_text' : {                  'type': 'text',                  'analyzer': 'english_analyzer'                },                'keyword': {                'type': 'keyword',                'ignore_above': 256              }              }          },          'text': {            'type': 'text',            'analyzer': 'english_analyzer',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'title': {            'type': 'text',            'fields': {                'title_search' : {                  'type' : 'text',                  'analyzer': 'analyzer_shingle',                  'search_analyzer':'analyzer_shingle'                },                'title_text' : {                  'type': 'text',                  'analyzer': 'english_analyzer'                },                'keyword': {                'type': 'keyword',                'ignore_above': 256              }              }          },          'topicNorm': {            'type': 'float'          },          'venue': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          }        }      }    }}"
              
#     #command=str(command)
#     #command=json.dumps(command)
#     # In[40]:

#     #command = json.dumps(command)
#     #command = "{'settings':{'analysis':{'filter': {'alphabets_char_filter': {'pattern':'[^a-zA-Z]','type':'pattern_replace',     'replacement':''},'word_joiner': {\'catenate_all': 'true',              'type': 'word_delimiter'            },            'autocomplete_flter': {              'type': 'edge_ngram',              'min_gram': '1',              'max_gram': '150'            },            'edge_ngram': {              'token_chars': [                'letter',                'digit'              ],              'min_gram': '1',              'type': 'edgeNGram',              'max_gram': '50'            },            'english_poss_stemmer': {              'name': 'possessive_english',              'type': 'stemmer'            },            'filter_stop':{             'type':'stop',             'stopwords':  '_english_'          },            'filter_shingle':{             'type':'shingle',             'max_shingle_size':4,             'min_shingle_size':2,             'output_unigrams':'true'          }          },          'analyzer': {            'keyword_analyzer': {              'filter': [                'lowercase',                'alphabets_char_filter',                'trim',                'english_poss_stemmer'              ],              'tokenizer': 'standard'            },            'autocomplete': {              'filter': [                'lowercase',                'word_joiner',                'autocomplete_flter'              ],              'type': 'custom',              'tokenizer': 'my_tokenizer'            },            'edge_ngram_analyzer': {              'filter': [                'lowercase',                'alphabets_char_filter',                'trim',                'english_poss_stemmer',                'edge_ngram'              ],              'tokenizer': 'standard'            },            'analyzer_shingle':{              'tokenizer':'standard',               'filter':['standard', 'lowercase', 'stop', 'filter_shingle']            },            'english_analyzer': {              'type': 'standard',              'stopwords': '_english_'            }          },          'tokenizer': {            'my_tokenizer': {              'pattern': ';',              'type': 'pattern'            }          }        }  },    'mappings': {      'documents': {        'properties': {          'date': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'documentType': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'id': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'organizations': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'scientists': {            'type': 'text',            'fields': {              'scientists_search': {                'type' : 'text',                'search_analyzer': 'keyword_analyzer',                'analyzer': 'edge_ngram_analyzer'              },              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            },            'search_analyzer': 'keyword_analyzer',            'analyzer': 'edge_ngram_analyzer'          },          'source': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'sourceID': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'summary': {            'type': 'text',            'fields': {                'summary_search' : {                  'type' : 'text',                  'analyzer': 'analyzer_shingle',                  'search_analyzer':'analyzer_shingle'                },                'summary_text' : {                  'type': 'text',                  'analyzer': 'english_analyzer'                },                'keyword': {                'type': 'keyword',                'ignore_above': 256              }              }          },          'text': {            'type': 'text',            'analyzer': 'english_analyzer',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'title': {            'type': 'text',            'fields': {                'title_search' : {                  'type' : 'text',                  'analyzer': 'analyzer_shingle',                  'search_analyzer':'analyzer_shingle'                },                'title_text' : {                  'type': 'text',                  'analyzer': 'english_analyzer'                },                'keyword': {                'type': 'keyword',                'ignore_above': 256              }              }          },          'topicNorm': {            'type': 'float'          },          'venue': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          }        }      }    }}"

#     command="{'settings':{\
#     'analysis':{\
#           'filter':{\
#             'alphabets_char_filter':{\
#               'pattern':'[^a-zA-Z]',\
#               'type':'pattern_replace',\
#               'replacement':''\
#             },\
#             'word_joiner':{\
#               'catenate_all':'true',\
#               'type':'word_delimiter'\
#             },\
#             'autocomplete_flter':{\
#               'type': 'edge_ngram',\
#               'min_gram': '1',\
#               'max_gram': '150'\
#             },\
#             'edge_ngram': {\
#               'token_chars': [\
#                 'letter',\
#                 'digit'\
#               ],\
#               'min_gram': '1',\
#               'type': 'edgeNGram',\
#               'max_gram': '50'\
#             },\
#             'english_poss_stemmer': {\
#               'name': 'possessive_english',\
#               'type': 'stemmer'\
#             },\
#             'filter_stop':{\
#              'type':'stop',\
#              'stopwords':  '_english_'\
#           },\
#             'filter_shingle':{\
#              'type':'shingle',\
#              'max_shingle_size':4,\
#              'min_shingle_size':2,\
#              'output_unigrams':'true'\
#           }\
#           },\
#           'analyzer': {\
#             'keyword_analyzer': {\
#               'filter': [\
#                 'lowercase',\
#                 'alphabets_char_filter',\
#                 'trim',\
#                 'english_poss_stemmer'\
#               ],\
#               'tokenizer': 'standard'\
#             },\
#             'autocomplete': {\
#               'filter': [\
#                 'lowercase',\
#                 'word_joiner',\
#                 'autocomplete_flter'\
#               ],\
#               'type': 'custom',\
#               'tokenizer': 'my_tokenizer'\
#             },\
#             'edge_ngram_analyzer': {\
#               'filter': [\
#                 'lowercase',\
#                 'alphabets_char_filter',\
#                 'trim',\
#                 'english_poss_stemmer',\
#                 'edge_ngram'\
#               ],\
#               'tokenizer': 'standard'\
#             },\
#             'analyzer_shingle':{\
#               'tokenizer':'standard',\
#                'filter':['standard', 'lowercase', 'stop', 'filter_shingle']\
#             },\
#             'english_analyzer': {\
#               'type': 'standard',\
#               'stopwords': '_english_'\
#             }\
#           },\
#           'tokenizer': {\
#             'my_tokenizer': {\
#               'pattern': ';',\
#               'type': 'pattern'\
#             }\
#           }\
#         }\
#   },\
#     'mappings': {\
#       'documents': {\
#         'properties': {\
#           'date': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'documentType': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'id': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'organizations': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'scientists': {\
#             'type': 'text',\
#             'fields': {\
#               'scientists_search': {\
#                 'type' : 'text',\
#                 'search_analyzer': 'keyword_analyzer',\
#                 'analyzer': 'edge_ngram_analyzer'\
#               },\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             },\
#             'search_analyzer': 'keyword_analyzer',\
#             'analyzer': 'edge_ngram_analyzer'\
#           },\
#           'source': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'sourceID': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'summary': {\
#             'type': 'text',\
#             'fields': {\
#                 'summary_search' : {\
#                   'type' : 'text',\
#                   'analyzer': 'analyzer_shingle',\
#                   'search_analyzer':'analyzer_shingle'\
#                 },\
#                 'summary_text' : {\
#                   'type': 'text',\
#                   'analyzer': 'english_analyzer'\
#                 },\
#                 'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#               }\
#           },\
#           'text': {\
#             'type': 'text',\
#             'analyzer': 'english_analyzer',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           },\
#           'title': {\
#             'type': 'text',\
#             'fields': {\
#                 'title_search' : {\
#                   'type' : 'text',\
#                   'analyzer': 'analyzer_shingle',\
#                   'search_analyzer':'analyzer_shingle'\
#                 },\
#                 'title_text' : {\
#                   'type': 'text',\
#                   'analyzer': 'english_analyzer'\
#                 },\
#                 'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#               }\
#           },\
#           'topicNorm': {\
#             'type': 'float'\
#           },\
#           'venue': {\
#             'type': 'text',\
#             'fields': {\
#               'keyword': {\
#                 'type': 'keyword',\
#                 'ignore_above': 256\
#               }\
#             }\
#           }\
#         }\
#       }\
#     }\
# }"
#     command = 'curl -XPUT "ist-deacuna-s1:9201/kimun_version5" -H '+header+' d '+command 

    
#     #url='http://128.230.247.186:9201/kimun_version5'
#     #Host="http://ist-deacuna-n1.syr.edu"
   
#     #curl -XPUT -H 'Content-type: application/json' --data "$_json" http://128.230.247.186:9201/kimun_version5
#     #curl -X PUT 'Content-Type: application/json' -d '{"settings":}' ist-deacuna-s1:9201
#     #curl -XPUT http://128.230.247.186:9201/kimun_version5 -H + header+" d "+command
#    # command = 'curl -XPUT' + header +url
#             #-H '+header+' d '+command 
#     #command = curl -XPUT 'Content-Type:application/json' http://128.230.247.186:9201/kimun_version5
#     # In[42]:

  

#     p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


#     # In[43]:


#     out, err = p.communicate()


#     # In[44]:



#     logging.info("Create index result : \n",out)
#     print("Create index result : \n",out)
#     logging.info("\n\n\nCreate index error : \n",err)
#     print("\n\n\nCreate index error : \n",err)


#     # In[ ]:

#!/usr/bin/env python
# coding: utf-8

# In[17]:


import subprocess
import os
import logging

# In[39]:
if __name__ == '__main__':

    command = "{  'settings': {    'analysis': {          'filter': {            'alphabets_char_filter': {              'pattern': '[^a-zA-Z]',              'type': 'pattern_replace',              'replacement': ''            },            'word_joiner': {              'catenate_all': 'true',              'type': 'word_delimiter'            },            'autocomplete_flter': {              'type': 'edge_ngram',              'min_gram': '1',              'max_gram': '150'            },            'edge_ngram': {              'token_chars': [                'letter',                'digit'              ],              'min_gram': '1',              'type': 'edgeNGram',              'max_gram': '50'            },            'english_poss_stemmer': {              'name': 'possessive_english',              'type': 'stemmer'            },            'filter_stop':{             'type':'stop',             'stopwords':  '_english_'          },            'filter_shingle':{             'type':'shingle',             'max_shingle_size':4,             'min_shingle_size':2,             'output_unigrams':'true'          }          },          'analyzer': {            'keyword_analyzer': {              'filter': [                'lowercase',                'alphabets_char_filter',                'trim',                'english_poss_stemmer'              ],              'tokenizer': 'standard'            },            'autocomplete': {              'filter': [                'lowercase',                'word_joiner',                'autocomplete_flter'              ],              'type': 'custom',              'tokenizer': 'my_tokenizer'            },            'edge_ngram_analyzer': {              'filter': [                'lowercase',                'alphabets_char_filter',                'trim',                'english_poss_stemmer',                'edge_ngram'              ],              'tokenizer': 'standard'            },            'analyzer_shingle':{              'tokenizer':'standard',               'filter':['standard', 'lowercase', 'stop', 'filter_shingle']            },            'english_analyzer': {              'type': 'standard',              'stopwords': '_english_'            }          },          'tokenizer': {            'my_tokenizer': {              'pattern': ';',              'type': 'pattern'            }          }        }  },    'mappings': {      'documents': {        'properties': {          'date': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'documentType': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'id': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'organizations': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'scientists': {            'type': 'text',            'fields': {              'scientists_search': {                'type' : 'text',                'search_analyzer': 'keyword_analyzer',                'analyzer': 'edge_ngram_analyzer'              },              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            },            'search_analyzer': 'keyword_analyzer',            'analyzer': 'edge_ngram_analyzer'          },          'source': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'sourceID': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'summary': {            'type': 'text',            'fields': {                'summary_search' : {                  'type' : 'text',                  'analyzer': 'analyzer_shingle',                  'search_analyzer':'analyzer_shingle'                },                'summary_text' : {                  'type': 'text',                  'analyzer': 'english_analyzer'                },                'keyword': {                'type': 'keyword',                'ignore_above': 256              }              }          },          'text': {            'type': 'text',            'analyzer': 'english_analyzer',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          },          'title': {            'type': 'text',            'fields': {                'title_search' : {                  'type' : 'text',                  'analyzer': 'analyzer_shingle',                  'search_analyzer':'analyzer_shingle'                },                'title_text' : {                  'type': 'text',                  'analyzer': 'english_analyzer'                },                'keyword': {                'type': 'keyword',                'ignore_above': 256              }              }          },          'topicNorm': {            'type': 'float'          },          'venue': {            'type': 'text',            'fields': {              'keyword': {                'type': 'keyword',                'ignore_above': 256              }            }          }        }      }    }}"


    # In[40]:


    header = "Content-Type: application/json"


    # In[41]:


    command = 'curl -XPUT "http://128.230.247.186:9201/kimun_version5" -H '+header+' d '+command 

    # In[42]:


    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    # In[43]:


    out, err = p.communicate()


    # In[44]:



    logging.info("Create index result : \n",out)
    print("Create index result : \n",out)
    logging.info("\n\n\nCreate index error : \n",err)
    print("\n\n\nCreate index error : \n",err)


    # In[ ]:





