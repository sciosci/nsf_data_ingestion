#!/usr/bin/env python
# coding: utf-8

# In[43]:


import subprocess
import os
import logging


# In[44]:

if __name__ == '__main__':

    healthcheck = 'curl -XGET "http://128.230.247.186:9201/_cat/indices"'


    # In[45]:



    p_healthcheck = subprocess.Popen(healthcheck, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    # In[46]:


    out_health, err_health = p_healthcheck.communicate()


    # In[47]:



    logging.info("Last Snap Shot : ",out_health)
    print("Last Snap Shot : ",out_health)


    # In[48]:


    delete_kimun = 'curl -XDELETE "http://128.230.247.186:9201/kimun_version5"'


    # In[49]:



    p_deletekimun = subprocess.Popen(delete_kimun, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


    # In[50]:


    out_delete, err_delete = p_deletekimun.communicate()


    # In[51]:


    logging.info("delete index error : \n ",out_delete)
    print("delete index error : \n",out_delete)
    logging.info("\n\n\delete index error : \n",err_delete)
    print("\n\n\delete index error : \n",err_delete)

    # In[ ]:




