
# coding: utf-8

# # Grant Python Script

# In[ ]:

### Script: To read grant dataset and extract useful information ####
### Author: Manas Sikri ###
### Date: 09/29/2016 ###
### Syracuse University: School of Information Studies ###


# # Document File

# In[ ]:

################################## Document File #####################################################


# In[55]:

# importing the pandas library
import pandas as pd 
import numpy as np


# In[56]:

# assigning the column name for the read_csv function
colnames = ['SM_Application_ID', 'Project_Terms', 'Project_Title', 'Department', 'Agency', 'IC_Center', 'Project_Number'
            , 'Project_Start_Date', 'Project_End_Date','Contact_PI_Project_Leader','Other_PIs','Congressional_District'
           , 'DUNS_Number', 'Organizational_Name','Organization_City', 'Organization_State', 'Organization_Zip'
           , 'Organization_Country','Budget_Start_Date','Budget_End_Date', 'CFDA_Code', 'FY', 'FY_Total_Cost'
           ,'FY_Total_Cost_Sub_Projects'] 

colnames # view the column names specified


# In[57]:

# reading the .csv file and storing it in grants variable
Grants = pd.read_csv('FedRePORTER_PRJ_C_FY2015.csv', header=None, names=colnames, encoding = "ISO-8859-1")


# In[58]:

Grants # view the grant content


# ###### Transformation for document file

# In[59]:

## transforming the data from grants to be included in a different CSV file

# save the column list into seperate variables
Grants_ID = Grants.SM_Application_ID.tolist()            # Grant ID column
Project_Title = Grants.Project_Title.tolist()            # Project_Title column
Summary = Grants.Project_Terms.tolist()                  # Project Summary column
Year = Grants.FY.tolist()                                # Year column
Start_Date = Grants.Project_Start_Date.tolist()          # Start_Date column
End_Date = Grants.Project_End_Date.tolist()              # End_Date column

my_dataframe = pd.DataFrame(Grants_ID, columns=["Grant_ID"]) # creating a dataframe from a list

# creating a series 
series_title = pd.Series(Project_Title)  # series for Project_Title column
series_summary = pd.Series(Summary)      # series for Project Summary column
series_year = pd.Series(Year)            # series for Year column
series_start = pd.Series(Start_Date)     # series for Start_Date column
series_end = pd.Series(End_Date)         # series for End_Date column

# adding the series to the dataframe
my_dataframe['Project_Title'] = series_title.values
my_dataframe['Summary'] = series_summary.values
my_dataframe['Year'] = series_year.values
my_dataframe['Start_Date'] = series_start.values
my_dataframe['End_Date'] = series_end.values


# In[60]:

# droppng the first row
my_dataframe = my_dataframe.ix[1:]

my_dataframe = my_dataframe.drop('Summary', 1) # removing the summary from the data frame

# reading the abstracts from the abstract files
colnames_abs = ['SM_Application_ID', 'Abstract']
Grants_abs = pd.read_csv('FedRePORTER_PRJABS_C_FY2015.csv', header=None, names=colnames_abs, encoding = "ISO-8859-1")

# Deleting the first row for the abstract table
Grants_abs = Grants_abs.ix[1:]


# In[61]:

my_dataframe


# ###### Merge and Deleting the redundancy

# In[62]:

# left merge for document table
document_merge = pd.merge(left = my_dataframe, right = Grants_abs, how = 'left'
                              , left_on = 'Grant_ID', right_on = 'SM_Application_ID')

# deleting the redundancy by deleting the column SM_application_ID 
document_merge = document_merge.drop('SM_Application_ID', 1) # removing the SM_APPLICATION_ID from the data frame


# In[14]:

document_merge


# ###### Writing the document.csv

# In[63]:

# creating a new csv and writing the contents from data frame
document_merge.to_csv('Grants_Document_2015.csv', index=False) 


# # Scientist File

# In[ ]:

#################################################### Scientist File ###################################################


# ###### Working with scientist name

# In[64]:

# save the column list into seperate variables
Scientist_Name = Grants.Contact_PI_Project_Leader.tolist() 

# split the name into first and the last name
## creating a list
Name_List = Grants.Contact_PI_Project_Leader.tolist()
# Creating a dataframe from a list
my_dataframe_test = pd.DataFrame(Name_List, columns=["Name"])

# deleting the first row from dataframe
my_dataframe_test = my_dataframe_test.ix[1:]

my_dataframe_test['Name'] = my_dataframe_test['Name'].str.replace('.','')

# replacing commas with space
my_dataframe_test['Name'] = my_dataframe_test['Name'].str.replace('\,','')

my_dataframe_test


# In[65]:

splits = my_dataframe_test['Name'].str.split()


# In[66]:

# extract the first and Last Name from the string
Last_Name = splits.str[0]   # Last_Name
First_Name = splits.str[1]  # First Name
Middle_Name = splits.str[2] # Middle Name

type(Last_Name) # checking the type of variable

# contactinate two data frames along the columns
my_dataframe_scientist = pd.concat([First_Name, Last_Name, Middle_Name], axis=1) 
my_dataframe_scientist.columns = ['First_Name', 'Last_Name', 'Middle_Name']

# adding the Grant ID column to the dataframe
series_grantID = pd.Series(Grants_ID)
series_grantID_2 = series_grantID.drop([0]) # dropping the first value

series_grantID_2
my_dataframe_scientist['Grant_ID'] = series_grantID_2.values

# add the columns roles to the scienstist dataframe
my_dataframe_scientist['Role'] = pd.Series(['PI' for x in range(len(my_dataframe_scientist.index)+1)])

len(my_dataframe_scientist.index)+1

my_dataframe_scientist # display the scientist dataframe


# ###### Working with Other PI column

# In[67]:

# extracting other PI columns from grant csv file
# save the column list into seperate variables
Other_PIs = Grants.Other_PIs.tolist()
Grant_ID2 = Grants.SM_Application_ID.tolist()

# creating a series 
series_other_PIs = pd.Series(Other_PIs)
#series_other_Grant_ID2 = pd.Series(Grant_ID2)

my_dataframe_sci2 = pd.DataFrame(Grant_ID2, columns=["Grant_ID"]) # creating a dataframe from a list

# adding the series to the dataframe
my_dataframe_sci2['Other_PIs'] = series_other_PIs.values
# deleting the first row from dataframe
my_dataframe_sci2 = my_dataframe_sci2.ix[1:]

# deleting all the null values
my_dataframe_sci2 = my_dataframe_sci2[my_dataframe_sci2.Other_PIs.notnull()]

# replacing the commas with blank value
my_dataframe_sci2['Other_PIs'] = my_dataframe_sci2['Other_PIs'].str.replace('\,','')

my_dataframe_sci2 # display the intermediate file


# In[68]:

# splitting the columns into rows
import numpy as np
my_dataframe_sci3 = pd.concat([pd.Series(row['Grant_ID'] , row['Other_PIs'].split(';'))
          for _, row in my_dataframe_sci2.iterrows()]).reset_index()
my_dataframe_sci3.columns = ['Other_PIs', 'Grant_ID']

# deleting the rows with blank Value
my_dataframe_sci3 = my_dataframe_sci3[my_dataframe_sci3.Other_PIs != ' ']

# replacing commas with space
my_dataframe_sci3['Other_PIs'] = my_dataframe_sci3['Other_PIs'].str.replace('\.','')

my_dataframe_sci3 = my_dataframe_sci3[my_dataframe_sci3.Other_PIs != '']


# In[69]:

splits2 = my_dataframe_sci3['Other_PIs'].str.split()
splits2


# In[70]:

# extract the first and Last Name from the string
Last_Name2 = splits2.str[0]   # Last_Name
First_Name2 = splits2.str[1]  # First Name
Middle_Name2 = splits2.str[2] # Middle Name

my_dataframe_sci4 = pd.concat([First_Name2, Last_Name2, Middle_Name2], axis=1) # contactinate two data frames along the columns
my_dataframe_sci4.columns = ['First_Name', 'Last_Name', 'Middle_Name']

GrantID_OT = my_dataframe_sci3.Grant_ID.tolist()
series_GrantID_other =pd.Series(GrantID_OT)
my_dataframe_sci4['Grant_ID'] = series_GrantID_other.values

# add the columns roles to the scienstist dataframe
my_dataframe_sci4['Role'] = 'Other PI'
my_dataframe_sci4


# ###### Concatinate PI and other PI dataset

# In[71]:

# concatinate for the final version
# stack the DataFrames on top of each other
my_dataframe_scientist_final = pd.concat([my_dataframe_scientist, my_dataframe_sci4])
my_dataframe_scientist_final


# In[72]:

# resetting the index
my_dataframe_scientist_final = my_dataframe_scientist_final.reset_index()
del my_dataframe_scientist_final['index']


# ###### Scientist ID creation

# In[73]:

# to add a new column to dataframe with Scientist_ID

my_dataframe_scientist_final['IM_ID'] = my_dataframe_scientist_final['First_Name'].str[:1].astype(str).str.cat(my_dataframe_scientist_final['Last_Name'].str[:1].astype(str), sep='')
my_dataframe_scientist_final['Scientist_ID'] = my_dataframe_scientist_final['IM_ID'].astype(str).str.cat(my_dataframe_scientist_final['Grant_ID'].astype(str), sep='')

# deleting the intermediate column
del my_dataframe_scientist_final['IM_ID']
my_dataframe_scientist_final


# ###### Writing to Scientist.csv file

# In[74]:

# creating a new csv and writing the contents from data frame
my_dataframe_scientist_final.to_csv('Grants_Scientist_2015.csv', index=False) 


# # Organization File

# In[ ]:

##################################### Organization File #########################################


# In[75]:

# save the column list into seperate variables
Organization_Name = Grants.Organizational_Name.tolist()          # Organization Name column
Organization_City = Grants.Organization_City.tolist()            # Organization City column
Organization_State = Grants.Organization_State.tolist()          # Organization State column
Organization_Zip = Grants.Organization_Zip.tolist()              # Organization Zip column


# In[76]:

# creating an initial dataframe with only Organization Name
my_dataframe_org = pd.DataFrame(Organization_Name, columns=["Organization_Name"]) # creating a dataframe from a list


# In[77]:

# creating a series 
series_City = pd.Series(Organization_City)      # series for city column
series_State = pd.Series(Organization_State)    # series for state column
series_Zip = pd.Series(Organization_Zip)        # series for Zip column


# In[78]:

# adding the series to the dataframe
my_dataframe_org['Organization_City'] = series_City.values
my_dataframe_org['Organization_State'] = series_State.values
my_dataframe_org['Organization_Zip'] = series_Zip.values


# In[79]:

# deleting the first row from dataframe
my_dataframe_org = my_dataframe_org.ix[1:]


# In[80]:

# adding the Grant ID column to the dataframe
series_grantID = pd.Series(Grants_ID)
series_grantID_2 = series_grantID.drop([0]) # dropping the first value
my_dataframe_org['Grant_ID'] = series_grantID_2.values


# In[33]:

my_dataframe_org.head()


# ###### Creating Organization ID

# In[81]:

# creating a Organizaton ID column in organizational dataframe
#my_dataframe_org['Org_ID'] = my_dataframe_org['Organization_Name'].str[:2] + my_dataframe_org['Organization_City'].str[:2] + my_dataframe_org['Grant_ID'].str[:]
#my_dataframe_org.head()

my_dataframe_org['IM_ID2'] = my_dataframe_org['Organization_Name'].str[:2].astype(str).str.cat(my_dataframe_org['Organization_City'].str[:2].astype(str), sep='')
my_dataframe_org['Org_ID'] = my_dataframe_org['IM_ID2'].astype(str).str.cat(my_dataframe_org['Grant_ID'].astype(str), sep='')



# deleting the intermediate column
del my_dataframe_org['IM_ID2']
my_dataframe_org.tail()


# ###### Writing to Organizational.csv

# In[82]:

# creating a new csv and writing the contents from data frame
my_dataframe_org.to_csv('Grants_Organizational_2015.csv', index=False) 


# In[ ]:

################################ End of Program ####################################


# ###### Files to Hadoop Cluster

# In[83]:

# Writing the csv files to the Hadoop cluster
get_ipython().system('hdfs dfs -put Grants_Document_2015.csv            ')
get_ipython().system('hdfs dfs -put Grants_Scientist_2015.csv')
get_ipython().system('hdfs dfs -put Grants_Organizational_2015.csv')


# In[3]:

import os
os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/CDH-5.8.0-1.cdh5.8.0.p0.42/lib/spark"


# In[4]:

import findspark
findspark.init()


# In[5]:

import pyspark
conf = pyspark.SparkConf().    setAppName('test_app').    set('spark.yarn.appMasterEnv.PYSPARK_PYTHON', '/home/deacuna/anaconda3/bin/python').    set('spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON', '/home/deacuna/anaconda3/bin/python').    setMaster('yarn-client').    set('executor.memory', '1g').    set('spark.yarn.executor.memoryOverhead', '4098').    set('spark.sql.codegen', 'true').    set('spark.yarn.executor.memory', '500m').    set('yarn.scheduler.minimum-allocation-mb', '500m').    set('spark.dynamicAllocation.maxExecutors', '3').    set('jars', 'hdfs://eggs/graphframes-0.1.0-spark1.6.jar').    set('spark.driver.maxResultSize', '4g')


# In[42]:

from pyspark.sql import SQLContext, HiveContext
sc = pyspark.SparkContext(conf=conf)
sqlContext = HiveContext(sc)


# In[84]:

# Document File 
# Converting to a parquet file format

# Reading the csv file format
document_csv = pd.read_csv('Grants_Document_2015.csv')
document_df = pd.DataFrame(document_csv)

# converting each column to string in dataframe
document_df.Grant_ID = document_df.Grant_ID.astype(str)
document_df.Project_Title = document_df.Project_Title.astype(str)
document_df.Year = document_df.Year.astype(str)
document_df.Start_Date = document_df.Start_Date.astype(str)
document_df.End_Date = document_df.End_Date.astype(str)
document_df.Abstract = document_df.Abstract.astype(str)

# converting the dataframe to a spark dataframe using SQLcontext
document_spark_df = sqlContext.createDataFrame(document_df)

# convert it to parquet format
document_spark_df.write.parquet('Grant_Document_2015.parquet')


# In[85]:

# Converting to a parquet file format

# Reading the csv file format
scientist_csv = pd.read_csv('Grants_Scientist_2015.csv')
scientist_df = pd.DataFrame(scientist_csv)

# converting each column to string in dataframe
scientist_df.First_Name = scientist_df.First_Name.astype(str)
scientist_df.Last_Name = scientist_df.Last_Name.astype(str)
scientist_df.Middle_Name = scientist_df.Middle_Name.astype(str)
scientist_df.Grant_ID = scientist_df.Grant_ID.astype(str)
scientist_df.Role = scientist_df.Role.astype(str)
scientist_df.Scientist_ID = scientist_df.Scientist_ID.astype(str)

# converting the dataframe to a spark dataframe using SQLcontext
scientist_spark_df = sqlContext.createDataFrame(scientist_df)

# convert it to parquet format
scientist_spark_df.write.parquet('Grant_Scientist_2015.parquet')


# In[86]:

# Converting to a parquet file format

# Reading the csv file format
org_csv = pd.read_csv('Grants_Organizational_2015.csv')
org_df = pd.DataFrame(org_csv)

# converting each column to string in dataframe
org_df.Organization_Name = org_df.Organization_Name.astype(str)
org_df.Organization_City = org_df.Organization_City.astype(str)
org_df.Organization_State = org_df.Organization_State.astype(str)
org_df.Organization_Zip = org_df.Organization_Zip.astype(str)
org_df.Grant_ID = org_df.Grant_ID.astype(str)
org_df.Org_ID = org_df.Org_ID.astype(str)

# converting the dataframe to a spark dataframe using SQLcontext
org_spark_df = sqlContext.createDataFrame(org_df)

# convert it to parquet format
org_spark_df.write.parquet('Grant_Organization_2015.parquet')


# In[5]:

document_spark_df_parquet = sqlContext.read.parquet('Grant_Document.parquet')


# In[6]:

document_spark_df_parquet.show()


# In[116]:

sc.stop()

