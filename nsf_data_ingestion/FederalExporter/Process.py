
# coding: utf-8

# ## Download

# In[1]:

# Script to download and process Grant data
# This script required 3 arguments to be passed to it
    # 1. Start Year - Year from which you want the data to be extracted and processes
    # 2. End Year - Final year till which you want the data to be extracted and processed
    # 4. Base path - Path in hdfs where the parquet files needs to be pushed and the end of script


# In[2]:

# function to download the files using wget utility
# unzip utility to unzip the files
# -O option in wget to give filename the output
# -o option in unzip to overwrite the existing file
# the project csv file download

def download_project_csv(year):
    url = "https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJ_C_FY" + str(year) +".zip"
    filename = "FedRePORTER_PRJ_C_FY" + str(year) + ".zip"
    get_ipython().system('wget -O $filename $url')
    get_ipython().system('unzip -o $filename')


# In[3]:

# function to download the files using wget utility
# unzip utility to unzip the files
# -O option in wget to give filename the output
# -o option in unzip to overwrite the existing file
# the abstract csv file download

def download_abstract_csv(year):
    url2 = "https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJABS_C_FY" + str(year) +".zip"
    filename2 = "FedRePORTER_PRJABS_C_FY" + str(year) + ".zip"
    get_ipython().system('wget -O $filename2 $url2')
    get_ipython().system('unzip -o $filename2')


# In[ ]:

# input the start and end year with help of system argument
import sys
start_year = input("Enter the Start Year: ")
start_year = int(start_year)
end_year = input("Enter the End Year: ")
end_year = int(end_year)


# In[ ]:

# calling the function to download all the files
for year in range(start_year,end_year + 1):
    download_project_csv(year)                  # download the project file as per the year called
    download_abstract_csv(year)                 # download the abstract file as per the year called


# # Process

# In[4]:

# importing the pandas library
import pandas as pd 
import numpy as np
import sys
import os


# In[5]:

# assigning the column name for the read_csv function
colnames = ['SM_Application_ID', 'Project_Terms', 'Project_Title', 'Department', 'Agency', 'IC_Center', 'Project_Number'
            , 'Project_Start_Date', 'Project_End_Date','Contact_PI_Project_Leader','Other_PIs','Congressional_District'
           , 'DUNS_Number', 'Organizational_Name','Organization_City', 'Organization_State', 'Organization_Zip'
           , 'Organization_Country','Budget_Start_Date','Budget_End_Date', 'CFDA_Code', 'FY', 'FY_Total_Cost'
           ,'FY_Total_Cost_Sub_Projects'] 


# In[6]:

# function to process the document file
def document(year):
    filename = "FedRePORTER_PRJ_C_FY" + str(year) + ".csv"
    #print(filename)
    Grants = pd.read_csv(filename, header=None, names=colnames, encoding = "ISO-8859-1", low_memory = False)
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
    
    # droppng the first row
    my_dataframe = my_dataframe.ix[1:]

    my_dataframe = my_dataframe.drop('Summary', 1) # removing the summary from the data frame

    # reading the abstracts from the abstract files
    filename2 = "FedRePORTER_PRJABS_C_FY" + str(year) + ".csv"
    colnames_abs = ['SM_Application_ID', 'Abstract']
    Grants_abs = pd.read_csv(filename2, header=None, names=colnames_abs, encoding = "ISO-8859-1")

    # Deleting the first row for the abstract table
    Grants_abs = Grants_abs.ix[1:]
    
    # left merge for document table
    document_merge = pd.merge(left = my_dataframe, right = Grants_abs, how = 'left'
                              , left_on = 'Grant_ID', right_on = 'SM_Application_ID')

    # deleting the redundancy by deleting the column SM_application_ID 
    document_merge = document_merge.drop('SM_Application_ID', 1) # removing the SM_APPLICATION_ID from the data frame
    
    # creating a new csv and writing the contents from data frame
    outputfile = "Grant_Document_" + str(year) + ".csv"
    #print(outputfile)
    document_merge.to_csv(outputfile, index=False)


# In[7]:

# function to process the scientist file
def scientist(year):
    filename = "FedRePORTER_PRJ_C_FY" + str(year) + ".csv"
    Grants = pd.read_csv(filename, header=None, names=colnames, encoding = "ISO-8859-1", low_memory = False)
    
    # processing
    
    # save the column list into seperate variables
    Grants_ID = Grants.SM_Application_ID.tolist()            # Grant ID column
    Project_Title = Grants.Project_Title.tolist()            # Project_Title column
    Summary = Grants.Project_Terms.tolist()                  # Project Summary column
    Year = Grants.FY.tolist()                                # Year column
    Start_Date = Grants.Project_Start_Date.tolist()          # Start_Date column
    End_Date = Grants.Project_End_Date.tolist()              # End_Date column
    
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
    
    splits = my_dataframe_test['Name'].str.split()
    
    # extract the first and Last Name from the string
    Last_Name = splits.str[0]   # Last_Name
    First_Name = splits.str[1]  # First Name
    Middle_Name = splits.str[2] # Middle Name

    #type(Last_Name) # checking the type of variable

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
    splits2 = my_dataframe_sci3['Other_PIs'].str.split()
    
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
    
    # concatinate for the final version
    # stack the DataFrames on top of each other
    my_dataframe_scientist_final = pd.concat([my_dataframe_scientist, my_dataframe_sci4])
    my_dataframe_scientist_final
    
    # resetting the index
    my_dataframe_scientist_final = my_dataframe_scientist_final.reset_index()
    del my_dataframe_scientist_final['index']
    
    # to add a new column to dataframe with Scientist_ID
    my_dataframe_scientist_final['IM_ID'] = my_dataframe_scientist_final['First_Name'].str[:1].astype(str).str.cat(my_dataframe_scientist_final['Last_Name'].str[:1].astype(str), sep='')
    my_dataframe_scientist_final['Scientist_ID'] = my_dataframe_scientist_final['IM_ID'].astype(str).str.cat(my_dataframe_scientist_final['Grant_ID'].astype(str), sep='')

    # deleting the intermediate column
    del my_dataframe_scientist_final['IM_ID']
    my_dataframe_scientist_final
    
    # creating a new csv and writing the contents from data frame
    outputfile = "Grant_Scientist_" + str(year) + ".csv"
    #print(outputfile)
    
    # creating a new csv and writing the contents from data frame
    my_dataframe_scientist_final.to_csv(outputfile, index=False)


# In[8]:

# function to process the organization file
def organization(year):
    filename = "FedRePORTER_PRJ_C_FY" + str(year) + ".csv"
    Grants = pd.read_csv(filename, header=None, names=colnames, encoding = "ISO-8859-1", low_memory = False)
    
    # processing
    
    # save the column list into seperate variables
    Grants_ID = Grants.SM_Application_ID.tolist()            # Grant ID column
    Project_Title = Grants.Project_Title.tolist()            # Project_Title column
    Summary = Grants.Project_Terms.tolist()                  # Project Summary column
    Year = Grants.FY.tolist()                                # Year column
    Start_Date = Grants.Project_Start_Date.tolist()          # Start_Date column
    End_Date = Grants.Project_End_Date.tolist()              # End_Date column
    
    # save the column list into seperate variables
    Organization_Name = Grants.Organizational_Name.tolist()            # Organization Name column
    Organization_City = Grants.Organization_City.tolist()            # Organization City column
    Organization_State = Grants.Organization_State.tolist()          # Organization State column
    Organization_Zip = Grants.Organization_Zip.tolist()              # Organization Zip column
    
    # creating an initial dataframe with only Organization Name
    my_dataframe_org = pd.DataFrame(Organization_Name, columns=["Organization_Name"]) # creating a dataframe from a list
    
    # creating a series 
    series_City = pd.Series(Organization_City)      # series for city column
    series_State = pd.Series(Organization_State)    # series for state column
    series_Zip = pd.Series(Organization_Zip)        # series for Zip column
    
    # adding the series to the dataframe
    my_dataframe_org['Organization_City'] = series_City.values
    my_dataframe_org['Organization_State'] = series_State.values
    my_dataframe_org['Organization_Zip'] = series_Zip.values
    
    # deleting the first row from dataframe
    my_dataframe_org = my_dataframe_org.ix[1:]
    
    # adding the Grant ID column to the dataframe
    series_grantID = pd.Series(Grants_ID)
    series_grantID_2 = series_grantID.drop([0]) # dropping the first value
    my_dataframe_org['Grant_ID'] = series_grantID_2.values
    
    # creating a Organizaton ID column in organizational dataframe
    my_dataframe_org['IM_ID2'] = my_dataframe_org['Organization_Name'].str[:2].astype(str).str.cat(my_dataframe_org['Organization_City'].str[:2].astype(str), sep='')
    my_dataframe_org['Org_ID'] = my_dataframe_org['IM_ID2'].astype(str).str.cat(my_dataframe_org['Grant_ID'].astype(str), sep='')

    # deleting the intermediate column
    del my_dataframe_org['IM_ID2']
    
    # creating a new csv and writing the contents from data frame
    outputfile = "Grant_Organization_" + str(year) + ".csv"
    #print(outputfile)
    
    # creating a new csv and writing the contents from data frame
    my_dataframe_org.to_csv(outputfile, index=False) 


# In[ ]:

# calling the function to process the document file
# input the start and end year
import sys
start_year = input("Enter the Start Year: ")
start_year = int(start_year)
end_year = input("Enter the End Year: ")
end_year = int(end_year)

# call the function with the help of for loop
for year in range(start_year,end_year + 1):
    document(year)
    scientist(year)
    organization(year)


# In[11]:

# adding the file to the hadoop distirbuted file system
get_ipython().system('hdfs dfs -put Grant_Document_*.csv /user/msikri')
get_ipython().system('hdfs dfs -put Grant_Scientist_*.csv /user/msikri')
get_ipython().system('hdfs dfs -put Grant_Organization_*.csv /user/msikri')


# ## SPARK Connection

# In[1]:

import os
os.environ['SPARK_HOME'] ="/opt/cloudera/parcels/SPARK2/lib/spark2"
import findspark
findspark.init()


# In[2]:

from pyspark.sql import SparkSession

spark = SparkSession.builder.    appName("MEDLINE-TFIDF").    enableHiveSupport().    config('spark.yarn.appMasterEnv.PYSPARK_PYTHON', '/home/deacuna/anaconda3/bin/python').    config('spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON', '/home/deacuna/anaconda3/bin/python').    getOrCreate()


# ## Parquet Creation

# In[14]:

def document_parquet(year):
    fileread = "Grant_Document_" + str(year) + ".csv"
    # Reading the csv file format
    document_csv = spark.read.format("csv").option("header", "true").load(fileread)
    
    # convert it to parquet format
    fileout = "Grant_Document_" + str(year) + ".parquet"
    basepath = sys.argv[3]                                   # system input for path variable to store the file
    finalpath = os.path.join(basepath,fileout)               # combining to create the final path to store parquet file
    document_csv.write.parquet(finalpath)                    # writing to a parquet file


# In[15]:

def scientist_parquet(year):
    fileread2 = "Grant_Scientist_" + str(year) + ".csv"
    # Reading the csv file format
    document_sci = spark.read.format("csv").option("header", "true").load(fileread2)

    # convert it to parquet format
    fileout = "Grant_Scientist_" + str(year) + ".parquet"
    basepath = sys.argv[3]                                      # system input for path variable to store the file
    finalpath = os.path.join(basepath,fileout)                  # combining to create the final path to store parquet file
    document_sci.write.parquet(finalpath)                       # writing to a parquet file


# In[16]:

def organization_parquet(year):
    fileread3 = "Grant_Organization_" + str(year) + ".csv"
    # Reading the csv file format
    document_org = spark.read.format("csv").option("header", "true").load(fileread3)

    # convert it to parquet format
    fileout = "Grant_Organization_" + str(year) + ".parquet"
    basepath = sys.argv[3]                                        # system input for path variable to store the file
    finalpath = os.path.join(basepath,fileout)                    # combining to create the final path to store parquet file
    document_org.write.parquet(finalpath)                         # writing to a parquet file


# In[ ]:

# call the function with the help of for loop
import sys

for year in range(start_year,end_year+1):
    document_parquet(year)
    scientist_parquet(year)
    organization_parquet(year)


# ## End of Script ##

# In[ ]:



