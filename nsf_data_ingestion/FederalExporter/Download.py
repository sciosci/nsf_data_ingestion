
# coding: utf-8

# In[1]:

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


# In[2]:

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
start_year = sys.argv[1]
end_year = sys.argv[2]


# In[ ]:

# calling the function to download all the files
for year in range(start_year,end_year + 1):
    download_project_csv(year)                  # download the project file as per the year called
    download_abstract_csv(year)                 # download the abstract file as per the year called


# In[ ]:

# put the downloaded files in the hdfs folder
get_ipython().system('hdfs dfs -put FedRePORTER_PRJ_C_FY*.csv /user/msikri/FederalExporterDownload')
get_ipython().system('hdfs dfs -put FedRePORTER_PRJABS_C_FY*.csv /user/msikri/FederalExporterDownload')

