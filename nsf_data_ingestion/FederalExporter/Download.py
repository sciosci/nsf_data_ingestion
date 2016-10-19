
# coding: utf-8

# In[ ]:

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


# In[48]:

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

# calling the function to download all the files
for year in range(2004:2015):
    download_project_csv(year)                  # download the project file as per the year called
    download_abstract_csv(year)                 # download the abstract file as per the year called

