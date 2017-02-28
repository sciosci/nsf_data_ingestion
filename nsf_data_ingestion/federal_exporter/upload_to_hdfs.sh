#!/bin/bash

wget https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJ_X_FY{2004..2016}.zip -nv

# put projects xml in hdfs
unzip -p "*FedRePORTER_PRJ_X_FY*.zip" | hdfs dfs -put - "$1"/data/raw/federal_exporter/projects.xml

# put abstracts xml in hdfs
wget https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJABS_X_FY{2004..2016}.zip -nv

unzip -p "*FedRePORTER_PRJABS_X_FY*.zip" | hdfs dfs -put - "$1"/data/raw/federal_exporter/abstracts.xml