#!/bin/bash

# download
wget ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz

mkdir pubmedoa

# untar to pubmedoa folder
tar -xzf comm_use.A-B.xml.tar.gz --directory pubmedoa/
python pubmedoa_hdfs.py
