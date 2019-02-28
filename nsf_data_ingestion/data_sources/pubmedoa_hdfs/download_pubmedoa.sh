#!/bin/bash

# download
wget ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz

mkdir pubmedoa

# untar to pubmedoa folder
for f in *.tar.gz; do tar -xzf $f --directory pubmedoa/; done

python pubmedoa_hdfs.py "$1"
