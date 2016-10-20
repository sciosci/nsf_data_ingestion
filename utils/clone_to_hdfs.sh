#!/bin/bash

git clone https://github.com/sciosci/nsf_data_ingestion

hdfs dfs -rm -r "$1"

hdfs dfs -put nsf_data_ingestion "$1"/nsf_data_ingestion
