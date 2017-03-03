#!/usr/bin/env bash
# Prepare HDFS structure
# The structure of the project should be like this:
# nsf_project/
#  src/
#    nsf_data_ingestion/
#  data/
#    raw/
#      arxiv/
#      ...
#    processed/
#      arxiv/
#      ...

projectpath="$1"
projects="arxiv medline nber pubmed_oa federal_exporter grants_gov"
dirs=()
for p in ${projects}
do
    dirs+=("$projectpath/data/raw/$p")
    dirs+=("$projectpath/data/processed/$p")
    dirs+=("$projectpath/data/models/$p")
done
# create all dirs in bulk
hdfs dfs -mkdir -p ${dirs[@]}
# make all folders writeable
hdfs dfs -chmod -R a+wx "$projectpath/data"