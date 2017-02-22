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
projects="arxiv medline nber pubmed_oa"

function check_or_create() {
    # Check or create a folder in hdfs
    if ! hdfs dfs -test -d "$1"
    then
        # delete folder
        hdfs dfs -mkdir "$1"
    fi
}

check_or_create "$projectpath/data"
check_or_create "$projectpath/data/raw"
check_or_create "$projectpath/data/processed"

for p in ${projects}
do
    check_or_create "$projectpath/data/raw/$p"
    check_or_create "$projectpath/data/processed/$p"
done