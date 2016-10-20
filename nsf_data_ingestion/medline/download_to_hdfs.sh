#!/bin/bash
uploadpath="$1"
lastupdate=$(hdfs dfs -cat "$uploadpath"/LASTUPDATE || echo 0)
currentdate=$(date +%s)
# number of days between updates
secondsinaweek=$("$2"*60*60*24)

if ((currentdate > (lastupdate + secondsinaweek)));
then
  wget ftp://ftp.nlm.nih.gov/nlmdata/.medleasebaseline/gz/*.xml.gz
  wget ftp://ftp.nlm.nih.gov/nlmdata/.medlease/gz/*.xml.gz

  # check if folder exists
  if hdfs dfs -test -d "$uploadpath"
  then
    # delete folder
    hdfs dfs -rm -r "$uploadpath"
  else
    hdfs dfs -mkdir "$uploadpath"
  fi

  # puts files there
  hadoop fs -put ./*.xml.gz "$uploadpath"/
  # create file with last update (in Unit epoch)
  date +%s | hdfs dfs -put - "$uploadpath"/LASTUPDATE
else
  echo "File updated within the last week."
fi
