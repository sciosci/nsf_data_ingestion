"""Download xml-extract, save into HDFS, and create synopsis and forecast as parquet files
This script is required to be called with option

"""
import sys
import datetime
from subprocess import call
import findspark
findspark.init()
from pyspark.sql import SparkSession


def upload_xml_to_hdfs(spark, project_folder):
    """Get the link from xml-extract"""
    # go through each of the days, starting from today and try to download data
    today = datetime.date.today()
    count = 0
    while True:
        daystr = today.strftime("%Y%m%d")
        filename = "GrantsDBExtract" + daystr + "v2.zip"
        url = "https://www.grants.gov/web/grants/xml-extract.html?download=" + filename
        if call('wget ' + url + ' -O ' + filename + ' -nv', shell=True) == 0:
            break
        else:
            today = today - datetime.timedelta(days=1)
            count += 1
            if count >= 3:
                raise Exception("No file found from last 3 days")
    assert call('unzip ' + filename, shell=True) == 0
    # put the data into HDFS
    assert call('hdfs dfs -put *.xml ' + project_folder + '/data/raw/grats_gov', shell=True) == 0
    # load XMl and pull out synopsis and
    synopsis = \
        spark.read.format('com.databricks.spark.xml').\
        options(rowTag='OpportunitySynopsisDetail_1_0').\
        load(project_folder + '/data/raw/grats_gov/*.xml')
    forecast = \
        spark.read.format('com.databricks.spark.xml'). \
            options(rowTag='OpportunityForecastDetail_1_0'). \
            load(project_folder + '/data/raw/grats_gov/*.xml')
    synopsis.write.parquet(project_folder + '/data/raw/grants_gov/synopsis.parquet')
    forecast.write.parquet(project_folder + '/data/raw/grants_gov/forecast.parquet')


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    project_folder = sys.argv[1]
    upload_xml_to_hdfs(spark, project_folder)