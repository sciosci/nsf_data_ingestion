import sys
import os
import findspark
from subprocess import call
findspark.init()
from pyspark.sql import SparkSession

def delete_files(project_folder):
    call('hdfs dfs -rm -r %s/data/raw/federal_exporter/*' % project_folder, shell=True)

def upload_to_hdfs(start_year, end_year, project_folder):
    """Upload Federal Exporter files into Hadoop"""
    download_base_url = ("https://federalreporter.nih.gov/"+
                         "FileDownload/DownloadFile?"+
                         "fileToDownload=")
    commands = []
    for year in range(start_year, end_year+1):
        get_projects_cmd = ("wget " + download_base_url + \
                            "FedRePORTER_PRJ_X_FY%s.zip -nv") % year
        commands.append(get_projects_cmd)
    unzip_projects_to_hdfs = ('unzip -p "*FedRePORTER_PRJ_X_FY*.zip" | ' +
                     'hdfs dfs -put - ' +
                     '%s/data/raw/federal_exporter/projects.xml') % \
                     project_folder
    commands.append(unzip_projects_to_hdfs)
    for year in range(start_year, end_year+1):
        get_abstracts_cmd = ("wget " + download_base_url + \
                         "FedRePORTER_PRJABS_X_FY%s.zip -nv") % year
        commands.append(get_abstracts_cmd)
        
    unzip_abstracts_to_hdfs = ('unzip -p "*FedRePORTER_PRJABS_X_FY*.zip" | ' +
                     'hdfs dfs -put - ' +
                     '%s/data/raw/federal_exporter/abstracts.xml') % \
                     project_folder
    commands.append(unzip_abstracts_to_hdfs)
    
    for command in commands:
        print(command)
        if call(command, shell=True) != 0:
            raise
            
def convert_to_parquet(spark, project_folder):
    file_list = ['projects', 'abstracts']
    for filename in file_list:
        location = '%s/data/raw/federal_exporter/%s' % \
           (project_folder, filename)
        df = spark.read.\
            format('com.databricks.spark.xml').\
            options(rowTag='ROW').\
            load(location + '.xml')
        df.write.parquet(location + '.parquet')

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        "--packages com.databricks:spark-xml_2.11:0.4.1 pyspark-shell"
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    project_folder = sys.argv[3]
    spark = SparkSession.builder.getOrCreate()
    delete_files(project_folder)
    upload_to_hdfs(start_year, end_year, project_folder)
    convert_to_parquet(spark, project_folder)
    