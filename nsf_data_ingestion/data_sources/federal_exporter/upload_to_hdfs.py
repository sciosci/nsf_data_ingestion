import sys
import os
import findspark
findspark.init()
from pyspark.sql import SparkSession


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
    project_folder = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()
    convert_to_parquet(spark, project_folder)
