#spark_home='/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/'
spark_home='/opt/cloudera/parcels/SPARK2/lib/spark2/'
appname='nsf_data_engine'
exec_instance='4'
exec_mem='30g'
exec_cores='5'
exec_max_cores='5'

#HDFS Parameters
minPartitions = 10000


#/home/eileen/nsf_data_ingestion/libraries/pubmed_parser-0.1.0-py3.6.egg')
#spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/Unidecode-1.1.1-py3.6.egg')