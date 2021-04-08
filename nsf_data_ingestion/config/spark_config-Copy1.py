spark_home='/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/'
appname='nsf_data_engine'
exec_instance='4'
exec_mem='30g'
exec_cores='5'
exec_max_cores='5'

libraries_list = ['/home/sghosh08/nsf_new/nsf_data_ingestion/libraries/pubmed_parser_lib.zip', '/home/sghosh08/nsf_new/nsf_data_ingestion/libraries/unidecode_lib.zip']

#HDFS Parameters
minPartitions = 10000
