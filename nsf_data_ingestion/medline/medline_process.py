import sys
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/config/')
import spark_config
from spark_config import *

class Parquet_Write(object):

  def __init__(self, data_path, parquet_path, libraries_list):
        """Return a Customer object whose name is *name* and starting
        balance is *balance*."""
        self.libraries_list = libraries_list
        self.data_path = data_path
        self.parquet_path = parquet_path
        self.exec_instance = exec_instance
	self.exec_mem = exec_mem
	self.exec_cores = exec_cores

  def createSession(self, libraries_list, exec_instance, exec_mem, exec_cores):
    spark = SparkSession.builder.\
            config("spark.executor.instances", exec_instance).\
	    config("spark.executor.memory", exec_mem).\
            config('spark.executor.cores', exec_cores).\
            config('spark.cores.max', exec_cores).\
            getOrCreate()
    for library in libraries_list:
        logging.info('Adding Librarier')	
        spark.sparkContext.addPyFile(library)    # adding libraries
        spark.sparkContext.addPyFile(library)
        # adding libraries
    return spark


  def parse_gzip_medline_str(self, gzip_str):
    logging.info('Importing Libraries')
    import unidecode as unidecode
    import pubmed_parser as pp
    filepath = gzip_str[0]
    gzip_content = gzip_str[1]
    _, file_name = path.split(filepath)
    # decompress gzip

    xml_string = gzip_content.split("\n", 3)[3];
    articles = pp.parse_medline_xml(xml_string)
    return [Row(file_name=file_name, **article_dict)
            for article_dict in articles]




  def process(self, data_path, parquet_path, libraries_list): 
    print(sys.path)
    logging.info('Creating Spark Session')
    spark = self.createSession(libraries_list, exec_instance, exec_mem, exec_cores, exec_cores)
    print("Reading from {} and writing to {}.".format(data_path, parquet_path))
    spark_fEill = spark.sparkContext.wholeTextFiles(os.path.join(data_path, '*.xml.gz'), minPartitions=10000)
    logging.info('Running Job')
    preprocess = spark_fEill.flatMap(self.parse_gzip_medline_str)
    logging.info('Writing to DF')
    medline_df = preprocess.toDF()
    
    window = Window.partitionBy(['pmid']).orderBy(desc('file_name'))
    # only get the last version of documents
    last_medline_df = medline_df.select(
        max('delete').over(window).alias('is_deleted'),
        rank().over(window).alias('pos'), '*').\
        where('is_deleted = False and pos = 1').\
        drop('is_deleted').drop('pos').drop('delete')
    
    if not call(["hdfs","dfs", "-test", "-d", parquet_path]):
      call(["hdfs","dfs", "-rm", "-r", "-f",  parquet_path])

    logging.info('Generating Parquet Files at - %s', parquet_path)
    last_medline_df.write.parquet(parquet_path)
    logging.info('Parquet Files Write Complete')
    spark.stop()


libraries_list = ['/home/ananth/nsf_data_ingestion/libraries/pubmed_parser_lib.zip', '/home/ananth/nsf_data_ingestion/libraries/unidecode_lib.zip']
obj1 = Parquet_Write('/user/ananth/medline/medline_data/','/user/ananth/medline/parquet',libraries_list)
obj1.process(obj1.data_path, obj1.parquet_path, obj1.libraries_list, 
	     spark_config.exec_instance, spark_config.exec_mem, spark_config.exec_cores,
             spark_config.exec_cores)


