import sys
import zipfile
import io
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/config/')
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/main/objects/')
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/main/config/')
from data_source_params import *
from constants import *
import spark_config
from spark_config import *
from config import *

class Parquet_Write(object):

    def __init__(self, param_list, libraries_list, exec_instance, exec_mem, exec_cores):
        """Return a Customer object whose name is *name* and starting
        balance is *balance*."""
        self.libraries_list = libraries_list
        self.param_list = param_list
        self.exec_instance = exec_instance
        self.exec_mem = exec_mem
        self.exec_cores = exec_cores

    def createSession(self):
        try: spark = SparkSession.builder.\
                config("spark.executor.instances", exec_instance).\
                config("spark.executor.memory", exec_mem).\
                config('spark.executor.cores', exec_cores).\
                config('spark.cores.max', exec_cores).\
                getOrCreate()
        except:
            print('Not Enough Resources')
            if spark:
                spark.stop()

        for library in self.libraries_list:
            logging.info('Adding Librarier')
            spark.sparkContext.addPyFile(library)    # adding libraries
            spark.sparkContext.addPyFile(library)
            # adding libraries
        return spark
    
    
    def zip_extract(self, x):
        in_memory_data = io.BytesIO(x[1])
        file_obj = zipfile.ZipFile(in_memory_data, "r")
        files = [i for i in file_obj.namelist()]
        return zip(files, [file_obj.open(file).read() for file in files])


    def parse_gzip_pubmed_str(self, gzip_str):
        logging.info('Importing Libraries')
        import unidecode as unidecode
        import pubmed_parser as pp
        import re
        filepath = gzip_str[0]
        gzip_content = gzip_str[1].decode("utf-8")
        gzip_content = re.sub("<\?xml.*\?>","",gzip_content)
#         if gzip_content.split('\n', 1)[0] in '<?xml version="1.0" encoding="UTF-8"?>':
#             gzip_content = gzip_content.split("\n", 3)[3];
        articles = pp.parse_pubmed_xml(gzip_content)
        return [Row(file_name=filepath, ** articles)]
    
    
    def read_hdfs_data(self, spark):
        data_path = self.param_list.get('xml_path')
        if(self.param_list.get('hdfs_read_type') == config.hdfs_read.BINARY):
            return spark.sparkContext.binaryFiles(os.path.join(data_path, '*.zip'), minPartitions=spark_config.minPartitions)
        elif(self.param_list.get('hdfs_read_type') == config.hdfs_read.WHOLEFILES):
            return spark.sparkContext.wholeTextFiles(os.path.join(data_path, '*.xml.gz'), minPartitions=spark_config.minPartitions)

        
    def process_hdfs(self, hdfs_data):
        if(self.param_list.get('data_source_name') == config.medline):
            return 0
        elif(self.param_list.get('data_source_name') == config.pubmed):
            return hdfs_data.flatMap(self.zip_extract).flatMap(self.parse_gzip_pubmed_str)
        
        
    def process(self):
        print(sys.path)
        logging.info('Creating Spark Session')
        spark = self.createSession()
        print("Reading from {} and writing to {}.".format(data_path, parquet_path))
        hdfs_data = self.read_hdfs_data(spark)
        preprocess = self.process_hdfs(hdfs_data)
        medline_df = preprocess.toDF()
        medline_df.show()
        spark.stop()
