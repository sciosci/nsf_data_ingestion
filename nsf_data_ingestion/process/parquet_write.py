import sys
import zipfile
import io
import logging
logging.getLogger().setLevel(logging.INFO)
from nsf_data_ingestion.spark_config import *
from nsf_data_ingestion import data_source_params, spark_config, nsf_config


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
        try: spark = SparkSession.builder.getOrCreate()
                # config("spark.executor.instances", exec_instance).\
                # config("spark.executor.memory", exec_mem).\
                # config('spark.executor.cores', exec_cores).\
                # config('spark.cores.max', exec_cores).\
        except:
            print('Not Enough Resources')
            if spark:
                spark.stop()

        for library in self.libraries_list:
            logging.info('Adding Librarier', library)
            spark.sparkContext.addPyFile(library)    # adding libraries
            spark.sparkContext.addPyFile(library)
            # adding libraries
        return spark
    


    def read_hdfs_data(self, spark):
        data_path = self.param_list.get('xml_path')
        parquet_path = self.param_list.get('parquet_path')
        print("Reading from {} and writing to {}.".format(data_path, parquet_path))
        if(self.param_list.get('hdfs_read_type') == nsf_config.hdfs_read_BINARY):
            return spark.sparkContext.binaryFiles(os.path.join(data_path, '*.zip'), minPartitions=spark_config.minPartitions)
        elif(self.param_list.get('hdfs_read_type') == nsf_config.hdfs_read_WHOLEFILES):
            return spark.sparkContext.wholeTextFiles(os.path.join(data_path, '*.xml.gz'), minPartitions=spark_config.minPartitions)


    def zip_extract(self, x):
        in_memory_data = io.BytesIO(x[1])
        file_obj = zipfile.ZipFile(in_memory_data, "r")
        files = [i for i in file_obj.namelist()]
        return zip(files, [file_obj.open(file).read() for file in files])


    def parse_gzip_pubmed_str(self, gzip_str):
        logging.info('Importing Libraries')
        import pubmed_parser as pp
        import re
        filepath = gzip_str[0]
        gzip_content = gzip_str[1].decode("utf-8")
        gzip_content = re.sub("<\?xml.*\?>","",gzip_content)
#         if gzip_content.split('\n', 1)[0] in '<?xml version="1.0" encoding="UTF-8"?>':
#             gzip_content = gzip_content.split("\n", 3)[3];
        articles = pp.parse_pubmed_xml(gzip_content)
        return [Row(file_name=filepath, ** articles)]

    def parse_gzip_medline_str(self, gzip_str):
        logging.info('Importing Libraries')
        import pubmed_parser as pp
        filepath = gzip_str[0]
        gzip_content = gzip_str[1]
        _, file_name = path.split(filepath)
        # decompress gzip

        xml_string = gzip_content.split("\n", 3)[3];
        articles = pp.parse_medline_xml(xml_string)
        return [Row(file_name=file_name, **article_dict)
                for article_dict in articles]


        
    def process_hdfs(self, hdfs_data):
        if(self.param_list.get('data_source_name') == nsf_config.medline):
            return hdfs_data.flatMap(self.parse_gzip_medline_str)
        elif(self.param_list.get('data_source_name') == nsf_config.pubmed):
            return hdfs_data.flatMap(self.zip_extract).flatMap(self.parse_gzip_pubmed_str)
        
        
    def process(self):
        print(sys.path)
        logging.info('Creating Spark Session')
        spark = self.createSession()
        hdfs_data = self.read_hdfs_data(spark)
        preprocess = self.process_hdfs(hdfs_data)
        medline_df = preprocess.toDF()
        medline_df.show()

def main(data_source_name):
    libraries_list = ['/home/ananth/PycharmProjects/nsf_data_ingestion/libraries/pubmed_parser_lib.zip']
    obj1 = Parquet_Write(data_source_params.mapping.get(data_source_name), libraries_list, spark_config.exec_instance, spark_config.exec_mem, spark_config.exec_cores)
    obj1.process()
