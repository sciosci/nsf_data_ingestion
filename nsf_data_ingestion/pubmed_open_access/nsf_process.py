import sys
sys.path.append('/home/ananth/nsf_data_ingestion/nsf_data_ingestion/config/')
import spark_config
from spark_config import *
import zipfile
import io

class Parquet_Write(object):

    def __init__(self, data_path, parquet_path, libraries_list, exec_instance, exec_mem, exec_cores):
        """Return a Customer object whose name is *name* and starting
        balance is *balance*."""
        self.libraries_list = libraries_list
        self.data_path = data_path
        self.parquet_path = parquet_path
        self.exec_instance = exec_instance
        self.exec_mem = exec_mem
        self.exec_cores = exec_cores
    

    def zip_extract(self, x):
        in_memory_data = io.BytesIO(x[1])
        file_obj = zipfile.ZipFile(in_memory_data, "r")
        files = [i for i in file_obj.namelist()]
        return zip(files, [file_obj.open(file).read() for file in files])
    

    def createSession(self):
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
    

    def parse_gzip_pubmed_str(self, gzip_str):
        filepath = gzip_str[0]
        gzip_content = gzip_str[1].decode("utf-8")
        _, file_name = path.split(filepath)
        articles = pp.parse_pubmed_xml(gzip_content)
        return [Row(file_name=file_name, **article_dict)
                for article_dict in articles]
    
    

    def process(self, data_path, parquet_path, libraries_list, exec_instance, exec_mem, exec_cores):
        print(sys.path)
        logging.info('Creating Spark Session')
        spark = self.createSession()
        
        print("Reading from {} and writing to {}.".format(data_path, parquet_path))
        zips = spark.sparkContext.binaryFiles(os.path.join(data_path, '*.zip'))
        files_data = zips.flatMap(self.zip_extract)
        
        logging.info('Running Job')
        preprocess = files_data.flatMap(self.parse_gzip_pubmed_str)
        
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

def generate_pub_parquet_files(data_path, parquet_path, libraries_list):
    spark_instance = Parquet_Write(data_path, parquet_path, libraries_list,
                     spark_config.exec_instance, spark_config.exec_mem, spark_config.exec_cores)

    spark_instance.process(spark_instance.data_path, spark_instance.parquet_path, spark_instance.libraries_list,
                   spark_instance.exec_instance, spark_instance.exec_mem, spark_instance.exec_cores)
