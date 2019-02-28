libraries_list = ['/home/ananth/nsf_data_ingestion/libraries/pubmed_parser_lib.zip', '/home/ananth/nsf_data_ingestion/libraries/unidecode_lib.zip']
source_path = '/home/ananth/nsf_data_ingestion/'
spark_path = '/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/'

#PUBMED VARIABLES
pub_chunk_size = 500
pub_ftp_server = 'ftp.ncbi.nlm.nih.gov'
pub_ftp_directory = '/pub/pmc/oa_bulk/'
pub_directory_path_data = '/home/ananth/airflow/nsf_data/'
pub_directory_untar_data = pub_directory_path_data + 'untar_data/'
pub_directory_path_processed = pub_directory_path_data + 'chunk_data'
pub_directory_path_compressed = pub_directory_path_data + 'compressed_pubmed_data'
pub_hadoop_directory = '/user/ananth/pubmed/'
pub_parquet_path = '/user/ananth/pub/parquet/'

#MEDLINE VARIABLES
medline_directory_path_data = '/home/ananth/airflow/medline_data/'
timestamp_file = 'time_stamp.txt'
medline_ftp_server = 'ftp.nlm.nih.gov'
medline_medleasebaseline = '/nlmdata/.medleasebaseline/gz/'
medline_medlease = '/nlmdata/.medlease/gz/'
medline_hdfs_path = '/user/ananth/medline/'
medline_xml_path = '/user/ananth/medline/data'
medline_parquet_path = '/user/ananth/medline/parquet/'

#FEDERAL REPORTER VARIABLES
fed_FedRePORTER_PRJ_url = 'https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJ_X_FY'
fed_FedRePORTER_PRJABS_url = 'https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJABS_X_FY'
federal_directory_path_data = '/home/ananth/airflow/federal_data/'
federal_hdfs_path = '/user/ananth/federal/'
federal_parquet_path = '/user/ananth/federal/parquet/'