libraries_list = ['//home/eileen/nsf_data_ingestion/nsf_data_ingestion/libraries/pubmed_parser_lib.zip',
                  '/home/eileen/nsf_data_ingestion/nsf_data_ingestion/libraries/libraries/config.zip',
                  '/home/eileen/nsf_data_ingestion/nsf_data_ingestion/libraries/unidecode_lib.zip']
source_path = '/home/eileen/nsf_data_ingestion/nsf_data_ingestion/'
spark_path = '/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/'

#PUBMED VARIABLES
pub_chunk_size = 500
pub_ftp_server = 'ftp.ncbi.nlm.nih.gov'
pub_ftp_directory = '/pub/pmc/oa_bulk/'
#pub_directory_path_data = '/home/ananth/airflow/nsf_data/'
pub_directory_path_data = '/home/eileen/airflow/nsf_data/'
pub_directory_untar_data = pub_directory_path_data + 'untar_data/'
pub_directory_path_processed = pub_directory_path_data + 'chunk_data'
pub_directory_path_compressed = pub_directory_path_data + 'compressed_pubmed_data'
# pub_hadoop_directory = '/user/ananth/pub/'
# pub_xml_path = '/user/ananth/pub/compressed_pubmed_data'
# pub_parquet_path = '/user/ananth/pub/parquet/'
pub_hadoop_directory = '/user/eileen/pub/'
pub_xml_path = '/user/eileen/pub/compressed_pubmed_data'
pub_parquet_path = '/user/eileen/pub/parquet/'

#MEDLINE VARIABLES
medline_directory_path_data = '/home/eileen/airflow/medline_data/'
timestamp_file = 'time_stamp.txt'
medline_ftp_server = 'ftp.nlm.nih.gov'
medline_medleasebaseline = '/nlmdata/.medleasebaseline/gz/'
# medline_medlease = '/nlmdata/.medlease/gz/'
medline_medlease_urls = ['/nlmdata/.medleasebaseline/gz/', '/nlmdata/.medlease/gz/']
# medline_hdfs_path = '/user/ananth/medline/'
# medline_xml_path = '/user/ananth/medline/medline_data'
# medline_parquet_path = '/user/ananth/medline/parquet/'
medline_hdfs_path = '/user/eileen/medline/'
medline_xml_path = '/user/eileen/medline/medline_data'
medline_parquet_path = '/user/eileen/medline/parquet/'

#FEDERAL REPORTER VARIABLES
fed_FedRePORTER_PRJ_url = 'https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJ_X_FY'
fed_FedRePORTER_PRJABS_url = 'https://federalreporter.nih.gov/FileDownload/DownloadFile?fileToDownload=FedRePORTER_PRJABS_X_FY'
timestamp_file = 'time_stamp.txt'
federal_directory_path_data = '/home/eileen/federal_data/'
federal_hdfs_path = '/user/eileen/federal/'
federal_xml_path = '/user/eileen/federal/xml/'
federal_parquet_path = '/user/eileen/federal/parquet/'

#GRANTS GOV VARIABLES
grants_gov_url = 'https://www.grants.gov/web/grants/xml-extract.html'
grants_gov_path_data = '/home/eileen/grants_gov/'
grants_gov_hdfs_path = '/user/eileen/grants/'
grants_gov_xml_path = '/user/eileen/grants/data/'
grants_gov_parquet_path = '/user/eileen/grants/parquet/'


#ARXIV VARIABLES
arxiv_raw_url = 'http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=oai_dc'
arxiv_resume_url = 'http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken=%s'
## arxiv_directory_path_data = '/home/ananth/airflow/arxiv_data/'
# arxiv_hadoop_directory = '/user/ananth/arxiv_data/'
# arxiv_xml_path = '/user/ananth/arxiv_data/arxiv_data/'
# arxiv_parquet_path = '/user/ananth/arxiv_data/parquet/'
arxiv_directory_path_data = '/home/eileen/airflow/arxiv_data/'
arxiv_hadoop_directory = '/user/eileen/arxiv_data/'
arxiv_xml_path = '/user/eileen/arxiv_data/arxiv_data/'
arxiv_parquet_path = '/user/eileen/arxiv_data/parquet/'


#TFIDF VARIABLES
stop_words_url = 'http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words'
tfidf_path = '/user/eileen/tfidf.parquet'
tfidf_topic_path = '/user/eileen/topic_svd/' 
num_topics = 100
power_iters = 2
extra_dims = 10

#HDFS Parameters
hdfs_read_BINARY = 'binary'
hdfs_read_WHOLEFILES = 'wholefiles'
hdfs_read_FILE = 'file'


#DATA SOURCE NAMES
medline = 'medline'
pubmed = 'pubmed'
federal_reporter = 'federal_reporter'
grants_gov = 'grants_gov'
arxiv = 'arxiv'
tfdif = 'tfdif'
svd_compute = 'svd_compute'