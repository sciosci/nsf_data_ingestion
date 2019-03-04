from .nsf_config import *

federal_reporter_param = {'data_source_name' : federal_reporter,
                          'FedRePORTER_PRJ_url': fed_FedRePORTER_PRJ_url,
                          'FedRePORTER_PRJABS_url': fed_FedRePORTER_PRJABS_url,
                          'hdfs_path': federal_hdfs_path,
                          'xml_path': federal_xml_path,
                          'parquet_path': federal_parquet_path,
                          'directory_path': federal_directory_path_data,
                          'hdfs_read_type': hdfs_read_BINARY}

medline_param =          {'data_source_name' : medline,
                          'ftp_server': medline_ftp_server,
                          'medleasebaseline_url': medline_medleasebaseline,
                          'medlease_url': medline_medlease,
                          'hdfs_path': medline_hdfs_path,
                          'xml_path': medline_xml_path,
                          'parquet_path': medline_parquet_path,
                          'directory_path': medline_directory_path_data,
                          'timestamp_file': timestamp_file,
                          'hdfs_read_type': hdfs_read_WHOLEFILES}

pubmed_param =           {'data_source_name' : pubmed,
                          'ftp_server': pub_ftp_server,
                          'ftp_directory': pub_ftp_directory,
                          'download_path': pub_directory_path_data,
                          'unzip_path': pub_directory_untar_data,
                          'chunk_size': pub_chunk_size,
                          'chunked_path': pub_directory_path_processed,
                          'directory_path': pub_directory_path_compressed,
                          'hdfs_path': pub_hadoop_directory,
                          'xml_path': pub_xml_path,
                          'parquet_path': pub_parquet_path,
                          'hdfs_read_type': hdfs_read_BINARY}

arxiv_param =            {'data_source_name' : arxiv,
                          'hdfs_path' : arxiv_hadoop_directory,
                          'download_path': arxiv_directory_path_data}

mapping =                {'federal_reporter': federal_reporter_param,
                           'medline': medline_param,
                           'pubmed': pubmed_param,
                           'arxiv': arxiv_param}