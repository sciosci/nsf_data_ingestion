import sys
sys.path.append('/home/eileen/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config

federal_reporter_param = {'data_source_name' : nsf_config.federal_reporter,
                          'hdfs_path': nsf_config.federal_hdfs_path,
                          'xml_path': nsf_config.federal_xml_path,
                          'parquet_path': nsf_config.federal_parquet_path,
                          'directory_path': nsf_config.federal_directory_path_data,
                          'hdfs_read_type': nsf_config.hdfs_read_BINARY,
                          'FedRePORTER_PRJ_url': nsf_config.fed_FedRePORTER_PRJ_url,
                          'FedRePORTER_PRJABS_url': nsf_config.fed_FedRePORTER_PRJABS_url,
                          'timestamp_file': nsf_config.timestamp_file
                         }

medline_param =          {'data_source_name' : nsf_config.medline,
                          'hdfs_path': nsf_config.medline_hdfs_path,
                          'xml_path': nsf_config.medline_xml_path,
                          'parquet_path': nsf_config.medline_parquet_path,
                          'directory_path': nsf_config.medline_directory_path_data,
                          'hdfs_read_type': nsf_config.hdfs_read_WHOLEFILES, 
                          'ftp_server': nsf_config.medline_ftp_server,
                          'medline_medlease_urls': nsf_config.medline_medlease_urls,
                          'timestamp_file': nsf_config.timestamp_file
                         }
grants_gov_param =       {'data_source_name' : nsf_config.grants_gov,
                          'hdfs_path': nsf_config.grants_gov_hdfs_path,
                          'xml_path': nsf_config.grants_gov_xml_path,
                          'parquet_path': nsf_config.grants_gov_parquet_path,
                          'directory_path': nsf_config.grants_gov_path_data,
                          'hdfs_read_type': nsf_config.hdfs_read_WHOLEFILES,
                          'grants_gov_urls': nsf_config.grants_gov_url,
                          'timestamp_file': nsf_config.timestamp_file
                         }

pubmed_param =           {'data_source_name' : nsf_config.pubmed,
                          'hdfs_path': nsf_config.pub_hadoop_directory,
                          'xml_path': nsf_config.pub_xml_path,
                          'parquet_path': nsf_config.pub_parquet_path,
                          'hdfs_read_type': nsf_config.hdfs_read_BINARY,
                          'download_path': nsf_config.pub_directory_path_data,
                          'unzip_path': nsf_config.pub_directory_untar_data,
                          'chunk_size': nsf_config.pub_chunk_size,
                          'chunked_path': nsf_config.pub_directory_path_processed,
                          'directory_path': nsf_config.pub_directory_path_compressed,
                          'ftp_server': nsf_config.pub_ftp_server,
                          'ftp_directory': nsf_config.pub_ftp_directory,
                          'timestamp_file': nsf_config.timestamp_file
                         }

arxiv_param =            {'data_source_name' : nsf_config.arxiv,
                          'hdfs_path' : nsf_config.arxiv_hadoop_directory,
                          'directory_path': nsf_config.arxiv_directory_path_data,
                          'xml_path': nsf_config.arxiv_xml_path,
                          'parquet_path': nsf_config.arxiv_parquet_path,
                          'hdfs_read_type': nsf_config.hdfs_read_BINARY,
                          'arxiv_raw_url': nsf_config.arxiv_raw_url,
                          'arxiv_resume_url': nsf_config.arxiv_resume_url,
                          'timestamp_file': nsf_config.timestamp_file
                         }


tfidf_params =           {'stop_words_url': nsf_config.stop_words_url,
                          'tfidf_path': nsf_config.tfidf_path,
                          'tfidf_topic_path': nsf_config.tfidf_topic_path,
                          'num_topics': nsf_config.num_topics,
                          'power_iters': nsf_config.power_iters, 
                          'extra_dims': nsf_config.extra_dims
                         }

svd_params =             {'data_source_name': nsf_config.svd_compute,
                          'stop_words_url': nsf_config.stop_words_url,
                          'tfidf_path': nsf_config.tfidf_path,
                          'tfidf_topic_path': nsf_config.tfidf_topic_path,
                          'num_topics': nsf_config.num_topics,
                          'power_iters': nsf_config.power_iters, 
                          'extra_dims': nsf_config.extra_dims
                         }

mapping =                {'federal_reporter': federal_reporter_param,
                           'medline': medline_param,
                           'pubmed': pubmed_param,
                           'arxiv': arxiv_param,
                           'tfidf': tfidf_params,
                           'grants_gov' : grants_gov_param,
                           'svd_compute': svd_params
                         }