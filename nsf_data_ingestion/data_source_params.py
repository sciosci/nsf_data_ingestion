from nsf_data_ingestion import nsf_config

federal_reporter_param = {'data_source_name' : nsf_config.federal_reporter,
                          'FedRePORTER_PRJ_url': nsf_config.fed_FedRePORTER_PRJ_url,
                          'FedRePORTER_PRJABS_url': nsf_config.fed_FedRePORTER_PRJABS_url,
                          'hdfs_path': nsf_config.federal_hdfs_path,
                          'xml_path': nsf_config.federal_xml_path,
                          'parquet_path': nsf_config.federal_parquet_path,
                          'directory_path': nsf_config.federal_directory_path_data,
                          'hdfs_read_type': nsf_config.hdfs_read_BINARY}

medline_param =          {'data_source_name' : nsf_config.medline,
                          'ftp_server': nsf_config.medline_ftp_server,
                          'medleasebaseline_url': nsf_config.medline_medleasebaseline,
                          'medlease_url': nsf_config.medline_medlease,
                          'hdfs_path': nsf_config.medline_hdfs_path,
                          'xml_path': nsf_config.medline_xml_path,
                          'parquet_path': nsf_config.medline_parquet_path,
                          'directory_path': nsf_config.medline_directory_path_data,
                          'timestamp_file': nsf_config.timestamp_file,
                          'hdfs_read_type': nsf_config.hdfs_read_WHOLEFILES}

pubmed_param =           {'data_source_name' : nsf_config.pubmed,
                          'ftp_server': nsf_config.pub_ftp_server,
                          'ftp_directory': nsf_config.pub_ftp_directory,
                          'download_path': nsf_config.pub_directory_path_data,
                          'unzip_path': nsf_config.pub_directory_untar_data,
                          'chunk_size': nsf_config.pub_chunk_size,
                          'chunked_path': nsf_config.pub_directory_path_processed,
                          'directory_path': nsf_config.pub_directory_path_compressed,
                          'hdfs_path': nsf_config.pub_hadoop_directory,
                          'xml_path': nsf_config.pub_xml_path,
                          'parquet_path': nsf_config.pub_parquet_path,
                          'hdfs_read_type': nsf_config.hdfs_read_BINARY}

mapping =                {'federal_reporter': federal_reporter_param,
                           'medline': medline_param,
                           'pubmed': pubmed_param}