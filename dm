[33mcommit 8b4bb4fdeb74bc43d24108e504b78e1cb0ae3076[m
Author: rashika510 <rsingh37@syr.edu>
Date:   Mon Nov 9 18:59:31 2020 -0500

    testing

[1mdiff --git a/nsf_data_ingestion/objects/data_source_params-test.py b/nsf_data_ingestion/objects/data_source_params-test.py[m
[1mnew file mode 100644[m
[1mindex 0000000..652e6a6[m
[1m--- /dev/null[m
[1m+++ b/nsf_data_ingestion/objects/data_source_params-test.py[m
[36m@@ -0,0 +1,87 @@[m
[32m+[m[32mimport sys[m
[32m+[m[32msys.path.append('/home/eileen/nsf_data_ingestion/')[m
[32m+[m[32mfrom nsf_data_ingestion.config import nsf_config[m
[32m+[m
[32m+[m[32mfederal_reporter_param = {'data_source_name' : nsf_config.federal_reporter,[m
[32m+[m[32m                          'hdfs_path': nsf_config.federal_hdfs_path,[m
[32m+[m[32m                          'xml_path': nsf_config.federal_xml_path,[m
[32m+[m[32m                          'parquet_path': nsf_config.federal_parquet_path,[m
[32m+[m[32m                          'directory_path': nsf_config.federal_directory_path_data,[m
[32m+[m[32m                          'hdfs_read_type': nsf_config.hdfs_read_BINARY,[m
[32m+[m[32m                          'FedRePORTER_PRJ_url': nsf_config.fed_FedRePORTER_PRJ_url,[m
[32m+[m[32m                          'FedRePORTER_PRJABS_url': nsf_config.fed_FedRePORTER_PRJABS_url,[m
[32m+[m[32m                          'timestamp_file': nsf_config.timestamp_file[m
[32m+[m[32m                         }[m
[32m+[m
[32m+[m[32mmedline_param =          {'data_source_name' : nsf_config.medline,[m
[32m+[m[32m                          'hdfs_path': nsf_config.medline_hdfs_path,[m
[32m+[m[32m                          'xml_path': nsf_config.medline_xml_path,[m
[32m+[m[32m                          'parquet_path': nsf_config.medline_parquet_path,[m
[32m+[m[32m                          'directory_path': nsf_config.medline_directory_path_data,[m
[32m+[m[32m                          'hdfs_read_type': nsf_config.hdfs_read_WHOLEFILES,[m
[32m+[m[32m                          'ftp_server': nsf_config.medline_ftp_server,[m
[32m+[m[32m                          'medline_medlease_urls': nsf_config.medline_medlease_urls,[m
[32m+[m[32m                          'timestamp_file': nsf_config.timestamp_file[m
[32m+[m[32m                         }[m
[32m+[m[32mgrants_gov_param =       {'data_source_name' : nsf_config.grants_gov,[m
[32m+[m[32m                          'hdfs_path': nsf_config.grants_gov_hdfs_path,[m
[32m+[m[32m                          'xml_path': nsf_config.grants_gov_xml_path,[m
[32m+[m[32m                          'parquet_path': nsf_config.grants_gov_parquet_path,[m
[32m+[m[32m                          'directory_path': nsf_config.grants_gov_path_data,[m
[32m+[m[32m                          'hdfs_read_type': nsf_config.hdfs_read_WHOLEFILES,[m
[32m+[m[32m                          'parquet_path': nsf_config.grants_gov_parquet_path,[m
[32m+[m[32m                          'grants_gov_urls': nsf_config.grants_gov_url,[m
[32m+[m[32m                          'timestamp_file': nsf_config.timestamp_file[m
[32m+[m[32m                         }[m
[32m+[m
[32m+[m[32mpubmed_param =           {'data_source_name' : nsf_config.pubmed,[m
[32m+[m[32m                          'hdfs_path': nsf_config.pub_hadoop_directory,[m
[32m+[m[32m                          'xml_path': nsf_config.pub_xml_path,[m
[32m+[m[32m                          'parquet_path': nsf_config.pub_parquet_path,[m
[32m+[m[32m                          'hdfs_read_type': nsf_config.hdfs_read_BINARY,[m
[32m+[m[32m                          'download_path': nsf_config.pub_directory_path_data,[m
[32m+[m[32m                          'unzip_path': nsf_config.pub_directory_untar_data,[m
[32m+[m[32m                          'chunk_size': nsf_config.pub_chunk_size,[m
[32m+[m[32m                          'chunked_path': nsf_config.pub_directory_path_processed,[m
[32m+[m[32m                          'directory_path': nsf_config.pub_directory_path_compressed,[m
[32m+[m[32m                          'ftp_server': nsf_config.pub_ftp_server,[m
[32m+[m[32m                          'ftp_directory': nsf_config.pub_ftp_directory,[m
[32m+[m[32m                          'timestamp_file': nsf_config.timestamp_file[m
[32m+[m[32m                         }[m
[32m+[m
[32m+[m[32marxiv_param =            {'data_source_name' : nsf_config.arxiv,[m
[32m+[m[32m                          'hdfs_path' : nsf_config.arxiv_hadoop_directory,[m
[32m+[m[32m                          'directory_path': nsf_config.arxiv_directory_path_data,[m
[32m+[m[32m                          'xml_path': nsf_config.arxiv_xml_path,[m
[32m+[m[32m                          'parquet_path': nsf_config.arxiv_parquet_path,[m
[32m+[m[32m                          'hdfs_read_type': nsf_config.hdfs_read_BINARY,[m
[32m+[m[32m                          'arxiv_raw_url': nsf_config.arxiv_raw_url,[m
[32m+[m[32m                          'arxiv_resume_url': nsf_config.arxiv_resume_url,[m
[32m+[m[32m                          'timestamp_file': nsf_config.timestamp_file[m
[32m+[m[32m                         }[m
[32m+[m
[32m+[m
[32m+[m[32mtfidf_params =           {'stop_words_url': nsf_config.stop_words_url,[m
[32m+[m[32m                          'tfidf_path': nsf_config.tfidf_path,[m
[32m+[m[32m                          'tfidf_topic_path': nsf_config.tfidf_topic_path,[m
[32m+[m[32m                          'num_topics': nsf_config.num_topics,[m
[32m+[m[32m                          'power_iters': nsf_config.power_iters,[m[41m [m
[32m+[m[32m                          'extra_dims': nsf_config.extra_dims[m
[32m+[m[32m                         }[m
[32m+[m
[32m+[m[32msvd_params =             {'data_source_name': nsf_config.svd_compute,[m
[32m+[m[32m                          'stop_words_url': nsf_config.stop_words_url,[m
[32m+[m[32m                          'tfidf_path': nsf_config.tfidf_path,[m
[32m+[m[32m                          'tfidf_topic_path': nsf_config.tfidf_topic_path,[m
[32m+[m[32m                          'num_topics': nsf_config.num_topics,[m
[32m+[m[32m                          'power_iters': nsf_config.power_iters,[m[41m [m
[32m+[m[32m                          'extra_dims': nsf_config.extra_dims[m
[32m+[m[32m                         }[m
[32m+[m
[32m+[m[32mmapping =                {'federal_reporter': federal_reporter_param,[m
[32m+[m[32m                           'medline': medline_param,[m
[32m+[m[32m                           'pubmed': pubmed_param,[m
[32m+[m[32m                           'arxiv': arxiv_param,[m
[32m+[m[32m                           'tfidf': tfidf_params,[m
[32m+[m[32m                           'svd_compute': svd_params[m
[32m+[m[32m                         }[m
\ No newline at end of file[m

[33mcommit 280ac6c041c617719e1a31b6f357db1839b3453e[m
Author: sourabhghosh29 <sourabhghosh29@gmail.com>
Date:   Fri Sep 18 11:44:34 2020 -0400

    git clone pipeline test

[1mdiff --git a/nsf_data_ingestion/utils/test.py b/nsf_data_ingestion/utils/test.py[m
[1mdeleted file mode 100644[m
[1mindex e69de29..0000000[m

[33mcommit 02181d1f4f8b9aa62f9caed10c18bd6c8630ac56[m
Author: sourabhghosh29 <sourabhghosh29@gmail.com>
Date:   Fri Sep 18 02:10:55 2020 -0400

    git clone code

[1mdiff --git a/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py b/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[1mindex 3ce2591..1eba1b4 100644[m
[1m--- a/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[1m+++ b/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[36m@@ -52,12 +52,12 @@[m [mdefault_args = {[m
 # dag = DAG('nsf_data_ingestion', default_args = default_args, catchup=False)[m
 dag = DAG('nsf_data_ingestion', default_args = default_args, schedule_interval=timedelta(days = 7), catchup=False)[m
 [m
[31m-# GitClone = PythonOperator([m
[31m-#     task_id='GitClone',[m
[31m-#     python_callable = utils_functions.pull,[m
[31m-#     retries=3,[m
[31m-#     dag=dag,[m
[31m-# )[m
[32m+[m[32mGitClone = PythonOperator([m
[32m+[m[32m    task_id='GitClone',[m
[32m+[m[32m    python_callable = utils_functions.pull,[m
[32m+[m[32m    retries=3,[m
[32m+[m[32m    dag=dag,[m
[32m+[m[32m)[m
 [m
 TFDIF_Model = PythonOperator([m
     task_id='TFDIF_Model',[m
[36m@@ -102,11 +102,11 @@[m [mArxiv = BashOperator([m
     dag=dag,[m
 )[m
 [m
[31m-# Medline.set_upstream(GitClone)[m
[31m-# Pubmed.set_upstream(GitClone)[m
[31m-# Federal_Reporter.set_upstream(GitClone)[m
[31m-# Grants_Gov.set_upstream(GitClone)[m
[31m-# Arxiv.set_upstream(GitClone)[m
[32m+[m[32mMedline.set_upstream(GitClone)[m
[32m+[m[32mPubmed.set_upstream(GitClone)[m
[32m+[m[32mFederal_Reporter.set_upstream(GitClone)[m
[32m+[m[32mGrants_Gov.set_upstream(GitClone)[m
[32m+[m[32mArxiv.set_upstream(GitClone)[m
 [m
 Medline_Download = PythonOperator([m
     task_id='Medline_Download',[m
[1mdiff --git a/nsf_data_ingestion/utils/utils_functions.py b/nsf_data_ingestion/utils/utils_functions.py[m
[1mindex 5c88d55..9eba716 100644[m
[1m--- a/nsf_data_ingestion/utils/utils_functions.py[m
[1m+++ b/nsf_data_ingestion/utils/utils_functions.py[m
[36m@@ -28,7 +28,7 @@[m [mdef pull():[m
 #     os.chdir(nsf_config.source_path)[m
     os.chdir("/home/eileen/nsf_data_ingestion/")[m
     output = subprocess.check_output(["git", "pull", "origin", "origin/feature/nsf_grants_merged"])[m
[31m-    logging.info('Checked Out Branch.....')[m
[32m+[m[32m    logging.info('Pulled Out Branch.....')[m
     print(os.curdir)[m
 [m
 [m

[33mcommit 14a55251b35a8d38a1f690a816af652f56c6e7c9[m
Author: sourabhghosh29 <sourabhghosh29@gmail.com>
Date:   Thu Sep 17 21:22:59 2020 -0400

    test_checkout

[1mdiff --git a/nsf_data_ingestion/utils/test.py b/nsf_data_ingestion/utils/test.py[m
[1mnew file mode 100644[m
[1mindex 0000000..e69de29[m

[33mcommit c42ccc500ed9aab346652473fe28ddaa4a3d49ab[m
Author: sourabhghosh29 <sourabhghosh29@gmail.com>
Date:   Thu Sep 17 21:16:04 2020 -0400

    test push

[1mdiff --git a/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py b/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[1mindex 4ccf3d3..3ce2591 100644[m
[1m--- a/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[1m+++ b/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[36m@@ -273,9 +273,10 @@[m [mArxiv_Persist.set_upstream(Arxiv_Download)[m
 Arxiv_Parquet.set_upstream(Arxiv_Persist)[m
 [m
 TFDIF_Model.set_upstream(Medline_Parquet)[m
[31m-# TFDIF_Model.set_upstream(Federal_Parquet)[m
[32m+[m[32mTFDIF_Model.set_upstream(Federal_Parquet)[m
 TFDIF_Model.set_upstream(Arxiv_Parquet)[m
 TFDIF_Model.set_upstream(Grants_Parquet)[m
[32m+[m[32mTFDIF_Model.set_upstream(Pubmed_Parquet)[m
 [m
 [m
 SVD_Compute = PythonOperator([m
[1mdiff --git a/nsf_data_ingestion/utils/utils_functions.py b/nsf_data_ingestion/utils/utils_functions.py[m
[1mindex 5991484..5c88d55 100644[m
[1m--- a/nsf_data_ingestion/utils/utils_functions.py[m
[1m+++ b/nsf_data_ingestion/utils/utils_functions.py[m
[36m@@ -2,9 +2,9 @@[m [mimport os[m
 import subprocess[m
 import sys[m
 # sys.path.append('/home/ananth/nsf_data_ingestion/')[m
[31m-sys.path.append('/home/sghosh08/nsf_new/nsf_data_ingestion/')[m
[31m-from nsf_data_ingestion.config import nsf_config[m
[31m-from nsf_data_ingestion.objects import data_source_params[m
[32m+[m[32msys.path.append('/home/eileen/nsf_data_ingestion/nsf_data_ingestion/')[m
[32m+[m[32m# from nsf_data_ingestion.config import nsf_config[m
[32m+[m[32m# from nsf_data_ingestion.objects import data_source_params[m
 from shutil import copyfile[m
 from shutil import rmtree[m
 import tarfile[m
[36m@@ -25,8 +25,9 @@[m [mdef get_archive_file_list():[m
 [m
 [m
 def pull():[m
[31m-    os.chdir(nsf_config.source_path)[m
[31m-    output = subprocess.check_output(["git", "pull", "origin", "feature/nsf_new_workflow"])[m
[32m+[m[32m#     os.chdir(nsf_config.source_path)[m
[32m+[m[32m    os.chdir("/home/eileen/nsf_data_ingestion/")[m
[32m+[m[32m    output = subprocess.check_output(["git", "pull", "origin", "origin/feature/nsf_grants_merged"])[m
     logging.info('Checked Out Branch.....')[m
     print(os.curdir)[m
 [m
[36m@@ -54,3 +55,6 @@[m [mdef get_last_load(directory_path_data, timestamp_file):[m
         logging.info('Directory Path Doesnot Exist....Creating New')[m
         os.makedirs(directory_path_data)[m
         get_last_load(directory_path_data, timestamp_file)[m
[32m+[m[41m        [m
[32m+[m[32mif __name__ == "__main__":[m
[32m+[m[32m    pull()[m

[33mcommit 76c078bdfd3f6204dc216ad1aa6401ba55cc32e1[m
Author: sourabhghosh29 <sourabhghosh29@gmail.com>
Date:   Mon Mar 2 15:53:45 2020 -0500

    pipeline update

[1mdiff --git a/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py b/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[1mindex 7bd23f6..4ccf3d3 100644[m
[1m--- a/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[1m+++ b/nsf_data_ingestion/airflow/nsf_data_ingestion_dag.py[m
[36m@@ -39,9 +39,17 @@[m [mdefault_args = {[m
     'depends_on_past': False,[m
     'retry_delay': timedelta(minutes=3),[m
     'retries': 3,[m
[31m-    'start_date': datetime.now(),[m
[32m+[m[32m    'start_date': datetime(2020, 2, 24),[m
 }[m
[31m-[m
[32m+[m[32m# default_args = {[m
[32m+[m[32m#     'owner':'nsf_data_ingestion',[m
[32m+[m[32m#     'depends_on_past': False,[m
[32m+[m[32m#     'retry_delay': timedelta(minutes=3),[m
[32m+[m[32m#     'retries': 3,[m
[32m+[m[32m#     'start_date': datetime(2020, 2, 24),[m
[32m+[m[32m#     'schedule_interval': '@weekly'[m
[32m+[m[32m# }[m
[32m+[m[32m# dag = DAG('nsf_data_ingestion', default_args = default_args, catchup=False)[m
 dag = DAG('nsf_data_ingestion', default_args = default_args, schedule_interval=timedelta(days = 7), catchup=False)[m
 [m
 # GitClone = PythonOperator([m
[36m@@ -292,5 +300,20 @@[m [mKimun_Index = BashOperator([m
 #     retries=8,[m
 #     dag=dag,[m
 # )[m
[32m+[m[32mes_delete = BashOperator([m
[32m+[m[32m    task_id='es_delete',[m
[32m+[m[32m    bash_command='python /home/eileen/nsf_data_ingestion/nsf_data_ingestion/kimun_loader/es_deleteindex.py',[m
[32m+[m[32m    retries=3,[m
[32m+[m[32m    dag=dag,[m
[32m+[m[32m)[m
[32m+[m
[32m+[m[32mes_create = BashOperator([m
[32m+[m[32m    task_id='es_create',[m
[32m+[m[32m    bash_command='python /home/eileen/nsf_data_ingestion/nsf_data_ingestion/kimun_loader/es_createindex.py',[m
[32m+[m[32m    retries=3,[m
[32m+[m[32m    dag=dag,[m
[32m+[m[32m)[m
 [m
[31m-Kimun_Index.set_upstream(SVD_Compute)[m
[32m+[m[32mes_delete.set_upstream(SVD_Compute)[m
[32m+[m[32mes_create.set_upstream(es_delete)[m
[32m+[m[32mKimun_Index.set_upstream(es_create)[m
[1mdiff --git a/nsf_data_ingestion/kimun_loader/Elastic_search_test.ipynb b/nsf_data_ingestion/kimun_loader/Elastic_search_test.ipynb[m
[1mnew file mode 100644[m
[1mindex 0000000..8d4d6c3[m
[1m--- /dev/null[m
[1m+++ b/nsf_data_ingestion/kimun_loader/Elastic_search_test.ipynb[m
[36m@@ -0,0 +1,137 @@[m
[32m+[m[32m{[m
[32m+[m[32m "cells": [[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 43,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "import subprocess\n",[m
[32m+[m[32m    "import os\n",[m
[32m+[m[32m    "import logging"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 44,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "# command = 'curl -XGET \"http://128.230.247.186:9201/_cat/indices\"'\n",[m
[32m+[m[32m    "healthcheck = 'curl -XGET \"http://128.230.247.186:9201/_cat/indices\"'"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 45,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "\n",[m
[32m+[m[32m    "p_healthcheck = subprocess.Popen(healthcheck, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)\n"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 46,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "out_health, err_health = p_healthcheck.communicate()"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 47,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [[m
[32m+[m[32m    {[m
[32m+[m[32m     "name": "stdout",[m
[32m+[m[32m     "output_type": "stream",[m
[32m+[m[32m     "text": [[m
[32m+[m[32m      "Last Snap Shot :  b'yellow open blogs                           MpHcw2GCTK2CgbdIlcI0MA 5 1        3       0   7.6kb   7.6kb\\nyellow open kimun_version5                  bUWAdseQTiOvbVObgMCrrA 5 1        0       0   1.2kb   1.2kb\\ngreen  open .monitoring-es-6-2020.02.09     r-tiXolUS8-H-lt4pZX1Aw 1 0   310232    2775 174.8mb 174.8mb\\nyellow open my_index                        OazYD5yUT9O1He0mxiAb8A 5 1        0       0   1.2kb   1.2kb\\ngreen  open .monitoring-es-6-2020.02.13     r7HpsqBpQtOJLIe263cByQ 1 0   256923    2379 130.8mb 130.8mb\\nyellow open eileen_keyword_count_index_3    ChZ6oKn1Tw-qnZBN_aXtOw 5 1   838503       0  78.3mb  78.3mb\\ngreen  open .monitoring-kibana-6-2020.02.12 sajNacrQQXuJpHriEgTJvQ 1 0       97       0 109.6kb 109.6kb\\nyellow open test                            GUqlh2oMS82qjnwD6v5E0g 5 1        1       0   4.4kb   4.4kb\\nyellow open kimun2                          73pCZTonQJ2hNslUMzDzjw 5 1        0       0   1.2kb   1.2kb\\nyellow open eileen_keyword_index_1          Ui5vb5zTSWyhbt8qzKxRUA 5 1 10829103       0   1.3gb   1.3gb\\ngreen  open .monitoring-kibana-6-2020.02.09 5iHL0aPrT0qJWbVXHzR4iA 1 0    43195       0   4.5mb   4.5mb\\ngreen  open .monitoring-kibana-6-2020.02.13 vusMK34WRjGLbjMjaY9DvA 1 0    73609       0   7.3mb   7.3mb\\nyellow open pubmed_index                    LUPIVF-KQNmQsjpkt6cf1A 5 1        1       0   5.5kb   5.5kb\\nyellow open eileen_keyword_count_index_temp zOIQ0M7FQNybyrbV9SRRLw 5 1   838503       0  80.1mb  80.1mb\\nyellow open eileen_keyword_index            oLNVGifnT6uWrFJ2JkwCqQ 5 1 11935459       0   1.6gb   1.6gb\\ngreen  open .monitoring-kibana-6-2020.02.08 w_yKXQ2HRKizzktx0pyqGQ 1 0    43200       0   4.4mb   4.4mb\\nyellow open kimun_filtered                  emlUS8zrRQK_iL_OLU6cdA 5 1      359       0   2.6mb   2.6mb\\nyellow open company                         w1XBarASR6qnQBjzhLceRg 5 1        0       0   1.2kb   1.2kb\\nyellow open eileen_keyword_count_index      sxkSkMGgROi1R--Fa6wOgA 5 1  5102703       0   489mb   489mb\\ngreen  open .kibana                         7uTO-999S4eFIjrH-Cib0g 1 0        9       0  48.3kb  48.3kb\\nyellow open kimun                           FRzANCF_R4KOlcjXOD5X3g 5 1 27999431       0   142gb   142gb\\ngreen  open .monitoring-es-6-2020.02.14     Xv8dd8QPRY-5XyBAhyI0jw 1 0   134833     726  84.6mb  84.6mb\\ngreen  open .monitoring-kibana-6-2020.02.10 NrH_4WJHT6y2_M5L68cEMw 1 0    26680       0   2.7mb   2.7mb\\nyellow open eileen_keyword_index_na         wVvYJDnTQ362dgRGUt04tQ 5 1 10829103       0   1.2gb   1.2gb\\nyellow open kimun_version4                  Bxq_pJiqTNOF0UEgD2SFew 5 1 25871670 5749145 276.6gb 276.6gb\\ngreen  open .monitoring-es-6-2020.02.12     xzLMY_thTnmYdxqiOcjE0A 1 0      573       0 180.8kb 180.8kb\\ngreen  open .monitoring-es-6-2020.02.08     EeJpKpgNROCiZnkUkuuZlA 1 0   315881    2220 170.3mb 170.3mb\\ngreen  open .monitoring-kibana-6-2020.02.14 5iDeh4SORW-7a0N1po9Q6Q 1 0    38160       0   7.2mb   7.2mb\\nyellow open posts                           RJ0FJvWOQ3KplAHa99H7EA 5 1        0       0   1.2kb   1.2kb\\ngreen  open .monitoring-es-6-2020.02.10     ynWZORkuTsaGh2n7DnGGKQ 1 0   182542   40543 102.9mb 102.9mb\\n'\n"[m
[32m+[m[32m     ][m
[32m+[m[32m    }[m
[32m+[m[32m   ],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "\n",[m
[32m+[m[32m    "logging.info(\"Last Snap Shot : \",out_health)\n",[m
[32m+[m[32m    "print(\"Last Snap Shot : \",out_health)"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 48,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "delete_kimun = 'curl -XDELETE \"http://128.230.247.186:9201/kimun_version5\"'"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 49,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "\n",[m
[32m+[m[32m    "p_deletekimun = subprocess.Popen(delete_kimun, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 50,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "out_delete, err_delete = p_deletekimun.communicate()"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": 51,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [[m
[32m+[m[32m    {[m
[32m+[m[32m     "name": "stdout",[m
[32m+[m[32m     "output_type": "stream",[m
[32m+[m[32m     "text": [[m
[32m+[m[32m      "Last Snap Shot :  b'{\"acknowledged\":true}'\n"[m
[32m+[m[32m     ][m
[32m+[m[32m    }[m
[32m+[m[32m   ],[m
[32m+[m[32m   "source": [[m
[32m+[m[32m    "logging.info(\"Last Snap Shot : \",out_delete)\n",[m
[32m+[m[32m    "print(\"Last Snap Shot : \",out_delete)"[m
[32m+[m[32m   ][m
[32m+[m[32m  },[m
[32m+[m[32m  {[m
[32m+[m[32m   "cell_type": "code",[m
[32m+[m[32m   "execution_count": null,[m
[32m+[m[32m   "metadata": {},[m
[32m+[m[32m   "outputs": [],[m
[32m+[m[32m   "source": [][m
[32m+[m[32m  }[m
[32m+[m[32m ],[m
[32m+[m[32m "metadata": {[m
[32m+[m[32m  "kernelspec": {[m
[32m+[m[32m   "display_name": "Python 3",[m
[32m+[m[32m   "language": "python",[m
[32m+[m[32m   "name": "python3"[m
[32m+[m[32m  },[m
[32m+[m[32m  "language_info": {[m
[32m+[m[32m   "codemirror_mode": {[m
[32m+[m[32m    "name": "ipython",[m
[32m+[m[32m    "version": 3[m
[32m+[m[32m   },[m
[32m+[m[32m   "file_extension": ".py",[m
[32m+[m[32m   "mimetype": "text/x-python",[m
[32m+[m[32m   "name": "python",[m
[32m+[m[32m   "nbconvert_exporter": "python",[m
[32m+[m[32m   "pygments_lexer": "ipython3",[m
[32m+[m[32m   "version": "3.7.3"[m
[32m+[m[32m  }[m
[32m+[m[32m },[m
[32m+[m[32m "nbformat": 4,[m
[32m+[m[32m "nbformat_minor": 2[m
[32m+[m[32m}[m
[1mdiff --git a/nsf_data_ingestion/kimun_loader/Untitled.ipynb b/nsf_data_ingestion/kimun_loader/Untitled.ipynb[m
[1mindex 6655ce4..756abf6 100644[m
[1m--- a/nsf_data_ingestion/kimun_loader/Untitled.ipynb[m
[1m+++ b/nsf_data_ingestion/kimun_loader/Untitled.ipynb[m
[36m@@ -2,7 +2,7 @@[m
  "cells": [[m
   {[m
    "cell_type": "code",[m
[31m-   "execution_count": 4,[m
[32m+[m[32m   "execution_count": 8,[m
    "metadata": {},[m
    "outputs": [],[m
    "source": [[m
[36m@@ -67,7 +67,7 @@[m
     "    es_write_conf = {\n",[m
     "            \"es.nodes\" : \"128.230.247.186\",\n",[m
     "            \"es.port\" : \"9201\",\n",[m
[31m-    "            \"es.resource\" : 'kimun_version4/documents',\n",[m
[32m+[m[32m    "            \"es.resource\