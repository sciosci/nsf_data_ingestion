import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
import sys
sys.path.append('/home/sghosh08/nsf_new/nsf_data_ingestion/')
from nsf_data_ingestion.pubmed import pubmed_download
from nsf_data_ingestion.pubmed.pubmed_download import *
from nsf_data_ingestion.federal_reporter import federal_reporter_download
from nsf_data_ingestion.grants_gov import grants_gov_download_put_hdfs
from nsf_data_ingestion.grants_gov.grants_gov_download_put_hdfs import *
from nsf_data_ingestion.federal_reporter.federal_reporter_download import *
from nsf_data_ingestion.medline import medline_download
from nsf_data_ingestion.medline.medline_download import *
from nsf_data_ingestion.medline import medline_parquet_write
from nsf_data_ingestion.federal_reporter import federal_parquet_write
from nsf_data_ingestion.grants_gov import write_to_parquet_grants
from nsf_data_ingestion.models import tfidf_aggregate
from nsf_data_ingestion.models import large_scale_svd
from nsf_data_ingestion.arxiv import arxiv_download
from nsf_data_ingestion.arxiv import arxiv_parquet_write
from nsf_data_ingestion.kimun_loader import kimun_loader
from nsf_data_ingestion.arxiv.arxiv_download import *

from nsf_data_ingestion.objects import data_source_params
from nsf_data_ingestion.utils import utils_functions

from nsf_data_ingestion.config.spark_config import *

#from nsf_data_ingestion.models import tfidf_aggregate
from datetime import datetime, timedelta
import subprocess
from subprocess import call

default_args = {
    'owner':'nsf_data_ingestion',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=3),
    'retries': 3,
    'start_date': datetime.now(),
}

dag = DAG('nsf_data_ingestion', default_args = default_args, schedule_interval=timedelta(days=7), catchup=False)

# GitClone = PythonOperator(
#     task_id='GitClone',
#     python_callable = utils_functions.pull,
#     retries=3,
#     dag=dag,
# )

TFDIF_Model = PythonOperator(
    task_id='TFDIF_Model',
    python_callable = tfidf_aggregate.main,
    op_kwargs={'data_source': nsf_config.tfdif},
    retries=3,
    dag=dag,
)

Medline = BashOperator(
    task_id='Medline',
    bash_command='date',
    retries=3,
    dag=dag,
)

Pubmed = BashOperator(
    task_id='Pubmed',
    bash_command='date',
    retries=3,
    dag=dag,
)

Federal_Reporter = BashOperator(
    task_id='Federal_Reporter',
    bash_command='date',
    retries=3,
    dag=dag,
)

Grants_Gov = BashOperator(
    task_id='Grants_Gov',
    bash_command='date',
    retries=3,
    dag=dag,
)

Arxiv = BashOperator(
    task_id='Arxiv',
    bash_command='date',
    retries=3,
    dag=dag,
)

# Medline.set_upstream(GitClone)
# Pubmed.set_upstream(GitClone)
# Federal_Reporter.set_upstream(GitClone)
# Grants_Gov.set_upstream(GitClone)
# Arxiv.set_upstream(GitClone)

Medline_Download = PythonOperator(
    task_id='Medline_Download',
    python_callable = medline_download.download,
    op_kwargs={'data_source_name': nsf_config.medline},
    retries=3,
    dag=dag,
)

Medline_Persist = PythonOperator(
    task_id='Medline_Persist',
    python_callable = medline_download.persist_hdfs,
    op_kwargs={'data_source_name': nsf_config.medline},
    retries=3,
    dag=dag,
)

Medline_Parquet = BashOperator(
    task_id='Medline_Parquet',
    bash_command = 'python /home/sghosh08/nsf_new/nsf_data_ingestion/nsf_data_ingestion/medline/medline_parquet_write.py',
    retries=3,
    dag=dag,
)

Medline_Download.set_upstream(Medline)
Medline_Persist.set_upstream(Medline_Download)
Medline_Parquet.set_upstream(Medline_Persist)

Pubmed_Download = PythonOperator(
    task_id='Pubmed_Download',
    python_callable = pubmed_download.download,
    op_kwargs={'data_source_name': nsf_config.pubmed},
    retries=3,
    dag=dag,
)

Pubmed_Untar = PythonOperator(
    task_id='Pubmed_Untar',
    python_callable = pubmed_download.untar,
    op_kwargs={'data_source_name': nsf_config.pubmed},
    retries=3,
    dag=dag,
)

Pubmed_Chunk = PythonOperator(
    task_id='Pubmed_Chunk',
    python_callable = pubmed_download.chunking,
    op_kwargs={'data_source_name': nsf_config.pubmed},
    retries=3,
    dag=dag,
)

Pubmed_Zip = PythonOperator(
    task_id='Pubmed_Zip',
    python_callable = pubmed_download.zipping,
    op_kwargs={'data_source_name': nsf_config.pubmed},
    retries=3,
    dag=dag,
)

Pubmed_Persist = PythonOperator(
    task_id='Pubmed_Persist',
    python_callable = pubmed_download.persist_hdfs,
    op_kwargs={'data_source_name': nsf_config.pubmed},
    retries=3,
    dag=dag,
)

Pubmed_Parquet = BashOperator(
    task_id='Pubmed_Parquet',
    bash_command='date',
    retries=3,
    dag=dag,
)

Pubmed_Download.set_upstream(Pubmed)
Pubmed_Untar.set_upstream(Pubmed_Download)
Pubmed_Chunk.set_upstream(Pubmed_Untar)
Pubmed_Zip.set_upstream(Pubmed_Chunk)
Pubmed_Persist.set_upstream(Pubmed_Zip)
Pubmed_Parquet.set_upstream(Pubmed_Persist)

Federal_Download = PythonOperator(
    task_id='Federal_Download',
    python_callable = federal_reporter_download.download,
    op_kwargs={'data_source_name': nsf_config.federal_reporter},
    retries=3,
    dag=dag,
)

Federal_Persist = PythonOperator(
    task_id='Federal_Persist',
    python_callable = federal_reporter_download.persist_hdfs,
    op_kwargs={'data_source_name': nsf_config.federal_reporter},
    retries=3,
    dag=dag,
)

Federal_Parquet = PythonOperator(
    task_id='Federal_Parquet',
    python_callable = federal_parquet_write.main,
    op_kwargs={'data_source_name': nsf_config.federal_reporter},
    retries=3,
    dag=dag,
)

Federal_Download.set_upstream(Federal_Reporter)
Federal_Persist.set_upstream(Federal_Download)
Federal_Parquet.set_upstream(Federal_Persist)

Grants_Download = PythonOperator(
    task_id='Grants_Gov_Download',
    python_callable = grants_gov_download_put_hdfs.download_grants_data,
#     op_kwargs={'data_source_name': nsf_config.grants_gov},
    retries=3,
    dag=dag,
)

Grants_Persists = PythonOperator(
    task_id='Grants_Persists',
    python_callable = grants_gov_download_put_hdfs.hdfs_put_grants_data,
#     op_kwargs={'data_source_name': nsf_config.grants_gov},
    retries=3,
    dag=dag,
)

Grants_Parquet = BashOperator(
    task_id='Grants_Parquet',
    bash_command = 'python /home/sghosh08/nsf_new/nsf_data_ingestion/nsf_data_ingestion/grants_gov/write_to_parquet_grants.py',
    retries=3,
    dag=dag,
)

Grants_Download.set_upstream(Grants_Gov)
Grants_Persists.set_upstream(Grants_Download)
Grants_Parquet.set_upstream(Grants_Persists)


Arxiv_Download = PythonOperator(
    task_id='Arxiv_Download',
    python_callable = arxiv_download.download,
    op_kwargs={'data_source_name': nsf_config.arxiv},
    retries=3,
    dag=dag,
)

Arxiv_Persist = PythonOperator(
    task_id='Arxiv_Persist',
    python_callable = arxiv_download.persist_hdfs,
    op_kwargs={'data_source_name': nsf_config.arxiv},
    retries=3,
    dag=dag,
)

Arxiv_Parquet = BashOperator(
    task_id='Arxiv_Parquet',
    bash_command='python /home/sghosh08/nsf_new/nsf_data_ingestion/nsf_data_ingestion/arxiv/arxiv_parquet_write.py',
    retries=3,
    dag=dag,
)

Arxiv_Download.set_upstream(Arxiv)
Arxiv_Persist.set_upstream(Arxiv_Download)
Arxiv_Parquet.set_upstream(Arxiv_Persist)

TFDIF_Model.set_upstream(Medline_Parquet)
# TFDIF_Model.set_upstream(Federal_Parquet)
TFDIF_Model.set_upstream(Arxiv_Parquet)
TFDIF_Model.set_upstream(Grants_Parquet)


SVD_Compute = PythonOperator(
    task_id='SVD_Compute',
    python_callable = large_scale_svd.tfidf_large_scale,
    op_kwargs={'data_source_name': nsf_config.svd_compute},
    retries=3,
    dag=dag,
)

SVD_Compute.set_upstream(TFDIF_Model)

# Kimun_Index = BashOperator(
#     task_id='Kimun_Index',
#     bash_command='date',
#     dag=dag)

Kimun_Index = PythonOperator(
    task_id='Kimun_Index',
    python_callable = kimun_loader.kimun_load,
#     op_kwargs={'data_source_name': nsf_config.svd_compute},
    retries=8,
    dag=dag,
)

Kimun_Index.set_upstream(SVD_Compute)
