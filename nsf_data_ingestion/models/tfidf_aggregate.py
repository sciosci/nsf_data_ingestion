import sys
sys.path.append('/home/ananth/nsf_data_ingestion/')
from nsf_data_ingestion.config import nsf_config
from nsf_data_ingestion.objects import data_source_params
import os
import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')

import pyspark
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, RegexTokenizer, StopWordsRemover, IDF
from pyspark.ml import Pipeline
from pyspark.sql import Row

import requests
# getting stop words
stop_words = requests.get(nsf_config.stop_words_url).text.split()

from functools import reduce

def create_spark_session(name):
    spark = SparkSession.builder.config("spark.executor.memory", '30g')\
    .config('spark.executor.cores', '7')\
    .config('spark.cores.max', '7')\
    .appName('tfdf')\
    .getOrCreate()
    return spark

def read_medline(spark, processed_path):
    """Creates a dataframe with the columns:
    `id`: global id
    `source`: medline
    `source_id`: PMID
    `type`: publication
    `title`
    `venue`: journal
    `abstract`
    `scientists`: authors
    `organizations`: affiliation
    `date`: publication date
    `content`: concatenation of abstract, affiliation, author, and journal
    """
    medline_path = processed_path
    medline_df = spark.read.parquet(medline_path)
    return medline_df.select(
            fn.concat(fn.lit('medline_'), fn.col('pmid')).alias('id'),
            fn.lit('medline').alias('source'),
            fn.col('pmid').astype('string').alias('source_id'),
            fn.lit('publication').alias('type'),
            'title',
            fn.col('journal').alias('venue'),
            'abstract',
            fn.col('author').alias('scientists'),
            fn.col('affiliation').alias('organizations'),
            fn.col('pubdate').alias('date'),
            fn.concat_ws(' ',
                      fn.col('abstract'),
                      fn.col('affiliation'),
                      fn.col('author'),
                      fn.col('journal')).alias('content'),
            fn.lit(None).astype('string').alias('end_date'),
            fn.lit(None).astype('string').alias('city'),
            fn.lit(None).astype('string').alias('country'),
            fn.lit(None).astype('string').alias('other_id')
    )

def read_federal_exporter(spark, processed_path):
    """Creates a dataframe with the columns:
    `id`: global id
    `source`: federal_exporter
    `source_id`: project id
    `type`: grant
    `title`: PROJECT_TITLE
    `venue`: AGENCY
    `abstract`
    `scientists`: CONTACT_PI + OTHER_PIS
    `organizations`: ORGANIZATION_NAME
    `date`: BUDGET_START_DATE
    `content`: concatenation of abstract, title, PIs, agency, and organization name
    `end_date`: BUDGET_END_DATE
    `city`: ORGANIZATION_CITY
    `country`: ORGANIZATION_COUNTRY
    `other_id`: PROJECT_NUMBER
    """
    abstracts_df = spark.read.parquet(os.path.join(processed_path, 'abstracts.parquet'))
    projects_df = spark.read.parquet(os.path.join(processed_path, 'projects.parquet'))
    together_df = projects_df.join(abstracts_df, 'PROJECT_ID')
    return together_df.select(fn.concat(fn.lit('fe_'), fn.col('PROJECT_ID')).alias('id'),
        fn.lit('federal_exporter').alias('source'),
        fn.col('PROJECT_ID').astype('string').alias('source_id'),
        fn.lit('grant').alias('type'),
        fn.col('PROJECT_TITLE').alias('title'),
        fn.col('AGENCY').alias('venue'),
        fn.col('ABSTRACT').alias('abstract'),
        fn.concat_ws('; ',
                     fn.col('CONTACT_PI_PROJECT_LEADER'),
                     fn.col('OTHER_PIS')).alias('scientists'),
        fn.col('ORGANIZATION_NAME').astype('string').alias('organizations'),
        fn.col('BUDGET_START_DATE').alias('date'),
        fn.concat_ws(' ',
                  fn.col('ABSTRACT'),
                  fn.col('PROJECT_TITLE'),
                  fn.concat_ws(' ', fn.col('CONTACT_PI_PROJECT_LEADER'), fn.col('OTHER_PIS')),
                  fn.col('AGENCY'),
                  fn.col('ORGANIZATION_NAME')
            ).alias('content'),
        fn.col('BUDGET_END_DATE').alias('end_date'),
        fn.col('ORGANIZATION_CITY').alias('city'),
        fn.col('ORGANIZATION_COUNTRY').alias('country'),
        fn.col('PROJECT_NUMBER').alias('other_id')
)

def read_arxiv(spark, processed_path):
    """Creates a dataframe with the columns:
    `id`: global id
    `source`: arxiv
    `source_id`: arxiv id
    `type`: publication
    `title`
    `venue`: concatenation of subjects
    `abstract`
    `scientists`: authors
    `organizations`: null
    `date`: publication date
    `content`: concatenation of abstract, affiliation, author, and journal
    """
    arxiv_path = os.path.join(processed_path)
    arxiv_df = spark.read.parquet(arxiv_path)
    return arxiv_df.select(
        fn.concat(fn.lit('arxiv_'), fn.col('id')).alias('id'),
        fn.lit('arxiv').alias('source'),
        fn.col('id').astype('string').alias('source_id'),
        fn.lit('publication').alias('type'),
        'title',
        fn.concat_ws('; ', 'subjects').alias('venue'),
        'abstract',
        fn.concat_ws(';', 'authors').alias('scientists'),
        fn.lit(None).astype('string').alias('organizations'),
        fn.col('datastamp').alias('date'),
        fn.concat_ws(' ',
                  fn.col('abstract'),
                  fn.col('title'),
                  fn.concat_ws(' ', 'authors'),
                  fn.concat_ws(' ', 'subjects')).alias('content'),
        fn.lit(None).astype('string').alias('end_date'),
        fn.lit(None).astype('string').alias('city'),
        fn.lit(None).astype('string').alias('country'),
        fn.lit(None).astype('string').alias('other_id')
    )


def add_rowid(x):
    """Called on a RDD when zipWithIndex() is used"""
    return Row(row_id = x[1], **x[0].asDict())

def fit_tfidf_pipeline(content_df):
    tokenizer = RegexTokenizer(). \
        setGaps(False). \
        setPattern('\\p{L}+'). \
        setInputCol('content'). \
        setOutputCol('words')

    sw = StopWordsRemover() \
        .setStopWords(stop_words) \
        .setCaseSensitive(False) \
        .setInputCol("words") \
        .setOutputCol("filtered")

    cv = CountVectorizer(). \
        setInputCol('filtered'). \
        setOutputCol('tf'). \
        setMinTF(1). \
        setMinDF(10). \
        setVocabSize(2 ** 17)

    # fit dataframe_df
    cv_transformer = Pipeline(stages=[tokenizer, sw, cv]).fit(content_df)

    idf = IDF(minDocFreq=10). \
        setInputCol('tf'). \
        setOutputCol('tfidf')

    tfidf_transformer = Pipeline(stages=[cv_transformer, idf]).fit(content_df)

    return tfidf_transformer

def main(data_source):
    processed_path = '/user/ananth/medline/parquet/'
    fed_processed_path = '/user/ananth/data/raw/federal_exporter/'
    models_path = '/user/ananth/tdif/'
    tfidf_path = '/user/ananth/tdifupdate/'
    arxiv_path = '/user/ananth/arxiv/parquet/'
    spark = create_spark_session('tfdf')

    arxiv_df = read_arxiv(spark, arxiv_path)
    medline_df = read_medline(spark, processed_path)
    fe_df = read_federal_exporter(spark, fed_processed_path)

    dataframe_list = [medline_df, fe_df]
    all_data_df = content_df = reduce(DataFrame.unionAll, dataframe_list)

    tfidf_transformer = fit_tfidf_pipeline(all_data_df)
    tfidf_model_path = os.path.join(models_path, 'tfidf_transformer.model')

    tfidf_transformer.write().overwrite().save(tfidf_model_path)
    tfidf_df = tfidf_transformer.transform(all_data_df). \
            select(all_data_df.columns + ['tfidf'])
    tfidf_df.write.parquet(tfidf_path, mode='overwrite')