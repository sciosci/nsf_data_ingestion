import sys
import os
import findspark
findspark.init()

import pyspark
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, RegexTokenizer, StopWordsRemover, IDF
from pyspark.ml import Pipeline


from pyspark.sql import Row

import requests
# getting stop words
stop_words = requests.get('http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words').text.split()

from functools import reduce


def create_spark_session(name):
    spark = SparkSession.builder.\
        appName(name).\
        enableHiveSupport().\
        config('spark.yarn.executor.memoryOverhead', '2g').\
        getOrCreate()
    return spark


def read_medline(spark, processed_path):
    """Creates a dataframe with the columns `source` == 'medline', `id` == `pmid`, and
    `content` == concatenation of abstract, affiliation, author, and journal
    """
    medline_path = os.path.join(processed_path, 'medline_last.parquet')
    medline_df = spark.read.parquet(medline_path)
    return medline_df.select(fn.lit('medline').alias('source'),
                             fn.col('pmid').astype('int').alias('id'),
                             fn.concat_ws(' ',
                                          fn.col('abstract'),
                                          fn.col('affiliation'),
                                          fn.col('author'),
                                          fn.col('journal')).alias('content')
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


if __name__ == "__main__":
    # location where processed parquet files are
    processed_path = sys.argv[1]
    # model location
    models_path = sys.argv[2]
    # tfidf result location
    tfidf_path = sys.argv[3]

    spark = create_spark_session('tfidf-computation')

    # read source
    medline_df = read_medline(spark, processed_path)
    # in the future, other sources will be added
    # ...
    dataframe_list = [medline_df, ]
    # put all content together
    content_df = reduce(DataFrame.unionAll, dataframe_list)

    tfidf_transformer = fit_tfidf_pipeline(content_df)

    tfidf_model_path = os.path.join(models_path, 'tfidf_transformer.model')
    tfidf_transformer.write().overwrite().save(tfidf_model_path)
    # add row number
    content_windex_rdd = content_df.rdd.zipWithIndex()
    content_windex_df = content_windex_rdd.map(add_rowid).toDF()

    tfidf_df = tfidf_transformer.transform(content_windex_df). \
        select('row_id', 'source', 'id', 'tfidf')

    tfidf_df.save.parquet(tfidf_path, mode='overwrite')
    spark.stop()