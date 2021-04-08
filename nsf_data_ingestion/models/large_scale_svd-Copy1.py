from gensim.models.lsimodel import Projection
import findspark
findspark.init('/opt/cloudera/parcels/SPARK2-2.3.0.cloudera3-1.cdh5.13.3.p0.458809/lib/spark2/')
import pyspark
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as fn
from pyspark.sql import SparkSession
from subprocess import call
from pyspark.ml.linalg import VectorUDT
import logging
logging.getLogger().setLevel(logging.INFO)

def create_spark_session(name):
    logging.info('Creating Spark Session.....')
    spark = SparkSession.builder.config("spark.executor.instances", '3')\
    .config("spark.executor.memory", '30g')\
    .config('spark.executor.cores', '7')\
    .config('spark.cores.max', '7')\
    .config('spark.kryoserializer.buffer.max.mb', '2000')\
    .appName(name)\
    .getOrCreate()
    logging.info('Spark Session Created.....')
    
    logging.info('Adding Libraries....')
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/gensim.zip')
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/boto3.zip')
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/botocore.zip')
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/jmespath.zip')
    spark.sparkContext.addPyFile('/home/eileen/nsf_data_ingestion/libraries/smart_open.zip')
    spark.sparkContext.addPyFile('/home/ananth/nsf_data_ingestion/dist/nsf_data_ingestion-0.0.1-py3.6.egg')
    return spark


def create_projection(m, k, docs, power_iters=2, extra_dims=10):
    yield Projection(m, k, docs=docs, use_svdlibc=False, power_iters=power_iters, extra_dims=extra_dims)


def merge(p1, p2, decay=1.):
    p1.merge(p2, decay=decay)
    return p1


def binary_aggregate(rdd, f):
    """Aggregate rdd using function f in a binary tree.
    By definition, it will return an RDD with one partition
    """

    zeroValue = None, True

    def op(x, y):
        if x[1]:
            return y
        elif y[1]:
            return x
        else:
            return f(x[0], y[0]), False

    combOp = op
    seqOp = op

    def aggregatePartition(iterator):
        acc = zeroValue
        for obj in iterator:
            acc = seqOp(acc, obj)
        yield acc

    partiallyAggregated = rdd. \
        map(lambda x: (x, False)). \
        mapPartitions(aggregatePartition)

    numPartitions = partiallyAggregated.getNumPartitions()

    # binary partitions
    scale = 2

    while numPartitions > scale:
        numPartitions /= scale
        curNumPartitions = int(numPartitions)

        def mapPartition(i, iterator):
            for obj in iterator:
                yield (i % curNumPartitions, obj)

        partiallyAggregated = partiallyAggregated \
            .mapPartitionsWithIndex(mapPartition) \
            .reduceByKey(combOp, curNumPartitions) \
            .values()

    # by definition it should be one partition
    return partiallyAggregated.keys()



def compute_svd(corpus_rdd, m, k, power_iters=2, extra_dims=10):
    """Compute SVD using GenSim Projection class. Each entry in `corpus_rdd` should a tuple array with tuples
    of the form (token_id, value). For example, each entry could be the sparse tfidf representation of a document
    """
    logging.info('Computing SVD........')
    # Build one project per partition
    projections_rdd = corpus_rdd. \
        mapPartitions(lambda x: create_projection(m, k, list(x), power_iters=power_iters, extra_dims=extra_dims))

    # Merge projects one by one on the mappers
    return binary_aggregate(projections_rdd, merge)


def write_parquet(topic_df, topic_path):
    logging.info('Writing parquet Files.....')
    
#     if not call(["hdfs", "dfs", "-test", "-d", topic_path]):
#         logging.info('Parquet Files Exist....... Deleting Old Parquet Files')
#         call(["hdfs", "dfs", "-rm", "-r", "-f", topic_path])

    topic_df.write.parquet(topic_path, mode="overwrite")
    logging.info('Files Persisted to - %s', topic_path)


def tfidf_large_scale(data_source_name):
    
    spark = create_spark_session(data_source_name)

    import nsf_data_ingestion as nsf
    from nsf_data_ingestion.config import nsf_config
    from nsf_data_ingestion.objects import data_source_params
    param_list = data_source_params.mapping.get(data_source_name)
    
    print(param_list)
    
    # tfidf result location
    tfidf_path = '/user/eileen/tfidf.parquet'
    # where to save tfidf with SVD
    topic_path = '/user/eileen/topic_svd/'
    # number of dimensions
    num_topics = 100
#     tfidf_path = param_list.get('tfidf_path')
    # where to save tfidf with SVD
#     topic_path = param_list.get('tfidf_topic_path')
    # number of dimensions
#     num_topics = param_list.get('num_topics')

    logging.info('Reading TFIDF Parquet Files.....')
    tfidf_all = spark.read.parquet(tfidf_path)

    m = tfidf_all.first().tfidf.size
    
    corpus_rdd = tfidf_all.\
                 select('tfidf').rdd.\
                 map(lambda row: tuple(zip(row.tfidf.indices, row.tfidf.values)))
    
    
    model = compute_svd(corpus_rdd, m, num_topics).first()
    
    u = model.u
    sinv = 1 / model.s
    
    u_bc = spark.sparkContext.broadcast(u)
    sinv_bc = spark.sparkContext.broadcast(sinv)
    
    def transform(tfidf):
        return Vectors.dense((sinv_bc.value * tfidf.dot(u_bc.value)))


    udf_transform = fn.udf(transform, VectorUDT())
    topic_df = tfidf_all.select('*', udf_transform('tfidf').alias('topic')).drop('tfidf')
    
    write_parquet(topic_df, topic_path)
    spark.stop()


