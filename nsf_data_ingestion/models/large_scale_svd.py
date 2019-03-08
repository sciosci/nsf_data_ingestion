"""
This module defines

"""
from gensim.models.lsimodel import Projection
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as fn
from .tfidf_model import create_spark_session


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
    # Build one project per partition
    projections_rdd = corpus_rdd. \
        mapPartitions(lambda x: create_projection(m, k, list(x), power_iters=power_iters, extra_dims=extra_dims))

    # Merge projects one by one on the mappers
    return binary_aggregate(projections_rdd, merge)


if __name__ == "__main__":
    # tfidf result location
    tfidf_path = sys.argv[1]
    # where to save tfidf with SVD
    topic_path = sys.argv[2]

    spark = create_spark_session('svd-computation')

    tfidf_all = spark.read.parquet(tfidf_path)

    m = tfidf_all.first().tfidf
    # number of dimensions
    num_topics = 100

    corpus_rdd = tfidf_all. \
        select('tfidf').rdd. \
        map(lambda row: tuple(zip(row.tfidf.indices, row.tfidf.values)))

    # find SVD
    model = compute_svd(corpus_rdd, m, num_topics).first()
    u = model.u
    sinv = 1 / model.s
    # distribute
    u_bc = spark.sparkContext.broadcast(u)
    sinv_bc = spark.sparkContext.broadcast(sinv)


    def transform(tfidf):
        return Vectors.dense((sinv_bc.value * tfidf.dot(u_bc.value)))


    udf_transform = fn.udf(transform, VectorUDT())
    topic_df = tfidf_all.select('*', udf_transform('tfidf').alias('topic')).drop('tfidf')

    # save results
    topic_df.write.parquet(topic_path, mode="overwrite")