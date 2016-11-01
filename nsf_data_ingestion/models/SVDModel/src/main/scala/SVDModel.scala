import org.apache.spark.sql.{SparkSession}

import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.ml.linalg._
import org.apache.spark.mllib.linalg.Vectors


object SVDModels {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("svd-models")
      .getOrCreate()

    import spark.sqlContext.implicits._
    val k = args(0).toInt

    val tfidf_path = args(1)
    val S_path = args(2)
    val U_path = args(3)
    val V_path = args(4)


    var tfidf_wrow_df = spark.read.parquet(args(1))

    var vectors = tfidf_wrow_df
        .select("tfidf")
        .rdd.map(x => Vectors.dense(x.get(0).asInstanceOf[SparseVector].toArray))
        .cache()

    var mat = new RowMatrix(vectors)
    var svd = mat.computeSVD(k, computeU = true)

    val S = spark.sparkContext.parallelize(svd.s.toArray).toDF()
    val U = svd.U.rows.map(x => x.toArray).toDF()
    val V = spark.sparkContext
        .parallelize(svd.V.transpose.toArray.grouped(svd.V.numCols).toList)
        .toDF()

    S.write.mode("overwrite").parquet(S_path)
    U.write.mode("overwrite").parquet(U_path)
    V.write.mode("overwrite").parquet(V_path)

    spark.stop()
  }
}
