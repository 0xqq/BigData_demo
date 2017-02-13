import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by waip on 11/10/2016.
 */
object TfIdfTest {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("TfIdfTest")
    val sc = new SparkContext(conf)

    // Load documents (one per line).
    val documents: RDD[Seq[String]] = sc.textFile("...").map(_.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[linalg.Vector] = hashingTF.transform(documents)
    //    val tf: RDD[Vector] = transform

    tf.cache()
    val idf = new IDF().fit(tf)
    val transform: RDD[linalg.Vector] = idf.transform(tf)
    //    val tfidf: RDD[Vector] = transform
  }
}
