
import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by waip on 11/11/2016.
 */
object Test {
  def main(args : Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName("贝叶斯分类")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //(500000,[2749,21346,23618,34257,55903,69])

    var a = "2749,21346,23618,34257"
      //[2749.0,21346.0,23618.0,34257.0]
    val dense: Vector = Vectors.dense(2749, 21346, 23618, 34257)
    println(dense)
    a = "[2749.0,21346.0,23618.0,34257.0]"
    Vectors.dense(a.asInstanceOf[Vector].toArray)
    //(0.0,[2749.0,21346.0,23618.0,34257.0])
    val point: LabeledPoint = LabeledPoint(0.toDouble, dense)
    println(point)
    sc.stop()
  }
}
