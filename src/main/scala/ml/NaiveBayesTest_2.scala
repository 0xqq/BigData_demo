import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint


object NaiveBayesTest_2 {

  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local[4]").setAppName("贝叶斯分类")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //读取并处理数据
    val data = sc.textFile("C:\\Users\\waip\\Desktop\\sougou-train\\")
    val documents: RDD[Seq[String]] = sc.textFile("C:\\Users\\waip\\Desktop\\sougou-train\\").map(_.split(",").toSeq)

   //计算TF
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    //计算IDF
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val parsedData = data.map { line =>
      val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    // 切分数据为训练数据和测试数据
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    //训练模型
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    //测试数据
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
  }
}
