import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

/**
 * KMeans分类
 */
object KMeansClustering {
  def main(args: Array[String]) {
    //    if (args.length < 5) {
    //
    //      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations runTimes")
    //      sys.exit(1)
    //    }

    val conf = new
        SparkConf().setAppName("Spark MLlib Exercise:K-Means Clustering").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /**
     * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
     * 2 3
     12669 9656 7561 214 2674 1338
     * 2 3 7057 9810 9568 1762 3293 1776
     * 2 3 6353 8808
     7684 2405 3516 7844
     */

    val dots: util.List[String] = ReadColorTest.getDots("C:\\Users\\waip\\Desktop\\904499178939556664.jpg").asInstanceOf[util.List[String]]
    val iter: util.Iterator[String] = dots.iterator()
    var list: List[(Int,Int,Int)] = List()
    while (iter.hasNext){
       val ds: Array[String] = iter.next().split("-")
       list = (ds(0).toInt,ds(1).toInt,ds(2).toInt) :: list
    }

    val rdd: RDD[(Int, Int, Int)] = sc.parallelize(list)
    val parsedTrainingData =
      rdd.map(line => {
        Vectors.dense(line._1.toDouble,line._2.toDouble,line._3.toDouble)
      }).cache()

    // Cluster the data into two classes using KMeans

    val numClusters = 20
    val numIterations = 20
    var clusterIndex: Int = 0
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations)
    val ssd = clusters.computeCost(parsedTrainingData)
    println("sum of squared distances of points to their nearest center when k=" + clusters + " -> " + ssd)

    println("Cluster Number:" + clusters.clusterCenters.length)

    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {

        println("Center Point of Cluster " + clusterIndex + ":")

        println(x)
        clusterIndex += 1
      })

    //begin to check which cluster each test data belongs to based on the clustering result

    /*val rawTestData = sc.textFile("C:\\Users\\waip\\Desktop\\test.csv")
    val parsedTestData = rawTestData.filter(!isColumnNameLine(_)).map(line => {

      Vectors.dense(line.split("\\,").map(_.trim).filter(!"".equals(_)).map(_.toDouble))

    })
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex:
      Int = clusters.predict(testDataLine)

      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering test finished.")*/
  }

  private def isColumnNameLine(line: String): Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}