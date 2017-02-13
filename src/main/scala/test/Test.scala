import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._
/**
  * Created by waip on 12/27/2016.
  */
object Test {


  def main(args: Array[String]): Unit = {
    //配置环境
    val spark = SparkSession
      .builder()
        .master("local[4]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.csv("/home/data/ai_idfa.csv").map{ r =>
      (r.getString(1) + r.getString(2) + "",r.getString(1),r.getString(2),r.getString(3),r.getString(4),r.getString(5)) //date,appid,appname,count,jsondata
    }//.toDF("ID","DATE","APPID","APPNAME","COUNT","JSONDATA")

//    df.show(20)


    df.rdd.saveToPhoenix("AI_IDFA",
      Seq("ID","DATE","APPID","APPNAME","COUNT","JSONDATA"),
      zkUrl = Some("dsj01:2181"))
//    df.saveToPhoenix("AI_IDFA",HBaseConfiguration.create(),Some("dsj01:2181"))

    spark.stop()
  }
}
