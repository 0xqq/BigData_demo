package demo

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by waip on 2/13/2017.
  */
object SparkSQL_demo {

  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SQLOnSpark")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._

    /** ******************************************************************
      * ***********************从json文件创建dataFrame**********************
      * *******************************************************************/

    var df: DataFrame = sqlContext.read.json("hdfs://master:9000/user/spark/data/people.json")
    df.registerTempTable("person")
    var teenegers: DataFrame = sqlContext.sql("select name,age from person")
    teenegers.map(x => "name:" + x(0) + " " + "age:" + x(1)).collect().foreach(println)

    /** ****************************************************************
      * *********************从parquet文件创建dataFrame********************
      * ******************************************************************/

    df = sqlContext.read.parquet("hdfs://master:9000/user/spark/data/namesAndAges.parquet")
    df.registerTempTable("person")
    teenegers = sqlContext.sql("select name,age from person")
    teenegers.map(x => "name:" + x(0) + " " + "age:" + x(1)).collect().foreach(println)

    /** ****************************************************************
      * *********************从普通RDD创建dataFrame_1********************
      * ****************************************************************/

    df = sc.textFile("hdfs://master:9000/user/spark/data/people.txt")
      .map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt))
      .toDF
    df.registerTempTable("people")
    teenegers = sqlContext.sql("select name,age from people")
    teenegers.map(x => "name:" + x(0) + " " + "age:" + x(1)).collect().foreach(println)

    /** ****************************************************************
      * ********          从普通RDD创建dataFrame_2          *************
      * ****************************************************************/

    val rowRDD = sc.textFile("hdfs://master:9000/user/spark/data/people.txt").map(_.split(",")).map(x => Row(x(0), x(1).trim))
    val schemaString = "name age"
    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    df = sqlContext.createDataFrame(rowRDD, schema)
    df.registerTempTable("people")
    var teenagers = sqlContext.sql("select name,age from people")
    teenagers.map(x => "name:" + x(0) + " " + "age:" + x(1)).collect().foreach(println)

    /** ****************************************************************
      * ********         测试dataframe的read和save方法      *************
      * ****************************************************************/

    //注意load方法默认是加载parquet文件
    df = sqlContext.read.load("hdfs://master:9000/user/spark/data/namesAndAges.parquet")
    df.select("name").write.save("hdfs://master:9000/user/spark/data/name.parquet")

    /** ****************************************************************
      * ********         测试dataframe的read和save方法      *************
      * ***********      可通过手动设置数据源和保存测mode   ****************
      * ****************************************************************/

    df = sqlContext.read.format("json").load("hdfs://master:9000/user/spark/ data/people.json")
    df.select("age").write.format("parquet").mode(SaveMode.Append)
      .save("hdfs://master:9000/user/spark/data/ages.parquet")

    /** ****************************************************************
      * ********             直接使用sql查询数据源            *************
      * ****************************************************************/

    df = sqlContext.sql("SELECT * FROM parquet.`hdfs://master:9000/user/spark/data/ages.parquet`")
    df.map(x => "name:" + x(0)).foreach(println)

    /** ****************************************************************
      * ********        parquest文件的读写     *************
      * ****************************************************************/

    val people = sc.textFile("hdfs://master:9000/user/spark/data/people.txt").toDF
    // The RDD is implicitly converted to a DataFrame by implicits, allowing it to be stored using Parquet.
    people.write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://master:9000/user/spark/data/people.parquet")
    // Read in the parquet file created above. Parquet files are self-describing so the schema is preserved.
    // The result of loading a Parquet file is also a DataFrame.
    val parquetFile = sqlContext.read.parquet("hdfs://master:9000/user/spark/data/people.parquet")
    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("parquetFile")
    teenagers = sqlContext.sql("SELECT name FROM parquetFile")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    /** ****************************************************************
      * *****************        Schema Merging     ********************
      * ****************************************************************/
    // Create a simple DataFrame, stored into a partition directory
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/user/spark/data/test_table/key=1")
    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/user/spark/data/test_table/key=2")
    // Read the partitioned table
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://master:9000/user/spark/data/test_table")
    df3.printSchema()
    df3.show()

    /** **************************************************************
      * *******************hive metastore ****************************
      * **************************************************************/

    sqlContext.setConf("spark.sql.shuffle.partitions", "5")
    sqlContext.sql("use my_hive")
    sqlContext.sql("create table if not exists sogouInfo (time STRING,id STRING,webAddr STRING,downFlow INT,upFlow INT,url STRING) row format delimited fields terminated by '\t'")
    sqlContext.sql("LOAD DATA LOCAL INPATH '/root/testData/SogouQ1.txt' overwrite INTO TABLE sogouInfo")
    sqlContext.sql(
      "select " +
        "count(distinct id) as c " +
        "from sogouInfo " +
        "group by time order by c desc limit 10"
    ).collect().foreach(println)

    /** **************************************************************
      * *******************df from jdbc eg:mysql *********************
      * **************************************************************/

     val jdbcDF = sqlContext.read.format("jdbc").options(
       Map("driver" -> "com.mysql.jdbc.Driver",
           "url" -> "jdbc:mysql://192.168.0.65:3306/test?user=root&password=root",
         "dbtable" -> "trade_total_info_copy")).load()
     jdbcDF.registerTempTable("trade_total_info_copy")
     sqlContext.sql("select * from trade_total_info_copy").foreach(println)

  }
}
