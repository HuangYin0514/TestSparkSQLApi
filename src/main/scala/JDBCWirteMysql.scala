import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JDBCWirteMysql {
  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  val conf: SparkConf = new SparkConf().setAppName("CreateTable").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    val file: RDD[String] = sc.textFile("hdfs://mini1:9000/ceshi/testjson.txt")
    val fileContext: RDD[Array[String]] = file.map(_.split(","))
    val personRDD: RDD[Person] = fileContext.map(x => Person(x(0).toLong, x(1), x(2).toInt))
    import sqlContext.implicits._
    val personDF: DataFrame = personRDD.toDF
    val psersonTable: Unit = personDF.registerTempTable("t_person")
    //写数据库
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    personDF.write.mode("append").jdbc("jdbc:mysql://192.168.25.185/bigdata", "person", properties)
    //写文件到json
    personDF.write.json("C:\\Users\\10713\\Desktop\\output2")
  }
}
