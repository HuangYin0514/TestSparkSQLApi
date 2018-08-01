import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JDBCReadMysql {
  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  val conf: SparkConf = new SparkConf().setAppName("CreateTable").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    val jdbcDF: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://192.168.25.185/bigdata",
        "dbtable" -> "emp",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "password" -> "root"
      )).load()
    jdbcDF.show()

  }

}
