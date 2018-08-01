import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestWriteRdd {
  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  val conf: SparkConf = new SparkConf().setAppName("CreateTable").setMaster("local[2]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val file: RDD[String] = sc.textFile("C:\\Users\\10713\\Desktop\\testjson.json")
    val context: RDD[Array[String]] = file.map(_.split(","))
    context.saveAsTextFile("C:\\Users\\10713\\Desktop\\output.json")
  }

}
