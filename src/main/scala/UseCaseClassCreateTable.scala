import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UseCaseClassCreateTable {

  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  val conf = new SparkConf().setAppName("CreateTable").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    val file: RDD[String] = sc.textFile("hdfs://mini1:9000/ceshi/testjson.txt")
    val personArr: RDD[Array[String]] = file.map(_.split(","))
    val person: RDD[Person] = personArr.map(personArr =>Person(personArr(0).toLong, personArr(1), personArr(2).toInt))
    import sqlContext.implicits._
    val personDF: DataFrame = person.toDF
    personDF.registerTempTable("t_person")
    val resDF: DataFrame = sqlContext.sql("select  * from t_person where age>18 limit 2")
    resDF.show()
  }

}

