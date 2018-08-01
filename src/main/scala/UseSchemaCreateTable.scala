import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UseSchemaCreateTable {

  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  LoggerLevels.setStreamingLogLevels()

  val conf = new SparkConf().setAppName("CreateTable").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    val file: RDD[String] = sc.textFile("hdfs://mini1:9000/ceshi/testjson.txt")
    val personArr: RDD[Array[String]] = file.map(_.split(","))
    val schema = StructType {
      List(
        StructField("id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("age", IntegerType, false)
      )
    }
    val personRDD: RDD[Row] = personArr.map(x => Row(x(0).toInt,x(1),x(2).toInt))
    val personDF: DataFrame = sqlContext.createDataFrame(personRDD,schema)
    personDF.registerTempTable("t_person")
    val resDF: DataFrame = sqlContext.sql("select  * from t_person where age>18 limit 2")
    resDF.show()
  }

}

