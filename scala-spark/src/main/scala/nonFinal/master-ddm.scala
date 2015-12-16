import examples.Record
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ThirstyTM on 2015-12-14.
  */

case class RecordR(a: Int, b: Int, c: Int, value: Int)
case class RecordA(a: Int, x: String)
case class RecordB(b: Int, y: String)
case class RecordC(c: Int, z: String)

class SparkSQLJoin(dataset: String){
  val currentDir = System.getProperty("user.dir") // get the current directory
  System.setProperty("hadoop.home.dir", currentDir)

  def sparkConf(n: Int) = new SparkConf().setMaster("local[" + n + "]").setAppName("SparkSQLJoin")

  def sqlJoin = {
    val sc = new SparkContext(sparkConf(2))
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create RDDs for each relation and register all of them as tables.
    val relR = sc.textFile(dataset)
      .map(_.split(","))
      .filter(p => p(0) equals "R")
      .map(p => RecordR(p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt)).toDF()
    relR registerTempTable "R"

    val relA = sc.textFile(dataset)
      .map(_.split(","))
      .filter(p => p(0).equals("A"))
      .map(p => RecordA(p(1).trim.toInt, p(2))).toDF()
    relA registerTempTable "A"

    val relB = sc.textFile(dataset)
      .map(_.split(","))
      .filter(p => p(0).equals("B"))
      .map(p => RecordB(p(1).trim.toInt, p(2))).toDF()
    relB registerTempTable "B"

    val relC = sc.textFile(dataset)
      .map(_.split(","))
      .filter(p => p(0).equals("C"))
      .map(p => RecordC(p(1).trim.toInt, p(2))).toDF()
    relC registerTempTable "C"


    val joinSQL = sqlContext.sql("SELECT * FROM R, A, B, C WHERE R.a = A.a AND R.b = B.b AND R.c = C.c")

    joinSQL.collect().foreach(println)
  }
}