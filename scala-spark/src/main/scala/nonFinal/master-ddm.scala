import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ThirstyTM on 2015-12-14.
  */

abstract class Record
case class RecordR(a: Int, b: Int, c: Int, value: Int) extends Record
case class RecordA(a: Int, x: String) extends Record
case class RecordB(b: Int, y: String) extends Record
case class RecordC(c: Int, z: String) extends Record
case class KeyMap(i: Int, j: Int, k: Int)

class SparkJoin(dataset: String){

  val currentDir = System.getProperty("user.dir") // get the current directory
  System.setProperty("hadoop.home.dir", currentDir)

  val sparkConf = (n: Int) => new SparkConf().setMaster("local[" + n + "]").setAppName("SparkJoin")
  val cores = Runtime.getRuntime.availableProcessors //specify as many cores as the actual machine cores (physical or logical)
  val sc = new SparkContext(sparkConf(cores))
  val records = (sc textFile dataset).map(_ split ",")

  //split relations by type
  val rel = (relType: String) => records.filter(p => p(0) equals relType)

  def sqlJoin : DataFrame = {
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create RDDs for each relation and register all of them as tables.
    val relRFinal = rel("R").map(p => RecordR(p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt)).toDF()
    relRFinal registerTempTable "R"

    val relAFinal = rel("A").map(p => RecordA(p(1).trim.toInt, p(2))).toDF()
    relAFinal registerTempTable "A"

    val relBFinal = rel("B").map(p => RecordB(p(1).trim.toInt, p(2))).toDF()
    relBFinal registerTempTable "B"

    val relCFinal = rel("C").map(p => RecordC(p(1).trim.toInt, p(2))).toDF()
    relCFinal registerTempTable "C"

    val joinSQL = sqlContext.sql("SELECT R.a, R.b, R.c, R.value, A.x, B.y, C.z FROM R, A, B, C WHERE R.a = A.a AND R.b = B.b AND R.c = C.c")
    joinSQL
  }

  //Performs "chained" join in the following order ((R x A) x B) x C
  def join: RDD[(Int, Int, Int, Int, String, String, String)] = {
    val relRFinal = rel("R").map(r => (r(1).trim.toInt, (r(1).trim.toInt, r(2).trim.toInt, r(3).trim.toInt, r(4).trim.toInt))) //['R', a, b, c, value] to (a, [a, b, c, value])
    val relAFinal = rel("A").map(a => (a(1).trim.toInt, a(2))) //['A', a, x] to (a, x)
    val relBFinal = rel("B").map(b => (b(1).trim.toInt, b(2))) //['B', b, y] to (b, y)
    val relCFinal = rel("C").map(c => (c(1).trim.toInt, c(2))) //['C', c, z] to (c, z)

    // join relR and relA so that the result will be (a, ([a, b, c, value], x))
    // then transform result to (b, [a, b, c, value, x])
    val relRxA = relRFinal.join(relAFinal)
      .map{case(key, ((a, b, c, value), x)) => (b, (a, b, c, value, x))}
    // join relRxA and relB so the result will be: (b, ([a, b, c, value, x], y))
    // then transform result to (c, [a, b, c, value, x, y])
    val relRxAxB = relRxA.join(relBFinal)
      .map{case(key, ((a, b, c, value, x), y)) => (c, (a, b, c, value, x, y))}
    // join relRxAxB and relC so the result will be: (c, ([a, b, c, value, x, y], z))
    // then transform result to [a, b, c, value, x, y, z])
    relRxAxB.join(relCFinal).map{case(key, ((a, b, c, value, x, y), z)) => (a, b, c, value, x, y, z)}
  }


  /**
    * a = sqrt3(k * d1 * d1 / (d2 * d3))
    * b = sqrt3(k * d2 * d2 / (d1 * d3))
    * c = sqrt3(k * d3 * d3 / (d1 * d2))
    * d
    */
  def getAttrShares(reducers: Int): (Int, Int, Int) ={
    val d1 = rel("A").count()
    val d2 = rel("B").count()
    val d3 = rel("C").count()

    //compute attribute shares a,b,c according to the paper's instructions
    val k = reducers // = a * b * c
    val a = Math.cbrt(k * d1 * d1 / (d2 * d3)).toInt
    val b = Math.cbrt(k * d2 * d2 / (d1 * d3)).toInt
    val c = Math.cbrt(k * d3 * d3 / (d1 * d2)).toInt
    (a, b, c)
  }

  def starJoin (reducers: Int) = {

    val currentDir = System.getProperty("user.dir") // get the current directory
    System.setProperty("hadoop.home.dir", currentDir)
    val (a, b, c) = getAttrShares(reducers: Int)

    //hash functions matching each record value to a partial mapkey
    val hashFun = (n: Int, sz: Int) => n % sz + 1
    val hA = (n: Int) => hashFun(n, a)
    val hB = (n: Int) => hashFun(n, b)
    val hC = (n: Int) => hashFun(n, c)

    //mapper: assigns a unique mapkey to each record
    val mapFun: (Array[String] => Seq[((Int, Int, Int), (String, String))]) = (sArr: Array[String]) => {
      sArr(0) match{
        case "R" => {
          val recR = RecordR(sArr(1).trim.toInt, sArr(2).trim.toInt, sArr(3).trim.toInt, sArr(4).trim.toInt)
          val sValue = sArr(1) + "," + sArr(2) + "," + sArr(3) + "," + sArr(4)
          Seq(((hA(recR.a), hB(recR.b), hC(recR.c)), (sValue, "R")))
        }
        case "A" => {
          val recA = RecordA(sArr(1).trim.toInt, sArr(2).trim)
          val sValue = sArr(2)
          for {
            j <- 1 to b;
            k <- 1 to c
          } yield ((hA(recA.a), j, k), (sValue, "A"))
        }
        case "B" => {
          val recB = RecordB(sArr(1).trim.toInt, sArr(2).trim)
          val sValue = sArr(2)
          for {
            i <- 1 to a;
            k <- 1 to c
          } yield ((i, hB(recB.b), k), (sValue, "B"))
        }
        case "C" => {
          val recC = RecordC(sArr(1).trim.toInt, sArr(2).trim)
          val sValue = sArr(2)
          for {
            i <- 1 to a;
            j <- 1 to b
          } yield ((i, j, hC(recC.c)), (sValue, "C"))
        }
      }
    }

    val redFun = (a: (String, String), b: (String, String)) => {
      if (a._2.contains(b._2))
        (a._1, a._2)
      else if (b._2.contains(a._2))
        (b._1, b._2)
      else
        (a._1 + "," + b._1, "(" + a._2 + "x" + b._2 + ")")
    }

    val mapped = records.flatMap(mapFun) //assigns a mapkey to each record
    val saveDir = Array(System.getProperty("user.dir"), "output", "starJoinMapped").mkString(java.io.File.separator)
    mapped.saveAsTextFile(saveDir)
    mapped.reduceByKey((a, b) => redFun(a, b))
  }
}
