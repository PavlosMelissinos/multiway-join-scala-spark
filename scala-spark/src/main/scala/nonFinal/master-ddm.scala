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

  val relR = records.filter(p => p(0) equals "R")
  val relA = records.filter(p => p(0) equals "A")
  val relB = records.filter(p => p(0) equals "B")
  val relC = records.filter(p => p(0) equals "C")

  def sqlJoin : DataFrame = {
//    val sc = new SparkContext(sparkConf(2))
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create RDDs for each relation and register all of them as tables.
    val relRFinal = relR.map(p => RecordR(p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt)).toDF()
    relRFinal registerTempTable "R"

    val relAFinal = relA.map(p => RecordA(p(1).trim.toInt, p(2))).toDF()
    relAFinal registerTempTable "A"

    val relBFinal = relB.map(p => RecordB(p(1).trim.toInt, p(2))).toDF()
    relBFinal registerTempTable "B"

    val relCFinal = relC.map(p => RecordC(p(1).trim.toInt, p(2))).toDF()
    relCFinal registerTempTable "C"

    val joinSQL = sqlContext.sql("SELECT R.a, R.b, R.c, R.value, A.x, B.y, C.z FROM R, A, B, C WHERE R.a = A.a AND R.b = B.b AND R.c = C.c")
    joinSQL
//    joinSQL foreach println
//    val jsqlrdd = joinSQL.rdd
//    jsqlrdd foreach println
//    jsqlrdd
  }

  //Performs "chained" join in the following order ((R x A) x B) x C
  def join: RDD[(Int, Int, Int, Int, String, String, String)] = {

    val relRFinal = relR.map(r => (r(1).trim.toInt, (r(1).trim.toInt, r(2).trim.toInt, r(3).trim.toInt, r(4).trim.toInt))) //['R', a, b, c, value] to (a, [a, b, c, value])

    val relAFinal = relA.map(a => (a(1).trim.toInt, a(2))) //['A', a, x] to (a, x)

    // join relR and relA so that the result will be (a, ([a, b, c, value], x))
    // then transform result to (b, [a, b, c, value, x])
    val relRxA = relRFinal.join(relAFinal)
      .map{case(key, ((a, b, c, value), x)) => (b, (a, b, c, value, x))}

    val relBFinal = relB.map(b => (b(1).trim.toInt, b(2))) //['B', b, y] to (b, y)

    // join relRxA and relB so the result will be: (b, ([a, b, c, value, x], y))
    // then transform result to (c, [a, b, c, value, x, y])
    val relRxAxB = relRxA.join(relBFinal)
      .map{case(key, ((a, b, c, value, x), y)) => (c, (a, b, c, value, x, y))}

    val relCFinal = relC.map(c => (c(1).trim.toInt, c(2))) //['C', c, z] to (c, z)

    // join relRxAxB and relC so the result will be: (c, ([a, b, c, value, x, y], z))
    // then transform result to [a, b, c, value, x, y, z])
    val relRxAxBxC = relRxAxB.join(relCFinal)
      .map{case(key, ((a, b, c, value, x, y), z)) => (a, b, c, value, x, y, z)}

//    relRxAxBxC.foreach(println)
    relRxAxBxC
  }


  //    : RDD[Array[String]]
  /**
    * a = sqrt3(k * d1 * d1 / (d2 * d3))
    * b = sqrt3(k * d2 * d2 / (d1 * d3))
    * c = sqrt3(k * d3 * d3 / (d1 * d2))
    * d
    */
  def starJoin (reducers: Int) = {

    val d1 = relA.count()
    val d2 = relB.count()
    val d3 = relC.count()

    val currentDir = System.getProperty("user.dir") // get the current directory
    System.setProperty("hadoop.home.dir", currentDir)

    //compute a,b,c according to the paper's instructions
    val k = reducers // = a * b * c
    val a = Math.cbrt(k * d1 * d1 / (d2 * d3)).toInt
    val b = Math.cbrt(k * d2 * d2 / (d1 * d3)).toInt
    val c = Math.cbrt(k * d3 * d3 / (d1 * d2)).toInt

    //hash functions mapping each record to a reducer
    val hA = (n: String) => n.trim.toInt % a
    val hB = (n: String) => n.trim.toInt % b
    val hC = (n: String) => n.trim.toInt % c
    //    def toInt(a: String) = a.trim.toInt

//    def mapFun(x: Array[String]): Array[String] = x match {
//    val mapFun: (Array[String] => Array[String]) = (x: Array[String]) => x match {

//    def mapFun(r: Record) = r match {
//      case RecordR => ((hA(r.a), r.b % b, r.c), (r.value, 'R'))
//      case Array("A", a: String, x: String)  => for {j <- 1 to b; k <- 1 to c} yield ((hA(a), j, k), (x, 'A'))
//      case Array("B", b: String, y)          => for {i <- 1 to a; k <- 1 to c} yield ((i, hB(b), k), (y, 'B'))
//      case Array("C", c, z)                  => for {i <- 1 to a; j <- 1 to b} yield ((i, j, hC(c)), (z, 'C'))
//    }

//    val toRecord = (sarr: Array[String]) => {
//      if (sarr(0) equals "R")
//    }
    def mapFun (rec: Array[String]): Seq[((Int, Int, Int), (Any, Char))]= {
//      rec match {
//        case ("R", a, b, c, value) => (hA(a), hB(b), hC(c))
//      }
      if (rec(0) equals "R") {
//      if (rec.isInstanceOf[RecordR]) {
        val recR = rec.asInstanceOf[RecordR]
        Seq(((recR.a % a, recR.b % b, recR.c % c), (recR.value, 'R'))) //new key
//        List((recR.a % a, recR.b % b, recR.c % c), (recR.value, 'R'))
      }
      else if (rec(0) equals "A") {
//      else if (rec.isInstanceOf[RecordA]) {
        val recA = rec.asInstanceOf[RecordA]
        for {
          j <- 1 to b;
          k <- 1 to c
        } yield ((recA.a % a, j, k), (recA.x, 'A'))
//        return res
//        res
      }
      else if (rec(0) equals "B") {
//      else if (rec.isInstanceOf[RecordB]) {
        val recB = rec.asInstanceOf[RecordB]
        for {
          i <- 1 to a;
          k <- 1 to c
        } yield ((i, recB.b % b, k), (recB.y, 'B'))
//        return res
      }
      else if (rec(0) equals "C") {
//      else if (rec.isInstanceOf[RecordC]) {
        val recC = rec.asInstanceOf[RecordC]
        for {
          i <- 1 to a;
          j <- 1 to b
        } yield ((i, j, recC.c % c), (recC.z, 'C'))
//        return res
      }
      else {
        return Seq[((0, 0, 0), "Sss", 'O')]
      }
//      else return null
    }

//    val redFun: ((Tuple2, Tuple2) => Tuple2) = (kv1: Tuple2, kv2: Tuple2) => {
//      kv1._2
//      kv2._2
//    }
//    import org.apache.spark.SparkContext._
    val mapped = records.flatMap(mapFun)
    mapped.foreach(println)
//    val res = mapped.reduceByKey(_ + _)
//
//    (res, "joint a assigned to " + a + " reducers, joint b assigned to " + b + " reducers, joint c assigned to " + c + " reducers,")
  }
}
