import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ThirstyTM on 2015-12-14.
  */

class SparkJoin(dataset: String){
  case class RecordR(a: Int, b: Int, c: Int, value: Int)
  case class RecordA(a: Int, x: String)
  case class RecordB(b: Int, y: String)
  case class RecordC(c: Int, z: String)

  val currentDir = System.getProperty("user.dir") // get the current directory
  System.setProperty("hadoop.home.dir", currentDir)

  def sparkConf(n: Int) = new SparkConf().setMaster("local[" + 8 + "]").setAppName("SparkJoin")

  val sc = new SparkContext(sparkConf(2))
  val records = (sc textFile dataset).map(_ split ",")

  def sqlJoin : RDD[Row] = {
    val sc = new SparkContext(sparkConf(2))
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create RDDs for each relation and register all of them as tables.
    val relR = records.filter(p => p(0) equals "R")
      .map(p => RecordR(p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt)).toDF()
    relR registerTempTable "R"

    val relA = records.filter(p => p(0) equals "A")
      .map(p => RecordA(p(1).trim.toInt, p(2))).toDF()
    relA registerTempTable "A"

    val relB = records.filter(p => p(0) equals "B")
      .map(p => RecordB(p(1).trim.toInt, p(2))).toDF()
    relB registerTempTable "B"

    val relC = records.filter(p => p(0) equals "C")
      .map(p => RecordC(p(1).trim.toInt, p(2))).toDF()
    relC registerTempTable "C"

    val joinSQL = sqlContext.sql("SELECT R.a, R.b, R.c, R.value, A.x, B.y, C.z FROM R, A, B, C WHERE R.a = A.a AND R.b = B.b AND R.c = C.c")

    joinSQL.rdd
  }

  //Performs "chained" join in the following order ((R x A) x B) x C
  def join: RDD[(Int, Int, Int, Int, String, String, String)] = {
    val sc = new SparkContext(sparkConf(2))
    val records = (sc textFile dataset).map(_ split ",")

    val relR = records.filter(p => p(0) equals "R")
      .map(r => (r(1).trim.toInt, (r(1).trim.toInt, r(2).trim.toInt, r(3).trim.toInt, r(4).trim.toInt))) //['R', a, b, c, value] to (a, [a, b, c, value])

    val relA = records.filter(p => p(0) equals "A")
      .map(a => (a(1).trim.toInt, a(2))) //['A', a, x] to (a, x)

    // join relR and relA so that the result will be (a, ([a, b, c, value], x))
    // then transform result to (b, [a, b, c, value, x])
    val relRxA = relR.join(relA)
      .map{case(key, ((a, b, c, value), x)) => (b, (a, b, c, value, x))}

    val relB = records.filter(p => p(0) equals "B")
      .map(b => (b(1).trim.toInt, b(2))) //['B', b, y] to (b, y)

    // join relRxA and relB so the result will be: (b, ([a, b, c, value, x], y))
    // then transform result to (c, [a, b, c, value, x, y])
    val relRxAxB = relRxA.join(relB)
      .map{case(key, ((a, b, c, value, x), y)) => (c, (a, b, c, value, x, y))}

    val relC = records.filter(p => p(0) equals "C")
      .map(c => (c(1).trim.toInt, c(2))) //['C', c, z] to (c, z)

    // join relRxAxB and relC so the result will be: (c, ([a, b, c, value, x, y], z))
    // then transform result to [a, b, c, value, x, y, z])
    val relRxAxBxC = relRxAxB.join(relC)
      .map{case(key, ((a, b, c, value, x, y), z)) => (a, b, c, value, x, y, z)}

//    relRxAxBxC.foreach(println)
    relRxAxBxC
  }

  val relR = records.filter(p => p(0) equals "R")
  val relA = records.filter(p => p(0) equals "A")
  val relB = records.filter(p => p(0) equals "B")
  val relC = records.filter(p => p(0) equals "C")
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

    val relR2 = relR.map(p => (
      (hA(p(1)), hA(p(2)), hA(p(3))), //new key
      (p(3).trim.toInt, 'R') // new value
      )
    )

    val relA2 = relA.flatMap(p => {
      for (
        j <- 1 to b;
        k <- 1 to c
      ) yield ((hA(p(1)), j, k), (p(2), 'A')) //replicate to j and k
    })

    val relB2 = relB.flatMap(p => {
      for (
        i <- 1 to a;
        k <- 1 to c
      ) yield ((i, hB(p(1)), k), (p(2), 'B')) //replicate to i and k
    })

    val relC2 = relC.flatMap(p => {
      for (
        i <- 1 to a;
        j <- 1 to b
      ) yield ((i, j, hC(p(1))), (p(2), 'C')) //replicate to i and j
    })

    relR2.join(relA2).join(relB2).join(relC2) //combine rdds
  }
}
