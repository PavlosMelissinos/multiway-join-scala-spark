import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

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

  //specify as many spark cores as the actual cpu cores (physical or logical)
  val cores = Runtime.getRuntime.availableProcessors
  val sc = new SparkContext(sparkConf(cores))

  val records = (sc textFile dataset).map(_ split ",")

  //split relations by type
  val rel = (relType: String) => records.filter(p => p(0) equals relType)

  def sqlJoin : DataFrame = {
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create RDDs for each relation and register each of them as a table.
    val relRFinal = rel("R").map(p => RecordR(p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt)).toDF()
    relRFinal registerTempTable "R"

    val relAFinal = rel("A").map(p => RecordA(p(1).trim.toInt, p(2))).toDF()
    relAFinal registerTempTable "A"

    val relBFinal = rel("B").map(p => RecordB(p(1).trim.toInt, p(2))).toDF()
    relBFinal registerTempTable "B"

    val relCFinal = rel("C").map(p => RecordC(p(1).trim.toInt, p(2))).toDF()
    relCFinal registerTempTable "C"

    sqlContext.sql("SELECT R.a, R.b, R.c, R.value, A.x, B.y, C.z FROM R, A, B, C WHERE R.a = A.a AND R.b = B.b AND R.c = C.c")
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
    * k is the number of reducers
    * a = sqrt3(k * d1 * d1 / (d2 * d3)) = d1 * Math.cbrt(k / d)
    * b = sqrt3(k * d2 * d2 / (d1 * d3)) = d2 * Math.cbrt(k / d)
    * c = sqrt3(k * d3 * d3 / (d1 * d2)) = d3 * Math.cbrt(k / d)
    * the reducers are logically (not physically) placed in the 3d plane according to each join attribute
    * for example,
    */
  def getAttrShares(reducers: Int): (Long, Long, Long) ={
    val d1 = rel("A").count()
    val d2 = rel("B").count()
    val d3 = rel("C").count()
    val d = d1 + d2 + d3

    //compute attribute shares a,b,c according to the paper's instructions
    val k = reducers // = a * b * c
    val a = d1 * Math.cbrt(k / d).toInt
    val b = d2 * Math.cbrt(k / d).toInt
    val c = d3 * Math.cbrt(k / d).toInt
    (a, b, c)
  }

  def starJoin (reducers: Int) = {

    val (a, b, c) = getAttrShares(reducers: Int)

    //hash functions matching each record value to a partial mapkey
    val hashFun = (n: Int, sz: Long) => n % sz + 1
    val hA = (n: Int) => hashFun(n, a)
    val hB = (n: Int) => hashFun(n, b)
    val hC = (n: Int) => hashFun(n, c)

    //mapper: assigns a unique mapkey to each record
    val mapFun: (Array[String] => Seq[((Long, Long, Long), (String, String))]) = (sArr: Array[String]) => {
      sArr(0) match{
        case "R" => {
          val recR = RecordR(sArr(1).trim.toInt, sArr(2).trim.toInt, sArr(3).trim.toInt, sArr(4).trim.toInt)
          val sValue = sArr(1) + "," + sArr(2) + "," + sArr(3) + "," + sArr(4)
          Seq(((hA(recR.a), hB(recR.b), hC(recR.c)), (sValue, "R")))
        }

          //in this case, A is to be joined on its 'a' attribute
          // therefore, it will be sent to reducers (hash(a), j, k) for every 1 <= j <= b (the attrbute share of b)
          // and 1 <= k <= c (the attribute share of c)
          // What the above means geometrically is that this relation is spread across the plane of the producers that
        case "A" => {
          val recA = RecordA(sArr(1).trim.toInt, sArr(2).trim)
          val sValue = sArr(2)
          for {
            j <- 1L to b;
            k <- 1L to c
          } yield ((hA(recA.a), j, k), (sValue, "A"))
        }
        case "B" => {
          val recB = RecordB(sArr(1).trim.toInt, sArr(2).trim)
          val sValue = sArr(2)
          for {
            i <- 1L to a;
            k <- 1L to c
          } yield ((i, hB(recB.b), k), (sValue, "B"))
        }
        case "C" => {
          val recC = RecordC(sArr(1).trim.toInt, sArr(2).trim)
          val sValue = sArr(2)
          for {
            i <- 1L to a;
            j <- 1L to b
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

    //saving middle state to disk
    //despite doing that, it seems that star join is still much faster than chained binary joins and sql join
    //
    val saveDir = Array(System.getProperty("user.dir"), "output", "starJoinMapped").mkString(java.io.File.separator)
    mapped.saveAsTextFile(saveDir)

    //reduce - Doesn't work, as the keys are messed up
    // Alternatively, the following could work:
    // a custom partitioner could be used to assign each record to a reducer according to the mapkeys specified by the paper
    // map data by join attributes (practically just assign keys)
    // After that, the reducer would just merge
    mapped.reduceByKey((a, b) => redFun(a, b))

    //The extra partitioning step shouldn't be much of an overhead, as it's essentially the same procedure followed here
  }
}
