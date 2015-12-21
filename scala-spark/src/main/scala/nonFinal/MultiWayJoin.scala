package nonFinal

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

class StarJoin(dataset: String, reducers: Int) {
  val currentDir = System.getProperty("user.dir") // get the current directory
  System.setProperty("hadoop.home.dir", currentDir)
  def sparkConf(n: Int) = new SparkConf().setMaster("local[" + n + "]").setAppName("SparkSQLJoin")

  def toRDDs ={
    val sc = new SparkContext(sparkConf(reducers))
    sc.textFile(dataset).map(_ split ",")
  }

  val records: RDD[Array[String]] = toRDDs

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
  def starJoin = {
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

    val joined = relR2.join(relA2).join(relB2).join(relC2) //combine rdds
//    joined.foreach(println) //print

  }
}
