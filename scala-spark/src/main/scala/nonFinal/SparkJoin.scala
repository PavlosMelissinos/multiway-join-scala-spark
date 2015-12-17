package nonFinal

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ThirstyTM on 2015-12-16.
  */

class SparkJoin(dataset: String){
  val currentDir = System.getProperty("user.dir") // get the current directory
  System.setProperty("hadoop.home.dir", currentDir)

  def sparkConf(n: Int) = new SparkConf().setMaster("local[" + n + "]").setAppName("SparkSQLJoin")

  def join = {
    val sc = new SparkContext(sparkConf(2))
    val records = sc.textFile(dataset)
      .map(_.split(","))

    val relR = records.filter(p => p(0) equals "R").map(r => (r(1), r))
    val relA = records.filter(p => p(0) equals "A").map(a => (a(1), a))
    val relRA = relR.join(relA)

//    relRA.map(r => r._2 (r(2), r))
    val relB = records.filter(p => p(0) equals "B").map(b => (b(1), b))

    val relC = records.filter(p => p(0) equals "C").map(c => (c(1), c))
  }
}