import nonFinal.StarJoin
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object EvalAll{
  def time(f: => Unit)={
    val s = System.currentTimeMillis
    f
    System.currentTimeMillis - s
  }
  def main(args: Array[String]): Unit = {
    val reducers = if (args.size > 0) args(0).trim.toInt else 64
    val inputFile = if (args.size > 1) args(1) else Array(System.getProperty("user.dir"), "data", "dataset.txt").mkString(java.io.File.separator)
    val outputFile = if (args.size > 2) args(2) else Array(System.getProperty("user.dir"), "output").mkString(java.io.File.separator)

    val sj = new SparkJoin(inputFile)
    println("Spark join, time:" + time(sparkJoin(sj, outputFile)))

    println("Sql join, time:" + time(sqlJoin(sj, outputFile)))

    println("Star join," + reducers + " reducers, time:" + time(starJoin(sj, outputFile, reducers)))
  }


  def sqlJoin(sc: SparkJoin, saveDir:String): Unit = {
    val res = sc.sqlJoin //run spark join
    res.saveAsTextFile(saveDir)
  }

  def sparkJoin(sc: SparkJoin, saveDir:String): Unit = {
    val res = sc.join //run spark join
    res.saveAsTextFile(saveDir)
  }

  def starJoin(sc: SparkJoin, saveDir:String, reducers: Int): Unit = {
    val res = sc.starJoin(reducers)
    res.saveAsTextFile(saveDir)
  }
}