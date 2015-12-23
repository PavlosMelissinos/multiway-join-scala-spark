object EvalAll{
  def time(f: => Unit)={
    val s = System.currentTimeMillis
    f
    System.currentTimeMillis - s
  }
  def sqlJoin(sj: SparkJoin, saveDir:String): Unit = {
    val res = sj.sqlJoin //run sql join
    res.write.save(saveDir)
  }
  def sparkJoin(sj: SparkJoin, saveDir:String): Unit = {
    val res = sj.join //run spark join
    res.saveAsTextFile(saveDir)
  }
  def starJoin(sj: SparkJoin, saveDir:String, reducers: Int): Unit = {
    val res = sj.starJoin(reducers)
    res.saveAsTextFile(saveDir)
  }

  def main(args: Array[String]): Unit = {
    val reducers = if (args.size > 0) args(0).trim.toInt else 64
    val inputFile = if (args.size > 1) args(1) else Array(System.getProperty("user.dir"), "data", "dataset.txt").mkString(java.io.File.separator)
    val outputFile = if (args.size > 2) args(2) else Array(System.getProperty("user.dir"), "output").mkString(java.io.File.separator)

    val sj = new SparkJoin(inputFile)
    println("Spark join, time:" + time(sparkJoin(sj, outputFile + java.io.File.separator + "sparkJoin")))
    println("Sql join, time:" + time(sqlJoin(sj, outputFile + java.io.File.separator + "sqlJoin")))
    println("Star join, " + reducers + " reducers, time:" + time(starJoin(sj, outputFile + java.io.File.separator + "starJoin", reducers)))
  }
}