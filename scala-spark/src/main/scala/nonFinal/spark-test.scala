import nonFinal.StarJoin
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object HelloSpark {
  def main(args: Array[String]): Unit = {
    println("Hello, Spark!")
  }
}

object LineCount {
  def main(args: Array[String]) {
    println("Hi, this is the LineCount application for Spark.")
    // Create spark configuration and spark context
    val currentDir = System.getProperty("user.dir") // get the current directory
    System.setProperty("hadoop.home.dir", currentDir)
    val conf = new SparkConf().setMaster("local[2]").setAppName("LineCount App")
    val sc = new SparkContext(conf)
//    val inputFile = "file://" + currentDir + "/leonardo.txt"
    val inputFile = currentDir + java.io.File.separator + "leonardo.txt"
    val myData = sc.textFile(inputFile, 2).cache()
    val num1 = myData.filter(line => line.contains("the")).count()
    val num2 = myData.filter(line => line.contains("and")).count()
    val totalLines = myData.map(line => 1).count
    println("Total lines: %s, lines with \"the\": %s, lines with \"and\": %s".format(totalLines, num1, num2))
    sc.stop()
  }
}

object Kyriakos {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Kyriakos") // config
    val sc = new SparkContext(sparkConf) // create spark context
    val data = sc.parallelize(1 to 10000000).collect().filter(_ < 1000)
    data.foreach(println)
  }

}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount") // config
    val sc = new SparkContext(sparkConf) // create spark context
    val currentDir = System.getProperty("user.dir") // get the current directory
    val inputFile = currentDir + java.io.File.separator + "leonardo.txt"
    val outputDir = currentDir + java.io.File.separator + "output"
    val txtFile = sc.textFile(inputFile)
    System.setProperty("hadoop.home.dir", currentDir)
    txtFile.flatMap(line => line.split(" ")) // split each line based on spaces
      .map(word => (word,1)) // map each word into a word,1 pair
      .reduceByKey(_+_) // reduce
      .saveAsTextFile(outputDir) // save the output
    sc.stop()
  }
}

object SQLJoin {
  def main(args: Array[String]): Unit = {
    val inputFile = Array(System.getProperty("user.dir"), "data", "dataset.txt") //create path in array form
      .mkString(java.io.File.separator) //join into normal path string
    val outputFile = Array(System.getProperty("user.dir"), "output") //create path in array form
        .mkString(java.io.File.separator) //join into normal path string
    val res = new SparkJoin(inputFile).sqlJoin //run sql join
    res.saveAsTextFile(outputFile)
  }
}

object Join{
  def main(args: Array[String]): Unit = {
    val inputFile = Array(System.getProperty("user.dir"), "data", "dataset.txt") //create path in array form
      .mkString(java.io.File.separator) //join into normal path string
    val outputFile = Array(System.getProperty("user.dir"), "output") //create path in array form
        .mkString(java.io.File.separator) //join into normal path string

    val res = new SparkJoin(inputFile).join //run spark join
    res.saveAsTextFile(outputFile)
  }
}

object StarJoin{
  def main(args: Array[String]): Unit = {
    val inputFile = Array(System.getProperty("user.dir"), "data", "dataset.txt") //create path in array form
      .mkString(java.io.File.separator) //join into normal path string
    val s = new StarJoin(inputFile, 27)
    s.starJoin
  }
}
