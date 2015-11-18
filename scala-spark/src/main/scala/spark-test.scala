import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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
    val conf = new SparkConf().setAppName("LineCount App")
    val sc = new SparkContext(conf)
    val currentDir = System.getProperty("user.dir") // get the current directory
    val inputFile = "file://" + currentDir + "/leonardo.txt"
    val myData = sc.textFile(inputFile, 2).cache()
    val num1 = myData.filter(line => line.contains("the")).count()
    val num2 = myData.filter(line => line.contains("and")).count()
    val totalLines = myData.map(line => 1).count
    println("Total lines: %s, lines with \"the\": %s, lines with \"and\": %s".format(totalLines, num1, num2))
    sc.stop()
  }
}


object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount") // config
    val sc = new SparkContext(sparkConf) // create spark context
    val currentDir = System.getProperty("user.dir") // get the current directory
    val inputFile = "file://" + currentDir + "/leonardo.txt"
    val outputDir = "file://" + currentDir + "/output"
    val txtFile = sc.textFile(inputFile)
    txtFile.flatMap(line => line.split(" ")) // split each line based on spaces
      .map(word => (word,1)) // map each word into a word,1 pair
      .reduceByKey(_+_) // reduce
      .saveAsTextFile(outputDir) // save the output
    sc.stop()
  }
}


//object PageRank {
//  def main(args: Array[String]) {
//    val iters = 10 // number of iterations for pagerank computation
//    val currentDir = System.getProperty("user.dir") // get the current directory
//    val inputFile = "file://" + currentDir + "/webgraph.txt"
//    val outputDir = "file://" + currentDir + "/output"
//    val sparkConf = new SparkConf().setAppName("PageRank")
//    val sc = new SparkContext(sparkConf)
//    val lines = sc.textFile(inputFile, 1)
//    val links = lines.map { s => val parts = s.split("\\s+")(parts(0), parts(1))}.distinct().groupByKey().cache()
//    var ranks = links.mapValues(v => 1.0)
//    for (i <- 1 to iters) {
//      println("Iteration: " + i)
//      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) => val size = urls.size
//        urls.map(url => (url, rank / size)) }
//      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
//    }
//    val output = ranks.collect()
//    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
//    sc.stop()
//  }
//}


