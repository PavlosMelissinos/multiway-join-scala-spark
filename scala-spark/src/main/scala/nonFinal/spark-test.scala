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

//object SparkJoin {
//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount") // config
//    val sc = new SparkContext(sparkConf) // create spark context
//
//    val currentDir = System.getProperty("user.dir") // get the current directory
//    System.setProperty("hadoop.home.dir", currentDir)
//    val inputFile = currentDir + java.io.File.separator + "dataset.txt"
//    val outputDir = currentDir + java.io.File.separator + "output" + java.io.File.separator
//
//    sc.textFile(inputFile).flatMap(line => line.split(" ")) // split each line based on spaces
//      .map(word => (word,1)) // map each word into a word,1 pair
//      .reduceByKey(_+_) // reduce
//      .saveAsTextFile(outputDir) // save the output
//    sc.stop()
//  }
//  def sqlJoin: Unit = {
//    case class Item(id:String, name:String, unit:Int, companyId:String)
//    case class Company(companyId:String, name:String, city:String)
//    val i1 = Item("1", "first", 2, "c1")
//    val i2 = i1.copy(id="2", name="second")
//    val i3 = i1.copy(id="3", name="third", companyId="c2")
//
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("sqlJoin") // config
//    val sc = new SparkContext(sparkConf) // create spark context
//    val items = sc.parallelize(List(i1,i2,i3))
//
//    val c1 = Company("c1", "company-1", "city-1")
//    val c2 = Company("c2", "company-2", "city-2")
//    val companies = sc.parallelize(List(c1,c2))
//    val groupedItems = items.groupBy( x => x.companyId)
//    val groupedComp = companies.groupBy(x => x.companyId)
//    groupedItems.join(groupedComp).take(10).foreach(println)
//  }
//
//  def join: Unit = {
//    val mappedItems = items.map(item => (item.companyId, item))
//    val mappedComp = companies.map(comp => (comp.companyId, comp))
//    mappedItems.join(mappedComp).take(10).foreach(println)
//  }
//}

//object ASparkSQLJoin {
//
//  case class Item(id:String, name:String, unit:Int, companyId:String)
//  case class Company(companyId:String, name:String, city:String)
//
//  def main(args: Array[String]) {
//    val sparkConf = new SparkConf()
//    val sc= new SparkContext(sparkConf)
//    val sqlContext = new sql.SQLContext(sc)
//
//    import sqlContext.createDataFrame
//
//    val i1 = Item("1", "first", 1, "c1")
//    val i2 = Item("2", "second", 2, "c2")
//    val i3 = Item("3", "third", 3, "c3")
//    val c1 = Company("c1", "company-1", "city-1")
//    val c2 = Company("c2", "company-2", "city-2")
//
//    val companies = sc.parallelize(List(c1,c2))
//    companies.registerAsTable("companies")
//
//    val items = sc.parallelize(List(i1,i2,i3))
//    items.registerAsTable("items")
//
//    val result = sqlContext.sql("SELECT * FROM companies C JOIN items I ON C.companyId= I.companyId").collect
//
//    result.foreach(println)
//
//  }
//}
