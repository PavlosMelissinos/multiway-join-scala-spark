import java.io.{File, PrintWriter}

/**
 * Created by ThirstyTM on 2015-11-21.
 */
class DatasetGenerator(aSize: Int, bSize: Int, cSize: Int, rSize: Int) {
  val random = new scala.util.Random
  val as = for(i <- 1 to aSize) yield generateWithPrimaryKey('A', i)
  val bs = for(i <- 1 to bSize) yield generateWithPrimaryKey('B', i)
  val cs = for(i <- 1 to cSize) yield generateWithPrimaryKey('C', i)
  val rs = for(i <- 1 to rSize) yield genR //TODO: remove duplicates??

  private def genR(id: Int) = {
    val rRow = List()
  }

  private def genString = {
    val str_len = 5 + random.nextInt(10)
    val rands = random.alphanumeric.take(str_len).mkString
    rands
  }

  private def generateWithPrimaryKey(rel:Char, id: Int) = {
    (rel, id, genString)
  }

//  private def genR(aSize: Int, bSize: Int, cSize: Int) ={
  private def genR = {
    val R = 'R'
    val a = random nextInt as.size
    val b = random nextInt bs.size
    val c = random nextInt cs.size
    val value = genString
    (R, a + 1, b + 1, c + 1, value)
  }

  private def tuple2str(tuple: Product): String ={
    tuple.productIterator.toList.mkString(",")
  }

  def serialize(filename: String) = {
//    val filename = "D:\\Programming\\repos\\pms-ddm\\scala-spark\\dataset.txt"
    val writer = new PrintWriter(new File(filename))
    as map tuple2str foreach (writer.println)
    as map tuple2str foreach (println)
    bs map tuple2str foreach (writer.println)
    cs map tuple2str foreach (writer.println)
    rs map tuple2str foreach (writer.println)
    writer close
  }
}
