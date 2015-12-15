package datagen

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

  private def randInt(min: Int, max: Int) = min + random.nextInt(max - min)

  private def genString = random.alphanumeric.take(randInt(5, 15)).mkString

  private def generateWithPrimaryKey(rel:Char, id: Int) = (rel, id, genString)

  private def genR = {
    val R = 'R'
    val a = random nextInt as.size
    val b = random nextInt bs.size
    val c = random nextInt cs.size
    (R, a + 1, b + 1, c + 1, genString)
  }

  private def tuple2str(tuple: Product): String = tuple.productIterator.toList.mkString(",")

  def serialize(filename: String) = {
    val writer = new PrintWriter(new File(filename))
    as map tuple2str foreach (writer.println)
    bs map tuple2str foreach (writer.println)
    cs map tuple2str foreach (writer.println)
    rs map tuple2str foreach (writer.println)
    writer close
  }
}
