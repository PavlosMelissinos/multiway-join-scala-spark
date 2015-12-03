/**
 * Created by ThirstyTM on 2015-11-21.
 */
class DatasetGenerator(aSize: Int, bSize: Int, cSize: Int, rSize: Int) {
  val random = new scala.util.Random
  val as = for(i <- 1 to aSize) yield generateWithPrimaryKey('A', i)
  val bs = for(i <- 1 to bSize) yield generateWithPrimaryKey('B', i)
  val cs = for(i <- 1 to cSize) yield generateWithPrimaryKey('C', i)
  val rs = for(i <- 1 to rSize) yield genR(as.size, bs.size, cs.size) //TODO: remove duplicates??

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

  private def genR(aSize: Int, bSize: Int, cSize: Int) ={
    val R = 'R'
    val a = random.nextInt(aSize)
    val b = random.nextInt(bSize)
    val c = random.nextInt(cSize)
    val value = genString
    (R, a, b, c, value)
  }

}