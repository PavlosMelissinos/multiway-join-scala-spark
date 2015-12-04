class Test(){
  val random = new scala.util.Random

  val s = "RABC"
  s(2)
  val relKeys = Array("R", "A", "B", "C")
  relKeys(random.nextInt(relKeys.size))
  s(random.nextInt(s.size))
  random.nextInt()

  val rels = Map(
    "R" -> List("a", "b", "c", "value"),
    "A" -> List("a", "x"),
    "B" -> List("b", "y"),
    "C" -> List("c", "z"))

  //val domains = Map(
  //  "a" -> Int,
  //  "b" -> Int,
  //  "c" -> Int,
  //  "value" -> Int,
  //  "x" -> String,
  //  "y" -> String,
  //  "z" -> String
  //)
  //val domains = List(
  //  a: Int, b: Int, c: Int, value: Int,
  //  x: String, y: String, z: String)


  // Generate a random string of length n from the given alphabet
  def randomString(alphabet: String)(n: Int): String =
    Stream.continually(random.nextInt(alphabet.size)).map(alphabet).take(n).mkString

  // Generate a random alphanumeric string of length n
  def randomAlphanumericString(n: Int) =
    randomString("abcdefghijklmnopqrstuvwxyz0123456789")(n)

  class Problem (
                  a: Int, b: Int, c: Int, value: Int,
                  x: String, y: String, z: String
                )

  object genDataset{
    def genInt(max: Int): Int = {
      random.nextInt(max)
    }
    // Generate a random string of length n from the given alphabet
    def genString(n: Int): String = {
      val strlen = random.nextInt(9)
      val s = genString(strlen + 1)
      s
    }

    def genR(max: Int): List[String] = {
      val strlen = random.nextInt(9)
      val s = genString(strlen + 1)
      val n = 100
      val res: List[String] = List("R") ++ List.tabulate(4)(n => genInt(n).toString)
      res
      //'R' ::: genInt(100) :: genInt(100), genInt(100), genInt(100)
    }
    def genAs(size: Int) = {
      for (i <- 1 to size)
        println(i)
    }
  }
}

object add{
  def addInt( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b

    return sum
  }
}
