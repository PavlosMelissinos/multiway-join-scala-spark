abstract class Record
case class RecordR(a: Int, b: Int, c: Int, value: Int) extends Record
case class RecordA(a: Int, x: String) extends Record
case class RecordB(b: Int, y: String) extends Record
case class RecordC(c: Int, z: String) extends Record

val a = 2
val b = 3
val c = 4

def mapFun (r:Either[RecordA, RecordB, RecordC]): ((Int, Int, Int), (String, Char)) = r match {
  case RecordR => {
    val rr = r.asInstanceOf[RecordR]
    ((rr.a % a, rr.b % b, rr.c % c), (rr.value, 'R'))
  }
  case RecordA => {
    val ra = r.asInstanceOf[RecordA]
    for {j <- 1 to b; k <- 1 to c} yield ((ra.a % a, j, k), (ra.x, 'A'))
  }
  case RecordB => {
    for {i <- 1 to a; k <- 1 to c} yield ((i, r.b % b, k), (r.y, 'B'))
  }
  case RecordC => {
    for {i <- 1 to a; j <- 1 to b} yield ((i, j, r.c % c), (r.z, 'C'))
  }
}

val t = mapFun(RecordA(4, "asts"))

val res = for {
  j <- 1 to b;
  k <- 1 to c
} yield ((4, j, k), (3, 'A'))