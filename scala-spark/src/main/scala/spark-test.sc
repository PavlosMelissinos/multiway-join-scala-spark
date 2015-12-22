val test = (a: Int) => {
  for {
    i <- 1 to a;
    j <- 1 to a
  } yield (i, j)
}

//test(5)
1 to 4
val l = List(1, 2, 3, 4, 5)
l