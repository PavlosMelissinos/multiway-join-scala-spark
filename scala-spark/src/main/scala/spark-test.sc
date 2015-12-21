def test(a: Int) = {
  for (
    i <- 1 to a;
    j <- 1 to a
  ) yield (i, j)
}

val range = 1 to 5
range.map(elem => (1 to 5, 'a'))
range.flatMap(elem => (1 to 5, 'a'))