def tuple2str(tuple: Product): String ={
  tuple.productIterator.toList.mkString(",")
}

val dataset = new DatasetGenerator(10, 10, 10, 100)

println(dataset.as.size)
println(dataset.as)
println(dataset.bs)
println(dataset.cs)
println(dataset.rs)
