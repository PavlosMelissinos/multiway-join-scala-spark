package datagen

/**
  * Created by ThirstyTM on 2015-12-15.
  */

object datagen {
  val aSize = 10
  val bSize = 10
  val cSize = 10
  val rSize = 100

  def main(args: Array[String]): Unit = {
    val dg = new DatasetGenerator(aSize, bSize, cSize, rSize)
    val currentDir = System.getProperty("user.dir")
    // get the current directory
    val datasetDir = currentDir + java.io.File.separator + "data"
    val dataset = datasetDir + java.io.File.separator + "dataset.txt"
    dg.serialize(dataset)
  }
}
