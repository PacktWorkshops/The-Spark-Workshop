import org.apache.spark.sql.SparkSession

object ConsumingData {

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .getOrCreate()

    val animals = Seq("dog", "cat", "bear")

    val animalData = spark.sparkContext.parallelize(animals)

    animalData.foreach(animal => {
      println("I am now a distributed " + animal.concat("!"))
    })
  }
}
