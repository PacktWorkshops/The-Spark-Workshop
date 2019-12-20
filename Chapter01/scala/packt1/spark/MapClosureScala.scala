package packt1.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt1.HelperScala

object MapClosureScala {

  def main(args: Array[String]): Unit = {
    val words = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")
    val session: SparkSession = HelperScala.createSession(2, "Scala map closure")
    val wordsRdd: RDD[String] = session.sparkContext.parallelize(words)
    val wordLengths: RDD[Int] = wordsRdd.map(word => word.length) // passing a closure to Spark's `map`
    println(wordLengths.collect().toList) // printing result to stdout
    // Result: List(11, 4, 9, 3, 3, 5, 2, 6, 4, 8, 2, 3, 9, 7, 2, 5, 10) order might differ
  }

}
