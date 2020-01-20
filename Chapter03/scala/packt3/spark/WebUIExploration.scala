package packt3.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object WebUIExploration {

  def main(args: Array[String]): Unit = {

    val words = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")

    val session: SparkSession = SparkSession.builder
      .master(s"local[3]") // program simulates a single executor with numThreads cores (one local JVM with numThreads threads)
      .appName("Scala map closure & Web UI")
      .getOrCreate()
    val wordsRdd: RDD[String] = session.sparkContext.parallelize(words)
    val wordLengths: RDD[Int] = wordsRdd.map(word => word.length)
    println(wordLengths.collect().toList) // printing result to stdout

    Thread.sleep(1000L * 600L)
  }
}
