package Chapter01.Exercise1_06

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext

class Exercise1_06_Unit_Test extends SparkFunSuite with SharedSQLContext {
  test("intersect") {
    val words1 = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")
    val words2 = List[String]("centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of")

    val wordsRdd1: RDD[String] = spark.sparkContext.parallelize(words1)
    val wordsRdd2: RDD[String] = spark.sparkContext.parallelize(words2)
    val intersectionRDD = wordsRdd1.intersection(wordsRdd2, 1)

    assert(intersectionRDD.count() == words1.size - 4)
    val intersectionLocal: Array[String] = intersectionRDD.collect() // creating a local Array
    assert(!intersectionLocal.contains(words1(0)))
    assert(!intersectionLocal.contains(words1(1)))
    assert(intersectionLocal.contains(words1(2))) // we only removed first 2 elements so third one is in intersection

  }
}