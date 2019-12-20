package packt1.spark

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext

class TestIntersect extends SparkFunSuite with SharedSQLContext {

  val words1 = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")
  val words2 = List[String]("centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of")

  test("intersect") {
    val wordsRdd1: RDD[String] = spark.sparkContext.parallelize(words1) // Creating distributed collections
    val wordsRdd2: RDD[String] = spark.sparkContext.parallelize(words2)
    val intersectionRDD = wordsRdd1.intersection(wordsRdd2, 1) // calling method we want to test
    assert(intersectionRDD.count() == words1.size - 4) // words2 = words1 minus the first and last 2 elements
    // Checking for elements themselves
    val intersectionLocal: Array[String] = intersectionRDD.collect() // creating a local Array
    assert(!intersectionLocal.contains(words1(0)))
    assert(!intersectionLocal.contains(words1(1)))
    assert(intersectionLocal.contains(words1(2))) // we only removed first 2 elements so 3rd one is present
  }
}
