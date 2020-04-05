package Chapter01.Exercise1_03

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext

class Exercise1_03_Unit_Test extends SparkFunSuite with SharedSQLContext {

  test("filterAndFlatMap") {
    // test code comes here
    val words = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")
    val wordsRdd: RDD[String] = spark.sparkContext.parallelize(words)

    val oWordsFilter: RDD[String] = wordsRdd.filter(word => word.startsWith("o"))
    val oWordsFlatMap: RDD[String] = wordsRdd.flatMap(word =>
      if (word.startsWith("o"))
        Some(word)
      else
        None
    )
    assert(oWordsFilter.collect().forall(word => word.startsWith("o")))
    assert(oWordsFlatMap.collect().forall(word => word.startsWith("o")))
    assert(oWordsFilter.collect() sameElements oWordsFlatMap.collect())

  }
}