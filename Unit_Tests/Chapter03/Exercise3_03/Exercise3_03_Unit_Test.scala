package Chapter03.Exercise3_03

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import Utilities02.WarcRecord
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, sampleWarcLoc}
import Utilities03.HeavyObject
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD


class Exercise3_03_Unit_Test extends SparkFunSuite with SharedSQLContext {
  test("PerRecordVsPerPartition") {
    val rawRecordsWarc: RDD[Text] = extractRawRecords(sampleWarcLoc)
    val warcRecords: RDD[WarcRecord] = rawRecordsWarc
      .flatMap(parseRawWarc)

    val idsAfterMap: RDD[Int] = warcRecords.map(_ => {
      val newHeavyObject = HeavyObject("map")
      newHeavyObject.getId
    })

    val idsAfterMapPartition: RDD[Int] = warcRecords.mapPartitions(partition => {
      val newHeavyObject = HeavyObject("mapPartition")
      partition.map(_ => newHeavyObject.getId)
    })

    val uniqueIdsMap: List[Int] = idsAfterMap.distinct().collect().toList
    val uniqueIdsMapPartition: List[Int] = idsAfterMapPartition.distinct().collect().toList

    println("@" * 50)
    val numberOfRecords: Long = warcRecords.count()
    val numberOfPartitions: Int = warcRecords.getNumPartitions
    println(s"@@ Number of records: $numberOfRecords")
    println(s"@@ Number of partitions: $numberOfPartitions")
    assert(uniqueIdsMap.size.toDouble >= numberOfRecords.toDouble * 0.95D)
    assert(uniqueIdsMapPartition.size == numberOfPartitions)
    println(uniqueIdsMap)
    println(uniqueIdsMapPartition)
  }

}
