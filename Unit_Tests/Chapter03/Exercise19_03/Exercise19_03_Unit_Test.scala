package Chapter03.Exercise19_03

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, sampleWarcLoc}
import Utilities02.WarcRecord
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

case class HeavyObject(prefix: String) {
  println(s"$prefix new heavy object created")

  def getId(): Int = System.identityHashCode(this)
}


class Exercise19_03_Unit_Test extends SparkFunSuite with SharedSQLContext {
  val inputLocWarc = sampleWarcLoc

  test("PerRecordVsPerPartition") {
    val rawRecordsWarc: RDD[Text] = extractRawRecords(inputLocWarc)
    val warcRecords: RDD[WarcRecord] = rawRecordsWarc
      .flatMap(parseRawWarc(_))

    val inputPartitions = warcRecords.getNumPartitions
    val numberOfRecords = warcRecords.count()

    val idsOfMap = warcRecords.map(_ => {
      val newHeavyObject = HeavyObject("Map")
      newHeavyObject.getId()
    })
      .distinct()
      .collect()

    println("@" * 50)

    val idsOfMapPartition = warcRecords.mapPartitions(partition => {
      val newHeavyObject = HeavyObject("MapPartition")
      partition.map(_ => newHeavyObject.getId())
    })
      .distinct()
      .collect()

    println("@" * 50)

    println(s"@@ Number of partitions: $inputPartitions")
    println(s"@@ Number of records: $numberOfRecords")
    assert(idsOfMap.length.toDouble >= numberOfRecords.toDouble * 0.95D)
    assert(idsOfMapPartition.length == inputPartitions)

  }

}
