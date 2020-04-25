package Exercise19_03


import scala.collection.mutable
import Utilities01.HelperScala.{calcAverage, createSession, getNeighbours, novellaLocation}
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet, sampleWarcLoc, sampleWetLoc}
import Utilities02.{WarcRecord}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession




 case class HeavyObject() {
  println("New heavy object created")
  def getId(): Int = System.identityHashCode(this)
 }



object Exercise19_03 extends App {

  val inputLocWarc = sampleWarcLoc
  implicit val session: SparkSession = createSession(3, "Submit Parser")


  val rawRecordsWarc: RDD[Text] = extractRawRecords(inputLocWarc)
  val warcRecords: RDD[WarcRecord] = rawRecordsWarc
    .flatMap(parseRawWarc(_))


  println(rawRecordsWarc.getNumPartitions)

//
  val ids = warcRecords.mapPartitions(partition => {
    val newHeavyObject = HeavyObject()
    partition.map(_ => newHeavyObject.getId())
  })

//  val ids = warcRecords.map(record => {
//    val newHeavyObject = HeavyObject()
//
//    newHeavyObject.getId()
//  })

//
  println(ids.distinct().collect().toList)

}
