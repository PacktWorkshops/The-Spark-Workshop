package Exercise2_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.sampleWetLoc

object Exercise2_01 {

  def main(args: Array[String]) {
    val session: SparkSession = createSession(2, "Default crawl parsing")
    session.sparkContext.setLogLevel("ERROR") // skips INFO messages
    val records: RDD[String] = session.sparkContext.textFile(sampleWetLoc)
    // code for subsequent steps comes here


    records.take(50).foreach(record => {
      println(record)
      println("-" * 20)
    })
    println("#" * 40)
    println(s"Total # of records: ${records.count()}")
  }
}
