package packt2.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.sampleWetLoc

object DefaultParsing {
  def main(args: Array[String]) {

    val inputLocWet: String = sampleWetLoc
    val session: SparkSession = createSession(2, "Default crawl parsing")
    session.sparkContext.setLogLevel("ERROR") // avoids printing of info messages
    val records: RDD[String] = session.sparkContext.textFile(inputLocWet)

    records.take(50).foreach(record => {
      println(record)
      println("---------------------------")
    })
    println("#############################")
    println(s"Total records: ${records.count()}")

  }
}
