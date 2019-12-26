package packt2.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala

object DefaultParsing {
  def main(args: Array[String]) {

    val inputLocWet: String = HelperScala.sampleWetLoc
    val session: SparkSession = HelperScala.createSession(2, "Default crawl parsing")
    session.sparkContext.setLogLevel("ERROR")  // avoids printing of info messages
    val records: RDD[String] = session.sparkContext.textFile(inputLocWet)

    records.take(50).foreach(record => {
      println(record)
      println("---------------------------")
    })
    println("#############################")
    println(s"Total records: ${records.count()}")

  }
}
