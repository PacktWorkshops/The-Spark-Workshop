package Exercise2_02

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.sampleWetLoc

object ProperParsing {

  def main(args: Array[String]) {
    val session: SparkSession = createSession(2, "Proper crawl parsing")
    session.sparkContext.setLogLevel("ERROR") // avoids printing of info messages

    val hadoopConf = session.sparkContext.hadoopConfiguration
    hadoopConf.set("textinputformat.record.delimiter", "WARC/1.0")

    val recordPairs: RDD[(LongWritable, Text)] = session.sparkContext
      .newAPIHadoopFile(sampleWetLoc, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
    val recordTexts: RDD[String] = recordPairs.map(_._2.toString.trim)

    recordTexts.take(5).foreach(line => {
      println(line)
      println("-" * 20)
    })
    println("#" * 40)
    println(s"Total # of records: ${recordTexts.count()}")

  }
}