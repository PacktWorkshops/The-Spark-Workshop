package packt2.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet}
import packt2.{HelperScala, WarcRecord, WetRecord}

object Slice extends App {

//  val inputLocWarc = "/Users/a/IdeaProjects/The-Spark-Workshop/warckopf.dat"
  val inputLocWarc = "/Users/a/inputs/*.txt"

  val session: SparkSession = SparkSession.builder
    .master(s"local[2]")
    .getOrCreate()

  import org.apache.spark.sql._
  import session.implicits._

  val records: RDD[String] = session.sparkContext.textFile(inputLocWarc)
  println(records.getNumPartitions)

}
