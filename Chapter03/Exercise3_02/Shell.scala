package Exercise3_02



import org.apache.spark.sql.SparkSession

object Shell extends App {
  val spark: SparkSession = SparkSession.builder
    .master(s"local[4]")
    .appName("Shell")
    .getOrCreate()

  import Utilities02.HelperScala.{sampleWarcLoc, extractRawRecords, parseRawWarc}
  import Exercise2_06.Exercise2_06.heavyComputation

  val rawRecords = extractRawRecords(sampleWarcLoc)(spark)
  val warcRecords = rawRecords.flatMap(parseRawWarc)
  val invokeHeavyRDD = warcRecords.map(heavyComputation)








}
