package Exercise23_04

import Utilities01.HelperScala.createSession

object Exercise21_04 {
  def main(args: Array[String]): Unit = {
    // 23 a
    val millionInts: List[Int] = List.fill(1000000)(-1)
    val millionLongs: List[Long] = List.fill(1000000)(-1L)

    import org.apache.spark.util.SizeEstimator
    println(SizeEstimator.estimate(millionInts))
    println(SizeEstimator.estimate(millionLongs))

    import Utilities02.WarcRecord
    val millionWarcs: List[WarcRecord] = millionInts.map(_ => WarcRecord.createDummyObject())
    println(SizeEstimator.estimate(millionWarcs))

    /////////////////////////////////////////////////////////////////////////////////////
    val spark = createSession(2, "UI Exploration")
    // B

    import org.apache.spark.rdd.RDD
    val millionLongsRDD: RDD[Long] = spark.sparkContext.range(0, 1000000)
    millionLongsRDD.cache()
    millionLongsRDD.count()

    millionLongsRDD.unpersist()
    val millionWarcsRDD: RDD[WarcRecord] = millionLongsRDD.map(_ => WarcRecord.createDummyObject())
    millionWarcsRDD.cache()
    millionWarcsRDD.count()

    // DataFrames
    millionWarcsRDD.unpersist()
    import org.apache.spark.sql.DataFrame
    import spark.implicits._
    val millionLongsDF: DataFrame = millionLongsRDD.toDF()
    millionLongsDF.cache()
    millionLongsDF.count()

    millionLongsDF.unpersist()
    val millionWarcsDF: DataFrame = millionWarcsRDD.toDF()
    millionWarcsDF.cache()
    millionWarcsDF.count()

    // Datasets:
    millionWarcsDF.unpersist()
    import org.apache.spark.sql.Dataset
    val millionLongsDS: Dataset[Long] = millionLongsRDD.toDS()
    millionLongsDS.cache()
    millionLongsDS.count()

    millionLongsDS.unpersist()
    val millionWarcsDS: Dataset[WarcRecord] = millionWarcsRDD.toDS()
    millionWarcsDS.cache()
    millionWarcsDS.count()

    millionWarcsDS.unpersist()

  }
}