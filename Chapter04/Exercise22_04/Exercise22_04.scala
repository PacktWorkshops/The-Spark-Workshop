package Exercise22_04



import Utilities01.HelperScala.createSession
import Utilities02.WarcRecord


import org.apache.spark.util.SizeEstimator

object Exercise21_04 {
  def main(args: Array[String]): Unit = {
//    val millionList: List[Int] = List.fill(1000000)(-1)
//    val millionListLong: List[Long] = List.fill(1000000)(-1L)
//    println(SizeEstimator.estimate(millionList))
//    println(SizeEstimator.estimate(millionListLong))
//    val millionWarcList: List[WarcRecord] = (0 until 1000000).toList.map(_ => WarcRecord("warcType", 1L, "recordID", 1, "contentType", "infoID", "concurrentTo", "ip", "targetURI", "payloadDigest", "blockDigest", "payloadType", "htmlContentType", Some("language"), 2, "htmlSource"))
//    println(SizeEstimator.estimate(millionWarcList))

//    val session = createSession(2, "UI Exploration")
//    val numbersRDD: RDD[Long] = session.sparkContext.range(0, 1000000)


    val spark = createSession(2, "Activity 3")

    import org.apache.spark.rdd.RDD
    val millionListRDD: RDD[Long] = spark.sparkContext.range(0, 1000000)
    millionListRDD.cache()
    millionListRDD.count()
    millionListRDD.unpersist()

    import Utilities02.WarcRecord
    val millionWarcListRDD: RDD[WarcRecord] = millionListRDD.map(_ => WarcRecord.createDummyObject())
    millionWarcListRDD.cache()
    millionWarcListRDD.count()
    millionWarcListRDD.unpersist()



    import org.apache.spark.sql.DataFrame
    import spark.implicits._

    val millionListDataFrame: DataFrame = millionListRDD.toDF()
    millionListDataFrame.cache()
    millionListDataFrame.count()
    millionListDataFrame.unpersist()
//
    import org.apache.spark.sql.Dataset
    val millionListDataSet: Dataset[Long] = millionListRDD.toDS()
    millionListDataSet.cache()
    millionListDataSet.count()
    millionListDataSet.unpersist()
//
//
    val millionWarcListDataset: Dataset[WarcRecord] = millionWarcListRDD.toDS()
    millionWarcListDataset.cache()
    millionWarcListDataset.count()
    millionWarcListDataset.unpersist()



    val millionWarcListDataFrame: DataFrame = millionWarcListRDD.toDF()
    millionWarcListDataFrame.cache()
    millionWarcListDataFrame.count()
    millionWarcListDataFrame.unpersist()



//    Thread.sleep(100000000L)
  }
}