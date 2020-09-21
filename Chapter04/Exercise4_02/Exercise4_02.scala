package Chapter04.Exercise4_02

import Utilities01.HelperScala.createSession
import org.apache.spark.sql.{SparkSession}

object Exercise4_02 extends App{
  val ints100k: List[Int] = List.fill(100000)(-1)
  val longs100k: List[Long] = List.fill(100000)(-1L)

  import org.apache.spark.util.SizeEstimator
  SizeEstimator.estimate(ints100k) // 2400032
  SizeEstimator.estimate(longs100k) // 2400040
//
  import Utilities02.WetRecord
  val wets100k: List[WetRecord] = ints100k.map(_ => WetRecord.createDummy())
  SizeEstimator.estimate(wets100k) // 86400016

  implicit val spark: SparkSession = createSession(2, "Shell Footprints")
  import org.apache.spark.rdd.RDD
  val longs100kRdd: RDD[Long] = spark.sparkContext.range(0, 100000)
  longs100kRdd.cache() // 781.3 KB
  longs100kRdd.count()


  longs100kRdd.unpersist()
  val wets100kRdd: RDD[WetRecord] = longs100kRdd.map(_ => WetRecord.createDummy())
  wets100kRdd.cache() // 80.5 MB
  wets100kRdd.count()


  wets100kRdd.unpersist()
  import org.apache.spark.sql.DataFrame
  import spark.implicits._
  val longs100kDf: DataFrame = longs100kRdd.toDF()
  longs100kDf.cache() // 100KB
  longs100kDf.count()



  longs100kDf.unpersist()
  val wets100kDf: DataFrame = wets100kRdd.toDF()
  wets100kDf.cache() // 27.1 MB
  wets100kDf.count()


  wets100kDf.unpersist()
  import org.apache.spark.sql.Dataset
  val millionLongsDS: Dataset[Long] = longs100kRdd.toDS()
  millionLongsDS.cache() // 100KB
  millionLongsDS.count()

  millionLongsDS.unpersist()
  val wets100kDs: Dataset[WetRecord] = wets100kRdd.toDS()
  wets100kDs.cache() // 27.1 MB
  wets100kDs.count()
  Thread.sleep(9999999)
}
