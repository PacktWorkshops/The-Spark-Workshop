package Exercise25_04

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

object Exercise25_04 {
  def executeInScala(rdd: JavaRDD[Any]): RDD[String] = {
    val scalaRDD: RDD[Any] = rdd.rdd
    scalaRDD.map(record => {
      println("@@ Executing Scala code")
      record + "_executedInScala"
    })
  }
}
