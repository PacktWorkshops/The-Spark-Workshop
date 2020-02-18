package Exercise24_04

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

object Exercise24_04 {

  def executeInScala(rdd: JavaRDD[Any]): RDD[String] = {
    rdd.rdd.map(_ => {
      println("@@ Executing  Scala code")
      "executeInScala"
    })
  }
}
