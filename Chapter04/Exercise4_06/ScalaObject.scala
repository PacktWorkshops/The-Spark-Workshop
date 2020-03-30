package Exercise4_06

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

object ScalaObject {
  def executeInScala(rdd: JavaRDD[Any]): RDD[String] = {
    val scalaRDD: RDD[Any] = rdd.rdd
    scalaRDD.map(record => {
      println("@@ Executing Scala code")
      record + "_executedInScala"
    })
  }
}
