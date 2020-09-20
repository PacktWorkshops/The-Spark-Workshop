package Exercise4_05

import Utilities01.HelperScala.createSession

object Exercise4_05 {

  def main(args: Array[String]): Unit = {
    val session = createSession(3, "JVM/PySpark Memory Limits")
    val numbersRDD = session.sparkContext.range(0, 10, 1, 3)
    val mappedNumbersRDD = numbersRDD.mapPartitionsWithIndex((index, partition) => {
      println("@@ Starting with array creation at partition " + index)
      val arr = Array.fill(110000000)(-1) // ~419MB
      println("@@ Succeeded with array creation at partition " + index)
      partition.map(record => arr.size + "_" + record)
    })
    println(mappedNumbersRDD.collect().toList)
  }

}
