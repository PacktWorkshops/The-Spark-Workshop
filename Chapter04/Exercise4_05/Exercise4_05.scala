package Exercise4_05

import Utilities01.HelperScala.createSession

object Exercise4_05 {

  def main(args: Array[String]): Unit = {
    val session = createSession(4, "JVM/PySpark Memory Limits")
    val numbersRDD = session.sparkContext.range(0, 10)
    val mappedNumbersRDD = numbersRDD.mapPartitionsWithIndex((index, partition) => {
      println("@@ Starting with list creation at partition " + index)
      val tenMillionList = List.fill(10000000)(-1)
      println("@@ Succeeded with list creation at partition " + index)
      partition.map(record => tenMillionList.size + "_" + record)
    })
    println(mappedNumbersRDD.collect().toList)
  }

}
