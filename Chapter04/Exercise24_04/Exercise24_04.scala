package Exercise24_04

import Utilities01.HelperScala.createSession

object Exercise24_04 {
  def main(args: Array[String]): Unit = {
    val session = createSession(4, "Memory Limits")
    val numbersRDD = session.sparkContext.range(0, 10)
    val mappedNumbersRDD = numbersRDD.mapPartitionsWithIndex((index, partition) => {
      println("@@ Starting with list creation at partition " + index)
      val tenMillionList = List.fill(10000000)(-1)
      print("@@ Succeeded with list creation at partition " + index)
      partition.map(record => tenMillionList.size + "_" + record)
    })
    println(mappedNumbersRDD.collect().toList)
  }

}
