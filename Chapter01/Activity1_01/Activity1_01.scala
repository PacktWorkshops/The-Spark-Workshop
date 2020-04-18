package Activity1_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import scala.collection.mutable

object Activity1_01 {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = createSession(2, "Inverted Index")
    val lines: RDD[String] = session.sparkContext.textFile("/Users/a/IdeaProjects/The-Spark-Workshop/resources/HoD_numbered.txt")

    val numberLine: RDD[(Int, String)] = lines
      .flatMap(numberLine => {
        val pair: Array[String] = numberLine.split("@")
        if (pair.length < 2) { // skipping empty lines
          None
        }
        else {
          val lineNumber = pair(0).toInt
          val line = pair(1)
          Some((lineNumber, line.trim().toLowerCase()))
        }
      })

    val tokenNumbered: RDD[(String, Int)] = numberLine.flatMap(numberLine => {
      val tokens = numberLine._2.split("\\W+")
      tokens.filter(_.nonEmpty).map(token => (token, numberLine._1))
    })

    def seqOp(acc: mutable.Set[Int], lineNumber: Int): mutable.Set[Int] = acc + lineNumber
    def combOp(acc1: mutable.Set[Int], acc2: mutable.Set[Int]): mutable.Set[Int] = acc1 ++ acc2
    val invertedIndex = tokenNumbered.aggregateByKey(mutable.Set.empty[Int])(seqOp, combOp)

    val invertedIndexTsv = invertedIndex.map { case (token, lineNumbers) => token + "\t" + lineNumbers.mkString("@") }
    invertedIndexTsv.saveAsTextFile("invertedIndex")

  }

}
