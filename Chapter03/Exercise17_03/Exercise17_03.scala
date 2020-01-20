package Exercise17_03

import scala.collection.mutable
import Utilities01.HelperScala.{createSession, novellaLocation, getNeighbours, calcAverage}

object Exercise17_03 {
  // main method of Exercise4_01.scala comes here
  def main(args: Array[String]): Unit = {

    val session = createSession(2, "Aggregations RDD")
    val lines = session.sparkContext.textFile(novellaLocation)
    val tokens = lines.flatMap(line => line.split("\\s+"))
    // Aggregation code comes here

    /////////////////////////////////////////////////////////////////////////////////////
    // Single Element Aggregations
    val totalLength = tokens
      .map(_.length) // transforming each token into its length
      .reduce((len1, len2) => len1 + len2)
    println(totalLength)
    // Result: 188421

    ////////////////////////////////////////////////////
    type CharMap = mutable.Map[Char, Int]
    val zeroValue = mutable.Map.empty[Char, Int]
      .withDefaultValue(0) // convenient if key not yet present in Map
    val seqOp = (acc: CharMap, ele: String) => {
      ele.foldLeft(acc)((acc, char) => {
        acc(char) += 1 // incrementing the value of key `char`
        acc // returning the accumulator
      })
    }
    val combOp = (acc1: CharMap, acc2: CharMap) => {
      for (mapEntry <- acc2) // merging acc2 into acc1 by iterating through elements
        acc1(mapEntry._1) += mapEntry._2 // of acc2 and adding them to acc1
      acc1 // returning merged accumulator
    }
    val aggregated = lines
      .aggregate(zeroValue)(seqOp, combOp)
    println(aggregated)
    // Map(e -> 22198, S -> 202, n -> 11838, ” -> 41, w -> 4114, _ -> 34, h -> 10520, z -> 208, M -> 129, V -> 19, ) -> 38, ; -> 229, q -> 136, 2 -> 15,   -> 37747, D -> 99, 5 -> 13, , -> 2995, / -> 1, P -> 137, Y -> 110, t -> 16188, b -> 2588, G -> 142, k -> 1673, J -> 23, 8 -> 11, / -> 25, & -> 1, A -> 307, “ -> 209, j -> 214, s -> 10755, . -> 2604, v -> 1673, d -> 7681, [ -> 2, R -> 96, m -> 4566, @ -> 2, 7 -> 6, I -> 1468, : -> 56, C -> 94, U -> 52, 1 -> 63, ( -> 38, y -> 3333, L -> 75, g -> 3858, p -> 3087, a -> 13948, O -> 139, X -> 2, 4 -> 9, F -> 104, % -> 1, x -> 284, W -> 197, i -> 11164, r -> 9968, N -> 132, E -> 182, Z -> 1, ? -> 155, c -> 4085, 6 -> 8, - -> 1584, Q -> 2, H -> 324, $ -> 2, l -> 7051, u -> 5184, 0 -> 21, 9 -> 15, T -> 572, f -> 4148, ] -> 2, K -> 135, ' -> 1178, o -> 13534, B -> 124, * -> 28, 3 -> 12, ! -> 160)

    /////////////////////////////////////////////////////////////////////////////////////
    // Pair RDD Aggregations
    val counts = tokens
      .map(word => (word, 1))
      .reduceByKey((c1, c2) => c1 + c2)
    //      .reduceByKey(_ + _) // short form
    println(counts.take(5).toList)

    ////////////////////////////////////////////////////
    val tokenWithNeighbours = lines.flatMap(getNeighbours)

    val zeroValueByKey = (0, 0)
    val seqOpByKey = (acc: (Int, Int), neighbours: Int) => (acc._1 + 1, acc._2 + neighbours)
    val combOpByKey = (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)

    val countWithNeighbours = tokenWithNeighbours
      .aggregateByKey(zeroValueByKey)(seqOpByKey, combOpByKey)

    val averages = countWithNeighbours.map(calcAverage)
    println(averages.take(5).toList)

    Thread.sleep(100L * 60L * 1000L)
  }

}