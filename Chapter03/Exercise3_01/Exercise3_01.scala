package Exercise3_01

import scala.collection.mutable
import Utilities01.HelperScala.{createSession, novellaLocation, getNeighbours, calcAverage}

object Exercise3_01 {

  def main(args: Array[String]): Unit = {

    val session = createSession(2, "Aggregations RDD")
    val lines = session.sparkContext.textFile(novellaLocation)
    val tokens = lines.flatMap(line => line.split("\\W+"))
    // Aggregation code comes here

    /////////////////////////////////////////////////////////////////////////////////////
    // Single Element Aggregations
    val totalLength = tokens
      .map(_.length) // transforming each token into its length
      .reduce((len1, len2) => len1 + len2)
    println(totalLength)
    // Result: 163483

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
    // Map(e -> 20493, S -> 130, n -> 10866, ” -> 30, w -> 3858, — -> 640, h -> 10020, z -> 206, M -> 108, V -> 10, ) -> 18,   -> 37900, ; -> 229, q -> 126, D -> 50, , -> 2850, P -> 29, Y -> 82, t -> 14766, b -> 2350, G -> 32, k -> 1544, J -> 14, & -> 7, A -> 231, – -> 258, “ -> 198, j -> 130, ’ -> 739, s -> 10057, v -> 1568, . -> 2393, d -> 7187, [ -> 1, m -> 4226, R -> 22, I -> 1374, C -> 55, : -> 33, U -> 7, ( -> 18, y -> 3035, L -> 22, g -> 3552, p -> 2745, a -> 13009, O -> 80, F -> 39, ‘ -> 431, x -> 258, W -> 181, i -> 10145, r -> 8862, N -> 75, E -> 57, Z -> 1, ? -> 156, c -> 3520, 6 -> 1, - -> 9, Q -> 1, H -> 294, l -> 6625, u -> 4690, 0 -> 1, T -> 470, ] -> 1, f -> 3823, K -> 126, ' -> 3, o -> 12245, B -> 85, ! -> 159, é -> 1)
    /////////////////////////////////////////////////////////////////////////////////////
    // Pair RDD Aggregations
    val counts = tokens
      .map(word => (word, 1))
      .reduceByKey((c1, c2) => c1 + c2)
    //      .reduceByKey(_ + _) // short form
    println(counts.take(5).toList)
    // List((pate,1), (stonily,1), (propped,2), (bone,3), (shot,4))

    ////////////////////////////////////////////////////
    val tokenWithNeighbours = lines.flatMap(getNeighbours)

    val zeroValueByKey = (0, 0)
    val seqOpByKey = (acc: (Int, Int), neighbours: Int) => (acc._1 + 1, acc._2 + neighbours)
    val combOpByKey = (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)

    val countWithNeighbours = tokenWithNeighbours
      .aggregateByKey(zeroValueByKey)(seqOpByKey, combOpByKey)

    val averages = countWithNeighbours.map(calcAverage)
    println(averages.take(5).toList)
    // List((pate,712.0), (stonily,312.0), (propped,191.0), (bone,981.0), (shot,264.75))

    Thread.sleep(10L * 60L * 1000L)

  }

}