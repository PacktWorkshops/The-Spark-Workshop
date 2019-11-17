package packt1.core

import scala.collection.mutable

object FunctionalBasics {

  def main(args: Array[String]): Unit = {
    val words = List[String]("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background")

    // function literals
    val mapped1 = words.map(word => word.length) // parameter explicitly mentioned
    val mapped2 = words.map((word: String) => word.length) // parameter with type
    val mapped3 = words.map(_.length) // single parameter can be replaced by wildcard _
    val mapped4 = words map { word => word.length } // ??
    val mapped5 = words map { (word: String) => word.length }


    // named functions/methods in Scala
    def lengthMethod(token: String): Int = token.length
    val lengthFunction: (String => Int) = token => token.length // function type is a function mapping a String to an integer

    val mapped6 = words.map(word => lengthMethod(word))
    val mapped7 = words.map(lengthMethod)
    val mapped8 = words.map(word => lengthFunction(word))
    val mapped9 = words.map(lengthFunction)


    // flatMap
//    val stringList = List[String]("This", "is", "a", "list", "of", "type", "String")
//    val integerList = stringList.flatMap( word => Array[Int](word.length, -word.length, 0))
//    println(integerList)

    val oWordsLengths = words.flatMap(word =>
      if (word.startsWith("o"))
        Some((word, word.length)) // mapping to a pair (word, length of word)
      else // skipping words not starting with an `o` letter
        None
    )
    println(oWordsLengths) // List((old,3), (on,2), (of,2))


    val wordLengths = List[Int](11, 4, 9, 3, 3, 5, 2, 6, 4, 8, 2, 3, 9, 7, 2, 5, 10)
    val totalLength = wordLengths.reduce((leftNum: Int, rightNum: Int) => leftNum + rightNum)
    val totalLength2 = wordLengths.reduce(_ + _)

    val stringList = List[String]("This", "is", "a", "list", "of", "type", "String")
    val folded = stringList.fold("<")((acc, word) => acc + "|" + word)
    // Result: <|This|is|a|list|of|type|String


    println(folded)


    //    val wordLengths = List[Int](11, 4, 9, 3, 3, 5, 2, 6, 4, 8, 2, 3, 9, 7, 2, 5, 10)
//    val totalLength: Int = wordLengths.reduce((leftNum: Int, rightNum: Int) => leftNum * rightNum) // Result: 93
//    val totalLength2: Int = wordLengths.reduce(_ * _) // shorter using wildcard
//
//    val uniqueLengths = wordLengths.foldLeft(Set.empty[Int])((acc: Set[Int], ele: Int) => acc + ele) // Result: Set(5, 10, 6, 9, 2, 7, 3, 11, 8, 4)
//    println(uniqueLengths) // Set(5, 10, 6, 9, 2, 7, 3, 11, 8, 4)

//    val sentence = "Settlements some centuries old and still no bigger than pinheads on the untouched expanse of their background"
//    val z = mutable.Map.empty[Char, Int]
//      .withDefaultValue(0) // convenient if key not yet present in Map
//    val seqop = (acc: mutable.Map[Char, Int], ele: Char) => {
//      acc(ele) += 1 // incrementing the value of key `ele`
//      acc // returning the accumulator
//    }
//    val combop = (acc1: mutable.Map[Char, Int], acc2: mutable.Map[Char, Int]) => {
//      for(mapEntry <- acc2) // merging acc2 into acc1 by iterating through elements
//        acc1(mapEntry._1) += mapEntry._2 // of acc2 and adding them to acc1
//      acc1 // returning merged accumulator
//    }
//   val aggregated = sentence.aggregate(z)(seqop, combop)
//    // Result: Map(S -> 1, e -> 13, n -> 10, h -> 5,   -> 16, k -> 1, b -> 2, t -> 9, s -> 6, d -> 5, m -> 2, p -> 2, g -> 3, a -> 5, x -> 1, r -> 4, i -> 5, c -> 3, l -> 4, u -> 4, f -> 1, o -> 7)
//
//    println(aggregated)


//    val sentence = "Settlements some centuries old and still no bigger than pinheads on the untouched expanse of their background"
//    val z = mutable.Map.empty[Char, Int]
//      .withDefaultValue(0) // convenient if key not yet present in Map
//    val seqop = (acc: mutable.Map[Char, Int], ele: Char) => {
//      acc(ele) += 1 // incrementing the value of key `ele`
//      acc // returning the accumulator
//    }
//    val combop = (acc1: mutable.Map[Char, Int], acc2: mutable.Map[Char, Int]) => {
//      for (mapEntry <- acc2) { // merging acc2 into acc1 by iterating through elements
//        acc1(mapEntry._1) += mapEntry._2 // of acc2 and adding them to acc1
//      }
//      acc1 // returning merged accumulator
//    }
//    val aggregated = sentence.aggregate(z)(seqop, combop)
//    // Result: Map(S -> 1, e -> 13, n -> 10, h -> 5,   -> 16, k -> 1, b -> 2, t -> 9, s -> 6, d -> 5, m -> 2, p -> 2, g -> 3, a -> 5, x -> 1, r -> 4, i -> 5, c -> 3, l -> 4, u -> 4, f -> 1, o -> 7)
//
//    println(aggregated)

    //    val res = words.fold(Set.empty[Int])((acc, ele) =>  acc + ele.length)
    //    println(res)
    //    val oWordsLengths = words.flatMap(word =>
    //      if (word.startsWith("o"))
    //        Some((word, word.length)) // mapping to a pair (word, length of word)
    //      else // skipping words not starting with an `o` letter
    //        None
    //    )
    //    println(oWordsLengths)


    //    // filter
    //    val oWords = words.filter(word => word.startsWith("o"))
    //
    //    // reduce
    //    val concatenated = words.reduce((word1, word2) => word1 + "|" + word2)
    //    val concatenated2 = words.reduce(_ + "|" + _) // equivalent to above, using wildcard `_` instead of explicit names
    //    // Result: Settlements|some|centuries|old|and|still|no|bigger|than|pinheads|on|the|untouched|expanse|of|their|background
    //    println(concatenated)
    //    println(concatenated2)
    ////    val totalWordLengths = words.reduce((word1, word2) => word1.length + word2.length)
    ////    println(totalWordLengths)
    //    import java.util
    //    val tokens = new util.ArrayList[String](util.Arrays.asList("Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background"))
    //
    //

    //        // declare len as a function that takes a String and returns an Int, and define it as returning the length of its argument
    //        val len: String => Int = { _.length }
    //
    //        // or, a more verbose version that uses '=>' in both ways
    //        val len: String => Int = { (s: String) => s.length }
    //
    //        val tokenLengths = tokens.map(lengthFunction)
    //
    //        ////////////////////////////////////////////
    //        // flatMap
    //        ////////////////////////////////////////////
    //        def lowerLengthMethod(token: String): Option[Int] = {
    //          if (token.charAt(0).isUpper) // skip elements starting with uppercase letter
    //            None
    //          else // else return the length of the lowercase token
    //            Some(token.length)
    //        }
    //
    //        val lowerTokenLengths = tokens.flatMap(lowerLengthMethod)
    //
    //        ////////////////////////////////////////////
    //        // reduce
    //        ////////////////////////////////////////////
    //        def minLengthMethod(tokenLengths: (Int, Int)): Int = {
    //          if (tokenLengths._1 <= tokenLengths._2)
    //            tokenLengths._1
    //          else
    //            tokenLengths._2
    //        }
    //
    //        val minLength: Int = tokens
    //          .map(lengthFunction)
    //          .reduce((length1: Int, length2: Int) => minLengthMethod(length1, length2)) // or shorter:  .reduce(minLengthMethod(_,_))
    //
    //        ////////////////////////////////////////////
    //        // foldLeft
    //        ////////////////////////////////////////////
    //
    //        def addInitialToSet(set: mutable.Set[String], nextToken: String): mutable.Set[String] = {
    //          set.add(nextToken.substring(0, 1))
    //          set
    //        }
    //
    //        val distinctInitials = tokens
    //          .foldLeft(mutable.Set.empty[String])((accumulator: mutable.Set[String], nextToken: String) => addInitialToSet(accumulator, nextToken))
    //        // or shorter:  .foldLeft(mutable.Set.empty[String])(addInitialToSet)

  }
}
