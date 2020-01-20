package packt3

import Utilities01.HelperScala
import org.apache.spark.sql.SparkSession


object Testoo extends App {


  implicit val session: SparkSession = HelperScala.createSession(3, "Submit Parser")



  val props = session.conf
  println(props)
Thread.sleep(10000000)



}
