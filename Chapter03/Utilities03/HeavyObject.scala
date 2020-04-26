package Utilities03

case class HeavyObject(prefix: String) {
  println(s"$prefix => new heavy object created")
  def getId: Int = System.identityHashCode(this)
}
