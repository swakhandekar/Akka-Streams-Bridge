package models

class Learn extends App {
  def doSomething() {
    val number = Option("String")
    val number2 = Option("String")

    (number, number2) match {
      case (Some(num1), None) => println("ikde")
    }
  }
}
