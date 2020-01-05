val number = Option("String")
val number2 = Option("String")

(number, number2) match {
  case (Some(num1), None) => println("ikde")
  case (None, None) => println("ikde asddsa")
  case (Some(num1), Some(num2)) => println("ikde tari")
}
