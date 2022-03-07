package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  // values and variables
  var aBoolean: Boolean = false

  // expressions
  val anIfExpression = if(2>3) "bigger" else "smaller"

  // instructions vs expression
  val theUnit = println("Hello, Scala") // Unit = void

  // functions
  def myFunction(x:Int) = 42

  // OOP
  class Animal
  class Dog extends Animal
  // Defining an Interface
  trait ICarnivor {
    def eat(animal: Animal): Unit
  }
  class Crocodile extends Animal with ICarnivor {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // Singleton pattern: in one line, defines the type & the instance
  object MySingleton

  // Companions: a singleton & a class that have the same name.
  // Then, Scala allows each to see private members of the other
  object Carnivore

  // Generics
  trait MyList[A] // trait MyList[+A]

  // method notation
  var x = 1 + 2
  var y = 1.+(2)

  // Functional Programming
  val incrementer_v1: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }
  val incrementer_v2: (Int => Int) = new (Int =>Int) {
    override def apply(x: Int): Int = x + 1
  }
  val incrementer_v3: (Int => Int) =  x => x + 1

  val incremented = incrementer_v3(42)
  val processedList = List(1, 2, 3).map(incrementer_v3)

  // Pattern Matching
  val unknown: Any = 45
  var ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future
  // this global, is an implicit value used as a platform
  // for running thread. This allows us to create futures.
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // run some expensive computation, whitch runs on another thread
    // then we return a value.
    // Futures are typed with the value that when they return
    // after having run on whatever thread they have ran on.
    42
  }
  aFuture.onComplete {
    case Success(number) => println(s"Number is $number")
    case Failure(ex) => println(s"I have failed: $ex")
  }

  // Partial functions
  val aPartialFunction_v1 = (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }
  val aPartialFunction_vV: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits
  // Auto-injection by the compiler
  // You can think of it like a more powerfull version of default argument
  def methodWithImplicitArgument(implicit x: Int)  = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument
  // Same as
  // val implicitCall = methodWithImplicitArgument(67)
  /* How dit the compiler knew which variable to use? It made a search, in that order
    - local scope,
    - imported scope,
    - companion objects of the type involved in the method call
   */
  var sortedList = List(1, 2, 3).sorted
  // The sorted methode takes an implicit parameter: Ordering
  // Thus, the compiler will look for an implicit ordering, either in the companion of of List,
  // or the companion object of the type, Int here. Fortunately for Ints, there is somewhere an
  // implicit Ordering.

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet
  // How did it work?
  // When the compiler compile, it sees that string has not a method called "greet"
  // thus, it will search for all functions calls appliable to your object,
  // that will return an object that has a method "greet",
  // here: fromStringToPerson("Bob").greet

  // implicit conversions - implicit classes
  // Implicite classes are almost always preferable over implicit defs
  implicit class Dog2(name: String) {
    def bark = println("Bark!")
  }
  "Oscar".bark
}
