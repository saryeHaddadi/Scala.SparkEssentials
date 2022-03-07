package part3typedatasets

import org.apache.spark.sql.functions.{avg, col, count}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

object d_Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  numbersDF.printSchema()

  // Typing a single type
//  implicit val intEncoder = Encoders.scalaInt
//  val numberDS: Dataset[Int] = numbersDF.as[Int]
//  numberDS.filter(_ < 100).show()

  // Typing a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )
  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")
  val carsDF = readDF("cars.json")
  // 3 - define an encoder (importing the implicits)
  //implicit val carEncoder = Encoders.product[Car]
  import spark.implicits._
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // map, flatMap, fold, reduce, for-comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()


  /**
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

  // 1.
  val carsCount = carsDS.count
  println(carsCount)
  // 2.
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  // 3.
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)
  carsDS.select(avg(col("Horsepower"))).show





}
