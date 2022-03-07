package part3typedatasets

import org.apache.spark.sql.functions.{array_contains, avg, col, count}
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

  /* Joins */
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)
  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayersBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
  guitarPlayersBandsDS.show

  /**
   * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
   */
  val playersAndTheirGuitarDS = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
  playersAndTheirGuitarDS.show

  // Grouping DS
  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  /*
  * Remember, Joins & Groups are claster-Wide transformtions, ie. will induce shuffling operations.
  * Thus, some data will be moved in-between node, which is expensive.
   */






}
