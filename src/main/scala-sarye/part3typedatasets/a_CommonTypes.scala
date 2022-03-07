package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object a_CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // How do we add a column to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter).show()
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  // Examples
  moviesWithGoodnessFlagsDF.where("good_movie").show()
  moviesWithGoodnessFlagsDF.where(not(col("good_movie"))).show()

  // Numbers
  // Math Operators
  val moviesAvgRatingDF = moviesDF.select(
    col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  )

  // Correlation = number between -1 and 1. The Pearson correlation
  // below, corr is an ACTION. Meaning, it will trigger a computation whatever you do in the code.
  // 0.42 correlation: pretty low correlation.
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  carsDF.select(initcap(col("Name"))).show()
  carsDF.select("*").where(col("Name").contains("volkswagen")).show()

  val regexString = "volkswagen|vw" // '|' means 'or' in the regex grammar
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regexp_extract")
  ).where(col("regexp_extract") =!= "")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regexp_extract")
  ).show()

  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volskwagen|mercedes-benz|ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")

  // version 2 - contains
  val carNameFilters = getCarNames
    .map(_.toLowerCase())
    .map(name => col("Name").contains(name))
  // returns a list[Boolean] of 3 spark-columns
  // each spark column is empty, but is able to evaluate

  val bigFilter = carNameFilters
    .fold(lit(false))(
      (currentFilter, newCarNameFilter) => currentFilter or newCarNameFilter
    )
  carsDF.filter(bigFilter).show

  print(col("Name").toString())












}
