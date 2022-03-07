package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object b_ComplexeTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complexe Spark Types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDateDF = moviesDF.select(
    col("Title"),
    to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
  moviesWithReleaseDateDF.select("*")
    .where(col("Actual_Release").isNull)
    .show()

  /**
   * Exercise
   * 1. How do we deal with multiple date formats?
   * 2. Read the stocks DF and parse the dates
   */

  // 1 - parse the DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // Structures

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")) // create an array like sturcture
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit")) // extract data from the array

  // 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
    .show()

  // Arrays
  val moviesWithWords = moviesDF.select(
    col("Title"),
    split(col("Title"), " |,").as("Title_Words") // ARRAY of strings
  )
  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // indexing
    size(col("Title_Words")), // array size
    array_contains(col("Title_Words"), "Love") // look for value in array
  ).show()
























}
