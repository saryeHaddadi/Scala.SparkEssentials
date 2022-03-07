package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object c_ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")
  // Selecting
  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbole, auto-converted to colum
    $"Horsepower", //fancier interpolated string, returns a column object
    expr("Origin") // EXPRESSION
  ).show()

  // select with plain column names
  carsDF.select("Name", "Year").show()

  // Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  var carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression,
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectionExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // Renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // Selecting a column with spaces
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // Remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA") // operator: =!=, ===
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // Filtering with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  val americanCarsDF2 = carsDF.where("Origin = 'USA'")

  // Chain filters
  val americanPowerfulCarsDF = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF
    .filter(
      (col("Origin") === "USA").and(col("Horsepower") > 150)
    )
  val americanPowerfulCarsDF3 = carsDF
    .filter(
      (col("Origin") === "USA") and (col("Horsepower") > 150)
    )
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // Unioning = Adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  // Distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()


  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()

  // 1
  val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesReleaseDF2 = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )
  val moviesReleaseDF3 = moviesDF.selectExpr(
    "Title", "Release_Date"
  )

  // 2
  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  val moviesProfitDF3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 3
  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comediesDF3.show
}
