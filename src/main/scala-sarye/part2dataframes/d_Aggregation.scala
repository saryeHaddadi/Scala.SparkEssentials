package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object d_Aggregation extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // Counting
  // count non-null values
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)").show()

  // count include nulls
  moviesDF.select(count("*")).show()  // include nulls

  // count distinct
  moviesDF.select(countDistinct("Major_Genre")).show()  // include nulls

  // count distinct with very big datasets
  // do not scan the dataset row by row
  moviesDF.select(approx_count_distinct("Major_Genre")).show()  // include nulls

  // Min & Max
  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // Sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // Avg
  moviesDF.select(avg(col("US_Gross")))
  moviesDF.selectExpr("avg(US_Gross)")

  // data science stuff
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating")) // how close/far the values are from the mean
  ).show()

  /* ------------------- */
  // Aggregation
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // selection count(*) from moviesDF group by Major_Genre
  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
  avgRatingByGenreDF.show()

  val aggByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))
  aggByGenreDF.show()

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */


  // 1
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // 2
  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()











}
