package part3typedatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.IsNull

object c_ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Coalesce
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"))
  ).show()

  // Checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)
  // Ordering & nulls
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  // Removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop()
  // Replace nulls
  moviesDF.na.fill(-1, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show()
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 0,
    "Director" -> "Unknown",
  ))

  // Complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating*10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating*10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating*10) as nullif", // if equals, returns null, else first_arg
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating*10, 0.0) as nvl2", // if first_arg != null, return second_arg, else third_arg
  ).show()
















}
