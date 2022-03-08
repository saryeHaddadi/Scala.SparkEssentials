package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object a_RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext // entry point to work with RDDs

  /* How to create an RDD */
  // 1. Parallelize an existing collection
  val number = 1 to 1000000
  val numbersRDD = sc.parallelize(number)

  // 2.a Reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2.b Reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3. Read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers")

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD)

  /* Transformation */
  // counting & distinct
  var msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
  val msCount = msftRDD.count() // eager ACTION
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // lazy

  // min & max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  val msCount2 = msftRDD.min() // eager ACTION

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // grouping is very expensive. The data is spread among multiple nodes.
  // So this will induce shuffling.

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  // Coalesce
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */

  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movieList) => GenreAvgRating(genre, movieList.map(_.rating).sum / movieList.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}

/*
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+

  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
 */