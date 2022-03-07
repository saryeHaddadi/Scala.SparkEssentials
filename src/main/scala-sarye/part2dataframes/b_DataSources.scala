package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.a_DataFramesBasics.{carsDFSchema, spark}

object b_DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Source and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF
    - Format
    - Schema, or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsDFSchema)
    .option("mode", "failFast") // dropMalFormed, Permissive (default)
    .load("src/main/resources/data/cars.json")
  carsDF.show()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "path" -> "src/main/resources/data/cars.json",
    ))
    .load()
  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dup.json")


  /* ---------------------------- */
  val carsSchema_Date = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // Json flags
  spark.read
    .format("json")
    .schema(carsSchema_Date)
    .option("dateFormat", "yyyy-MM-DD")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .load("src/main/resources/data/cars.json")
  spark.read
    .schema(carsSchema_Date)
    .option("dateFormat", "yyyy-MM-DD")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbole", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .load("src/main/resources/data/stocks.csv")
  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")
//  carsDF.write
//    .format("parquet")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars.parquet")
//  carsDF.write
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/data/cars.parquet")

  // Text File
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Remote DB
//  val employeeDF = spark.read
//    .format("jdbc")
//    .option("driver", "org.postgresql.Driver")
//    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
//    .option("user", "docker")
//    .option("password", "docker")
//    .option("dbtable", "org.postgresql.Driver")
//    .load()
//  employeeDF.show()

  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */

  val moviesDF =   spark.read.json("src/main/resources/data/movies.json")
  moviesDF.show()

  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DF
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}
