package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

object e_Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")
  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")
  val bandDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // Joins
  val joinCondition = (guitarPlayersDF.col("band") === bandDF.col("id"))
  var guitarPlayersBandsDF = guitarPlayersDF.join(bandDF, joinCondition,"inner")
  guitarPlayersBandsDF.show()

  // Left/Right outer, Full outer
  guitarPlayersDF.join(bandDF, joinCondition, "left_outer").show()
  guitarPlayersDF.join(bandDF, joinCondition, "right_outer").show()
  guitarPlayersDF.join(bandDF, joinCondition, "full_outer").show()

  // Semi, anti
  guitarPlayersDF.join(bandDF, joinCondition, "left_semi").show // inner join, with only the left-side columns
  guitarPlayersDF.join(bandDF, joinCondition, "left_anti").show // only rows that have no right references

  // Things to bear in mind
  // guitarPlayersBandsDF.select("id", "band").show() // this crashes

  // option 1. rename the column on which we are joining
  guitarPlayersDF.join(bandDF.withColumnRenamed("id", "bandId"), joinCondition,"inner")
  // option 2 - drop the dupe column
  // this works because Spark maintains an unique id for all columns it operates on
  guitarPlayersBandsDF.drop(bandDF.col("id"))
  // option 3 - rename the offending column and keep the data, drawback, you still have 2 times the same data
  val bandsModDF = bandDF.withColumnRenamed("id", "bandId")
  val joinCondition2 = (guitarPlayersDF.col("band") === bandDF.col("bandId"))
  guitarPlayersDF.join(bandsModDF, joinCondition,"inner")

  // Using complex types
  // we can use any kind of expression as a join condition
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)")
  )

  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // 2
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()


















}
