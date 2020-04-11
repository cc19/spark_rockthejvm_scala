package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
import part2dataframes.DataSources.stockSchema

object ComplexDataTypes extends App{

  val spark = SparkSession.builder()
    .appName("Complex data types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //Dates

  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "dd-MMM-YY").as("Actual_Release")) //conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date())  //today
    .withColumn("Right Now", current_timestamp()) //this second
    .withColumn("Movie_Age",datediff(col("Today"), col("Actual_Release"))/365)  //date difference in days
  //also has date_add, date_sub

  /*
  1. How do we deal with multiple date formats?
  2. Read the stocks DF and parse th dates
   */

  //1 - Parse the DF multiple times, then union the small DFs

  //2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF.withColumn("Actual_date",to_date(col("date"), "MMM d yyyy"))


  /*Structures:
  Tuple from a column that is composed of multiple values
  */
  //1 - with column operators
    val structDF = moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))

  //extracting the values of a column from inside struct
  structDF
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))


  // 2 - with expressions
  val structDF2 = moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")

  structDF2.selectExpr("Title", "Profit.US_Gross")


  //Arrays
  val moviesWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_words")) //Array of strings

  moviesWords.select(
    col("Title"),
    size(col("Title_words")),
    expr("Title_words[0]"),
    array_contains(col("Title_words"), "Love")
  )

}
