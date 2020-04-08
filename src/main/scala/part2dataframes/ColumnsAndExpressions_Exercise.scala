package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions_Exercise extends App{

  val spark = SparkSession.builder()
    .appName("Columns and Expressions Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  /*
    1. Read the movies DF and select 2 columns of your choice
    2. Create another column summing up the total profit of the movies = US Gross + Worldwide Gross + US DVD Sales
    3. Select all comedy movies with IMDB rating above 6

    Use as many versions as possible
  */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //1
  moviesDF.select("Title", "Release_Date")

  //2
  val GrossProfit = col("US_Gross") + col("Worldwide_Gross")
  val grossProfitDF = moviesDF.select(col("Title"), GrossProfit.as("Gross_Profit"))
  val grossProfitDF2 = moviesDF.selectExpr("Title", "US_Gross + Worldwide_Gross as Total_Gross")

  //3
  val goodComedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy").filter(col("IMDB_Rating") > 6)
  val goodComedyMovies2 = moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating >6")
  val goodComedyMovies3 = moviesDF.select("Title", "IMDB_Rating").where("Major_Genre = 'Comedy' and IMDB_Rating >6")
}
