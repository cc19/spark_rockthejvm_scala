package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App{

  val spark = SparkSession.builder()
    .appName("Aggregations and grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  //counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))    //all values except nulls
  moviesDF.selectExpr("count(Major_Genre)") //gives same result

  //counting all
  moviesDF.select(count("*"))   //counts all rows including NULLs

  //count distinct
  moviesDF.select(countDistinct(col("Major_Genre")))

  //approx count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))   //for quick analysis of very big datasets

  //min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")   //shows same data

  //sum and average
  moviesDF.selectExpr("sum(US_Gross)")
  moviesDF.selectExpr("avg(US_Gross)")

  //data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  //Grouping
  val genresGroupedCountDF = moviesDF.groupBy(col("Major_Genre"))     //includes null
    .count() //equivalent to
  //select Major_Genre, count(*) from moviesDF group by Major_Genre

  //average IMDB Rating by Genre
  moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")

  //agg
  val aggregationByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    ).orderBy(col("Avg_Rating"))

}
