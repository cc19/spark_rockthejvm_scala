package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations_Exercise extends App{

  val spark = SparkSession.builder()
    .appName("Aggregations and grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  /*Exercise:
  1. Sum up ALL the profits of ALL the movies in the DF
  2. Count how many distinct directors we have
  3. Show the mean and standard deviation of US gross revenue for the movies
  4. COMPUTE the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  //1
  moviesDF.selectExpr("sum(US_Gross) + sum(Worldwide_Gross) as Total_Profit")

  //2
  moviesDF.select(countDistinct(col("Director")))

  //3
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )

  //4
  moviesDF.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross")
    ).orderBy(col("IMDB_Rating").desc_nulls_last)
    .show()

}
