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
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))).show()

}
