package part5LowLevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RDD_Exercise extends App {

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  /*Exercise
  1. Read the movies.json as an RDD using the below case class
  2. Show the distinct genres as an RDD
  3. Select all the movies in the drama genre with IMDB rating >6, with RDD transformations
  4. Show the average rating of movies by genre, with RDD transformations
   */

  case class Movie(Title: String, Major_Genre: String, IMDB_Rating: Double)

  import spark.implicits._

  //1
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  val moviesRDD = moviesDF
    .select(col("Title"), col("Major_Genre"), col("IMDB_Rating"))
    .where(col("Major_Genre").isNotNull and col("IMDB_Rating").isNotNull)
    .as[Movie] //Changing to DS
    .rdd
  //moviesRDD.toDF.show()

  //2
  val distinctGenresRDD = moviesRDD.map(_.Major_Genre).distinct()
  //distinctGenresRDD.toDF.show()

  //3
  moviesRDD.filter(_.Major_Genre == "Drama").filter(_.IMDB_Rating > 6) //.toDF.show()
  //Also can be written as
  moviesRDD.filter(movie => movie.Major_Genre == "Drama" && movie.IMDB_Rating > 6)


  //4
  case class GenreAvgRating(Major_Genre: String, Avg_IMDB_Rating: Double)

  val groupedRDD = moviesRDD.groupBy(_.Major_Genre)

  val avgRatingByGenreRDD = groupedRDD.map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.IMDB_Rating).sum / movies.size)
  }

  //avgRatingByGenreRDD.toDF.show()
  moviesRDD.toDF().groupBy(col("Major_Genre")).avg("IMDB_Rating").show()
}
