package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App{

  val spark = SparkSession.builder()
    .appName("Managing nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //select the movie titles where either of rotten tomatoes rating or imdb rating is not null
  moviesDF.select(
    col("title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating")*10 as ("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating")*10).as("Rating") //returns first non null value
  )

  //checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)  //returns records with rotten tomatoes rating as null

  //nulls when ordering
  moviesDF.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last)   //sorts in decreasing order with null values in the end

  //removing nulls
  moviesDF.select("Title","IMDB_Rating").na.drop()  //removes rows having imdb rating as null; na is a special object

  //replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))

  //replace nulls for different data formats
  moviesDF.na.fill(
    Map(
      "IMDB_Rating"-> 0,
      "Rotten_Tomatoes_Rating" ->10,
      "Director"->"Unknown"
    )
  )

  //complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating*10) as ifnull", //same as coalesce
    "nvl(Rotten_Tomatoes_Rating,IMDB_Rating*10) as nvl",  //same as above
    "nullif(Rotten_Tomatoes_Rating,IMDB_Rating*10) as nullif",//returns null if the 2 values are equal, else first value
    "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating*10,0.0) as nvl2" //if first value !=null, returns second value, else third
  )



}
