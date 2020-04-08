package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App{

  val spark = SparkSession.builder()
    .appName("Common Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
      .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //adding a plain value to a DF
  moviesDF.select(col("title"), lit(47).as ("plain value"))

  //Booleans
  val dramaFilter = col("Major_Genre") equalTo("Drama") //instead of equalTo, === can be used
  val GoodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredfilter = dramaFilter and GoodRatingFilter //we can join multiple filters using and , or

  moviesDF.select("Title").where(dramaFilter)
  //+ multiple ways of filtering

  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredfilter.as("Good_movie"))
  //filter on a boolean column
  moviesWithGoodnessFlagDF.where("Good_movie") //means where("Good_movie") === true
  //negation
  moviesWithGoodnessFlagDF.where(not(col("Good_movie")))

  //Numbers
  //math operators
  val moviesAvgRating = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating")/10
    + col("IMDB_Rating")/2).as("AvgRating"))

  //correlation (Imp. for Data Science)= number between -1 and 1 : the closer we get to -1 or 1,the stronger our correlation gets
  //gets closer to -1 means negative correlation increases, 1 for positive
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) //corr is an ACTION
  //o/p: 0.4259708986248317. This is pretty  low.

  //Strings
  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  //capitalization : initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  //contains
  carsDF.select("*").where(col("Name").contains("volkswagen")) //but this will ignore the values which have initials of volkswagen
  //For such scenario, regex is useful

  //regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(col("Name"),
  regexp_extract(col("Name"), regexString, 0).as("regex_extract"))
  .where(col("regex_extract") =!="").drop("regex_extract") //0 because we want the first row only

  vwDF.select(col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /*
  Exercise:
  Filter the carsDF by a list of car names obtained by an API call
  getCarNames: List[String] = ???
   */
  def getCarNames: List[String] = List("volkswagen", "toyota", "bmw", "chevrolet", "ford")
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") //volkswagen|toyota|bmw|chevrolet|ford
  carsDF.select(col("Name"),
    regexp_extract(col("Name"), complexRegex , 0).as("filtered_car")
  ).where(col("filtered_car") =!= "").drop("filtered_car").show()

}
