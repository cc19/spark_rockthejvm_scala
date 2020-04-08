package part2dataframes

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}


object DF_Exercises extends App {
  val spark=SparkSession.builder()        //creating a spark session
    .appName("DataFrames Exercises")   //providing a name to the session
    .config("spark.master","local")       //since we are running our spark programs in our computer, so local
    .getOrCreate()

  /*
  Exercises:
  1.Create a manual DF describing smartphones
    - make
    - model
    - screen dimension
    - camera megapixels

    Show the data using show and printSchema
   */

  val phones = Seq(
    ("Nokia","Lumia","5 inches",12),
    ("Samsung","Note 5 Pro","8 inches",22),
    ("OnePlus","6T","5.5 inches",20),
    ("Apple","IPhone 6","7 inches",32),
    ("Redmi","MI 8","6 inches",16)
  )
  import spark.implicits._
  val phonesDF=phones.toDF("Make","Model","Screen Dimension","Camera")

  phonesDF.show()
  phonesDF.printSchema()

  /*
  2. Read another file from the data folder, e.g. movies.json
    - print its schema
    - count the number of rows, call count()
   */

  val movies = spark.read
    .format("json")
    .option("Inferred schema", "true")
    .load("src/main/resources/data/movies.json")

  //movies.show()
  movies.printSchema()
  println(s"The movies DF has ${movies.count()} rows")
}
