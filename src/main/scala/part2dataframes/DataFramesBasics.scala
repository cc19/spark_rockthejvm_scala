package part2dataframes

import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._

object DataFramesBasics extends App {
  val spark=SparkSession.builder()        //creating a spark session
    .appName("DataFrames Basics")   //providing a name to the session
    .config("spark.master","local")       //since we are running our spark programs in our computer, so local
    .getOrCreate()                        //defining all attributes of the session and then actually creating it

  //reading a DF
  val firstDF=spark.read
    .format("json")               //since we are reading a json file
    .option("inferSchema","true")        //Infer schema will automatically guess the data types for each field.
                                        // If we set this option to TRUE, the API will read some sample records
                                        //from the file to infer the schema. If we want to set this value to false,
                                        // we must specify a schema explicitly.
    .load("src/main/resources/data/cars.json")  //providing the path of the file to load

  //showing a DF
  firstDF.show()                                          //shows data in tabular format
  firstDF.printSchema()

  firstDF.take(10).foreach(println)                      //showing top 10 rows as an Array

  //spark types
  val longType=LongType

  //schema
  val carsSchema = StructType (
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )


  //reading a DF with your own schema
  val cardDFwithSchema=spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  //creating rows by hand
  val myRow=Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  //create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF= spark.createDataFrame(cars)           //schema auto-inferred

  //DFs have schemas, rows do not

  //create DFs with implicit
  import spark.implicits._                  //This spark is the spark session object we created
  val manualCarsDFWithImplicits = cars.toDF("Name","MPG","Cylinder","Displacement","HP","Weight","Accelerator","Year","CountryOrigin")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()



}


