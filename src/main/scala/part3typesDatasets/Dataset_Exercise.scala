package part3typesDatasets

import org.apache.spark.sql.SparkSession

object Dataset_Exercise extends App{

  val spark = SparkSession.builder()
    .appName("Dataset Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  Exercise 1
  1. Count how many cars we have
  2. Count how many powerful cars we have (HP > 140)
  3. Average HP for the entire dataset
   */

  case class Car(
                  Name:String,
                  Miles_per_Gallon:Option[Double],
                  Cylinders:Long,
                  Displacement:Double,
                  Horsepower:Option[Long],
                  Weight_in_lbs:Long,
                  Acceleration:Double,
                  Year:String,
                  Origin:String
                )
  val carsDF=spark.read.option("inferSchema","true").json("src/mai/resources/data/cars.json")

  import spark.implicits._
  val carsDS = carsDF.as[Car]

  //carsDS.count()

  //2

  //3

}
