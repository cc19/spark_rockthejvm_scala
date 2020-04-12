package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Dataset_Exercise extends App{

  val spark = SparkSession.builder()
    .appName("Dataset Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  def readDF(filename:String)=spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  /*
  Exercise 1:
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
  val carsDF=readDF("cars.json")

  import spark.implicits._
  val carsDS = carsDF.as[Car]

  //1
  val carsCount = carsDS.count()
  println(carsCount)

  //2
  println(carsDS.filter(_.Horsepower.getOrElse(0L)>140).count)

  //3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_+_)/carsCount)
  //can also be done using DF functions
  carsDS.select(avg(col("Horsepower")))

  /*All dataset has access to all DF functions since DF is a dataset of rows
  type DataFrame = Dataset[Row]
   */

  /*
  Exercise 2:
  1.Join the guitarDS and guitarPlayerDS, in an outer join
  (hint: use array_contains)
   */
  case class Guitar(id:Long, make:String, model:String, guitarType:String)
  case class GuitarPlayers(id:Long, name:String, guitars:Seq[Long], band:Long)
  case class bands(id:Long, name:String, hometown:String, bandYear:Long)

  val bandsDS = readDF("bands.json").as[bands]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayers]
  val guitarsDS = readDF("guitars.json").as[Guitar]

  val guitarGuitarPlayers=guitarsDS.joinWith(guitarPlayersDS,array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")


}
