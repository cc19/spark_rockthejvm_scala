package part3typesDatasets

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import java.sql.Date

object Datasets extends App{

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/numbers.csv")

  //convert DF to dataset
  import spark.implicits._
  val numbersDS: Dataset[Int]= numbersDF.as[Int]

  numbersDS.filter(_>5000)

  //dataset of a complex type
  //1 - define your case class
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

  //2 -  read the DF from the file
  def readDF(filename:String)=spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF=readDF("cars.json")

  //3 - define an encoder
  import spark.implicits._

  //4 - convert the DF to DS
  val carsDS=carsDF.as[Car]

  //DS collection functions
  numbersDS.filter(_>5000)

  //map, flatMap, fold, reduce, for comprehensions.....
  val carNameDS = carsDS.map(
    car => car.Name.toUpperCase()
  )


  //JOINS
  case class Guitar(id:Long, make:String, model:String, guitarType:String)
  case class GuitarPlayers(id:Long, name:String, guitars:Seq[Long], band:Long)
  case class bands(id:Long, name:String, hometown:String, bandYear:Long)

  val bandsDS = readDF("bands.json").as[bands]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayers]
  val guitarsDS = readDF("guitars.json").as[Guitar]

  //join method returns a DF while join with returns Dataset
  val guitarPlayerBands:Dataset[(GuitarPlayers, bands)]=guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band")===bandsDS.col("id"))
  guitarPlayerBands.withColumnRenamed("_1","Guitar Players")
                   .withColumnRenamed("_2","Bands")


  //Grouping datasets
  //Group cars as per their origin
  carsDS.groupByKey(_.Origin).count()

  /*joins and groups are wide transformations i.e. they change the no. of partitions (involve shuffle operation)
  which is expensive since data moves between nodes
   */




}
