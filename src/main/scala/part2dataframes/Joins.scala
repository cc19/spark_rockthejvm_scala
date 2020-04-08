package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr}

object Joins extends App{

  val spark = SparkSession.builder()
    .appName("Dataframe Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  //join guitarPlayers with bands based on band id
  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitarPlayerBand = guitarPlayersDF.join(bandsDF, joinCondition, "inner")
  //Third parameter is type of join. "Inner" is the default value. This parameter is optional.
  //other values: left_outer, right_outer, outer

  val guitarPlayerBandLF = guitarPlayersDF.join(bandsDF, joinCondition, "left_outer")
  guitarPlayerBandLF

  guitarPlayersDF.join(bandsDF, joinCondition, "right_outer")
  guitarPlayersDF.join(bandsDF, joinCondition, "outer")

  //semi-joins
  guitarPlayersDF.join(bandsDF, joinCondition, "left_semi") //returns only left DF data from the inner join resultset

  //anti join
  guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")
  //returns missing rows from the left DF that are not satisfying the join condition
 //OR everything from the left DF for which there is no row in the right DF satisfying the join condition

  //Things to note:
  //If we try to select id column from guitarPlayerBand DF, it throws exception since there are 2 columns with the name "id"
        //guitarPlayerBand.select("id", "band")    //Exception: Reference 'id' is ambiguous

  //Solution:
  // Option 1: Rename the column on which we are joining
  guitarPlayersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  //Option 2: Drop the duplicate column
  guitarPlayerBand.drop(bandsDF.col("id"))

  //Option 3: Rename the offending column and keep the data
  val bandsModifiedDF = bandsDF.withColumnRenamed("id", "band_id")
  guitarPlayersDF.join(bandsModifiedDF, guitarPlayersDF.col("band") === bandsModifiedDF.col("band_id"))

  //using complex types
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)")
  )

}
