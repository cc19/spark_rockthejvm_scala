package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  /*Columns: special objects that will allow us to obtain new data frames out of some source data frames by
  processing the values inside
   */
  val firstColumn = carsDF.col("Name")
  //selecting the column (called projection)
  val carNamesDF = carsDF.select(firstColumn) //select returns a new data frame

  //various select methods
  import spark.implicits._ //for fancier ways we need this
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),    //carsDF.col, col and column does the same thing

    //fancier ways
   'Year, //Scala symbol, auto converted to column
    $"Horsepower", //fancier interpolated string, returns Column object
    expr("Origin")  //EXPRESSION
  )

  //select with plain column names
  carsDF.select("Name", "Year") //returns a DF

  //EXPRESSIONS

  val simplestExpression = carsDF.col("Weight_in_lbs")  //returns column object
  //After we write a simple expression we can chain it with different operators
  val weightInKg = carsDF.col("Weight_in_lbs") /2.2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKg.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_expr") //gives same result as with the previous column
  )

  //selectExpr
  val carsWithSelectExpr = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as Weight_in_kg"
  )

  //DF Processing

  //Adding a new column to an existing DF and creating a new DF with it
  val carsWithNewColumn = carsDF.withColumn("Weight_in_kg", col("Weight_in_lbs") / 2.2)

  //Renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  //careful with column names: If the column name has space in it, then it should be used with single quotes with double quotes
  carsWithColumnRenamed.selectExpr("'Weight in pounds'")

  //Remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  //Filtering

  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA") //does the same thing

  //Filtering with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  //chain filters
  val americanPowerfulCars = carsDF.filter("Origin = 'USA'").filter("Horsepower > 150")
  val americanPowerfulCars2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCars3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //union: adding more rows

  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)    //works only if both the DFs have same schema

  //distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
}
