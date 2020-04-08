package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataFramesBasics.spark

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /* Reading a DF:
      - format
      - Schema or inferSchema true
      - path
      - zero or more options
      -
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) //enforcing a schema
    .option("mode", "failFast") // mode decides what spark should do if it encounters a malformed record
    /*other modes are- dropMalformed: ignores faulty rows
      permissive: default
      failFast: throws an exception for spark*/
    .option("path", "src/main/resources/data/cars.json") //another way of writing
    .load()

  //We can combine multiple options into one
  val carsDFwithOptionMap = spark.read
    .format("json")
    .options(
      Map (
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      )
    )
    .load()

  /*Writing DFs
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  (These values specify what spark should do if the file we are writing to already exists in our file system)
  - path where will save the DF
  - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  //JSON Flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd")  //couple with Schema. If Spark fails parsing, it will put null.
  //if we want hour, minute values we can use timestamp format
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //this is the default value for compression.
  // Others are: bzip2,gzip, lz4,snappy, deflate
    .load("src/main/resources/data/cars.json")
  //instead of .format("json") and .load("src/main/resources/data/cars.json")
  //we can write .json("src/main/resources/data/cars.json")

  //CSV Flags
  val stockSchema = StructType(Array(
    StructField("symbol",StringType),
    StructField("date",DateType),
    StructField("price",DoubleType)
  )
  )

  spark.read
    .format("csv")
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") //while reading the data spark will ignore the first row because it is the header row
    .option("sep", ",") //specifying the separator. It can be ; or TAB depending on the csv file to load
    .option("nullValue","")   //there is no concept of NULL in csv.
    // So using this flag will make spark consider blank as null in the parsing DF
    //There are other options for csv files, but these 3 (header, sep, nullValue are the most important
    .load("src/main/resources/data/stocks.csv")
  //instead of .format("csv") and .load("src/main/resources/data/stocks.csv")
  //we can write .csv("src/main/resources/data/stocks.csv")

  //Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  //Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")  //class name which will act as a connector to jdbc postgres
  //When we use another kind of DB, we need to use another kind of jdbc driver
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()



}

