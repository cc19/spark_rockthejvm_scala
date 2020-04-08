package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataFramesBasics.spark

object DataSource_Exercise extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and formats exercise")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  Exercise: read the movies DF and then write it as:
  - tab separated values file
  - snappy Parquet
  - table 'public.movies' in the Postgres DB
   */

  //Reading the DF
  val moviesDF = spark.read
    .format("json")
    .option("inferschema", "true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  //Writing in tab separated values file
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/moviesTab.csv")

  //Writing in snappy parquet
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  //Writing in postgres DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  moviesDF.write
      .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", driver)
    .option("url", url)
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}
