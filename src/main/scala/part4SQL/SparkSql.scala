package part4SQL

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part2dataframes.DataSource_Exercise.{driver, url}

object SparkSql extends App {

  val spark=SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    //Fow allowing overwriting of DB tables transferred in spark, Spark 2.4 onwards needs the below config
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF=spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //Regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  //use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars
      |where Origin = 'USA'
      |""".stripMargin
  )
  //We can run ANY SQL statement
  spark.sql("create database rtjvm")    //a folder gets created in the project "spark-warehouse"
  // and within this folder the database is created
  //to change the path where this folder gets created, we add a config in sparksession

  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  //transfer tables from DB to spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String]) = tableNames.foreach { tableName =>
    val tableDF =   readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
    tableDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  transferTables(List("employees","departments", "titles", "dept_emp", "salaries", "dept_manager"))

  //read DF from loaded tables
  val employeesDF2 = spark.read.table("employees")
}
