package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part4SQL.SparkSql.spark

object Joins_Exercise extends App{

  val spark = SparkSession.builder()
    .appName("Joins exercise")
    .config("spark.master", "local")
    .getOrCreate()

  /*
  1. Show all employees and their max salary
  2. Show all employees who were never managers
  3. Find the job titles of the best paid 10 employees in the company
   */

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
    /*tableDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)*/
  }

  transferTables(List("employees","departments", "titles", "dept_emp", "salaries", "dept_manager"))

  //1
  val salariesDF = spark.read.table("salaries")
  val maxSalaryDF = salariesDF.groupBy(col("emp_no")).agg(max("salary").as("maxSalary"))

  //2
  val titlesDF = spark.read.table("titles")
  titlesDF.select("emp_no", "title").where("title <> 'Manager'")
  //Also can be done as:
  val employeesDF = spark.read.table("employees")
  val deptManagerDF = spark.read.table("dept_manager")

  employeesDF.join(deptManagerDF, employeesDF.col("emp_no") === deptManagerDF.col("emp_no"), "left_anti")

  //3
  val mostRecentJobTitleDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))//.show()
  val bestPaidEmployees = maxSalaryDF.orderBy(col("maxSalary").desc).limit(10)//.show()
  val bestPaidJobsDF = bestPaidEmployees.join(mostRecentJobTitleDF, "emp_no").orderBy(col("maxSalary").desc)

  bestPaidJobsDF.show()

}
