package part4SQL

import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkSql_Exercise extends App {

  val spark = SparkSession.builder()
    .appName("Spark Sql Exercise")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse/rtjvm.db")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  /*
  1. Read the movies DF and store it as a spark table in the rtjvm database.
   */
  val moviesDF=spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  /*moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")*/ //This is needed but Commented out so that it does not run every time

  /*
  2. Count how many employess were hired between Jan 1 2000 and Jan 1 2001
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
  }
  transferTables(List("employees","departments", "titles", "dept_emp", "salaries", "dept_manager"))

  val q2 = "select count(*) from employees e where hire_date between '1999-01-01' and '2000-01-01'"
  spark.sql(q2).show()

  /*
  3. Show the average salaries for the employees hired in between those dates, grouped by department.
  Join employess, dept_emp and salaries.
   */
  val q3 = "select dept_no, avg(salary) from employees e inner join salaries s on e.emp_no=s.emp_no " +
    "inner join dept_emp d on e.emp_no=d.emp_no where hire_date between '1999-01-01' and '2000-01-01'" +
    "group by dept_no"
  spark.sql(q3).show()
  /*
  4. Show the name of the best paying department for employees hired in between those dates
   */
  val q4 = "select d.dept_name, avg(salary) AvgSal" +
    " from employees e " +
    " inner join salaries s on e.emp_no=s.emp_no " +
    " inner join dept_emp de on e.emp_no=de.emp_no" +
    " inner join departments d on d.dept_no = de.dept_no" +
    " where hire_date between '1999-01-01' and '2000-01-01'" +
    " group by d.dept_name order by AvgSal desc limit 1"
  spark.sql(q4).show()
}
