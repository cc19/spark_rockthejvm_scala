package part5LowLevel

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App{

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master","local")
    .getOrCreate()

  //Entry point for creating RDDs
  val sc=spark.sparkContext

  //Ways to use RDDs
  //1 - parallelize an existing collection
  val numbers = 1 to 100
  val numbersRDD = sc.parallelize(numbers)

  //2 - reading from files
  case class StockValues(symbol:String, date:String, price:Double)

  def readStocks(filename:String)=
    Source.fromFile(filename)
    .getLines()
    .drop(1)    //to drop the header
    .map(line=>line.split(","))
    .map(token=>StockValues(token(0), token(1), token(2).toDouble))
    .toList

  val stocksRDD=sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  //So it is very tough to read RDD from files compared to DF or DS

  //2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
                      .map(line=>line.split(","))
                      .filter(tokens=> tokens(0).toUpperCase()==tokens(0))
                      .map(token=>StockValues(token(0), token(1), token(2).toDouble))

  //3 - read from a DF (easiest way)
  val stocksDF = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValues]

  val stocksRDD3 = stocksDS.rdd //returns an RDD of type StockValues

  val stocksRDD4 = stocksDF.rdd //returns an RDD of type Row
  //To keep type information we need to use DS for creating RDD

  //Creating RDD to DF
  val numbersDF = numbersRDD.toDF("numbers")  //pass column name in the argument
  //when we transform RDD to DF, we lose type info

  //Creating RDD to DS
  val numberDS = spark.createDataset(numbersRDD)    //returns a dataset of Int
  //when we transform RDD to DS, we get to keep type info

  //Transformations

  //counting
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT")  //lazy transformation
  val msCount = msftRDD.count()   //eager action

  //distinct
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() //distinct is lazy transformation

  //min and max
  implicit val stockOrdering = Ordering.fromLessThan((sa:StockValues, sb:StockValues)=>sa.price<sb.price)
  //can also be written as: implicit val stockOrdering: Ordering[StockValues] = Ordering.fromLessThan((sa, sb)=>sa.price<sb.price)
  //also as: implicit val stockOrdering = Ordering.fromLessThan[StockValues]((sa, sb)=>sa.price<sb.price)
  val minMsft = msftRDD.min()   //action

  //reduce
  numbersRDD.reduce(_+_)

  //grouping
  val groupedStocks = stocksRDD.groupBy(_.symbol)      //expensive operation since involves shuffling

  //Partitioning

  val repartitionedStocksRDD = stocksRDD.repartition(30)  //partitioning the RDD into 30 partitions
  repartitionedStocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")  //creates 30 partition files
  /*Repartitioning is expensive. Involves shuffling.
  BEST PRACTICE: Partition early, then process that.
  Size of a partition should be between 10 and 100MB.
   */

  //coalesce
  val coalesedRDD = repartitionedStocksRDD.coalesce(15) //does not involve shuffling
  coalesedRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")






}
