package edu.ucr.cs.cs167.CS167_Project_Group25

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object TemporalAnalysis{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val operation: String = args(0)
    val parquetFile: String = args(1)

    val spark = SparkSession
      .builder()
      .appName("Temporal Analysis")
      .config(conf)
      .getOrCreate()

    try {
      var valid_operation = true
      val t1 = System.nanoTime
      operation match {
        case "part3" =>
          import java.time.LocalDate
          import java.time.format.DateTimeFormatter

          val dateFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
          val startDate = LocalDate.parse(args(2), dateFormatter)
          val endDate = LocalDate.parse(args(3), dateFormatter)

          var df: DataFrame = spark.read.parquet(parquetFile)
          df.createOrReplaceTempView("part3")

          df = spark.sql(
            """SELECT COMMON_NAME, to_date(OBSERVATION_DATE, 'yyyy-MM-dd') AS parsed_observation_date, OBSERVATION_COUNT
               FROM part3;
            """
          )
          df.createOrReplaceTempView("name_date_count")

          val result = spark.sql(
            s"""SELECT COMMON_NAME, SUM(OBSERVATION_COUNT) AS total_observations
                FROM name_date_count
                WHERE parsed_observation_date >= '$startDate'
                  AND parsed_observation_date <= '$endDate'
                GROUP BY COMMON_NAME;
            """.stripMargin
          )
          df.createOrReplaceTempView("filter_date")
          result.show()

          result.coalesce(1)
            .write
            .option("header", "true")
            .csv("eBirdObservationsTime")

        case _ => valid_operation = false
      }
      val t2 = System.nanoTime
      if (valid_operation)
        println(s"Operation $operation on file '$parquetFile' finished in ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    }finally {
      spark.stop()
    }
  }
}