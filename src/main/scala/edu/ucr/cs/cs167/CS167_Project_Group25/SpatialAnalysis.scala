package edu.ucr.cs.cs167.CS167_Project_Group25

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import edu.ucr.cs.bdlab.beast._
import org.apache.spark.beast.SparkSQLRegistration
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.rdd.RDD

object SpatialAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spatial Analysis")

    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    if (args.length < 2) {
      System.err.println("Usage: SpatialAnalysis <parquet_file_path> <species>")
      System.exit(1)
    }

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val parquetFilePath: String = args(0)
    val species: String = args(1)

    try{
      val dataset = sparkSession.read.parquet(parquetFilePath)

      dataset.createOrReplaceTempView("observations")

      val totalObservationsPerZIP = sparkSession.sql("SELECT ZIPCode, SUM(OBSERVATION_COUNT) AS total_observations " +
        "FROM observations " + "GROUP BY ZIPCode")

      val speciesObservationsPerZIP = sparkSession.sql("SELECT ZIPCode, SUM(OBSERVATION_COUNT) AS species_observations " +
        "FROM observations " +
        "WHERE COMMON_NAME = '" + species + "' " +
        "GROUP BY ZIPCode")

      val joinedData = totalObservationsPerZIP.join(speciesObservationsPerZIP, "ZIPCode")
        .withColumn("ratio", col("species_observations").divide(col("total_observations")))

      joinedData.createOrReplaceTempView("joined")

      val zipRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")

      sparkSession.read.format("shapefile").load("tl_2018_us_zcta510.zip").createOrReplaceTempView("zipDataset")

      val joinedRDD = sparkSession.sql("SELECT * FROM joined, zipDataset WHERE ZIPCode = ZCTA5CE10")

      joinedRDD.coalesce(1).write.format("shapefile").save("eBirdZIPCodeRatio");

    } finally {
      sparkSession.stop()
    }
  }
}
