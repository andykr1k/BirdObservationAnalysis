package edu.ucr.cs.cs167.CS167_Project_Group25

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataPrep {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Data Prep")

    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "preprocess" =>
          val ebirdDF = sparkSession.read.format("csv")
            .option("sep", ",")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(inputFile)

          val ebirdGeomDF: DataFrame = ebirdDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry")

          val convertedDF: DataFrame = ebirdGeomDF.select("geometry", "x", "y", "GLOBAL UNIQUE IDENTIFIER", "CATEGORY", "COMMON NAME", "SCIENTIFIC NAME", "OBSERVATION COUNT", "OBSERVATION DATE")

          val renamedDF : DataFrame = convertedDF.withColumnRenamed("GLOBAL UNIQUE IDENTIFIER", "GLOBAL_UNIQUE_IDENTIFIER")
            .withColumnRenamed("COMMON NAME", "COMMON_NAME")
            .withColumnRenamed("SCIENTIFIC NAME", "SCIENTIFIC_NAME")
            .withColumnRenamed("OBSERVATION COUNT", "OBSERVATION_COUNT")
            .withColumnRenamed("OBSERVATION DATE", "OBSERVATION_DATE")

          val renamedRDD: SpatialRDD = renamedDF.toSpatialRDD

          val zipRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
          val birdZipRDD: RDD[(IFeature, IFeature)] = renamedRDD.spatialJoin(zipRDD)
          val birdZip: DataFrame = birdZipRDD.map({ case (bird, zip) => Feature.append(bird, zip.getAs[String]("ZCTA5CE10"), "ZIPCode") })
            .toDataFrame(sparkSession)

          val birdZipDropGeom: DataFrame = birdZip.drop("geometry")
          birdZipDropGeom.printSchema()
          birdZipDropGeom.show()
          birdZipDropGeom.write.mode(SaveMode.Overwrite).parquet("eBird_ZIP")

      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}