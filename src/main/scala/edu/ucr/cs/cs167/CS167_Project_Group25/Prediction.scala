package edu.ucr.cs.cs167.CS167_Project_Group25

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer, VectorAssembler}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.functions.col


object Prediction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BirdCategoryPrediction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.parquet(args(0))

    val filteredDf = df.filter($"CATEGORY".isin("species", "form", "issf", "slash"))

    val tokenizerCommonName = new Tokenizer()
      .setInputCol("COMMON_NAME")
      .setOutputCol("common_name_tokens")

    val tokenizerScientificName = new Tokenizer().setInputCol("SCIENTIFIC_NAME").setOutputCol("scientific_name_tokens")

    val hashingTFCommonName = new HashingTF().setInputCol("common_name_tokens").setOutputCol("common_name_features").setNumFeatures(1000)

    val hashingTFScientificName = new HashingTF().setInputCol("scientific_name_tokens").setOutputCol("scientific_name_features").setNumFeatures(1000)

    val stringIndexer = new StringIndexer().setInputCol("CATEGORY").setOutputCol("label")

    val assembler = new VectorAssembler().setInputCols(Array("common_name_features", "scientific_name_features")).setOutputCol("features")

    val lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("label")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizerCommonName, tokenizerScientificName, hashingTFCommonName, hashingTFScientificName, stringIndexer, assembler, lr))

    val Array(trainingData, testData) = filteredDf.randomSplit(Array(0.7, 0.3))

    val startTime = System.nanoTime()

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    val recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)

    val endTime = System.nanoTime()
    val totalTime = (endTime - startTime) / 1e9

    println(s"Total time: $totalTime seconds")

    val requiredColumns = predictions.select(
      col("COMMON_NAME"),
      col("SCIENTIFIC_NAME"),
      col("CATEGORY"),
      col("label"),
      col("prediction")
    )

    println(s"Accuracy: $accuracy")
    println(s"Precision: $precision")
    println(s"Recall: $recall")

    requiredColumns.show(50, truncate = false)

    spark.stop()
  }
}