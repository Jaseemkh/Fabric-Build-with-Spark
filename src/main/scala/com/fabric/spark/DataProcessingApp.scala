package com.fabric.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Main Spark application for data processing in Microsoft Fabric.
 * This application demonstrates common data engineering patterns including:
 * - Reading data from various sources
 * - Data transformation and cleansing
 * - Aggregation and analytics
 * - Writing processed data back to storage
 */
object DataProcessingApp {

  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Fabric Data Engineering with Spark")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    try {
      println("=== Starting Fabric Spark Application ===")
      
      // Check if input path is provided
      val inputPath = if (args.length > 0) args(0) else "data/sample_data.csv"
      val outputPath = if (args.length > 1) args(1) else "output/processed_data"

      println(s"Input path: $inputPath")
      println(s"Output path: $outputPath")

      // Read and process data
      val processedData = processData(spark, inputPath)
      
      // Perform analytics
      val analytics = performAnalytics(processedData)
      
      // Display results
      println("\n=== Processed Data Sample ===")
      processedData.show(10)
      
      println("\n=== Analytics Results ===")
      analytics.show()
      
      // Write results
      writeData(processedData, outputPath)
      
      println("\n=== Application completed successfully ===")
      
    } catch {
      case e: Exception =>
        println(s"Error in application: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  /**
   * Read and process data from input source
   */
  def processData(spark: SparkSession, inputPath: String): DataFrame = {
    import spark.implicits._

    // Try to read CSV file if it exists, otherwise create sample data
    val df = try {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath)
    } catch {
      case _: Exception =>
        println("Input file not found, generating sample data...")
        createSampleData(spark)
    }

    // Data cleansing and transformation
    val processed = df
      .na.drop() // Remove null values
      .withColumn("processed_date", current_date())
      .withColumn("processed_timestamp", current_timestamp())
    
    processed
  }

  /**
   * Perform analytics on processed data
   */
  def performAnalytics(df: DataFrame): DataFrame = {
    // Get column names
    val columns = df.columns.filter(_ != "processed_date" && _ != "processed_timestamp")
    
    // Simple analytics - count and basic stats
    if (columns.nonEmpty) {
      df.select(
        count("*").as("total_records"),
        countDistinct(columns.head).as(s"distinct_${columns.head}")
      )
    } else {
      df.select(count("*").as("total_records"))
    }
  }

  /**
   * Write processed data to output location
   */
  def writeData(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode("overwrite")
      .option("header", "true")
      .parquet(outputPath)
    
    println(s"Data written to: $outputPath")
  }

  /**
   * Create sample data for demonstration
   */
  def createSampleData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    val data = Seq(
      ("user1", "2024-01-01", 100, "active"),
      ("user2", "2024-01-02", 150, "active"),
      ("user3", "2024-01-03", 200, "inactive"),
      ("user4", "2024-01-04", 120, "active"),
      ("user5", "2024-01-05", 180, "active"),
      ("user6", "2024-01-06", 90, "inactive"),
      ("user7", "2024-01-07", 210, "active"),
      ("user8", "2024-01-08", 160, "active"),
      ("user9", "2024-01-09", 140, "inactive"),
      ("user10", "2024-01-10", 190, "active")
    )
    
    data.toDF("user_id", "date", "value", "status")
  }
}
