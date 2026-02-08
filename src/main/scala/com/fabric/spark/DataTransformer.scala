package com.fabric.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Utility class for data transformation operations
 */
object DataTransformer {

  /**
   * Clean and standardize column names
   */
  def cleanColumnNames(df: DataFrame): DataFrame = {
    val cleanedColumns = df.columns.map(col => 
      col.toLowerCase.replaceAll("[^a-zA-Z0-9_]", "_")
    )
    
    df.columns.zip(cleanedColumns).foldLeft(df) { case (currentDf, (oldName, newName)) =>
      if (oldName != newName) currentDf.withColumnRenamed(oldName, newName)
      else currentDf
    }
  }

  /**
   * Add metadata columns for tracking
   */
  def addMetadataColumns(df: DataFrame, sourceSystem: String): DataFrame = {
    df.withColumn("source_system", lit(sourceSystem))
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("ingestion_date", current_date())
  }

  /**
   * Filter data based on date range
   */
  def filterByDateRange(df: DataFrame, dateColumn: String, startDate: String, endDate: String): DataFrame = {
    df.filter(col(dateColumn).between(startDate, endDate))
  }

  /**
   * Deduplicate data based on key columns
   */
  def deduplicateData(df: DataFrame, keyColumns: Seq[String]): DataFrame = {
    df.dropDuplicates(keyColumns)
  }

  /**
   * Aggregate data by specified columns
   */
  def aggregateData(df: DataFrame, groupByColumns: Seq[String], aggColumn: String): DataFrame = {
    df.groupBy(groupByColumns.map(col): _*)
      .agg(
        count("*").as("record_count"),
        sum(aggColumn).as(s"total_${aggColumn}"),
        avg(aggColumn).as(s"avg_${aggColumn}"),
        min(aggColumn).as(s"min_${aggColumn}"),
        max(aggColumn).as(s"max_${aggColumn}")
      )
  }
}
