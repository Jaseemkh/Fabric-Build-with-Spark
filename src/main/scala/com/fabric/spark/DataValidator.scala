package com.fabric.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Utility class for data validation operations
 */
object DataValidator {

  /**
   * Validate that required columns exist in DataFrame
   */
  def validateRequiredColumns(df: DataFrame, requiredColumns: Seq[String]): Either[String, Unit] = {
    val missingColumns = requiredColumns.filterNot(df.columns.contains)
    
    if (missingColumns.nonEmpty) {
      Left(s"Missing required columns: ${missingColumns.mkString(", ")}")
    } else {
      Right(())
    }
  }

  /**
   * Check for null values in critical columns
   */
  def checkNullValues(df: DataFrame, columns: Seq[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    
    val nullCounts = columns.map { col =>
      sum(when(df(col).isNull, 1).otherwise(0)).as(s"${col}_nulls")
    }
    
    df.agg(nullCounts.head, nullCounts.tail: _*)
  }

  /**
   * Validate data quality metrics
   */
  def validateDataQuality(df: DataFrame): Map[String, Any] = {
    Map(
      "total_records" -> df.count(),
      "total_columns" -> df.columns.length,
      "null_records" -> df.filter(df.columns.map(c => df(c).isNull).reduce(_ || _)).count()
    )
  }

  /**
   * Check for duplicate records
   */
  def checkDuplicates(df: DataFrame, keyColumns: Seq[String]): Long = {
    val totalRecords = df.count()
    val distinctRecords = df.dropDuplicates(keyColumns).count()
    totalRecords - distinctRecords
  }
}
