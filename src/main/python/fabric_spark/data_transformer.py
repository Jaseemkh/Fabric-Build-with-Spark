"""
Data transformation utilities for Fabric Spark applications
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List


class DataTransformer:
    """Utility class for data transformation operations"""
    
    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        """Clean and standardize column names"""
        import re
        
        new_columns = []
        for col_name in df.columns:
            # Convert to lowercase and replace special chars with underscore
            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name.lower())
            new_columns.append(clean_name)
        
        # Rename all columns
        for old_name, new_name in zip(df.columns, new_columns):
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
        
        return df
    
    @staticmethod
    def add_metadata_columns(df: DataFrame, source_system: str) -> DataFrame:
        """Add metadata columns for tracking"""
        return df \
            .withColumn("source_system", lit(source_system)) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_date", current_date())
    
    @staticmethod
    def filter_by_date_range(df: DataFrame, date_column: str, 
                            start_date: str, end_date: str) -> DataFrame:
        """Filter data based on date range"""
        return df.filter(col(date_column).between(start_date, end_date))
    
    @staticmethod
    def deduplicate_data(df: DataFrame, key_columns: List[str]) -> DataFrame:
        """Deduplicate data based on key columns"""
        return df.dropDuplicates(key_columns)
    
    @staticmethod
    def aggregate_data(df: DataFrame, group_by_columns: List[str], 
                      agg_column: str) -> DataFrame:
        """Aggregate data by specified columns"""
        return df.groupBy(*group_by_columns).agg(
            count("*").alias("record_count"),
            sum(agg_column).alias(f"total_{agg_column}"),
            avg(agg_column).alias(f"avg_{agg_column}"),
            min(agg_column).alias(f"min_{agg_column}"),
            max(agg_column).alias(f"max_{agg_column}")
        )
