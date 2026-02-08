"""
Data validation utilities for Fabric Spark applications
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from typing import List, Dict, Any, Tuple


class DataValidator:
    """Utility class for data validation operations"""
    
    @staticmethod
    def validate_required_columns(df: DataFrame, 
                                  required_columns: List[str]) -> Tuple[bool, str]:
        """
        Validate that required columns exist in DataFrame
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        missing_columns = [col for col in required_columns 
                          if col not in df.columns]
        
        if missing_columns:
            return False, f"Missing required columns: {', '.join(missing_columns)}"
        return True, ""
    
    @staticmethod
    def check_null_values(df: DataFrame, columns: List[str]) -> DataFrame:
        """Check for null values in critical columns"""
        null_counts = [
            sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls")
            for c in columns
        ]
        
        return df.agg(*null_counts)
    
    @staticmethod
    def validate_data_quality(df: DataFrame) -> Dict[str, Any]:
        """Validate data quality metrics"""
        total_records = df.count()
        
        # Check for any null values in any column
        null_condition = None
        for c in df.columns:
            if null_condition is None:
                null_condition = col(c).isNull()
            else:
                null_condition = null_condition | col(c).isNull()
        
        null_records = df.filter(null_condition).count() if null_condition else 0
        
        return {
            "total_records": total_records,
            "total_columns": len(df.columns),
            "null_records": null_records
        }
    
    @staticmethod
    def check_duplicates(df: DataFrame, key_columns: List[str]) -> int:
        """Check for duplicate records"""
        total_records = df.count()
        distinct_records = df.dropDuplicates(key_columns).count()
        return total_records - distinct_records
