"""
Fabric Spark Application - Python Implementation

This module provides data processing capabilities using PySpark
for Microsoft Fabric data engineering workloads.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from typing import Optional
import sys


class DataProcessingApp:
    """Main class for Spark data processing application"""
    
    def __init__(self, app_name: str = "Fabric Data Engineering with PySpark"):
        """Initialize the Spark application"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
    
    def process_data(self, input_path: str) -> DataFrame:
        """
        Read and process data from input source
        
        Args:
            input_path: Path to input data file
            
        Returns:
            Processed DataFrame
        """
        try:
            # Try to read CSV file
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(input_path)
        except Exception as e:
            print(f"Input file not found, generating sample data... ({e})")
            df = self._create_sample_data()
        
        # Data cleansing and transformation
        processed = df.na.drop() \
            .withColumn("processed_date", current_date()) \
            .withColumn("processed_timestamp", current_timestamp())
        
        return processed
    
    def perform_analytics(self, df: DataFrame) -> DataFrame:
        """
        Perform analytics on processed data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with analytics results
        """
        columns = [c for c in df.columns 
                  if c not in ["processed_date", "processed_timestamp"]]
        
        if columns:
            return df.select(
                count("*").alias("total_records"),
                countDistinct(columns[0]).alias(f"distinct_{columns[0]}")
            )
        else:
            return df.select(count("*").alias("total_records"))
    
    def write_data(self, df: DataFrame, output_path: str):
        """
        Write processed data to output location
        
        Args:
            df: DataFrame to write
            output_path: Output path for data
        """
        df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .parquet(output_path)
        
        print(f"Data written to: {output_path}")
    
    def _create_sample_data(self) -> DataFrame:
        """Create sample data for demonstration"""
        data = [
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
        ]
        
        return self.spark.createDataFrame(
            data, 
            ["user_id", "date", "value", "status"]
        )
    
    def run(self, input_path: str = "data/sample_data.csv", 
            output_path: str = "output/processed_data"):
        """
        Main execution method
        
        Args:
            input_path: Path to input data
            output_path: Path to output data
        """
        try:
            print("=== Starting Fabric Spark Application (Python) ===")
            print(f"Input path: {input_path}")
            print(f"Output path: {output_path}")
            
            # Read and process data
            processed_data = self.process_data(input_path)
            
            # Perform analytics
            analytics = self.perform_analytics(processed_data)
            
            # Display results
            print("\n=== Processed Data Sample ===")
            processed_data.show(10)
            
            print("\n=== Analytics Results ===")
            analytics.show()
            
            # Write results
            self.write_data(processed_data, output_path)
            
            print("\n=== Application completed successfully ===")
            
        except Exception as e:
            print(f"Error in application: {str(e)}")
            raise
        finally:
            self.spark.stop()


def main():
    """Main entry point"""
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/sample_data.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output/processed_data"
    
    app = DataProcessingApp()
    app.run(input_path, output_path)


if __name__ == "__main__":
    main()
