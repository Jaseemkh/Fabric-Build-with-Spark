# Accelerate Data Engineering on Fabric - Build with Spark

A comprehensive Apache Spark application designed for data engineering workloads on Microsoft Fabric. This repository provides both Scala and Python implementations for processing, transforming, and analyzing data at scale.

## 🚀 Features

- **Multi-Language Support**: Both Scala and Python (PySpark) implementations
- **Data Processing**: Read, transform, and write data from various sources
- **Data Quality**: Built-in validation and quality checking utilities
- **Data Transformation**: Comprehensive transformation utilities for common operations
- **Analytics**: Aggregation and analytical processing capabilities
- **Configurable**: YAML-based configuration for easy customization
- **Production Ready**: Includes logging, error handling, and best practices

## 📋 Prerequisites

- Java 11 or higher
- Apache Maven 3.6+ (for Scala/Java version)
- Apache Spark 3.3.3+ (optional, included in dependencies)
- Python 3.7+ (for Python version)
- PySpark 3.3.3+ (for Python version): `pip install pyspark>=3.3.3`

## 🏗️ Project Structure

```
Fabric-Build-with-Spark/
├── data/                          # Sample data files
│   └── sample_data.csv
├── scripts/                       # Build and run scripts
│   ├── build.sh
│   ├── run.sh
│   └── run_python.sh
├── src/
│   ├── main/
│   │   ├── scala/                # Scala source code
│   │   │   └── com/fabric/spark/
│   │   │       ├── DataProcessingApp.scala
│   │   │       ├── DataTransformer.scala
│   │   │       └── DataValidator.scala
│   │   ├── python/               # Python source code
│   │   │   └── fabric_spark/
│   │   │       ├── __init__.py
│   │   │       ├── data_processing_app.py
│   │   │       ├── data_transformer.py
│   │   │       └── data_validator.py
│   │   └── resources/            # Configuration files
│   │       ├── application.yaml
│   │       └── log4j.properties
│   └── test/                     # Test files
├── pom.xml                       # Maven build configuration
├── .gitignore
└── README.md
```

## 🔧 Building the Application

### Scala/Java Version (Maven)

```bash
# Build the application
./scripts/build.sh

# Or manually with Maven
mvn clean package
```

This creates a JAR file: `target/fabric-spark-application-1.0.0-jar-with-dependencies.jar`

### Python Version

No build step required. The Python scripts can be run directly.

## ▶️ Running the Application

### Using Scala/Java Version

```bash
# Run with default paths
./scripts/run.sh

# Run with custom input/output paths
./scripts/run.sh data/your_data.csv output/your_output

# Or use spark-submit directly
spark-submit \
  --class com.fabric.spark.DataProcessingApp \
  --master local[*] \
  target/fabric-spark-application-1.0.0-jar-with-dependencies.jar \
  data/sample_data.csv \
  output/processed_data
```

### Using Python Version

```bash
# Run with default paths
./scripts/run_python.sh

# Run with custom input/output paths
./scripts/run_python.sh data/your_data.csv output/your_output

# Or use spark-submit directly
spark-submit \
  --master local[*] \
  src/main/python/fabric_spark/data_processing_app.py \
  data/sample_data.csv \
  output/processed_data
```

## 📊 Application Components

### DataProcessingApp
Main application class that orchestrates the data processing pipeline:
- Reads data from CSV files
- Applies transformations and cleansing
- Performs analytics
- Writes results to Parquet format

### DataTransformer
Utility class for data transformation operations:
- Clean column names
- Add metadata columns
- Filter by date range
- Deduplicate data
- Aggregate data

### DataValidator
Utility class for data validation:
- Validate required columns
- Check for null values
- Validate data quality metrics
- Check for duplicates

## 🔌 Input/Output Formats

### Supported Input Formats
- CSV (with header and schema inference)
- JSON
- Parquet
- Delta Lake (with appropriate dependencies)

### Supported Output Formats
- Parquet (default)
- CSV
- JSON
- Delta Lake (with appropriate dependencies)

## ⚙️ Configuration

The application can be configured via `src/main/resources/application.yaml`:

```yaml
spark:
  config:
    spark.sql.shuffle.partitions: 4
    spark.sql.adaptive.enabled: true
    spark.driver.memory: "2g"
    spark.executor.memory: "2g"
```

## 📝 Sample Data

The repository includes sample data in `data/sample_data.csv` for testing:

```csv
user_id,date,value,status
user1,2024-01-01,100,active
user2,2024-01-02,150,active
...
```

## 🚀 Deployment

### Local Deployment
```bash
# Run locally with Spark standalone
./scripts/run.sh
```

### Cluster Deployment
```bash
# Submit to YARN cluster
spark-submit \
  --class com.fabric.spark.DataProcessingApp \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  --num-executors 3 \
  target/fabric-spark-application-1.0.0-jar-with-dependencies.jar \
  hdfs://path/to/input \
  hdfs://path/to/output
```

### Microsoft Fabric Deployment
1. Upload the JAR file to Fabric Lakehouse
2. Create a Spark Job Definition in Fabric
3. Configure the job with appropriate parameters
4. Schedule or run the job on-demand

## 🧪 Testing

```bash
# Run tests with Maven
mvn test

# Run tests with coverage
mvn test jacoco:report
```

## 📈 Performance Tuning

Key configurations for optimization:
- `spark.sql.shuffle.partitions`: Adjust based on data size
- `spark.sql.adaptive.enabled`: Enable adaptive query execution
- `spark.driver.memory`: Increase for large datasets
- `spark.executor.memory`: Increase for parallel processing
- `spark.executor.cores`: Optimize CPU utilization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Apache Spark Community
- Microsoft Fabric Team
- Contributors and maintainers

## 📞 Support

For issues, questions, or contributions, please open an issue in the GitHub repository.

---

**Built with ❤️ for Microsoft Fabric Data Engineering**