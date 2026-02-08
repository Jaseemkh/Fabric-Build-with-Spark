# Development Guide

## Setting Up Development Environment

### Prerequisites
- Java Development Kit (JDK) 11 or higher
- Apache Maven 3.6+
- Scala 2.12.x (for Scala development)
- Python 3.7+ (for Python development)
- Git
- IDE: IntelliJ IDEA (recommended) or VS Code with Scala/Python plugins

### Initial Setup

1. **Clone the repository**
```bash
git clone https://github.com/Jaseemkh/Fabric-Build-with-Spark.git
cd Fabric-Build-with-Spark
```

2. **Install dependencies**
```bash
# For Scala/Java
mvn clean install

# For Python
pip install pyspark pytest
```

3. **Import into IDE**
   - IntelliJ IDEA: File → Open → Select project directory
   - VS Code: File → Open Folder → Select project directory

## Project Architecture

### Scala Components

#### DataProcessingApp
- **Purpose**: Main application entry point
- **Responsibilities**:
  - Initialize Spark session
  - Orchestrate data processing pipeline
  - Handle errors and cleanup

#### DataTransformer
- **Purpose**: Data transformation utilities
- **Key Methods**:
  - `cleanColumnNames()`: Standardize column names
  - `addMetadataColumns()`: Add tracking columns
  - `filterByDateRange()`: Date-based filtering
  - `deduplicateData()`: Remove duplicates
  - `aggregateData()`: Perform aggregations

#### DataValidator
- **Purpose**: Data quality and validation
- **Key Methods**:
  - `validateRequiredColumns()`: Check column presence
  - `checkNullValues()`: Identify null values
  - `validateDataQuality()`: Quality metrics
  - `checkDuplicates()`: Duplicate detection

### Python Components

Similar structure to Scala components with Python-specific implementations.

## Coding Standards

### Scala
- Follow Scala style guide
- Use meaningful variable names
- Add ScalaDoc comments for public methods
- Use immutable data structures where possible
- Prefer functional programming patterns

```scala
/**
 * Process data from input source
 * 
 * @param spark SparkSession instance
 * @param inputPath Path to input data
 * @return Processed DataFrame
 */
def processData(spark: SparkSession, inputPath: String): DataFrame = {
  // Implementation
}
```

### Python
- Follow PEP 8 style guide
- Use type hints
- Add docstrings for all functions
- Use meaningful variable names

```python
def process_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Process data from input source
    
    Args:
        spark: SparkSession instance
        input_path: Path to input data
        
    Returns:
        Processed DataFrame
    """
    # Implementation
```

## Adding New Features

### 1. Create Feature Branch
```bash
git checkout -b feature/new-feature-name
```

### 2. Implement Feature

Example: Adding a new transformation method

**Scala**:
```scala
// In DataTransformer.scala
def filterByStatus(df: DataFrame, statusValue: String): DataFrame = {
  df.filter(col("status") === statusValue)
}
```

**Python**:
```python
# In data_transformer.py
@staticmethod
def filter_by_status(df: DataFrame, status_value: str) -> DataFrame:
    """Filter data by status value"""
    return df.filter(col("status") == status_value)
```

### 3. Add Tests

**Scala**:
```scala
// In src/test/scala/com/fabric/spark/DataTransformerTest.scala
class DataTransformerTest extends AnyFunSuite with BeforeAndAfterAll {
  test("filterByStatus should filter records correctly") {
    // Test implementation
  }
}
```

**Python**:
```python
# In src/test/python/test_data_transformer.py
def test_filter_by_status():
    # Test implementation
    pass
```

### 4. Update Documentation
- Add to README.md if user-facing
- Update DEPLOYMENT.md if affects deployment
- Add inline comments for complex logic

### 5. Test Locally
```bash
# Build
./scripts/build.sh

# Run with test data
./scripts/run.sh data/sample_data.csv output/test_output
```

### 6. Commit and Push
```bash
git add .
git commit -m "Add feature: filter by status"
git push origin feature/new-feature-name
```

## Testing

### Unit Tests

**Scala (ScalaTest)**:
```scala
import org.scalatest.funsuite.AnyFunSuite

class DataProcessingAppTest extends AnyFunSuite {
  test("createSampleData should create valid DataFrame") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    
    val df = DataProcessingApp.createSampleData(spark)
    assert(df.count() == 10)
    assert(df.columns.length == 4)
    
    spark.stop()
  }
}
```

**Python (pytest)**:
```python
import pytest
from pyspark.sql import SparkSession

def test_create_sample_data():
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    app = DataProcessingApp()
    df = app._create_sample_data()
    
    assert df.count() == 10
    assert len(df.columns) == 4
    
    spark.stop()
```

### Integration Tests

Test complete data pipeline:
```bash
# Run integration test
./scripts/run.sh data/test_input.csv output/test_output

# Verify output
ls -la output/test_output/
```

## Debugging

### Local Debugging

**IntelliJ IDEA**:
1. Set breakpoints in code
2. Right-click on main class
3. Select "Debug 'DataProcessingApp'"

**VS Code**:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "scala",
      "request": "launch",
      "name": "Debug DataProcessingApp",
      "mainClass": "com.fabric.spark.DataProcessingApp",
      "args": ["data/sample_data.csv", "output/debug_output"]
    }
  ]
}
```

### Spark UI
Access at http://localhost:4040 when running locally to:
- View job stages
- Monitor executors
- Check SQL queries
- Analyze performance

## Performance Optimization

### Best Practices
1. **Partitioning**: Choose appropriate partition count
2. **Caching**: Cache DataFrames used multiple times
3. **Broadcasting**: Broadcast small datasets for joins
4. **Predicate Pushdown**: Filter early in pipeline
5. **Column Pruning**: Select only needed columns

### Example Optimizations
```scala
// Cache frequently used DataFrame
val cachedDf = df.cache()

// Broadcast small lookup table
import org.apache.spark.sql.functions.broadcast
val result = largeDf.join(broadcast(smallDf), "key")

// Filter early
val filtered = df.filter(col("date") > "2024-01-01")
  .select("id", "value")
```

## Code Review Checklist

- [ ] Code follows style guidelines
- [ ] All tests pass
- [ ] Documentation updated
- [ ] No hardcoded values
- [ ] Error handling implemented
- [ ] Logging added for key operations
- [ ] Performance considerations addressed
- [ ] No sensitive data in code

## Release Process

1. Update version in `pom.xml`
2. Update CHANGELOG.md
3. Create release branch
4. Run full test suite
5. Build release artifacts
6. Tag release in Git
7. Deploy to target environment

## Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Scala Documentation](https://docs.scala-lang.org/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
