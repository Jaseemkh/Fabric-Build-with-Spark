# Deployment Guide

## Deploying to Microsoft Fabric

### Prerequisites
1. Access to Microsoft Fabric workspace
2. Lakehouse configured in your workspace
3. Spark compute resources enabled

### Step-by-Step Deployment

#### 1. Build the Application
```bash
./scripts/build.sh
```

#### 2. Upload JAR to Lakehouse
1. Navigate to your Fabric workspace
2. Open your Lakehouse
3. Go to "Files" section
4. Create a folder named `jars`
5. Upload `target/fabric-spark-application-1.0.0-jar-with-dependencies.jar`

#### 3. Upload Data Files
1. In the Lakehouse, go to "Files" section
2. Create a folder named `data`
3. Upload your input data files (e.g., `sample_data.csv`)

#### 4. Create Spark Job Definition
1. In your workspace, click "New" → "Spark job definition"
2. Name it "Fabric Data Processing Job"
3. Configure the job:
   - **Main class**: `com.fabric.spark.DataProcessingApp`
   - **Main jar file**: Browse and select your uploaded JAR
   - **Command line arguments**: 
     ```
     Files/data/sample_data.csv
     Files/output/processed_data
     ```

#### 5. Configure Spark Settings
Add these Spark configurations in the job definition:
```
spark.sql.shuffle.partitions=4
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

#### 6. Run the Job
1. Click "Run" to execute the job
2. Monitor progress in the job run details
3. View output logs for execution details

#### 7. Verify Output
1. Navigate to Lakehouse Files
2. Check the `output/processed_data` folder
3. Verify Parquet files are created

## Deploying to Other Platforms

### Databricks
```bash
# Upload JAR to DBFS
databricks fs cp target/fabric-spark-application-1.0.0-jar-with-dependencies.jar \
  dbfs:/FileStore/jars/

# Create and run job
databricks jobs create --json-file databricks-job-config.json
```

### AWS EMR
```bash
# Upload to S3
aws s3 cp target/fabric-spark-application-1.0.0-jar-with-dependencies.jar \
  s3://your-bucket/jars/

# Submit job to EMR
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="Fabric Data Processing",\
ActionOnFailure=CONTINUE,\
Args=[--class,com.fabric.spark.DataProcessingApp,\
s3://your-bucket/jars/fabric-spark-application-1.0.0-jar-with-dependencies.jar,\
s3://your-bucket/data/input,\
s3://your-bucket/data/output]
```

### Azure Synapse Analytics
1. Upload JAR to Azure Data Lake Storage
2. Create a Synapse Spark pool
3. Create a notebook or Spark job definition
4. Reference the JAR and execute

### Google Cloud Dataproc
```bash
# Upload to GCS
gsutil cp target/fabric-spark-application-1.0.0-jar-with-dependencies.jar \
  gs://your-bucket/jars/

# Submit job
gcloud dataproc jobs submit spark \
  --cluster=your-cluster \
  --class=com.fabric.spark.DataProcessingApp \
  --jars=gs://your-bucket/jars/fabric-spark-application-1.0.0-jar-with-dependencies.jar \
  -- gs://your-bucket/data/input gs://your-bucket/data/output
```

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Build and Deploy

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up JDK 11
      uses: actions/setup-java@v2
      with:
        java-version: '11'
    - name: Build with Maven
      run: mvn clean package
    - name: Upload to Fabric
      run: |
        # Add deployment script here
```

## Environment Variables

Set these environment variables for different environments:

```bash
# Development
export SPARK_MASTER=local[*]
export INPUT_PATH=data/sample_data.csv
export OUTPUT_PATH=output/processed_data

# Production
export SPARK_MASTER=yarn
export INPUT_PATH=abfss://container@storage.dfs.core.windows.net/data/
export OUTPUT_PATH=abfss://container@storage.dfs.core.windows.net/output/
```

## Monitoring and Logging

### View Spark UI
- Local: http://localhost:4040
- Fabric: Available in job run details
- EMR: Accessible via cluster master node
- Databricks: Built into workspace

### Configure Logging
Edit `src/main/resources/log4j.properties` to adjust log levels:
```properties
log4j.rootLogger=INFO, console
log4j.logger.com.fabric.spark=DEBUG
```

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**
   - Increase `spark.driver.memory` and `spark.executor.memory`
   - Reduce `spark.sql.shuffle.partitions`

2. **File Not Found**
   - Verify input path is correct
   - Check file permissions
   - Ensure Lakehouse is mounted correctly

3. **ClassNotFoundException**
   - Verify JAR file is uploaded correctly
   - Check main class name is correct
   - Ensure all dependencies are included

4. **Job Timeout**
   - Increase timeout settings in job configuration
   - Optimize Spark configurations
   - Check data volume and adjust resources
