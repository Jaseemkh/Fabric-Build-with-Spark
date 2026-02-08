#!/bin/bash

# Fabric Spark Application - Run Script (Scala/Java)

echo "==================================="
echo "Running Fabric Spark Application"
echo "==================================="

# Check if JAR exists
JAR_FILE="target/fabric-spark-application-1.0.0-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found. Please run ./scripts/build.sh first."
    exit 1
fi

# Set default arguments
INPUT_PATH="${1:-data/sample_data.csv}"
OUTPUT_PATH="${2:-output/processed_data}"

echo "Input path: $INPUT_PATH"
echo "Output path: $OUTPUT_PATH"
echo ""

# Check if spark-submit is available
if command -v spark-submit &> /dev/null; then
    echo "Running with spark-submit..."
    spark-submit \
        --class com.fabric.spark.DataProcessingApp \
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        "$JAR_FILE" \
        "$INPUT_PATH" \
        "$OUTPUT_PATH"
else
    echo "spark-submit not found. Running with java -jar..."
    echo "Note: This requires Spark to be in the classpath."
    java -cp "$JAR_FILE" com.fabric.spark.DataProcessingApp "$INPUT_PATH" "$OUTPUT_PATH"
fi

if [ $? -eq 0 ]; then
    echo ""
    echo "==================================="
    echo "Application completed successfully!"
    echo "==================================="
else
    echo ""
    echo "==================================="
    echo "Application failed!"
    echo "==================================="
    exit 1
fi
