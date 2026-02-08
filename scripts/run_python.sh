#!/bin/bash

# Fabric Spark Application - Python Run Script

echo "==================================="
echo "Running Fabric Spark Application (Python)"
echo "==================================="

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
        --master local[*] \
        --driver-memory 2g \
        --executor-memory 2g \
        src/main/python/fabric_spark/data_processing_app.py \
        "$INPUT_PATH" \
        "$OUTPUT_PATH"
elif command -v python3 &> /dev/null; then
    echo "spark-submit not found. Running with python3..."
    echo "Note: This requires PySpark to be installed (pip install pyspark)."
    python3 src/main/python/fabric_spark/data_processing_app.py "$INPUT_PATH" "$OUTPUT_PATH"
else
    echo "Error: Neither spark-submit nor python3 found."
    exit 1
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
