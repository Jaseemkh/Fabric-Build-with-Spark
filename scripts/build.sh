#!/bin/bash

# Fabric Spark Application - Build Script

echo "==================================="
echo "Building Fabric Spark Application"
echo "==================================="

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

# Clean previous builds
echo "Cleaning previous builds..."
mvn clean

# Compile and package the application
echo "Compiling and packaging application..."
mvn package -DskipTests

# Check build status
if [ $? -eq 0 ]; then
    echo ""
    echo "==================================="
    echo "Build completed successfully!"
    echo "==================================="
    echo ""
    echo "JAR files created in target/ directory:"
    ls -lh target/*.jar
    echo ""
    echo "To run the application, use:"
    echo "./scripts/run.sh"
else
    echo ""
    echo "==================================="
    echo "Build failed!"
    echo "==================================="
    exit 1
fi
