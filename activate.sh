#!/bin/bash

# Activate the virtual environment
source venv/bin/activate

# Set up Spark environment variables for PyDeequ
export SPARK_VERSION="3.5"
export JAVA_HOME="/opt/homebrew/opt/openjdk"
export SPARK_HOME="$(python -c 'import pyspark; print(pyspark.__path__[0])')"

echo "🚀 Data Quality App Environment Activated!"
echo "Python: $(python --version)"
echo "Java: $(java -version 2>&1 | head -n 1)"
echo "Spark Version: $SPARK_VERSION"
echo ""
echo "You can now run:"
echo "  streamlit run app.py"
echo "  python setup.py"
echo ""
echo "To deactivate, run: deactivate"
