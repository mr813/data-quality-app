# Data Quality & Observability Platform

A comprehensive data quality monitoring and anomaly detection platform built with Streamlit and PyDeequ.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+ (tested with Python 3.13)
- Java 8 or 11 (for Spark compatibility)

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd data_quality_app
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the application:**
   ```bash
   python run_app.py
   ```

The application will open in your browser at `http://localhost:8501`.

## 🔧 Spark Configuration Issues

### Common Spark Error on macOS

If you encounter this error:
```
java.lang.UnsupportedOperationException: getSubject is not supported
```

**This is normal and expected!** The application automatically handles this by falling back to pandas-only mode.

### Why This Happens

- **Java Version**: Newer Java versions (17+) have security restrictions that prevent Spark from accessing certain system properties
- **macOS Security**: macOS has additional security measures that can interfere with Spark's user authentication

### Solutions

#### Option 1: Use Pandas-Only Mode (Recommended)
The application automatically detects Spark failures and switches to pandas-only mode. All functionality works, just with slightly slower performance for very large datasets.

#### Option 2: Install Java 8 or 11
```bash
# Install Java 11
brew install openjdk@11

# Set JAVA_HOME
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Restart terminal and try again
```

#### Option 3: Use Docker
```bash
# Run with Docker (if you have Docker installed)
docker run -p 8501:8501 -v $(pwd):/app your-app-image
```

## 🧪 Testing Your Setup

Run the diagnostic script to check your configuration:

```bash
python test_spark.py
```

This will tell you:
- ✅ If Java is available
- ❌ If Spark fails (expected on macOS with newer Java)
- ✅ If pandas fallback works (this is what matters!)

## 📊 Features

### Data Quality Checks
- **Completeness**: Check for missing values
- **Uniqueness**: Verify data uniqueness
- **Consistency**: Validate data consistency
- **Range Checks**: Ensure values are within expected ranges
- **Pattern Checks**: Validate data patterns

### Anomaly Detection
- Statistical anomaly detection
- Configurable thresholds
- Visual anomaly reporting

### Data Profiling
- Comprehensive data analysis
- Statistical summaries
- Data type analysis

### Alert System
- Real-time quality monitoring
- Configurable alert thresholds
- Email and webhook notifications

## 🎯 Sample Datasets

The application includes several sample datasets with built-in quality issues:

1. **Sales Data**: E-commerce sales with missing values and outliers
2. **Customer Data**: Customer information with data quality issues
3. **Product Data**: Product catalog with pricing anomalies
4. **Transaction Data**: Financial transactions with built-in anomalies

## 🔍 Usage

1. **Select a Dataset**: Choose from the available sample datasets
2. **Configure Settings**: Set quality check parameters and thresholds
3. **Run Analysis**: Execute comprehensive quality checks and anomaly detection
4. **Review Results**: View detailed reports and visualizations
5. **Set Up Alerts**: Configure monitoring and alerting

## 🛠️ Troubleshooting

### Application Won't Start
```bash
# Check if virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt

# Run diagnostic
python test_spark.py
```

### Spark Issues
- **Expected**: Spark may fail on macOS with newer Java versions
- **Solution**: The app automatically uses pandas-only mode
- **Performance**: Slightly slower for very large datasets

### Memory Issues
- Reduce dataset size for testing
- Use pandas-only mode for smaller datasets
- Monitor system resources

## 📈 Performance Notes

- **Pandas Mode**: Suitable for datasets up to several GB
- **Spark Mode**: Better for very large datasets (if working)
- **Memory Usage**: Monitor system memory during large dataset processing

## 🔧 Configuration

### Environment Variables
- `SPARK_VERSION`: Set to '3.3' for PyDeequ compatibility
- `JAVA_HOME`: Set to Java 8 or 11 installation path (optional)

### Quality Check Thresholds
- **Completeness**: Default 95% minimum
- **Uniqueness**: Default 90% minimum
- **Anomaly Detection**: Default 2 standard deviations

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

If you encounter issues:

1. **Run the diagnostic**: `python test_spark.py`
2. **Check the logs**: Look for error messages in the console
3. **Try pandas-only mode**: The app should work even if Spark fails
4. **Report issues**: Include diagnostic output and error messages

## 🎉 Success Indicators

Your setup is working correctly if:
- ✅ The app starts without errors
- ✅ You can load sample datasets
- ✅ Quality checks run successfully
- ✅ Anomaly detection works
- ✅ Visualizations display properly

**Note**: Spark failures are expected and handled gracefully by the application. 