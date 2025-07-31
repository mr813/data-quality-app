#!/usr/bin/env python3
"""
Test script to diagnose Spark initialization issues on macOS
"""

import os
import sys
import subprocess
import platform

def check_java_version():
    """Check Java version and provide recommendations"""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        print("✅ Java is available")
        print(f"Java version output: {result.stderr}")
        
        # Check if it's a newer Java version that might cause issues
        if "version \"17" in result.stderr or "version \"21" in result.stderr:
            print("⚠️  You're using a newer Java version that may cause Spark issues")
            print("   Consider using Java 8 or 11 for better Spark compatibility")
        
    except FileNotFoundError:
        print("❌ Java not found. Please install Java 8 or 11")
        return False
    
    return True

def check_environment_variables():
    """Check and set environment variables for Spark"""
    print("\n🔧 Environment Variables:")
    
    # Set common environment variables for Spark on macOS
    env_vars = {
        'SPARK_VERSION': '3.3',
        'JAVA_HOME': os.environ.get('JAVA_HOME', 'Not set'),
        'SPARK_HOME': os.environ.get('SPARK_HOME', 'Not set'),
        'PYSPARK_PYTHON': sys.executable,
        'PYSPARK_DRIVER_PYTHON': sys.executable
    }
    
    for var, value in env_vars.items():
        print(f"   {var}: {value}")
    
    return env_vars

def test_spark_initialization():
    """Test Spark initialization with different configurations"""
    print("\n🧪 Testing Spark Initialization:")
    
    # Test 1: Basic Spark initialization
    try:
        import findspark
        findspark.init()
        
        from pyspark.sql import SparkSession
        
        print("   Testing basic Spark initialization...")
        
        spark = SparkSession.builder \
            .appName("TestApp") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config= -Djava.security.krb5.conf= -Djavax.security.auth.useSubjectCredsOnly=false") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config= -Djava.security.krb5.conf= -Djavax.security.auth.useSubjectCredsOnly=false") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.local.dir", "/tmp/spark-temp") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        print("   ✅ Spark initialized successfully!")
        
        # Test basic operations
        test_df = spark.createDataFrame([(1, "test"), (2, "test2")], ["id", "name"])
        count = test_df.count()
        print(f"   ✅ Basic Spark operations work (test DataFrame has {count} rows)")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"   ❌ Spark initialization failed: {e}")
        return False

def test_pandas_fallback():
    """Test pandas-only fallback functionality"""
    print("\n📊 Testing Pandas Fallback:")
    
    try:
        import pandas as pd
        import numpy as np
        from data_quality_engine import DataQualityEngine
        
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', None, 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000, 60000, 70000, 80000, 90000]
        })
        
        # Initialize quality engine without Spark
        quality_engine = DataQualityEngine(spark_session=None)
        
        # Test quality checks
        results = quality_engine.run_comprehensive_quality_check(test_data)
        
        if 'error' not in results:
            print("   ✅ Pandas fallback works correctly!")
            print(f"   Quality score: {quality_engine.calculate_quality_score(results):.2f}%")
            return True
        else:
            print(f"   ❌ Pandas fallback failed: {results['error']}")
            return False
            
    except Exception as e:
        print(f"   ❌ Pandas fallback test failed: {e}")
        return False

def provide_recommendations():
    """Provide recommendations for fixing Spark issues"""
    print("\n💡 Recommendations:")
    
    print("1. **If Spark fails to initialize:**")
    print("   - The app will automatically fall back to pandas-only mode")
    print("   - All data quality checks will still work")
    print("   - Performance may be slower for very large datasets")
    
    print("\n2. **To fix Spark issues on macOS:**")
    print("   - Install Java 8 or 11: brew install openjdk@11")
    print("   - Set JAVA_HOME: export JAVA_HOME=/opt/homebrew/opt/openjdk@11")
    print("   - Restart your terminal and try again")
    
    print("\n3. **Alternative solutions:**")
    print("   - Use Docker with Spark pre-configured")
    print("   - Use cloud-based Spark services (Databricks, AWS EMR)")
    print("   - Use pandas-only mode for smaller datasets")

def main():
    """Main diagnostic function"""
    print("🔍 Spark Diagnostic Tool for macOS")
    print("=" * 50)
    
    # Check system info
    print(f"OS: {platform.system()} {platform.release()}")
    print(f"Python: {sys.version}")
    
    # Run diagnostics
    java_ok = check_java_version()
    env_vars = check_environment_variables()
    spark_ok = test_spark_initialization()
    pandas_ok = test_pandas_fallback()
    
    # Summary
    print("\n📋 Summary:")
    print(f"   Java: {'✅' if java_ok else '❌'}")
    print(f"   Spark: {'✅' if spark_ok else '❌'}")
    print(f"   Pandas Fallback: {'✅' if pandas_ok else '❌'}")
    
    if spark_ok:
        print("\n🎉 Everything is working correctly!")
    elif pandas_ok:
        print("\n⚠️  Spark failed but pandas fallback works. The app will function normally.")
    else:
        print("\n❌ Both Spark and pandas fallback failed. Please check your installation.")
    
    provide_recommendations()

if __name__ == "__main__":
    main() 