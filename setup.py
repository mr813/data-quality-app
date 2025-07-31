#!/usr/bin/env python3
"""
Setup script for Data Quality & Observability Platform
"""

import subprocess
import sys
import os
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version}")
    return True

def check_java():
    """Check if Java is installed"""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java is installed")
            return True
        else:
            print("❌ Java is not properly installed")
            return False
    except FileNotFoundError:
        print("❌ Java is not installed")
        print("Please install Java 8 or higher")
        return False

def install_dependencies():
    """Install Python dependencies"""
    print("\n📦 Installing Python dependencies...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False

def test_pydeequ_import():
    """Test if PyDeequ can be imported successfully"""
    print("\n🔍 Testing PyDeequ import...")
    try:
        # Set SPARK_VERSION environment variable
        os.environ['SPARK_VERSION'] = '3.3'
        
        # Try to import PyDeequ
        import pydeequ
        print("✅ PyDeequ import successful")
        return True
    except Exception as e:
        print(f"❌ PyDeequ import failed: {e}")
        print("This might be due to Spark/Java configuration issues")
        return False

def create_sample_config():
    """Create sample configuration file"""
    config_content = """# Data Quality Platform Configuration

# Alert System Configuration
[alerts]
email_enabled = false
webhook_enabled = false

# Quality Check Thresholds
[thresholds]
quality_score_min = 80.0
anomaly_percentage_max = 5.0
completeness_min = 95.0
uniqueness_min = 90.0

# Spark Configuration
[spark]
app_name = "DataQualityApp"
master = "local[*]"
"""
    
    config_file = Path("config.ini")
    if not config_file.exists():
        with open(config_file, 'w') as f:
            f.write(config_content)
        print("✅ Created sample configuration file: config.ini")

def create_env_file():
    """Create .env file with environment variables"""
    env_content = """# Environment variables for Data Quality Platform
SPARK_VERSION=3.3
JAVA_HOME=/usr/libexec/java_home
"""
    
    env_file = Path(".env")
    if not env_file.exists():
        with open(env_file, 'w') as f:
            f.write(env_content)
        print("✅ Created .env file with environment variables")

def main():
    """Main setup function"""
    print("🚀 Data Quality & Observability Platform Setup")
    print("=" * 50)
    
    # Check prerequisites
    print("\n🔍 Checking prerequisites...")
    
    if not check_python_version():
        sys.exit(1)
    
    if not check_java():
        print("\n⚠️  Java is required for Apache Spark")
        print("Please install Java 8 or higher and try again")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        sys.exit(1)
    
    # Test PyDeequ import
    if not test_pydeequ_import():
        print("\n⚠️  PyDeequ import test failed")
        print("The application might still work, but there could be issues")
    
    # Create sample configuration
    create_sample_config()
    create_env_file()
    
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Run the application: streamlit run app.py")
    print("2. Open your browser to: http://localhost:8501")
    print("3. Select a dataset and start monitoring!")
    
    print("\n📚 For more information, see README.md")
    print("\n⚠️  Note: If you encounter PyDeequ issues, ensure:")
    print("   - Java 8+ is installed and JAVA_HOME is set")
    print("   - SPARK_VERSION=3.3 is set in environment")

if __name__ == "__main__":
    main() 