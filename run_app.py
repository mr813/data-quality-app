#!/usr/bin/env python3
"""
Run script for the Data Quality & Observability Platform
"""

import os
import sys
import subprocess

def main():
    """Run the Streamlit application"""
    
    # Activate virtual environment and run the app
    venv_python = os.path.join(os.path.dirname(__file__), 'venv', 'bin', 'python')
    
    if not os.path.exists(venv_python):
        print("❌ Virtual environment not found. Please run 'python -m venv venv' first.")
        return
    
    # Set environment variables for better compatibility
    env = os.environ.copy()
    env['SPARK_VERSION'] = '3.3'
    
    # Run the Streamlit app
    try:
        print("🚀 Starting Data Quality & Observability Platform...")
        print("📊 The app will automatically fall back to pandas-only mode if Spark fails")
        print("🌐 Opening in your browser at http://localhost:8501")
        print("⏹️  Press Ctrl+C to stop the application")
        print("-" * 60)
        
        subprocess.run([
            venv_python, '-m', 'streamlit', 'run', 'app.py',
            '--server.port', '8501',
            '--server.address', 'localhost'
        ], env=env, check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Application stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running the application: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    main() 