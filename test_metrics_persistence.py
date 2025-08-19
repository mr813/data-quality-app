#!/usr/bin/env python3
"""
Test script for metrics persistence functionality
"""

import pandas as pd
import numpy as np
from data_quality_engine import DataQualityEngine
import os

def test_metrics_persistence():
    """Test the metrics persistence functionality"""
    
    print("🧪 Testing Metrics Persistence Functionality")
    print("=" * 50)
    
    # Initialize quality engine
    print("📊 Initializing Data Quality Engine...")
    quality_engine = DataQualityEngine(metrics_dir="test_quality_metrics")
    
    # Create sample datasets
    print("📋 Creating sample datasets...")
    
    # Sample sales data
    sales_data = pd.DataFrame({
        'order_id': range(1, 101),
        'customer_id': np.random.randint(1, 21, 100),
        'product_id': np.random.randint(1, 11, 100),
        'amount': np.random.uniform(10, 1000, 100),
        'order_date': pd.date_range('2024-01-01', periods=100, freq='D').strftime('%Y-%m-%d')
    })
    
    # Sample customer data
    customer_data = pd.DataFrame({
        'customer_id': range(1, 21),
        'name': [f'Customer_{i}' for i in range(1, 21)],
        'email': [f'customer{i}@example.com' for i in range(1, 21)],
        'age': np.random.randint(18, 80, 20),
        'registration_date': pd.date_range('2023-01-01', periods=20, freq='D').strftime('%Y-%m-%d')
    })
    
    # Test running quality checks and saving metrics
    print("🔍 Running quality checks for Sales Data...")
    sales_results = quality_engine.run_comprehensive_quality_check(
        sales_data, 
        dataset_name="Sales Data",
        save_metrics=True
    )
    
    print(f"✅ Sales Data Quality Score: {sales_results.get('overall_quality_score', 0):.2f}%")
    print(f"📊 Run ID: {sales_results.get('run_id', 'N/A')}")
    
    print("\n🔍 Running quality checks for Customer Data...")
    customer_results = quality_engine.run_comprehensive_quality_check(
        customer_data, 
        dataset_name="Customer Data",
        save_metrics=True
    )
    
    print(f"✅ Customer Data Quality Score: {customer_results.get('overall_quality_score', 0):.2f}%")
    print(f"📊 Run ID: {customer_results.get('run_id', 'N/A')}")
    
    # Test retrieving run history
    print("\n📋 Retrieving run history...")
    run_history = quality_engine.get_run_history()
    print(f"📊 Total runs: {len(run_history)}")
    
    for run in run_history:
        print(f"  - {run['dataset_name']}: {run['quality_score']:.2f}% (Run ID: {run['run_id'][:8]}...)")
    
    # Test dataset summary
    print("\n📈 Testing dataset summary...")
    sales_summary = quality_engine.get_dataset_summary("Sales Data")
    if 'error' not in sales_summary:
        print(f"📊 Sales Data Summary:")
        print(f"  - Total runs: {sales_summary['total_runs']}")
        print(f"  - Average quality score: {sales_summary['quality_score_stats']['mean']:.2f}%")
        print(f"  - Min quality score: {sales_summary['quality_score_stats']['min']:.2f}%")
        print(f"  - Max quality score: {sales_summary['quality_score_stats']['max']:.2f}%")
    
    # Test downloading metrics
    print("\n💾 Testing metrics download...")
    if run_history:
        first_run = run_history[0]
        download_path = quality_engine.download_metrics_json(first_run['run_id'])
        if download_path and os.path.exists(download_path):
            print(f"✅ Metrics file available: {download_path}")
            file_size = os.path.getsize(download_path)
            print(f"📁 File size: {file_size} bytes")
        else:
            print("❌ Metrics file not found")
    
    # Test getting specific run metrics
    print("\n🔍 Testing specific run metrics retrieval...")
    if run_history:
        first_run = run_history[0]
        run_metrics = quality_engine.get_run_metrics(first_run['run_id'])
        if 'error' not in run_metrics:
            print(f"✅ Successfully retrieved metrics for run {first_run['run_id'][:8]}...")
            print(f"📊 Dataset: {run_metrics['dataset_name']}")
            print(f"📊 Quality Score: {run_metrics['metrics'].get('overall_quality_score', 0):.2f}%")
        else:
            print(f"❌ Error retrieving metrics: {run_metrics['error']}")
    
    print("\n🎉 Metrics persistence test completed!")
    print(f"📁 Metrics stored in: {quality_engine.metrics_dir}")
    print(f"📋 Run history file: {quality_engine.runs_file}")

if __name__ == "__main__":
    test_metrics_persistence()
