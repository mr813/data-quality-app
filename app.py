import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime, timedelta
import time
import threading
import os
import sys

# Set SPARK_VERSION environment variable for PyDeequ
os.environ['SPARK_VERSION'] = '3.3'

# Add the current directory to the path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_quality_engine import DataQualityEngine
from alert_system import AlertSystem
from sample_data import get_sample_datasets, get_data_quality_issues_summary

# Initialize Spark session
def init_spark():
    """Initialize Spark session for PyDeequ"""
    try:
        import findspark
        findspark.init()
        
        from pyspark.sql import SparkSession
        
        # Suppress Spark warnings
        import logging
        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger("pyspark").setLevel(logging.ERROR)
        
        # Add Java security configurations to resolve macOS issues
        spark = SparkSession.builder \
            .appName("DataQualityApp") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config= -Djava.security.krb5.conf= -Djavax.security.auth.useSubjectCredsOnly=false") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config= -Djava.security.krb5.conf= -Djavax.security.auth.useSubjectCredsOnly=false") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.local.dir", "/tmp/spark-temp") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        return spark
    except Exception as e:
        # Don't show error message since fallback works
        return None

# Initialize session state
def init_session_state():
    if 'spark' not in st.session_state:
        st.session_state.spark = init_spark()
        # Silently handle Spark initialization - fallback works automatically

    if 'quality_engine' not in st.session_state:
        # Initialize quality engine with or without Spark
        st.session_state.quality_engine = DataQualityEngine(st.session_state.spark)

    if 'alert_system' not in st.session_state:
        st.session_state.alert_system = AlertSystem()

    if 'current_dataset' not in st.session_state:
        st.session_state.current_dataset = None

    if 'quality_results' not in st.session_state:
        st.session_state.quality_results = None

    if 'anomaly_results' not in st.session_state:
        st.session_state.anomaly_results = None

# Page configuration
st.set_page_config(
    page_title="Data Quality & Observability Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .alert-high {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
    }
    .alert-medium {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
    }
    .alert-low {
        background-color: #e8f5e8;
        border-left: 4px solid #4caf50;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Main application function"""
    
    # Initialize session state
    init_session_state()
    
    # Header
    st.markdown('<h1 class="main-header">📊 Data Quality & Observability Platform</h1>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Dataset selection
        st.subheader("📁 Dataset Selection")
        dataset_options = {
            "Sales Data": "sales_data",
            "Customer Data": "customer_data", 
            "Product Data": "product_data",
            "Transaction Data": "transaction_data",
            "Email Data": "email_data"
        }
        
        selected_dataset = st.selectbox(
            "Choose a dataset:",
            list(dataset_options.keys())
        )
        
        if st.button("🔄 Load Dataset"):
            with st.spinner("Loading dataset..."):
                try:
                    datasets = get_sample_datasets()
                    dataset_key = dataset_options[selected_dataset]
                    if dataset_key in datasets:
                        st.session_state.current_dataset = datasets[dataset_key]
                        st.success(f"✅ Loaded {selected_dataset}")
                    else:
                        st.error(f"❌ Dataset '{dataset_key}' not found. Available datasets: {list(datasets.keys())}")
                except Exception as e:
                    st.error(f"❌ Error loading dataset: {e}")
                    st.info("Available datasets: sales_data, customer_data, product_data, transaction_data, email_data")
        
        # Quality check configuration
        st.subheader("⚙️ Quality Check Settings")
        
        # Range checks
        st.write("**Range Checks (Optional):**")
        range_checks = st.checkbox("Enable range checks")
        
        # Pattern checks
        st.write("**Pattern Checks (Optional):**")
        pattern_checks = st.checkbox("Enable pattern checks")
        
        # Anomaly detection settings
        st.subheader("🔍 Anomaly Detection")
        anomaly_threshold = st.slider(
            "Anomaly Threshold (Standard Deviations):",
            min_value=1.0,
            max_value=5.0,
            value=2.0,
            step=0.5
        )
        
        # Alert settings
        st.subheader("🚨 Alert Settings")
        alert_enabled = st.checkbox("Enable alerts")
        
        if alert_enabled:
            quality_threshold = st.slider(
                "Minimum Quality Score (%):",
                min_value=0,
                max_value=100,
                value=80
            )
            
            anomaly_threshold_alert = st.slider(
                "Maximum Anomaly Percentage (%):",
                min_value=0,
                max_value=20,
                value=5
            )
    
    # Main content area
    if st.session_state.current_dataset is not None:
        display_dashboard()
    else:
        display_welcome()

def display_welcome():
    """Display welcome screen"""
    st.markdown("""
    ## Welcome to the Data Quality & Observability Platform! 🎉
    
    This platform provides comprehensive data quality monitoring and anomaly detection capabilities using PyDeequ.
    
    ### Features:
    - 📊 **Data Quality Checks**: Completeness, uniqueness, consistency, and more
    - 🔍 **Anomaly Detection**: Statistical anomaly detection with configurable thresholds
    - 📈 **Data Profiling**: Comprehensive data analysis and profiling
    - 🚨 **Alert System**: Real-time alerts for data quality issues and anomalies
    - 📋 **Observability Dashboard**: Visual insights into data quality metrics
    
    ### Getting Started:
    1. Select a dataset from the sidebar
    2. Configure quality check settings
    3. Run quality checks and anomaly detection
    4. Monitor results and set up alerts
    
    ### Sample Datasets Available:
    - **Sales Data**: E-commerce sales with various quality issues
    - **Customer Data**: Customer information with missing values and invalid data
    - **Product Data**: Product catalog with pricing and inventory issues
    - **Transaction Data**: Financial transactions with built-in anomalies
    """)

def display_dashboard():
    """Display the main dashboard"""
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "🔍 Quality Analysis", 
        "🚨 Anomaly Detection", 
        "📈 Data Profiling", 
        "⚙️ Alert Management"
    ])
    
    with tab1:
        display_overview()
    
    with tab2:
        display_quality_analysis()
    
    with tab3:
        display_anomaly_detection()
    
    with tab4:
        display_data_profiling()
    
    with tab5:
        display_alert_management()

def display_overview():
    """Display overview dashboard"""
    
    df = st.session_state.current_dataset
    
    # Dataset info
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Rows", f"{len(df):,}")
    
    with col2:
        st.metric("📋 Total Columns", len(df.columns))
    
    with col3:
        missing_total = df.isnull().sum().sum()
        st.metric("❌ Missing Values", f"{missing_total:,}")
    
    with col4:
        duplicate_rows = len(df[df.duplicated()])
        st.metric("🔄 Duplicate Rows", f"{duplicate_rows:,}")
    
    # Data quality issues summary
    st.subheader("📋 Data Quality Issues Summary")
    
    issues_summary = get_data_quality_issues_summary(df)
    
    # Display missing values
    if issues_summary['missing_values']:
        st.write("**Missing Values by Column:**")
        missing_df = pd.DataFrame([
            {
                'Column': col,
                'Missing Count': data['count'],
                'Missing Percentage': f"{data['percentage']:.2f}%"
            }
            for col, data in issues_summary['missing_values'].items()
        ])
        st.dataframe(missing_df, use_container_width=True)
    
    # Data types
    st.write("**Data Types:**")
    dtype_df = pd.DataFrame([
        {'Column': col, 'Data Type': str(dtype)}
        for col, dtype in issues_summary['data_types'].items()
    ])
    st.dataframe(dtype_df, use_container_width=True)
    
    # Run quality checks button
    if st.button("🔍 Run Quality Checks", type="primary"):
        with st.spinner("Running comprehensive quality checks..."):
            try:
                quality_results = st.session_state.quality_engine.run_comprehensive_quality_check(df)
                quality_score = st.session_state.quality_engine.calculate_quality_score(quality_results)
                quality_results['quality_score'] = quality_score
                
                st.session_state.quality_results = quality_results
                st.success(f"✅ Quality checks completed! Overall score: {quality_score:.2f}%")
                
            except Exception as e:
                st.error(f"❌ Error running quality checks: {e}")

def display_quality_analysis():
    """Display quality analysis results"""
    
    if st.session_state.quality_results is None:
        st.info("👆 Run quality checks from the Overview tab to see results here.")
        return
    
    results = st.session_state.quality_results
    
    # Quality score
    quality_score = results.get('quality_score', 0)
    
    # Create gauge chart for quality score
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=quality_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Overall Quality Score"},
        delta={'reference': 80},
        gauge={
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': "lightgray"},
                {'range': [50, 80], 'color': "yellow"},
                {'range': [80, 100], 'color': "green"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 80
            }
        }
    ))
    
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)
    
    # Quality check results
    st.subheader("📋 Quality Check Results")
    
    if 'quality_checks' in results:
        for check_type, check_result in results['quality_checks'].items():
            if 'error' not in check_result:
                with st.expander(f"🔍 {check_type.replace('_', ' ').title()}"):
                    if 'results' in check_result:
                        for result in check_result['results']:
                            status_icon = "✅" if result.get('status') == "Success" else "❌"
                            st.write(f"{status_icon} {result.get('constraint', 'Unknown')}: {result.get('status', 'Unknown')}")
                    else:
                        st.write("No results available")

def display_anomaly_detection():
    """Display anomaly detection results"""
    
    df = st.session_state.current_dataset
    
    # Anomaly detection controls
    col1, col2 = st.columns(2)
    
    with col1:
        threshold = st.slider(
            "Anomaly Detection Threshold:",
            min_value=1.0,
            max_value=5.0,
            value=2.0,
            step=0.5
        )
    
    with col2:
        if st.button("🔍 Detect Anomalies", type="primary"):
            with st.spinner("Detecting anomalies..."):
                try:
                    anomaly_results = st.session_state.quality_engine.detect_anomalies(
                        df, threshold=threshold
                    )
                    st.session_state.anomaly_results = anomaly_results
                    st.success("✅ Anomaly detection completed!")
                except Exception as e:
                    st.error(f"❌ Error detecting anomalies: {e}")
    
    # Display anomaly results
    if st.session_state.anomaly_results and 'anomaly_detection' in st.session_state.anomaly_results:
        anomalies = st.session_state.anomaly_results['anomaly_detection']
        
        st.subheader("🚨 Anomaly Detection Results")
        
        # Summary metrics
        total_anomalies = sum(data['anomaly_count'] for data in anomalies.values())
        avg_anomaly_percentage = np.mean([data['anomaly_percentage'] for data in anomalies.values()])
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Anomalies", f"{total_anomalies:,}")
        
        with col2:
            st.metric("Average Anomaly %", f"{avg_anomaly_percentage:.2f}%")
        
        with col3:
            st.metric("Columns with Anomalies", len(anomalies))
        
        # Anomaly details by column
        st.subheader("📊 Anomaly Details by Column")
        
        for column, anomaly_data in anomalies.items():
            with st.expander(f"📈 {column}"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Statistics:**")
                    st.write(f"- Mean: {anomaly_data['mean']:.2f}")
                    st.write(f"- Std Dev: {anomaly_data['std']:.2f}")
                    st.write(f"- Lower Bound: {anomaly_data['lower_bound']:.2f}")
                    st.write(f"- Upper Bound: {anomaly_data['upper_bound']:.2f}")
                
                with col2:
                    st.write(f"**Anomaly Info:**")
                    st.write(f"- Anomaly Count: {anomaly_data['anomaly_count']}")
                    st.write(f"- Anomaly %: {anomaly_data['anomaly_percentage']:.2f}%")
                
                # Create histogram
                if len(anomaly_data['anomaly_values']) > 0:
                    fig = px.histogram(
                        x=anomaly_data['anomaly_values'],
                        title=f"Anomaly Distribution for {column}",
                        labels={'x': column, 'y': 'Count'}
                    )
                    fig.add_vline(x=anomaly_data['mean'], line_dash="dash", line_color="red")
                    fig.add_vline(x=anomaly_data['lower_bound'], line_dash="dash", line_color="orange")
                    fig.add_vline(x=anomaly_data['upper_bound'], line_dash="dash", line_color="orange")
                    st.plotly_chart(fig, use_container_width=True)

def display_data_profiling():
    """Display data profiling results"""
    
    df = st.session_state.current_dataset
    
    st.subheader("📊 Data Profiling")
    
    # Basic statistics
    st.write("**Basic Statistics:**")
    st.dataframe(df.describe(), use_container_width=True)
    
    # Missing values visualization
    st.write("**Missing Values Analysis:**")
    missing_data = df.isnull().sum()
    missing_data = missing_data[missing_data > 0]
    
    if len(missing_data) > 0:
        fig = px.bar(
            x=missing_data.index,
            y=missing_data.values,
            title="Missing Values by Column",
            labels={'x': 'Column', 'y': 'Missing Count'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Data types distribution
    st.write("**Data Types Distribution:**")
    dtype_counts = df.dtypes.value_counts()
    # Convert dtype objects to strings for JSON serialization
    dtype_counts_str = dtype_counts.astype(str)
    fig = px.pie(
        values=dtype_counts_str.values,
        names=dtype_counts_str.index.astype(str),  # Ensure names are also strings
        title="Data Types Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Column analysis
    st.subheader("📋 Column Analysis")
    
    for col in df.columns:
        with st.expander(f"📊 {col}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Data Type:** {str(df[col].dtype)}")
                st.write(f"**Unique Values:** {df[col].nunique()}")
                st.write(f"**Missing Values:** {df[col].isnull().sum()}")
                
                if df[col].dtype in ['int64', 'float64']:
                    st.write(f"**Min:** {df[col].min()}")
                    st.write(f"**Max:** {df[col].max()}")
                    st.write(f"**Mean:** {df[col].mean():.2f}")
            
            with col2:
                if df[col].dtype in ['int64', 'float64']:
                    fig = px.histogram(df, x=col, title=f"Distribution of {col}")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    # For categorical data, show value counts
                    value_counts = df[col].value_counts().head(10)
                    fig = px.bar(
                        x=value_counts.index,
                        y=value_counts.values,
                        title=f"Top 10 Values in {col}"
                    )
                    st.plotly_chart(fig, use_container_width=True)

def display_alert_management():
    """Display alert management interface"""
    
    st.subheader("🚨 Alert Management")
    
    # Alert configuration
    with st.expander("⚙️ Alert Configuration"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Email Alerts:**")
            email_enabled = st.checkbox("Enable email alerts")
            
            if email_enabled:
                smtp_server = st.text_input("SMTP Server", "smtp.gmail.com")
                smtp_port = st.number_input("SMTP Port", value=587)
                username = st.text_input("Email Username")
                password = st.text_input("Email Password", type="password")
                recipients = st.text_area("Recipients (one per line)")
        
        with col2:
            st.write("**Webhook Alerts:**")
            webhook_enabled = st.checkbox("Enable webhook alerts")
            
            if webhook_enabled:
                webhook_url = st.text_input("Webhook URL")
                webhook_headers = st.text_area("Headers (JSON format)")
    
    # Alert thresholds
    with st.expander("📊 Alert Thresholds"):
        col1, col2 = st.columns(2)
        
        with col1:
            quality_threshold = st.slider(
                "Minimum Quality Score (%)",
                min_value=0,
                max_value=100,
                value=80
            )
            
            anomaly_threshold = st.slider(
                "Maximum Anomaly Percentage (%)",
                min_value=0,
                max_value=20,
                value=5
            )
        
        with col2:
            completeness_threshold = st.slider(
                "Minimum Completeness (%)",
                min_value=0,
                max_value=100,
                value=95
            )
            
            uniqueness_threshold = st.slider(
                "Minimum Uniqueness (%)",
                min_value=0,
                max_value=100,
                value=90
            )
    
    # Test alerts
    if st.button("🧪 Test Alert System"):
        test_alert = {
            'type': 'test_alert',
            'severity': 'low',
            'message': 'This is a test alert from the data quality platform',
            'timestamp': datetime.now().isoformat()
        }
        
        st.session_state.alert_system.process_alerts([test_alert])
        st.success("✅ Test alert processed successfully!")
    
    # Alert history
    with st.expander("📋 Alert History"):
        alert_summary = st.session_state.alert_system.get_alert_summary(hours=24)
        
        if 'error' not in alert_summary:
            st.write(f"**Total Alerts (24h):** {alert_summary['total_alerts']}")
            
            if alert_summary['alert_counts']:
                st.write("**Alert Types:**")
                for alert_type, count in alert_summary['alert_counts'].items():
                    st.write(f"- {alert_type}: {count}")
            
            if alert_summary['severity_counts']:
                st.write("**Severity Distribution:**")
                for severity, count in alert_summary['severity_counts'].items():
                    st.write(f"- {severity}: {count}")
        else:
            st.write("No alert history available")

if __name__ == "__main__":
    main() 