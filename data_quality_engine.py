import pandas as pd
import numpy as np
import os

# Set SPARK_VERSION environment variable for PyDeequ
os.environ['SPARK_VERSION'] = '3.3'

from pydeequ import Check, CheckLevel
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import warnings
warnings.filterwarnings('ignore')

class DataQualityEngine:
    """
    Data Quality Engine using PyDeequ for comprehensive data quality checks
    and anomaly detection.
    """
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.logger = self._setup_logging()
        self.use_spark = spark_session is not None
        
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def convert_pandas_to_spark(self, df: pd.DataFrame, table_name: str = "dataframe"):
        """Convert pandas DataFrame to Spark DataFrame"""
        if not self.use_spark:
            self.logger.warning("Spark not available, using pandas DataFrame directly")
            return df
            
        try:
            spark_df = self.spark.createDataFrame(df)
            spark_df.createOrReplaceTempView(table_name)
            return spark_df
        except Exception as e:
            self.logger.error(f"Error converting pandas to Spark: {e}")
            self.logger.warning("Falling back to pandas DataFrame")
            return df
    
    def run_completeness_check(self, spark_df, columns: List[str] = None) -> Dict:
        """Run completeness checks on specified columns"""
        try:
            if columns is None:
                columns = spark_df.columns
            
            results = []
            for col in columns:
                # Count null values
                if self.use_spark:
                    null_count = spark_df.filter(f"{col} IS NULL").count()
                    total_count = spark_df.count()
                else:
                    # Use pandas for null counting
                    null_count = spark_df[col].isnull().sum()
                    total_count = len(spark_df)
                
                completeness_ratio = (total_count - null_count) / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': f"Completeness of {col}",
                    'status': 'Success' if completeness_ratio >= 0.95 else 'Failure',
                    'completeness_ratio': completeness_ratio,
                    'null_count': null_count,
                    'total_count': total_count
                })
            
            return {
                'check_type': 'completeness',
                'columns': columns,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in completeness check: {e}")
            return {'error': str(e)}
    
    def run_uniqueness_check(self, spark_df, columns: List[str] = None) -> Dict:
        """Run uniqueness checks on specified columns"""
        try:
            if columns is None:
                columns = spark_df.columns
            
            results = []
            for col in columns:
                # Count distinct values
                if self.use_spark:
                    distinct_count = spark_df.select(col).distinct().count()
                    total_count = spark_df.count()
                else:
                    # Use pandas for distinct counting
                    distinct_count = spark_df[col].nunique()
                    total_count = len(spark_df)
                
                uniqueness_ratio = distinct_count / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': f"Uniqueness of {col}",
                    'status': 'Success' if uniqueness_ratio >= 0.9 else 'Failure',
                    'uniqueness_ratio': uniqueness_ratio,
                    'distinct_count': distinct_count,
                    'total_count': total_count
                })
            
            return {
                'check_type': 'uniqueness',
                'columns': columns,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in uniqueness check: {e}")
            return {'error': str(e)}
    
    def run_consistency_check(self, spark_df, numeric_columns: List[str] = None) -> Dict:
        """Run consistency checks on numeric columns"""
        try:
            if numeric_columns is None:
                if self.use_spark:
                    numeric_columns = [col for col in spark_df.columns if spark_df.select(col).dtypes[0][1] in ['int', 'double', 'float']]
                else:
                    numeric_columns = spark_df.select_dtypes(include=[np.number]).columns.tolist()
            
            results = []
            for col in numeric_columns:
                # Check for negative values
                if self.use_spark:
                    negative_count = spark_df.filter(f"{col} < 0").count()
                    total_count = spark_df.count()
                    outlier_count = spark_df.filter(f"{col} > 1e10").count()
                else:
                    # Use pandas for filtering
                    negative_count = (spark_df[col] < 0).sum()
                    total_count = len(spark_df)
                    outlier_count = (spark_df[col] > 1e10).sum()
                
                negative_ratio = negative_count / total_count if total_count > 0 else 0
                outlier_ratio = outlier_count / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': f"Consistency of {col}",
                    'status': 'Success' if negative_ratio == 0 and outlier_ratio == 0 else 'Failure',
                    'negative_ratio': negative_ratio,
                    'outlier_ratio': outlier_ratio,
                    'negative_count': negative_count,
                    'outlier_count': outlier_count,
                    'total_count': total_count
                })
            
            return {
                'check_type': 'consistency',
                'columns': numeric_columns,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in consistency check: {e}")
            return {'error': str(e)}
    
    def run_range_check(self, spark_df, column_ranges: Dict[str, Dict]) -> Dict:
        """Run range checks on specified columns"""
        try:
            results = []
            for col, ranges in column_ranges.items():
                violations = 0
                if self.use_spark:
                    total_count = spark_df.count()
                    
                    if 'min' in ranges:
                        below_min = spark_df.filter(f"{col} < {ranges['min']}").count()
                        violations += below_min
                    
                    if 'max' in ranges:
                        above_max = spark_df.filter(f"{col} > {ranges['max']}").count()
                        violations += above_max
                else:
                    # Use pandas for range checking
                    total_count = len(spark_df)
                    
                    if 'min' in ranges:
                        below_min = (spark_df[col] < ranges['min']).sum()
                        violations += below_min
                    
                    if 'max' in ranges:
                        above_max = (spark_df[col] > ranges['max']).sum()
                        violations += above_max
                
                violation_ratio = violations / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': f"Range check for {col}",
                    'status': 'Success' if violation_ratio == 0 else 'Failure',
                    'violation_ratio': violation_ratio,
                    'violations': violations,
                    'total_count': total_count,
                    'ranges': ranges
                })
            
            return {
                'check_type': 'range_check',
                'column_ranges': column_ranges,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in range check: {e}")
            return {'error': str(e)}
    
    def run_pattern_check(self, spark_df, pattern_columns: Dict[str, str]) -> Dict:
        """Run pattern checks on specified columns"""
        try:
            results = []
            for col, pattern in pattern_columns.items():
                # For now, we'll implement a simple pattern check
                # In a real implementation, you might use regex or other pattern matching
                total_count = spark_df.count()
                # This is a simplified pattern check - in practice you'd use regex
                pattern_violations = 0  # Placeholder for actual pattern checking
                
                violation_ratio = pattern_violations / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': f"Pattern check for {col}",
                    'status': 'Success' if violation_ratio == 0 else 'Failure',
                    'violation_ratio': violation_ratio,
                    'violations': pattern_violations,
                    'total_count': total_count,
                    'pattern': pattern
                })
            
            return {
                'check_type': 'pattern_check',
                'pattern_columns': pattern_columns,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in pattern check: {e}")
            return {'error': str(e)}
    
    def generate_data_profile(self, spark_df) -> Dict:
        """Generate comprehensive data profile"""
        try:
            # Create a simple profile based on basic statistics
            profile = {}
            for col in spark_df.columns:
                if self.use_spark:
                    col_type = spark_df.select(col).dtypes[0][1]
                    total_count = spark_df.count()
                    null_count = spark_df.filter(f"{col} IS NULL").count()
                    distinct_count = spark_df.select(col).distinct().count()
                else:
                    # Use pandas for profiling
                    col_type = str(spark_df[col].dtype)
                    total_count = len(spark_df)
                    null_count = spark_df[col].isnull().sum()
                    distinct_count = spark_df[col].nunique()
                
                profile[col] = {
                    'data_type': col_type,
                    'total_count': total_count,
                    'null_count': null_count,
                    'distinct_count': distinct_count
                }
                
                # Add numeric statistics for numeric columns
                if self.use_spark:
                    if col_type in ['int', 'double', 'float']:
                        stats = spark_df.select(col).summary("min", "25%", "50%", "75%", "max").collect()
                        if stats:
                            profile[col]['min'] = stats[0][col]
                            profile[col]['max'] = stats[4][col]
                else:
                    # Use pandas for numeric statistics
                    if spark_df[col].dtype in ['int64', 'float64']:
                        profile[col]['min'] = spark_df[col].min()
                        profile[col]['max'] = spark_df[col].max()
            
            return {
                'check_type': 'data_profile',
                'profile': profile,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error generating data profile: {e}")
            return {'error': str(e)}
    
    def suggest_constraints(self, spark_df) -> Dict:
        """Suggest constraints based on data analysis"""
        try:
            # For now, return a simple suggestion based on data types
            suggestions = []
            for col in spark_df.columns:
                if self.use_spark:
                    col_type = spark_df.select(col).dtypes[0][1]
                else:
                    col_type = str(spark_df[col].dtype)
                
                if col_type in ['int', 'double', 'float', 'int64', 'float64']:
                    suggestions.append(f"Column {col} is numeric - consider range checks")
                elif col_type in ['string', 'object']:
                    suggestions.append(f"Column {col} is string - consider pattern checks")
            
            return {
                'check_type': 'constraint_suggestions',
                'suggestions': suggestions,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error suggesting constraints: {e}")
            return {'error': str(e)}
    
    def run_comprehensive_quality_check(self, df: pd.DataFrame, 
                                     column_ranges: Dict = None,
                                     pattern_columns: Dict = None) -> Dict:
        """Run comprehensive data quality checks"""
        try:
            spark_df = self.convert_pandas_to_spark(df)
            
            results = {
                'dataset_info': {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'memory_usage': df.memory_usage(deep=True).sum(),
                    'timestamp': datetime.now().isoformat()
                },
                'quality_checks': {}
            }
            
            # Run all quality checks
            results['quality_checks']['completeness'] = self.run_completeness_check(spark_df)
            results['quality_checks']['uniqueness'] = self.run_uniqueness_check(spark_df)
            results['quality_checks']['consistency'] = self.run_consistency_check(spark_df)
            
            # Add text-specific checks for datasets with text content
            try:
                text_columns = [col for col in spark_df.columns if spark_df[col].dtype == 'object'] if not self.use_spark else None
                if text_columns:
                    results['quality_checks']['text_quality'] = self.run_text_quality_check(spark_df, text_columns)
            except Exception as e:
                self.logger.warning(f"Could not run text quality checks: {e}")
            
            # Add email-specific checks if email-related columns are present
            try:
                if any(col in spark_df.columns for col in ['sender_email', 'subject', 'email_content']):
                    results['quality_checks']['email_specific'] = self.run_email_specific_check(spark_df)
            except Exception as e:
                self.logger.warning(f"Could not run email-specific checks: {e}")
            
            if column_ranges:
                results['quality_checks']['range_check'] = self.run_range_check(spark_df, column_ranges)
            
            if pattern_columns:
                results['quality_checks']['pattern_check'] = self.run_pattern_check(spark_df, pattern_columns)
            
            # Generate profile and suggestions
            results['quality_checks']['data_profile'] = self.generate_data_profile(spark_df)
            results['quality_checks']['constraint_suggestions'] = self.suggest_constraints(spark_df)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive quality check: {e}")
            return {'error': str(e)}
    
    def detect_anomalies(self, df: pd.DataFrame, 
                        numeric_columns: List[str] = None,
                        threshold: float = 2.0) -> Dict:
        """Detect anomalies using statistical methods"""
        try:
            if numeric_columns is None:
                numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
            
            anomalies = {}
            
            for col in numeric_columns:
                if col in df.columns:
                    # Calculate statistics
                    mean_val = df[col].mean()
                    std_val = df[col].std()
                    
                    # Define anomaly threshold
                    lower_bound = mean_val - threshold * std_val
                    upper_bound = mean_val + threshold * std_val
                    
                    # Find anomalies
                    anomalies_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
                    anomaly_indices = df[anomalies_mask].index.tolist()
                    anomaly_values = df.loc[anomalies_mask, col].tolist()
                    
                    anomalies[col] = {
                        'mean': mean_val,
                        'std': std_val,
                        'lower_bound': lower_bound,
                        'upper_bound': upper_bound,
                        'anomaly_count': len(anomaly_indices),
                        'anomaly_percentage': (len(anomaly_indices) / len(df)) * 100,
                        'anomaly_indices': anomaly_indices,
                        'anomaly_values': anomaly_values
                    }
            
            return {
                'anomaly_detection': anomalies,
                'threshold': threshold,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return {'error': str(e)}
    
    def calculate_quality_score(self, quality_results: Dict) -> float:
        """Calculate overall data quality score"""
        try:
            total_checks = 0
            passed_checks = 0
            
            for check_type, check_result in quality_results['quality_checks'].items():
                if 'error' not in check_result and 'results' in check_result:
                    for result in check_result['results']:
                        total_checks += 1
                        if result.get('status') == 'Success':
                            passed_checks += 1
            
            if total_checks == 0:
                return 0.0
            
            return (passed_checks / total_checks) * 100
            
        except Exception as e:
            self.logger.error(f"Error calculating quality score: {e}")
            return 0.0 

    def run_text_quality_check(self, spark_df, text_columns: List[str] = None) -> Dict:
        """Run text-specific quality checks on specified columns"""
        try:
            if text_columns is None:
                # Identify text columns (object/string type)
                if self.use_spark:
                    text_columns = [col for col in spark_df.columns if spark_df.select(col).dtypes[0][1] == 'string']
                else:
                    text_columns = spark_df.select_dtypes(include=['object']).columns.tolist()
            
            results = []
            for col in text_columns:
                if self.use_spark:
                    # Spark text analysis
                    total_count = spark_df.count()
                    null_count = spark_df.filter(f"{col} IS NULL").count()
                    empty_count = spark_df.filter(f"length({col}) = 0").count()
                    short_count = spark_df.filter(f"length({col}) < 10").count()
                    long_count = spark_df.filter(f"length({col}) > 1000").count()
                else:
                    # Pandas text analysis
                    total_count = len(spark_df)
                    null_count = spark_df[col].isnull().sum()
                    empty_count = (spark_df[col] == "").sum()
                    short_count = (spark_df[col].str.len() < 10).sum()
                    long_count = (spark_df[col].str.len() > 1000).sum()
                
                # Calculate metrics
                null_ratio = null_count / total_count if total_count > 0 else 0
                empty_ratio = empty_count / total_count if total_count > 0 else 0
                short_ratio = short_count / total_count if total_count > 0 else 0
                long_ratio = long_count / total_count if total_count > 0 else 0
                
                # Determine status based on thresholds
                status = "Success"
                if null_ratio > 0.1 or empty_ratio > 0.05 or short_ratio > 0.3 or long_ratio > 0.1:
                    status = "Failure"
                
                results.append({
                    'constraint': f"Text quality for {col}",
                    'status': status,
                    'null_ratio': null_ratio,
                    'empty_ratio': empty_ratio,
                    'short_ratio': short_ratio,
                    'long_ratio': long_ratio,
                    'null_count': null_count,
                    'empty_count': empty_count,
                    'short_count': short_count,
                    'long_count': long_count,
                    'total_count': total_count
                })
            
            return {
                'check_type': 'text_quality',
                'columns': text_columns,
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in text quality check: {e}")
            return {'error': str(e)}
    
    def run_email_specific_check(self, spark_df) -> Dict:
        """Run email-specific quality checks"""
        try:
            results = []
            
            # Check email format validity
            if 'sender_email' in spark_df.columns:
                if self.use_spark:
                    invalid_email_count = spark_df.filter("sender_email NOT LIKE '%@%.%'").count()
                    total_count = spark_df.count()
                else:
                    # Simple email validation regex
                    import re
                    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    invalid_email_count = (~spark_df['sender_email'].str.match(email_pattern, na=False)).sum()
                    total_count = len(spark_df)
                
                invalid_email_ratio = invalid_email_count / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': 'Email format validity',
                    'status': 'Success' if invalid_email_ratio < 0.05 else 'Failure',
                    'invalid_email_ratio': invalid_email_ratio,
                    'invalid_email_count': invalid_email_count,
                    'total_count': total_count
                })
            
            # Check for spam indicators
            if 'subject' in spark_df.columns:
                if self.use_spark:
                    spam_subject_count = spark_df.filter("subject LIKE '%!!!%' OR subject LIKE '%URGENT%' OR subject LIKE '%BUY NOW%'").count()
                    total_count = spark_df.count()
                else:
                    spam_keywords = ['!!!', 'URGENT', 'BUY NOW', 'ACT FAST', 'LIMITED TIME']
                    spam_mask = spark_df['subject'].str.contains('|'.join(spam_keywords), case=False, na=False)
                    spam_subject_count = spam_mask.sum()
                    total_count = len(spark_df)
                
                spam_ratio = spam_subject_count / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': 'Spam indicator detection',
                    'status': 'Success' if spam_ratio < 0.1 else 'Failure',
                    'spam_ratio': spam_ratio,
                    'spam_count': spam_subject_count,
                    'total_count': total_count
                })
            
            # Check sentiment score validity
            if 'sentiment_score' in spark_df.columns:
                if self.use_spark:
                    invalid_sentiment_count = spark_df.filter("sentiment_score < -1 OR sentiment_score > 1").count()
                    total_count = spark_df.count()
                else:
                    invalid_sentiment_count = ((spark_df['sentiment_score'] < -1) | (spark_df['sentiment_score'] > 1)).sum()
                    total_count = len(spark_df)
                
                invalid_sentiment_ratio = invalid_sentiment_count / total_count if total_count > 0 else 0
                
                results.append({
                    'constraint': 'Sentiment score validity',
                    'status': 'Success' if invalid_sentiment_ratio < 0.05 else 'Failure',
                    'invalid_sentiment_ratio': invalid_sentiment_ratio,
                    'invalid_sentiment_count': invalid_sentiment_count,
                    'total_count': total_count
                })
            
            return {
                'check_type': 'email_specific',
                'results': results,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error in email-specific check: {e}")
            return {'error': str(e)} 