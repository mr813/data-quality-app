import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from typing import Dict

def generate_sample_sales_data(rows: int = 1000) -> pd.DataFrame:
    """Generate sample sales data with various data quality issues"""
    
    # Generate base data
    np.random.seed(42)
    
    # Product categories
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
    
    # Generate dates
    start_date = datetime.now() - timedelta(days=365)
    dates = [start_date + timedelta(days=i) for i in range(rows)]
    
    # Generate data with some quality issues
    data = {
        'order_id': [f'ORD-{i:06d}' for i in range(1, rows + 1)],
        'customer_id': [f'CUST-{random.randint(1000, 9999)}' for _ in range(rows)],
        'product_name': [f'Product {i}' for i in range(1, rows + 1)],
        'category': [random.choice(categories) for _ in range(rows)],
        'quantity': np.random.randint(1, 10, rows),
        'unit_price': np.random.uniform(10, 500, rows),
        'order_date': [date.strftime('%Y-%m-%d') for date in dates],
        'customer_email': [f'customer{i}@example.com' for i in range(1, rows + 1)],
        'shipping_address': [f'Address {i}, City {i % 10}' for i in range(1, rows + 1)],
        'payment_method': [random.choice(['Credit Card', 'PayPal', 'Bank Transfer']) for _ in range(rows)],
        'order_status': [random.choice(['Completed', 'Pending', 'Cancelled']) for _ in range(rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Introduce data quality issues
    # 1. Missing values
    missing_indices = np.random.choice(rows, size=int(rows * 0.05), replace=False)
    df.loc[missing_indices, 'customer_email'] = None
    
    # 2. Duplicate values
    duplicate_indices = np.random.choice(rows, size=int(rows * 0.02), replace=False)
    df.loc[duplicate_indices, 'order_id'] = df.loc[duplicate_indices[0], 'order_id']
    
    # 3. Invalid values
    invalid_price_indices = np.random.choice(rows, size=int(rows * 0.03), replace=False)
    df.loc[invalid_price_indices, 'unit_price'] = -100
    
    # 4. Outliers
    outlier_indices = np.random.choice(rows, size=int(rows * 0.01), replace=False)
    df.loc[outlier_indices, 'quantity'] = 1000
    
    # 5. Invalid email format
    invalid_email_indices = np.random.choice(rows, size=int(rows * 0.02), replace=False)
    df.loc[invalid_email_indices, 'customer_email'] = 'invalid-email'
    
    return df

def generate_sample_customer_data(rows: int = 500) -> pd.DataFrame:
    """Generate sample customer data"""
    
    np.random.seed(42)
    
    # Generate data
    data = {
        'customer_id': [f'CUST-{i:04d}' for i in range(1, rows + 1)],
        'first_name': [f'First{i}' for i in range(1, rows + 1)],
        'last_name': [f'Last{i}' for i in range(1, rows + 1)],
        'email': [f'customer{i}@example.com' for i in range(1, rows + 1)],
        'phone': [f'+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}' for _ in range(rows)],
        'age': np.random.randint(18, 80, rows),
        'income': np.random.uniform(30000, 150000, rows),
        'registration_date': [(datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d') for _ in range(rows)],
        'is_active': [random.choice([True, False]) for _ in range(rows)],
        'preferred_category': [random.choice(['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']) for _ in range(rows)]
    }
    
    df = pd.DataFrame(data)
    
    # Introduce some data quality issues
    # Missing values
    missing_indices = np.random.choice(rows, size=int(rows * 0.08), replace=False)
    df.loc[missing_indices, 'phone'] = None
    
    # Invalid age
    invalid_age_indices = np.random.choice(rows, size=int(rows * 0.02), replace=False)
    df.loc[invalid_age_indices, 'age'] = 200
    
    # Invalid email
    invalid_email_indices = np.random.choice(rows, size=int(rows * 0.03), replace=False)
    df.loc[invalid_email_indices, 'email'] = 'invalid-email-format'
    
    return df

def generate_sample_product_data(rows: int = 200) -> pd.DataFrame:
    """Generate sample product data"""
    
    np.random.seed(42)
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
    brands = ['Brand A', 'Brand B', 'Brand C', 'Brand D', 'Brand E']
    
    data = {
        'product_id': [f'PROD-{i:04d}' for i in range(1, rows + 1)],
        'product_name': [f'Product {i}' for i in range(1, rows + 1)],
        'category': [random.choice(categories) for _ in range(rows)],
        'brand': [random.choice(brands) for _ in range(rows)],
        'price': np.random.uniform(10, 1000, rows),
        'stock_quantity': np.random.randint(0, 1000, rows),
        'weight_kg': np.random.uniform(0.1, 10, rows),
        'dimensions_cm': [f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}" for _ in range(rows)],
        'is_available': [random.choice([True, False]) for _ in range(rows)],
        'created_date': [datetime.now() - timedelta(days=random.randint(1, 365)) for _ in range(rows)],
        'rating': np.random.uniform(1, 5, rows)
    }
    
    df = pd.DataFrame(data)
    
    # Introduce data quality issues
    # Negative prices
    negative_price_indices = np.random.choice(rows, size=int(rows * 0.02), replace=False)
    df.loc[negative_price_indices, 'price'] = -50
    
    # Invalid ratings
    invalid_rating_indices = np.random.choice(rows, size=int(rows * 0.01), replace=False)
    df.loc[invalid_rating_indices, 'rating'] = 10
    
    # Missing stock information
    missing_stock_indices = np.random.choice(rows, size=int(rows * 0.05), replace=False)
    df.loc[missing_stock_indices, 'stock_quantity'] = None
    
    return df

def generate_sample_transaction_data(rows: int = 2000) -> pd.DataFrame:
    """Generate sample transaction data with anomalies"""
    
    np.random.seed(42)
    
    # Generate normal transactions
    normal_transactions = int(rows * 0.95)
    anomaly_transactions = rows - normal_transactions
    
    # Normal transaction amounts (most transactions)
    normal_amounts = np.random.normal(100, 30, normal_transactions)
    normal_amounts = np.clip(normal_amounts, 10, 500)  # Clip to reasonable range
    
    # Anomaly transaction amounts (very high or very low)
    anomaly_amounts = np.concatenate([
        np.random.uniform(1000, 5000, anomaly_transactions // 2),  # High amounts
        np.random.uniform(1, 5, anomaly_transactions // 2)  # Very low amounts
    ])
    
    # Combine normal and anomaly amounts
    all_amounts = np.concatenate([normal_amounts, anomaly_amounts])
    np.random.shuffle(all_amounts)
    
    # Generate transaction types
    transaction_types = ['Purchase', 'Refund', 'Transfer', 'Withdrawal', 'Deposit']
    
    data = {
        'transaction_id': [f'TXN-{i:06d}' for i in range(1, rows + 1)],
        'account_id': [f'ACC-{random.randint(1000, 9999)}' for _ in range(rows)],
        'transaction_type': [random.choice(transaction_types) for _ in range(rows)],
        'amount': all_amounts,
        'currency': ['USD'] * rows,
        'transaction_date': [(datetime.now() - timedelta(hours=random.randint(1, 8760))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(rows)],
        'merchant': [f'Merchant {random.randint(1, 100)}' for _ in range(rows)],
        'location': [f'Location {random.randint(1, 50)}' for _ in range(rows)],
        'status': [random.choice(['Completed', 'Pending', 'Failed']) for _ in range(rows)],
        'fraud_score': np.random.uniform(0, 1, rows)
    }
    
    df = pd.DataFrame(data)
    
    # Introduce additional data quality issues
    # Missing merchant information
    missing_merchant_indices = np.random.choice(rows, size=int(rows * 0.03), replace=False)
    df.loc[missing_merchant_indices, 'merchant'] = None
    
    # Invalid fraud scores
    invalid_fraud_indices = np.random.choice(rows, size=int(rows * 0.01), replace=False)
    df.loc[invalid_fraud_indices, 'fraud_score'] = 2.5
    
    # Duplicate transaction IDs
    duplicate_indices = np.random.choice(rows, size=int(rows * 0.01), replace=False)
    df.loc[duplicate_indices, 'transaction_id'] = df.loc[duplicate_indices[0], 'transaction_id']
    
    return df

def get_sample_datasets() -> Dict[str, pd.DataFrame]:
    """Get all sample datasets"""
    return {
        'sales_data': generate_sample_sales_data(1000),
        'customer_data': generate_sample_customer_data(500),
        'product_data': generate_sample_product_data(200),
        'transaction_data': generate_sample_transaction_data(2000)
    }

def get_data_quality_issues_summary(df: pd.DataFrame) -> Dict:
    """Get a summary of data quality issues in the dataset"""
    
    issues = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': {},
        'duplicate_rows': len(df[df.duplicated()]),
        'data_types': df.dtypes.to_dict(),
        'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
        'categorical_columns': df.select_dtypes(include=['object']).columns.tolist()
    }
    
    # Check missing values per column
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > 0:
            issues['missing_values'][col] = {
                'count': missing_count,
                'percentage': (missing_count / len(df)) * 100
            }
    
    return issues 