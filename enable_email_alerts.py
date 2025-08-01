#!/usr/bin/env python3
"""
Script to enable email alerts in the Data Quality App
"""

from alert_system import AlertSystem

def enable_email_alerts():
    """Enable email alerts with sample configuration"""
    
    # Sample email configuration (replace with your actual settings)
    email_config = {
        'email': {
            'enabled': True,
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'username': 'your-email@gmail.com',  # Replace with your email
            'password': 'your-app-password',     # Replace with your password
            'recipients': ['admin@company.com', 'manager@company.com']  # Replace with actual recipients
        },
        'webhook': {
            'enabled': False,
            'url': '',
            'headers': {}
        },
        'thresholds': {
            'quality_score_min': 80.0,
            'anomaly_percentage_max': 5.0,
            'completeness_min': 95.0,
            'uniqueness_min': 90.0
        },
        'schedule': {
            'enabled': False,
            'interval_minutes': 60
        }
    }
    
    # Create alert system with email enabled
    alert_system = AlertSystem(email_config)
    
    # Test the configuration
    print("📧 Email Alert Configuration:")
    print(f"   Enabled: {alert_system.config['email']['enabled']}")
    print(f"   SMTP Server: {alert_system.config['email']['smtp_server']}:{alert_system.config['email']['smtp_port']}")
    print(f"   Username: {alert_system.config['email']['username']}")
    print(f"   Recipients: {alert_system.config['email']['recipients']}")
    
    # Test alert
    test_alert = {
        'type': 'test_alert',
        'severity': 'low',
        'message': 'This is a test alert from the data quality platform',
        'timestamp': '2025-07-31T19:40:54'
    }
    
    print("\n🧪 Testing email alert...")
    alert_system.process_alerts([test_alert])
    
    return alert_system

if __name__ == "__main__":
    print("🚀 Enabling Email Alerts...")
    alert_system = enable_email_alerts()
    print("✅ Email alerts configured!")
    print("\n📝 To use this in your app:")
    print("1. Replace the email configuration with your actual settings")
    print("2. Update the app.py to use this configuration")
    print("3. Test with real SMTP credentials") 