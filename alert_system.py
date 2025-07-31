import smtplib
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import schedule
import time
import threading

class AlertSystem:
    """
    Alert System for data quality monitoring and anomaly detection
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._get_default_config()
        self.logger = self._setup_logging()
        self.alert_history = []
        self.alert_rules = {}
        
    def _get_default_config(self) -> Dict:
        """Get default configuration for alert system"""
        return {
            'email': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'username': '',
                'password': '',
                'recipients': []
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
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def add_alert_rule(self, rule_name: str, condition: Dict, actions: List[str]):
        """Add a new alert rule"""
        self.alert_rules[rule_name] = {
            'condition': condition,
            'actions': actions,
            'created_at': datetime.now().isoformat()
        }
        self.logger.info(f"Added alert rule: {rule_name}")
    
    def check_quality_thresholds(self, quality_results: Dict) -> List[Dict]:
        """Check quality results against thresholds and generate alerts"""
        alerts = []
        
        try:
            self.logger.info("🔍 Checking quality thresholds...")
            
            # Check overall quality score
            quality_score = quality_results.get('quality_score', 0)
            threshold = self.config['thresholds']['quality_score_min']
            self.logger.info(f"📊 Quality score: {quality_score:.2f}% (threshold: {threshold}%)")
            
            if quality_score < threshold:
                self.logger.warning(f"🚨 QUALITY SCORE ALERT: {quality_score:.2f}% < {threshold}%")
                alerts.append({
                    'type': 'quality_score_low',
                    'severity': 'high',
                    'message': f'Data quality score ({quality_score:.2f}%) is below threshold ({threshold}%)',
                    'timestamp': datetime.now().isoformat(),
                    'value': quality_score,
                    'threshold': threshold
                })
            else:
                self.logger.info(f"✅ Quality score acceptable: {quality_score:.2f}% >= {threshold}%")
            
            # Check completeness
            if 'quality_checks' in quality_results and 'completeness' in quality_results['quality_checks']:
                completeness_result = quality_results['quality_checks']['completeness']
                if 'results' in completeness_result:
                    for result in completeness_result['results']:
                        if result.status == 'Failure':
                            alerts.append({
                                'type': 'completeness_failure',
                                'severity': 'medium',
                                'message': f'Completeness check failed for column: {result.constraint}',
                                'timestamp': datetime.now().isoformat(),
                                'column': result.constraint
                            })
            
            # Check uniqueness
            if 'quality_checks' in quality_results and 'uniqueness' in quality_results['quality_checks']:
                uniqueness_result = quality_results['quality_checks']['uniqueness']
                if 'results' in uniqueness_result:
                    for result in uniqueness_result['results']:
                        if result.status == 'Failure':
                            alerts.append({
                                'type': 'uniqueness_failure',
                                'severity': 'medium',
                                'message': f'Uniqueness check failed for column: {result.constraint}',
                                'timestamp': datetime.now().isoformat(),
                                'column': result.constraint
                            })
            
        except Exception as e:
            self.logger.error(f"Error checking quality thresholds: {e}")
            alerts.append({
                'type': 'system_error',
                'severity': 'high',
                'message': f'Error in quality threshold checking: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })
        
        return alerts
    
    def check_anomaly_thresholds(self, anomaly_results: Dict) -> List[Dict]:
        """Check anomaly results against thresholds and generate alerts"""
        alerts = []
        
        try:
            self.logger.info("🔍 Checking anomaly thresholds...")
            threshold = self.config['thresholds']['anomaly_percentage_max']
            self.logger.info(f"📊 Anomaly threshold: {threshold}%")
            
            if 'anomaly_detection' in anomaly_results:
                for column, anomaly_data in anomaly_results['anomaly_detection'].items():
                    anomaly_percentage = anomaly_data.get('anomaly_percentage', 0)
                    anomaly_count = anomaly_data.get('anomaly_count', 0)
                    
                    self.logger.info(f"📊 Column '{column}': {anomaly_percentage:.2f}% anomalies ({anomaly_count} total)")
                    
                    if anomaly_percentage > threshold:
                        self.logger.warning(f"🚨 ANOMALY ALERT: {anomaly_percentage:.2f}% > {threshold}% in column '{column}'")
                        alerts.append({
                            'type': 'anomaly_detected',
                            'severity': 'high',
                            'message': f'High anomaly percentage ({anomaly_percentage:.2f}%) detected in column: {column}',
                            'timestamp': datetime.now().isoformat(),
                            'column': column,
                            'anomaly_percentage': anomaly_percentage,
                            'anomaly_count': anomaly_count
                        })
                    else:
                        self.logger.info(f"✅ Anomaly level acceptable in '{column}': {anomaly_percentage:.2f}% <= {threshold}%")
            else:
                self.logger.info("📊 No anomaly detection results found")
            
        except Exception as e:
            self.logger.error(f"Error checking anomaly thresholds: {e}")
            alerts.append({
                'type': 'system_error',
                'severity': 'high',
                'message': f'Error in anomaly threshold checking: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })
        
        return alerts
    
    def send_email_alert(self, alert: Dict):
        """Send email alert with detailed logging"""
        try:
            # Log alert trigger
            self.logger.info(f"🚨 EMAIL ALERT TRIGGERED: {alert['type']} - {alert['message']}")
            self.logger.info(f"📧 Email configuration: enabled={self.config['email']['enabled']}")
            
            if not self.config['email']['enabled']:
                self.logger.warning("❌ Email alerts are disabled in configuration")
                return
            
            # Log email composition
            self.logger.info(f"📝 Composing email alert for: {alert['type']}")
            self.logger.info(f"📤 From: {self.config['email']['username']}")
            self.logger.info(f"📥 To: {', '.join(self.config['email']['recipients'])}")
            self.logger.info(f"📋 Subject: Data Quality Alert: {alert['type']}")
            
            msg = MIMEMultipart()
            msg['From'] = self.config['email']['username']
            msg['To'] = ', '.join(self.config['email']['recipients'])
            msg['Subject'] = f"Data Quality Alert: {alert['type']}"
            
            body = f"""
            Data Quality Alert
            
            Type: {alert['type']}
            Severity: {alert['severity']}
            Message: {alert['message']}
            Timestamp: {alert['timestamp']}
            
            Additional Details:
            {json.dumps(alert, indent=2)}
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Log SMTP connection attempt
            self.logger.info(f"🔌 Connecting to SMTP server: {self.config['email']['smtp_server']}:{self.config['email']['smtp_port']}")
            
            server = smtplib.SMTP(self.config['email']['smtp_server'], self.config['email']['smtp_port'])
            self.logger.info("✅ SMTP connection established")
            
            # Log TLS handshake
            self.logger.info("🔒 Starting TLS encryption")
            server.starttls()
            self.logger.info("✅ TLS encryption enabled")
            
            # Log authentication
            self.logger.info(f"🔐 Authenticating with username: {self.config['email']['username']}")
            server.login(self.config['email']['username'], self.config['email']['password'])
            self.logger.info("✅ SMTP authentication successful")
            
            # Log email sending
            text = msg.as_string()
            self.logger.info(f"📤 Sending email to {len(self.config['email']['recipients'])} recipients")
            
            server.sendmail(self.config['email']['username'], self.config['email']['recipients'], text)
            self.logger.info("✅ Email successfully sent to SMTP server")
            
            # Log connection cleanup
            server.quit()
            self.logger.info("🔌 SMTP connection closed")
            
            # Log successful delivery
            self.logger.info(f"🎉 EMAIL ALERT DELIVERED: {alert['type']} - Sent to {len(self.config['email']['recipients'])} recipients")
            
        except smtplib.SMTPAuthenticationError as e:
            self.logger.error(f"❌ SMTP AUTHENTICATION FAILED: {e}")
            self.logger.error(f"🔐 Check username/password for: {self.config['email']['username']}")
        except smtplib.SMTPConnectError as e:
            self.logger.error(f"❌ SMTP CONNECTION FAILED: {e}")
            self.logger.error(f"🔌 Check server: {self.config['email']['smtp_server']}:{self.config['email']['smtp_port']}")
        except smtplib.SMTPRecipientsRefused as e:
            self.logger.error(f"❌ SMTP RECIPIENT REFUSED: {e}")
            self.logger.error(f"📥 Check recipient emails: {self.config['email']['recipients']}")
        except smtplib.SMTPServerDisconnected as e:
            self.logger.error(f"❌ SMTP SERVER DISCONNECTED: {e}")
        except smtplib.SMTPException as e:
            self.logger.error(f"❌ SMTP ERROR: {e}")
        except Exception as e:
            self.logger.error(f"❌ UNEXPECTED ERROR sending email alert: {e}")
            self.logger.error(f"📧 Alert type: {alert['type']}")
            self.logger.error(f"📧 Alert message: {alert['message']}")
    
    def send_webhook_alert(self, alert: Dict):
        """Send webhook alert"""
        try:
            if not self.config['webhook']['enabled']:
                return
            
            import requests
            
            response = requests.post(
                self.config['webhook']['url'],
                json=alert,
                headers=self.config['webhook']['headers']
            )
            
            if response.status_code == 200:
                self.logger.info(f"Webhook alert sent for: {alert['type']}")
            else:
                self.logger.error(f"Webhook alert failed with status: {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"Error sending webhook alert: {e}")
    
    def process_alerts(self, alerts: List[Dict]):
        """Process and send alerts with detailed logging"""
        if not alerts:
            self.logger.info("✅ No alerts to process")
            return
        
        self.logger.info(f"🚨 PROCESSING {len(alerts)} ALERTS")
        
        for i, alert in enumerate(alerts, 1):
            self.logger.info(f"📋 Processing alert {i}/{len(alerts)}: {alert['type']}")
            self.logger.info(f"📊 Alert details: Severity={alert['severity']}, Message={alert['message']}")
            
            # Store alert in history
            self.alert_history.append(alert)
            self.logger.info(f"💾 Alert stored in history (total: {len(self.alert_history)})")
            
            # Send email alerts
            if self.config['email']['enabled']:
                self.logger.info(f"📧 Sending email alert for: {alert['type']}")
                self.send_email_alert(alert)
            else:
                self.logger.info(f"📧 Email alerts disabled - skipping email for: {alert['type']}")
            
            # Send webhook alerts
            if self.config['webhook']['enabled']:
                self.logger.info(f"🔗 Sending webhook alert for: {alert['type']}")
                self.send_webhook_alert(alert)
            else:
                self.logger.info(f"🔗 Webhook alerts disabled - skipping webhook for: {alert['type']}")
            
            # Log alert completion
            self.logger.warning(f"✅ Alert processed: {alert['type']} - {alert['message']}")
        
        self.logger.info(f"🎉 All {len(alerts)} alerts processed successfully")
    
    def get_alert_summary(self, hours: int = 24) -> Dict:
        """Get alert summary for the specified time period"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            
            recent_alerts = [
                alert for alert in self.alert_history
                if datetime.fromisoformat(alert['timestamp']) > cutoff_time
            ]
            
            alert_counts = {}
            severity_counts = {}
            
            for alert in recent_alerts:
                alert_type = alert['type']
                severity = alert['severity']
                
                alert_counts[alert_type] = alert_counts.get(alert_type, 0) + 1
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            return {
                'total_alerts': len(recent_alerts),
                'alert_counts': alert_counts,
                'severity_counts': severity_counts,
                'time_period_hours': hours,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting alert summary: {e}")
            return {'error': str(e)}
    
    def start_monitoring(self, quality_engine, data_source_func, interval_minutes: int = 60):
        """Start continuous monitoring"""
        def monitoring_job():
            try:
                self.logger.info("🔄 Starting monitoring cycle...")
                
                # Get data from source
                self.logger.info("📊 Fetching data from source...")
                df = data_source_func()
                self.logger.info(f"📊 Data loaded: {df.shape[0]} rows, {df.shape[1]} columns")
                
                # Run quality checks
                self.logger.info("🔍 Running comprehensive quality checks...")
                quality_results = quality_engine.run_comprehensive_quality_check(df)
                quality_score = quality_engine.calculate_quality_score(quality_results)
                quality_results['quality_score'] = quality_score
                self.logger.info(f"📊 Quality score calculated: {quality_score:.2f}%")
                
                # Run anomaly detection
                self.logger.info("🔍 Running anomaly detection...")
                anomaly_results = quality_engine.detect_anomalies(df)
                self.logger.info("📊 Anomaly detection completed")
                
                # Check for alerts
                self.logger.info("🚨 Checking quality thresholds for alerts...")
                quality_alerts = self.check_quality_thresholds(quality_results)
                self.logger.info(f"📊 Quality alerts found: {len(quality_alerts)}")
                
                self.logger.info("🚨 Checking anomaly thresholds for alerts...")
                anomaly_alerts = self.check_anomaly_thresholds(anomaly_results)
                self.logger.info(f"📊 Anomaly alerts found: {len(anomaly_alerts)}")
                
                all_alerts = quality_alerts + anomaly_alerts
                
                if all_alerts:
                    self.logger.warning(f"🚨 ALERTS TRIGGERED: {len(all_alerts)} total alerts")
                    self.process_alerts(all_alerts)
                else:
                    self.logger.info("✅ No alerts triggered - all thresholds within acceptable ranges")
                
                self.logger.info(f"✅ Monitoring cycle completed. Quality score: {quality_score:.2f}%")
                
            except Exception as e:
                self.logger.error(f"❌ Error in monitoring job: {e}")
                self.logger.error(f"📊 Monitoring cycle failed - will retry on next interval")
        
        # Schedule the monitoring job
        schedule.every(interval_minutes).minutes.do(monitoring_job)
        
        # Run the job immediately
        monitoring_job()
        
        # Start the scheduler in a separate thread
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(1)
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        
        self.logger.info(f"Monitoring started with {interval_minutes} minute intervals")
    
    def update_config(self, new_config: Dict):
        """Update alert system configuration"""
        self.config.update(new_config)
        self.logger.info("Alert system configuration updated")
    
    def get_config(self) -> Dict:
        """Get current configuration"""
        return self.config.copy()
    
    def clear_alert_history(self):
        """Clear alert history"""
        self.alert_history.clear()
        self.logger.info("Alert history cleared") 