"""Production-ready email sender with SMTP support.

Provides email notification functionality with proper error handling
and configuration via environment variables.
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional

from anomaly.config import get_settings


logger = logging.getLogger(__name__)


class EmailSender:
    """Send emails via SMTP with configuration from environment."""
    
    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        from_email: Optional[str] = None
    ) -> None:
        """Initialize email sender with SMTP configuration.
        
        If parameters are not provided, they will be loaded from
        environment variables via Settings.
        
        Args:
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            smtp_user: SMTP authentication username
            smtp_password: SMTP authentication password
            from_email: Sender email address
        """
        settings = get_settings()
        
        self.smtp_host = smtp_host or settings.smtp_host
        self.smtp_port = smtp_port or settings.smtp_port
        self.smtp_user = smtp_user or settings.smtp_user
        self.smtp_password = smtp_password or settings.smtp_password
        self.from_email = from_email or settings.email_from
        
        # Validate configuration
        if not all([self.smtp_host, self.smtp_user, self.smtp_password, self.from_email]):
            logger.warning(
                "Email sender not fully configured. "
                "Set SMTP_HOST, SMTP_USER, SMTP_PASSWORD, and EMAIL_FROM "
                "in environment variables."
            )
            self.is_configured = False
        else:
            self.is_configured = True
            logger.info(f"Email sender configured with host: {self.smtp_host}:{self.smtp_port}")
    
    def send_email(
        self,
        to_addresses: List[str],
        subject: str,
        body: str,
        is_html: bool = False
    ) -> bool:
        """Send an email to one or more recipients.
        
        Args:
            to_addresses: List of recipient email addresses
            subject: Email subject line
            body: Email body content (plain text or HTML)
            is_html: Whether the body is HTML (default: plain text)
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        if not self.is_configured:
            logger.warning("Email sender not configured. Skipping email send.")
            return False
        
        if not to_addresses:
            logger.warning("No recipient addresses provided. Skipping email send.")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.from_email
            msg['To'] = ', '.join(to_addresses)
            msg['Subject'] = subject
            
            # Attach body
            mime_type = 'html' if is_html else 'plain'
            msg.attach(MIMEText(body, mime_type, 'utf-8'))
            
            # Connect to SMTP server and send
            logger.info(f"Connecting to SMTP server {self.smtp_host}:{self.smtp_port}")
            
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as server:
                # Enable TLS if port is 587
                if self.smtp_port == 587:
                    server.starttls()
                    logger.debug("TLS enabled")
                
                # Login
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                    logger.debug("SMTP authentication successful")
                
                # Send email
                server.send_message(msg)
                logger.info(f"Email sent successfully to {len(to_addresses)} recipient(s)")
                return True
        
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP authentication failed: {e}")
            return False
        
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error occurred: {e}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error sending email: {e}")
            return False
    
    def send_anomaly_alert(
        self,
        metric: str,
        value: float,
        z_score: float,
        mean: float,
        stddev: float,
        timestamp: float,
        to_addresses: List[str]
    ) -> bool:
        """Send an anomaly alert email.
        
        Args:
            metric: Metric name that triggered the anomaly
            value: Observed value
            z_score: Z-score of the anomaly
            mean: Historical mean value
            stddev: Historical standard deviation
            timestamp: Timestamp of the anomaly
            to_addresses: List of recipient email addresses
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        from datetime import datetime
        
        # Format timestamp
        dt = datetime.fromtimestamp(timestamp)
        time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Create subject
        subject = f"🚨 Anomaly Detected: {metric}"
        
        # Create HTML body
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background: #dc2626; color: white; padding: 20px; border-radius: 5px; }}
                .content {{ background: #f9fafb; padding: 20px; margin-top: 20px; border-radius: 5px; }}
                .metric {{ font-size: 24px; font-weight: bold; color: #667eea; }}
                .details {{ margin-top: 20px; }}
                .detail-row {{ display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #e5e7eb; }}
                .label {{ font-weight: 600; color: #666; }}
                .value {{ font-weight: bold; color: #333; }}
                .highlight {{ color: #dc2626; }}
                .footer {{ margin-top: 20px; font-size: 12px; color: #666; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚨 Anomaly Detected</h1>
                    <p>An anomaly has been detected in your metrics</p>
                </div>
                
                <div class="content">
                    <div class="metric">{metric}</div>
                    
                    <div class="details">
                        <div class="detail-row">
                            <span class="label">Detected At:</span>
                            <span class="value">{time_str}</span>
                        </div>
                        <div class="detail-row">
                            <span class="label">Observed Value:</span>
                            <span class="value highlight">{value:.2f}</span>
                        </div>
                        <div class="detail-row">
                            <span class="label">Z-Score:</span>
                            <span class="value highlight">{z_score:.2f}</span>
                        </div>
                        <div class="detail-row">
                            <span class="label">Historical Mean:</span>
                            <span class="value">{mean:.2f}</span>
                        </div>
                        <div class="detail-row">
                            <span class="label">Standard Deviation:</span>
                            <span class="value">{stddev:.2f}</span>
                        </div>
                        <div class="detail-row">
                            <span class="label">Deviation:</span>
                            <span class="value highlight">{abs(value - mean):.2f} ({abs(z_score):.1f}σ)</span>
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>Real-Time Anomaly Detection System</p>
                    <p>This is an automated alert. Please investigate the metric behavior.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return self.send_email(
            to_addresses=to_addresses,
            subject=subject,
            body=body,
            is_html=True
        )


# Convenience function for sending alerts
def send_anomaly_alert(
    metric: str,
    value: float,
    z_score: float,
    mean: float,
    stddev: float,
    timestamp: float
) -> bool:
    """Send an anomaly alert email using configuration from environment.
    
    Args:
        metric: Metric name that triggered the anomaly
        value: Observed value
        z_score: Z-score of the anomaly
        mean: Historical mean value
        stddev: Historical standard deviation
        timestamp: Timestamp of the anomaly
        
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    settings = get_settings()
    
    # Check if email is enabled
    if not settings.email_enabled:
        logger.debug("Email notifications are disabled")
        return False
    
    # Parse recipient addresses
    to_addresses = [
        addr.strip() 
        for addr in settings.email_to.split(',') 
        if addr.strip()
    ]
    
    if not to_addresses:
        logger.warning("No recipient email addresses configured")
        return False
    
    # Create sender and send alert
    sender = EmailSender()
    return sender.send_anomaly_alert(
        metric=metric,
        value=value,
        z_score=z_score,
        mean=mean,
        stddev=stddev,
        timestamp=timestamp,
        to_addresses=to_addresses
    )


if __name__ == "__main__":
    # Test email sending
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Send test email
    import time
    success = send_anomaly_alert(
        metric="cpu_usage",
        value=95.5,
        z_score=4.2,
        mean=45.3,
        stddev=11.9,
        timestamp=time.time()
    )
    
    if success:
        print("✅ Test email sent successfully")
        sys.exit(0)
    else:
        print("❌ Failed to send test email")
        sys.exit(1)
