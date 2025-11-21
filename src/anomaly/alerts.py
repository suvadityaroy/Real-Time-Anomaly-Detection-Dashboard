"""Alert notification system.

Handles broadcasting anomaly alerts via multiple channels:
- Redis Pub/Sub for real-time dashboard updates
- Email notifications with rate limiting
- Extensible for additional channels (Slack, PagerDuty, etc.)
"""

import logging
import time
import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional, List
from queue import Queue, Full
import json

import redis
from redis.exceptions import RedisError


logger = logging.getLogger(__name__)


class EmailSender:
    """Email notification sender with rate limiting.
    
    Sends email alerts with configurable rate limiting using a token bucket
    algorithm to prevent email flooding.
    """
    
    def __init__(
        self,
        smtp_host: Optional[str] = None,
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        from_addr: Optional[str] = None,
        to_addrs: Optional[List[str]] = None,
        rate_limit_seconds: float = 60.0,
        enabled: bool = False
    ) -> None:
        """Initialize email sender.
        
        Args:
            smtp_host: SMTP server hostname
            smtp_port: SMTP server port
            smtp_user: SMTP username for authentication
            smtp_password: SMTP password
            from_addr: From email address
            to_addrs: List of recipient email addresses
            rate_limit_seconds: Minimum seconds between emails
            enabled: Whether email sending is enabled
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_addr = from_addr or smtp_user
        self.to_addrs = to_addrs or []
        self.rate_limit_seconds = rate_limit_seconds
        self.enabled = enabled and smtp_host is not None
        
        # Rate limiting state
        self.last_sent_time = 0.0
        self.lock = threading.Lock()
        
        # Email queue for non-blocking sends
        self.email_queue: Queue[Dict[str, Any]] = Queue(maxsize=100)
        self.sender_thread: Optional[threading.Thread] = None
        self.running = False
        
        if self.enabled:
            logger.info(
                f"Email sender enabled: {self.smtp_host}:{self.smtp_port}, "
                f"from={self.from_addr}, to={len(self.to_addrs)} recipients"
            )
        else:
            logger.info("Email sender disabled (no SMTP configuration)")
    
    def start(self) -> None:
        """Start the email sender background thread."""
        if not self.enabled:
            return
        
        if self.running:
            logger.warning("Email sender already running")
            return
        
        self.running = True
        self.sender_thread = threading.Thread(
            target=self._send_loop,
            daemon=True
        )
        self.sender_thread.start()
        logger.info("Email sender background thread started")
    
    def stop(self) -> None:
        """Stop the email sender background thread."""
        if not self.running:
            return
        
        logger.info("Stopping email sender...")
        self.running = False
        
        if self.sender_thread and self.sender_thread.is_alive():
            self.sender_thread.join(timeout=5.0)
        
        logger.info("Email sender stopped")
    
    def send_anomaly_email(self, anomaly: Dict[str, Any]) -> bool:
        """Queue an anomaly alert email for sending.
        
        Args:
            anomaly: Anomaly dictionary with detection details
            
        Returns:
            bool: True if queued successfully, False otherwise
        """
        if not self.enabled:
            logger.debug("Email sending disabled, skipping")
            return False
        
        # Check rate limit
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_sent_time
            
            if elapsed < self.rate_limit_seconds:
                logger.debug(
                    f"Rate limit: {elapsed:.1f}s < {self.rate_limit_seconds}s, "
                    f"skipping email"
                )
                return False
            
            self.last_sent_time = current_time
        
        # Queue email for sending
        try:
            self.email_queue.put_nowait(anomaly)
            logger.debug("Anomaly email queued")
            return True
        except Full:
            logger.error("Email queue full, dropping email")
            return False
    
    def _send_loop(self) -> None:
        """Background loop for sending queued emails."""
        while self.running:
            try:
                # Wait for email with timeout
                anomaly = self.email_queue.get(timeout=1.0)
                self._send_email(anomaly)
            except Exception:
                # Queue empty or timeout
                continue
    
    def _send_email(self, anomaly: Dict[str, Any]) -> None:
        """Actually send an email (or log if in dummy mode).
        
        Args:
            anomaly: Anomaly dictionary with detection details
        """
        if not self.to_addrs:
            logger.warning("No email recipients configured")
            return
        
        # Format email content
        subject = f"Anomaly Alert: {anomaly.get('metric', 'Unknown')}"
        
        body = f"""
Anomaly Detected!

Metric: {anomaly.get('metric')}
Value: {anomaly.get('value', 0):.2f}
Mean: {anomaly.get('mean', 0):.2f}
Std Dev: {anomaly.get('stddev', 0):.2f}
Z-Score: {anomaly.get('z_score', 0):.2f}
Threshold: {anomaly.get('threshold', 0):.2f}

Detection Method: {anomaly.get('detection_method', 'unknown')}
Timestamp: {anomaly.get('timestamp', 0)}
Detected At: {anomaly.get('detected_at', 0)}

This is an automated alert from the Real-Time Anomaly Detection System.
"""
        
        try:
            # In production, send via SMTP
            if self.smtp_host and self.smtp_host not in ['localhost', '127.0.0.1', 'dummy']:
                self._send_smtp(subject, body)
            else:
                # Dummy mode: just log
                logger.info(
                    f"[DUMMY EMAIL] To: {self.to_addrs}\n"
                    f"Subject: {subject}\n"
                    f"Body:\n{body}"
                )
        except Exception as e:
            logger.error(f"Failed to send email: {e}", exc_info=True)
    
    def _send_smtp(self, subject: str, body: str) -> None:
        """Send email via SMTP server.
        
        Args:
            subject: Email subject
            body: Email body text
        """
        msg = MIMEMultipart()
        msg['From'] = self.from_addr
        msg['To'] = ', '.join(self.to_addrs)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            if self.smtp_user and self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            server.send_message(msg)
        
        logger.info(f"Email sent successfully to {len(self.to_addrs)} recipients")


class Alerts:
    """Alert broadcasting system.
    
    Coordinates multiple notification channels for anomaly alerts:
    - Redis Pub/Sub for real-time WebSocket updates
    - Email notifications with rate limiting
    """
    
    def __init__(
        self,
        redis_url: str,
        pubsub_channel: str = "anomalies",
        email_sender: Optional[EmailSender] = None
    ) -> None:
        """Initialize alerts system.
        
        Args:
            redis_url: Redis connection URL for Pub/Sub
            pubsub_channel: Redis channel name for publishing alerts
            email_sender: Optional email sender instance
        """
        self.pubsub_channel = pubsub_channel
        self.email_sender = email_sender
        
        # Connect to Redis for Pub/Sub
        try:
            self.redis_client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_timeout=5
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis for alerts: {redis_url}")
        except (RedisError, Exception) as e:
            logger.error(f"Failed to connect to Redis for alerts: {e}")
            self.redis_client = None  # type: ignore
        
        # Statistics
        self.total_broadcasted = 0
        self.total_emails_sent = 0
        self.total_errors = 0
        
        logger.info(
            f"Alerts system initialized: channel={pubsub_channel}, "
            f"email={'enabled' if email_sender and email_sender.enabled else 'disabled'}"
        )
    
    def broadcast(self, anomaly: Dict[str, Any]) -> bool:
        """Broadcast an anomaly alert to all channels.
        
        Args:
            anomaly: Anomaly dictionary with detection details
            
        Returns:
            bool: True if at least one channel succeeded
        """
        success = False
        
        # Publish to Redis Pub/Sub
        if self.redis_client:
            try:
                message = json.dumps(anomaly)
                subscribers = self.redis_client.publish(self.pubsub_channel, message)
                logger.info(
                    f"Published anomaly to Redis: {anomaly.get('metric')}, "
                    f"subscribers={subscribers}"
                )
                self.total_broadcasted += 1
                success = True
            except (RedisError, Exception) as e:
                logger.error(f"Failed to publish to Redis: {e}")
                self.total_errors += 1
        
        # Send email notification
        if self.email_sender:
            try:
                if self.email_sender.send_anomaly_email(anomaly):
                    self.total_emails_sent += 1
                    success = True
            except Exception as e:
                logger.error(f"Failed to send email alert: {e}")
                self.total_errors += 1
        
        return success
    
    def start(self) -> None:
        """Start alert channels (background threads)."""
        if self.email_sender:
            self.email_sender.start()
        logger.info("Alert channels started")
    
    def stop(self) -> None:
        """Stop alert channels."""
        if self.email_sender:
            self.email_sender.stop()
        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
        logger.info("Alert channels stopped")
    
    def get_stats(self) -> Dict[str, int]:
        """Get alert statistics.
        
        Returns:
            Dict: Statistics including broadcast and email counts
        """
        return {
            'total_broadcasted': self.total_broadcasted,
            'total_emails_sent': self.total_emails_sent,
            'total_errors': self.total_errors
        }
    
    def __repr__(self) -> str:
        """Return string representation of alerts system."""
        return (
            f"Alerts(channel={self.pubsub_channel}, "
            f"broadcasted={self.total_broadcasted}, "
            f"emails={self.total_emails_sent})"
        )
