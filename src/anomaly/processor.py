"""Event processing pipeline.

Consumes raw events from persistence, computes sliding window statistics,
detects anomalies, and publishes alerts. Handles backpressure and graceful
shutdown.
"""

import logging
import signal
import sys
import time
import uuid
import threading
from typing import Optional, Dict, Any

from pydantic import ValidationError

from anomaly.config import get_settings
from anomaly.models import Event
from anomaly.persistence import Persistence, RedisPersistence
from anomaly.window import MultiMetricWindow
from anomaly.detector import ZScoreDetector
from anomaly.alerts import Alerts, EmailSender


logger = logging.getLogger(__name__)


class Processor:
    """Event processing pipeline.
    
    Pulls raw events from persistence, maintains sliding windows per metric,
    detects anomalies, and broadcasts alerts. Supports graceful shutdown
    and backpressure handling.
    """
    
    def __init__(
        self,
        persistence: Persistence,
        alerts: Alerts,
        detector: Optional[ZScoreDetector] = None,
        window_seconds: float = 300.0,
        poll_timeout: float = 1.0,
        sleep_on_empty: float = 0.1
    ) -> None:
        """Initialize processor.
        
        Args:
            persistence: Persistence backend for reading events and storing results
            alerts: Alerts system for broadcasting anomalies
            detector: Anomaly detector (defaults to ZScoreDetector)
            window_seconds: Sliding window duration in seconds
            poll_timeout: Timeout for polling events from persistence
            sleep_on_empty: Sleep duration when queue is empty (prevents busy-spin)
        """
        self.persistence = persistence
        self.alerts = alerts
        self.detector = detector or ZScoreDetector(threshold=3.0, min_count=5)
        self.window_seconds = window_seconds
        self.poll_timeout = poll_timeout
        self.sleep_on_empty = sleep_on_empty
        
        # Multi-metric window manager
        self.windows = MultiMetricWindow(window_seconds)
        
        # Control state
        self.running = False
        self.paused = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        
        # Statistics
        self.total_processed = 0
        self.total_anomalies = 0
        self.total_errors = 0
        self.processing_times: list[float] = []
        
        logger.info(
            f"Initialized Processor: window={window_seconds}s, "
            f"detector={self.detector}"
        )
    
    def process_event(self, event: Event) -> Optional[Dict[str, Any]]:
        """Process a single event.
        
        Args:
            event: Event to process
            
        Returns:
            Optional[Dict]: Anomaly details if detected, None otherwise
        """
        start_time = time.time()
        
        try:
            # Get timestamp as float
            timestamp = event.get_timestamp_float()
            
            # Get or create window for this metric
            window = self.windows.get_window(event.metric)
            
            # Append to window
            window.append(timestamp, event.value)
            
            # Detect anomaly
            anomaly = self.detector.detect(
                metric=event.metric,
                window=window,
                value=event.value,
                timestamp=timestamp
            )
            
            if anomaly:
                # Generate unique anomaly ID
                anomaly_id = f"anomaly-{uuid.uuid4().hex[:16]}"
                anomaly['anomaly_id'] = anomaly_id
                anomaly['event_meta'] = event.meta
                
                # Save to persistence
                self.persistence.save_processed(anomaly_id, anomaly)
                
                # Broadcast alert
                self.alerts.broadcast(anomaly)
                
                self.total_anomalies += 1
                logger.info(f"Anomaly processed and saved: {anomaly_id}")
                
                return anomaly
            
            # Track processing time
            elapsed = time.time() - start_time
            self.processing_times.append(elapsed)
            
            # Keep only last 1000 processing times
            if len(self.processing_times) > 1000:
                self.processing_times = self.processing_times[-1000:]
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            self.total_errors += 1
            return None
    
    def _process_loop(self) -> None:
        """Main processing loop.
        
        Continuously polls for events, processes them, and handles
        backpressure with sleep on empty queue.
        """
        logger.info("Processing loop started")
        consecutive_empty = 0
        
        while self.running:
            # Check if paused
            if self.paused:
                time.sleep(0.5)
                continue
            
            try:
                # Poll for raw event
                event_json = self.persistence.pop_raw_event(
                    timeout=int(self.poll_timeout)
                )
                
                if event_json is None:
                    # Queue empty - backpressure handling
                    consecutive_empty += 1
                    
                    if consecutive_empty % 10 == 0:
                        logger.debug(
                            f"Queue empty for {consecutive_empty} iterations, "
                            f"sleeping {self.sleep_on_empty}s"
                        )
                    
                    time.sleep(self.sleep_on_empty)
                    continue
                
                # Reset empty counter
                consecutive_empty = 0
                
                # Parse event
                try:
                    event = Event.parse_raw(event_json)
                except ValidationError as e:
                    logger.error(f"Invalid event JSON: {e}")
                    self.total_errors += 1
                    continue
                
                # Process event
                self.process_event(event)
                self.total_processed += 1
                
                # Log statistics periodically
                if self.total_processed % 100 == 0:
                    stats = self.get_stats()
                    logger.info(
                        f"Processor stats: processed={stats['total_processed']}, "
                        f"anomalies={stats['total_anomalies']}, "
                        f"errors={stats['total_errors']}, "
                        f"avg_time={stats['avg_processing_time_ms']:.2f}ms"
                    )
                
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                self.total_errors += 1
                time.sleep(1.0)  # Brief pause on error
        
        logger.info("Processing loop stopped")
    
    def start(self, loop_forever: bool = True) -> None:
        """Start the processor.
        
        Args:
            loop_forever: If True, runs in current thread (blocking).
                If False, runs in background thread.
        """
        if self.running:
            logger.warning("Processor already running")
            return
        
        with self.lock:
            self.running = True
            self.paused = False
        
        # Start alerts system
        self.alerts.start()
        
        logger.info("Starting processor...")
        
        if loop_forever:
            # Run in current thread (blocking)
            self._process_loop()
        else:
            # Run in background thread
            self.thread = threading.Thread(
                target=self._process_loop,
                daemon=True
            )
            self.thread.start()
            logger.info("Processor started in background thread")
    
    def stop(self) -> None:
        """Stop the processor gracefully."""
        if not self.running:
            logger.warning("Processor not running")
            return
        
        logger.info("Stopping processor...")
        
        with self.lock:
            self.running = False
        
        # Wait for background thread
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
            if self.thread.is_alive():
                logger.warning("Processor thread did not stop within timeout")
        
        # Stop alerts system
        self.alerts.stop()
        
        logger.info("Processor stopped")
    
    def pause(self) -> None:
        """Pause processing (can be resumed)."""
        with self.lock:
            self.paused = True
        logger.info("Processor paused")
    
    def resume(self) -> None:
        """Resume processing after pause."""
        with self.lock:
            self.paused = False
        logger.info("Processor resumed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processor statistics.
        
        Returns:
            Dict: Statistics including counts, timing, and detector stats
        """
        avg_time = 0.0
        if self.processing_times:
            avg_time = sum(self.processing_times) / len(self.processing_times) * 1000
        
        return {
            'total_processed': self.total_processed,
            'total_anomalies': self.total_anomalies,
            'total_errors': self.total_errors,
            'avg_processing_time_ms': avg_time,
            'queue_size': self.persistence.get_queue_size(),
            'running': self.running,
            'paused': self.paused,
            'metrics_tracked': len(self.windows),
            'detector_stats': self.detector.get_stats(),
            'alert_stats': self.alerts.get_stats()
        }
    
    def __repr__(self) -> str:
        """Return string representation of processor."""
        return (
            f"Processor(processed={self.total_processed}, "
            f"anomalies={self.total_anomalies}, "
            f"running={self.running})"
        )


def setup_logging(log_level: str = "INFO", log_format: Optional[str] = None) -> None:
    """Configure logging for the application.
    
    Args:
        log_level: Logging level
        log_format: Custom log format string
    """
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def main() -> None:
    """Main entry point for the processor service.
    
    Loads configuration, sets up persistence, detector, alerts, and processor,
    then runs the processing loop. Handles graceful shutdown on SIGINT/SIGTERM.
    """
    # Load settings
    settings = get_settings()
    
    # Setup logging
    setup_logging(settings.log_level, settings.log_format)
    logger.info(f"Starting {settings.app_name} processor service")
    logger.info(f"Environment: {settings.environment}")
    
    # Initialize persistence
    logger.info(f"Connecting to Redis at {settings.redis_url}")
    persistence = RedisPersistence(
        redis_url=settings.redis_url,
        raw_events_key=settings.redis_raw_events_key,
        processed_prefix=settings.redis_processed_prefix
    )
    
    # Initialize email sender
    email_sender = EmailSender(
        smtp_host=getattr(settings, 'smtp_host', None),
        smtp_port=getattr(settings, 'smtp_port', 587),
        smtp_user=getattr(settings, 'smtp_user', None),
        smtp_password=getattr(settings, 'smtp_password', None),
        from_addr=getattr(settings, 'email_from', None),
        to_addrs=getattr(settings, 'email_to', '').split(',') if getattr(settings, 'email_to', '') else [],
        rate_limit_seconds=getattr(settings, 'email_rate_limit_seconds', 60.0),
        enabled=getattr(settings, 'email_enabled', False)
    )
    
    # Initialize alerts
    alerts = Alerts(
        redis_url=settings.redis_url,
        pubsub_channel=getattr(settings, 'alerts_channel', 'anomalies'),
        email_sender=email_sender
    )
    
    # Initialize detector
    detector = ZScoreDetector(
        threshold=getattr(settings, 'anomaly_threshold', 3.0),
        min_count=getattr(settings, 'anomaly_min_count', 5)
    )
    
    # Initialize processor
    processor = Processor(
        persistence=persistence,
        alerts=alerts,
        detector=detector,
        window_seconds=getattr(settings, 'window_seconds', 300.0),
        poll_timeout=settings.poll_timeout,
        sleep_on_empty=getattr(settings, 'sleep_on_empty', 0.1)
    )
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        processor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start processing
    try:
        processor.start(loop_forever=True)
    except Exception as e:
        logger.error(f"Fatal error in processor service: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Print final statistics
        stats = processor.get_stats()
        logger.info(f"Final statistics: {stats}")


if __name__ == "__main__":
    main()
