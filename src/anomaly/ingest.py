"""Kafka-based event ingestion service.

Consumes events from Kafka, validates them, and pushes to persistence layer
for downstream processing. Handles deserialization errors gracefully.
"""

import logging
import signal
import sys
import json
import uuid
import threading
from typing import Optional, Any
from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from anomaly.config import get_settings
from anomaly.models import Event
from anomaly.persistence import Persistence, RedisPersistence


logger = logging.getLogger(__name__)


class KafkaConsumerIngest:
    """Kafka consumer that ingests events and pushes to persistence.
    
    Subscribes to a Kafka topic, deserializes and validates events,
    then pushes valid events to the persistence layer for processing.
    """
    
    def __init__(
        self,
        persistence: Persistence,
        kafka_broker: str,
        kafka_topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
        session_timeout_ms: int = 10000
    ) -> None:
        """Initialize Kafka consumer ingest service.
        
        Args:
            persistence: Persistence backend for storing events
            kafka_broker: Kafka broker address(es)
            kafka_topic: Kafka topic to consume from
            group_id: Consumer group ID
            auto_offset_reset: Offset reset strategy ('earliest' or 'latest')
            session_timeout_ms: Consumer session timeout in milliseconds
        """
        self.persistence = persistence
        self.kafka_topic = kafka_topic
        self.running = False
        self.thread: Optional[threading.Thread] = None
        
        # Kafka consumer configuration
        conf = {
            'bootstrap.servers': kafka_broker,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': True,
            'session.timeout.ms': session_timeout_ms,
        }
        
        self.consumer = Consumer(conf)
        logger.info(
            f"Initialized Kafka consumer: broker={kafka_broker}, "
            f"topic={kafka_topic}, group={group_id}"
        )
        
        # Statistics
        self.total_consumed = 0
        self.total_valid = 0
        self.total_invalid = 0
        self.total_pushed = 0
        self.total_failed = 0
    
    def _process_message(self, message: str) -> bool:
        """Process a single Kafka message.
        
        Args:
            message: Raw message string from Kafka
            
        Returns:
            bool: True if successfully processed, False otherwise
        """
        try:
            # Parse JSON
            try:
                event_data = json.loads(message)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in message: {e}")
                self.total_invalid += 1
                return False
            
            # Validate against Event model
            try:
                event = Event(**event_data)
                self.total_valid += 1
            except ValidationError as e:
                logger.warning(f"Invalid event schema: {e}")
                self.total_invalid += 1
                return False
            
            # Serialize back to JSON for storage
            event_json = event.json()
            
            # Push to persistence
            success = self.persistence.push_raw_event(event_json)
            if success:
                self.total_pushed += 1
                logger.debug(
                    f"Pushed event: metric={event.metric}, "
                    f"value={event.value}, timestamp={event.timestamp}"
                )
            else:
                self.total_failed += 1
                logger.error(f"Failed to push event to persistence")
            
            return success
            
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}", exc_info=True)
            self.total_invalid += 1
            return False
    
    def _consume_loop(self, poll_timeout: float = 1.0) -> None:
        """Main consumption loop.
        
        Args:
            poll_timeout: Kafka poll timeout in seconds
        """
        try:
            # Subscribe to topic
            self.consumer.subscribe([self.kafka_topic])
            logger.info(f"Subscribed to topic: {self.kafka_topic}")
            
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=poll_timeout)
                
                if msg is None:
                    # No message available within timeout
                    continue
                
                if msg.error():
                    # Handle Kafka errors
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        logger.debug(
                            f"Reached end of partition: {msg.topic()} "
                            f"[{msg.partition()}] at offset {msg.offset()}"
                        )
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                # Process message
                self.total_consumed += 1
                message_value = msg.value()
                
                if message_value:
                    self._process_message(message_value.decode('utf-8'))
                
                # Log statistics periodically
                if self.total_consumed % 100 == 0:
                    logger.info(
                        f"Stats: consumed={self.total_consumed}, "
                        f"valid={self.total_valid}, invalid={self.total_invalid}, "
                        f"pushed={self.total_pushed}, failed={self.total_failed}"
                    )
        
        except KafkaException as e:
            logger.error(f"Kafka exception: {e}", exc_info=True)
            raise
        
        finally:
            # Clean up
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
            logger.info("Kafka consumer closed")
    
    def start(self, loop_forever: bool = True, poll_timeout: float = 1.0) -> None:
        """Start the ingestion service.
        
        Args:
            loop_forever: If True, runs in current thread. If False, runs in background thread.
            poll_timeout: Kafka poll timeout in seconds
        """
        if self.running:
            logger.warning("Ingest service is already running")
            return
        
        self.running = True
        logger.info("Starting Kafka consumer ingest service...")
        
        if loop_forever:
            # Run in current thread (blocking)
            self._consume_loop(poll_timeout)
        else:
            # Run in background thread
            self.thread = threading.Thread(
                target=self._consume_loop,
                args=(poll_timeout,),
                daemon=True
            )
            self.thread.start()
            logger.info("Ingest service started in background thread")
    
    def stop(self) -> None:
        """Stop the ingestion service gracefully."""
        if not self.running:
            logger.warning("Ingest service is not running")
            return
        
        logger.info("Stopping Kafka consumer ingest service...")
        self.running = False
        
        # Wait for background thread to finish
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5.0)
            if self.thread.is_alive():
                logger.warning("Background thread did not stop within timeout")
        
        logger.info("Ingest service stopped")
    
    def get_stats(self) -> dict[str, int]:
        """Get current statistics.
        
        Returns:
            dict: Statistics dictionary with consumption metrics
        """
        return {
            "total_consumed": self.total_consumed,
            "total_valid": self.total_valid,
            "total_invalid": self.total_invalid,
            "total_pushed": self.total_pushed,
            "total_failed": self.total_failed,
            "queue_size": self.persistence.get_queue_size()
        }


def setup_logging(log_level: str = "INFO", log_format: Optional[str] = None) -> None:
    """Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
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
    """Main entry point for the ingest service.
    
    Loads configuration, sets up persistence and Kafka consumer,
    and runs the ingestion loop. Handles graceful shutdown on SIGINT/SIGTERM.
    """
    # Load settings
    settings = get_settings()
    
    # Setup logging
    setup_logging(settings.log_level, settings.log_format)
    logger.info(f"Starting {settings.app_name} ingest service")
    logger.info(f"Environment: {settings.environment}")
    
    # Initialize persistence
    logger.info(f"Connecting to Redis at {settings.redis_url}")
    persistence = RedisPersistence(
        redis_url=settings.redis_url,
        raw_events_key=settings.redis_raw_events_key,
        processed_prefix=settings.redis_processed_prefix
    )
    
    # Initialize Kafka consumer
    ingest = KafkaConsumerIngest(
        persistence=persistence,
        kafka_broker=settings.kafka_broker,
        kafka_topic=settings.kafka_topic,
        group_id=settings.kafka_group_id,
        auto_offset_reset=settings.kafka_auto_offset_reset,
        session_timeout_ms=settings.kafka_session_timeout_ms
    )
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        ingest.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consuming
    try:
        ingest.start(loop_forever=True, poll_timeout=settings.poll_timeout)
    except Exception as e:
        logger.error(f"Fatal error in ingest service: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Print final statistics
        stats = ingest.get_stats()
        logger.info(f"Final statistics: {stats}")


if __name__ == "__main__":
    main()
