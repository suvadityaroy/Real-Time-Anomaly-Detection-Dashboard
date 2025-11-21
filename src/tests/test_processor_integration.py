"""Integration tests for the processing pipeline."""

import pytest
import time
import json
from unittest.mock import Mock, patch

from anomaly.models import Event
from anomaly.persistence import InMemoryPersistence
from anomaly.window import MultiMetricWindow
from anomaly.detector import ZScoreDetector
from anomaly.alerts import Alerts, EmailSender
from anomaly.processor import Processor


class TestProcessorIntegration:
    """Integration tests for the full processing pipeline."""
    
    @pytest.fixture
    def persistence(self) -> InMemoryPersistence:
        """Create in-memory persistence for testing."""
        return InMemoryPersistence()
    
    @pytest.fixture
    def email_sender(self) -> EmailSender:
        """Create dummy email sender for testing."""
        return EmailSender(
            smtp_host="dummy",
            enabled=True,
            to_addrs=["test@example.com"]
        )
    
    @pytest.fixture
    def alerts(self, email_sender: EmailSender) -> Alerts:
        """Create alerts system with mocked Redis."""
        with patch('anomaly.alerts.redis.from_url') as mock_redis:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_client.publish.return_value = 1
            mock_redis.return_value = mock_client
            
            alerts_system = Alerts(
                redis_url="redis://localhost:6379/0",
                email_sender=email_sender
            )
            return alerts_system
    
    @pytest.fixture
    def processor(
        self,
        persistence: InMemoryPersistence,
        alerts: Alerts
    ) -> Processor:
        """Create processor for testing."""
        detector = ZScoreDetector(threshold=2.5, min_count=5)
        processor = Processor(
            persistence=persistence,
            alerts=alerts,
            detector=detector,
            window_seconds=300.0,
            poll_timeout=0.1,
            sleep_on_empty=0.01
        )
        return processor
    
    def test_processor_initialization(self, processor: Processor) -> None:
        """Test processor initializes correctly."""
        assert processor.running is False
        assert processor.paused is False
        assert processor.total_processed == 0
        assert processor.total_anomalies == 0
    
    def test_process_single_event(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing a single event."""
        event = Event(
            timestamp=time.time(),
            metric="cpu",
            value=75.0
        )
        
        result = processor.process_event(event)
        
        # Should not be anomaly (insufficient data)
        assert result is None
        assert len(processor.windows.get_window("cpu")) == 1
    
    def test_process_event_sequence_no_anomaly(
        self,
        processor: Processor
    ) -> None:
        """Test processing normal event sequence with no anomalies."""
        base_time = time.time()
        
        # Process 10 normal events
        for i in range(10):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i * 2  # Values from 50 to 68
            )
            result = processor.process_event(event)
            
            # First 4 events: insufficient data, should be None
            # Remaining events: normal values, should be None
            assert result is None
        
        assert processor.total_processed == 0  # Not incremented in process_event
        assert processor.total_anomalies == 0
    
    def test_process_event_with_anomaly(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing events including an outlier."""
        base_time = time.time()
        
        # Build baseline with normal values
        for i in range(10):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i  # Values from 50 to 59
            )
            processor.process_event(event)
        
        # Process an anomalous value
        anomaly_event = Event(
            timestamp=base_time + 10,
            metric="cpu",
            value=200.0  # Way above normal
        )
        result = processor.process_event(anomaly_event)
        
        # Should detect anomaly
        assert result is not None
        assert result['is_anomaly'] is True
        assert result['metric'] == 'cpu'
        assert result['value'] == 200.0
        assert abs(result['z_score']) > 2.5
        assert processor.total_anomalies == 1
        
        # Check that anomaly was saved to persistence
        anomaly_id = result['anomaly_id']
        saved_anomaly = persistence.get_processed(anomaly_id)
        assert saved_anomaly is not None
        assert saved_anomaly['metric'] == 'cpu'
    
    def test_process_multiple_metrics(
        self,
        processor: Processor
    ) -> None:
        """Test processing events from multiple metrics."""
        base_time = time.time()
        
        # Add events for CPU
        for i in range(6):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i
            )
            processor.process_event(event)
        
        # Add events for memory
        for i in range(6):
            event = Event(
                timestamp=base_time + i,
                metric="memory",
                value=80.0 + i
            )
            processor.process_event(event)
        
        # Check that both metrics have windows
        assert "cpu" in processor.windows.get_metrics()
        assert "memory" in processor.windows.get_metrics()
        assert len(processor.windows) == 2
    
    def test_full_pipeline_with_persistence(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test full pipeline: push to persistence, processor pulls and detects."""
        base_time = time.time()
        
        # Push normal events to persistence
        for i in range(10):
            event = Event(
                timestamp=base_time + i,
                metric="requests",
                value=1000.0 + i * 10
            )
            persistence.push_raw_event(event.json())
        
        # Push an anomaly
        anomaly_event = Event(
            timestamp=base_time + 10,
            metric="requests",
            value=5000.0  # Huge spike
        )
        persistence.push_raw_event(anomaly_event.json())
        
        # Start processor in background
        processor.start(loop_forever=False)
        
        # Wait for processing
        time.sleep(1.0)
        
        # Stop processor
        processor.stop()
        
        # Check results
        assert processor.total_processed >= 10
        assert processor.total_anomalies >= 1
        
        # Queue should be empty or nearly empty
        assert persistence.get_queue_size() <= 1
    
    def test_processor_pause_resume(self, processor: Processor) -> None:
        """Test pausing and resuming the processor."""
        assert processor.paused is False
        
        processor.pause()
        assert processor.paused is True
        
        processor.resume()
        assert processor.paused is False
    
    def test_processor_get_stats(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test getting processor statistics."""
        base_time = time.time()
        
        # Process some events
        for i in range(5):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i
            )
            processor.process_event(event)
        
        stats = processor.get_stats()
        
        # Check stat fields
        assert 'total_processed' in stats
        assert 'total_anomalies' in stats
        assert 'total_errors' in stats
        assert 'avg_processing_time_ms' in stats
        assert 'queue_size' in stats
        assert 'running' in stats
        assert 'paused' in stats
        assert 'metrics_tracked' in stats
        assert 'detector_stats' in stats
        assert 'alert_stats' in stats
        
        assert stats['metrics_tracked'] == 1
        assert stats['running'] is False
    
    def test_anomaly_alert_broadcast(
        self,
        processor: Processor,
        alerts: Alerts
    ) -> None:
        """Test that anomalies are broadcast via alerts system."""
        base_time = time.time()
        
        # Build baseline
        for i in range(10):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i
            )
            processor.process_event(event)
        
        # Process anomaly
        anomaly_event = Event(
            timestamp=base_time + 10,
            metric="cpu",
            value=200.0
        )
        
        initial_broadcast_count = alerts.total_broadcasted
        
        result = processor.process_event(anomaly_event)
        
        assert result is not None
        # Alert should have been broadcast
        assert alerts.total_broadcasted > initial_broadcast_count
    
    def test_invalid_event_handling(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test that processor handles invalid events gracefully."""
        # Push invalid JSON to persistence
        persistence.push_raw_event("{invalid json")
        persistence.push_raw_event('{"timestamp": "invalid"}')
        
        # Push valid event
        event = Event(timestamp=time.time(), metric="cpu", value=50.0)
        persistence.push_raw_event(event.json())
        
        # Start processor
        processor.start(loop_forever=False)
        time.sleep(0.5)
        processor.stop()
        
        # Processor should have handled errors gracefully
        assert processor.total_errors >= 2
        # Valid event should have been processed
        assert processor.total_processed >= 1
    
    def test_event_with_metadata(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing events with metadata."""
        base_time = time.time()
        
        # Build baseline
        for i in range(10):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i,
                meta={"host": "server-1", "region": "us-west"}
            )
            processor.process_event(event)
        
        # Process anomaly with metadata
        anomaly_event = Event(
            timestamp=base_time + 10,
            metric="cpu",
            value=200.0,
            meta={"host": "server-2", "region": "us-east"}
        )
        result = processor.process_event(anomaly_event)
        
        assert result is not None
        # Metadata should be preserved in anomaly
        assert result['event_meta'] == {"host": "server-2", "region": "us-east"}
    
    def test_multiple_anomalies_same_metric(
        self,
        processor: Processor
    ) -> None:
        """Test detecting multiple anomalies for the same metric."""
        base_time = time.time()
        
        # Build baseline (values 50-59)
        for i in range(10):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + i
            )
            processor.process_event(event)
        
        # Process multiple anomalies
        anomaly1 = processor.process_event(
            Event(timestamp=base_time + 10, metric="cpu", value=200.0)
        )
        anomaly2 = processor.process_event(
            Event(timestamp=base_time + 11, metric="cpu", value=5.0)
        )
        normal = processor.process_event(
            Event(timestamp=base_time + 12, metric="cpu", value=55.0)
        )
        
        assert anomaly1 is not None
        assert anomaly2 is not None
        assert normal is None
        assert processor.total_anomalies == 2
    
    def test_backpressure_with_large_queue(
        self,
        processor: Processor,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processor handles large queue with backpressure."""
        base_time = time.time()
        
        # Push many events to persistence
        for i in range(100):
            event = Event(
                timestamp=base_time + i,
                metric="cpu",
                value=50.0 + (i % 10)
            )
            persistence.push_raw_event(event.json())
        
        initial_queue_size = persistence.get_queue_size()
        assert initial_queue_size == 100
        
        # Start processor
        processor.start(loop_forever=False)
        time.sleep(2.0)  # Let it process for a bit
        processor.stop()
        
        # Should have processed many events
        assert processor.total_processed > 50
        
        # Queue should be smaller
        final_queue_size = persistence.get_queue_size()
        assert final_queue_size < initial_queue_size
    
    def test_processor_repr(self, processor: Processor) -> None:
        """Test string representation."""
        repr_str = repr(processor)
        assert "Processor" in repr_str
        assert "processed=" in repr_str


class TestEmailIntegration:
    """Test email alert integration."""
    
    def test_email_rate_limiting(self) -> None:
        """Test that email rate limiting works."""
        sender = EmailSender(
            smtp_host="dummy",
            enabled=True,
            to_addrs=["test@example.com"],
            rate_limit_seconds=2.0
        )
        
        anomaly = {
            'metric': 'cpu',
            'value': 100.0,
            'mean': 50.0,
            'stddev': 10.0,
            'z_score': 5.0,
            'threshold': 3.0,
            'timestamp': time.time(),
            'detected_at': time.time()
        }
        
        # First email should succeed
        result1 = sender.send_anomaly_email(anomaly)
        assert result1 is True
        
        # Immediate second email should be rate limited
        result2 = sender.send_anomaly_email(anomaly)
        assert result2 is False
        
        # After waiting, should succeed again
        time.sleep(2.1)
        result3 = sender.send_anomaly_email(anomaly)
        assert result3 is True
