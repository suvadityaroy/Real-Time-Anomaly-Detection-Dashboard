"""Tests for ingestion service."""

import pytest
import json
import time
from unittest.mock import Mock, patch

from anomaly.ingest import KafkaConsumerIngest, setup_logging
from anomaly.persistence import InMemoryPersistence
from anomaly.models import Event


class TestKafkaConsumerIngest:
    """Test cases for KafkaConsumerIngest."""
    
    @pytest.fixture
    def persistence(self) -> InMemoryPersistence:
        """Create an in-memory persistence backend for testing."""
        return InMemoryPersistence(max_queue_size=100)
    
    @pytest.fixture
    def ingest(self, persistence: InMemoryPersistence) -> KafkaConsumerIngest:
        """Create a Kafka consumer ingest instance for testing."""
        # Mock the Kafka consumer to avoid actual Kafka connection
        with patch('anomaly.ingest.Consumer'):
            ingest = KafkaConsumerIngest(
                persistence=persistence,
                kafka_broker="localhost:9092",
                kafka_topic="test-events",
                group_id="test-group"
            )
            return ingest
    
    def test_ingest_initialization(self, ingest: KafkaConsumerIngest) -> None:
        """Test that ingest service initializes correctly."""
        assert ingest.kafka_topic == "test-events"
        assert ingest.running is False
        assert ingest.total_consumed == 0
        assert ingest.total_valid == 0
        assert ingest.total_invalid == 0
    
    def test_process_valid_message(
        self,
        ingest: KafkaConsumerIngest,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing a valid event message."""
        event_data = {
            "timestamp": "2023-11-20T10:30:00",
            "metric": "cpu",
            "value": 75.5,
            "meta": {"host": "server-1"}
        }
        message = json.dumps(event_data)
        
        success = ingest._process_message(message)
        
        assert success is True
        assert ingest.total_valid == 1
        assert ingest.total_invalid == 0
        assert ingest.total_pushed == 1
        assert persistence.get_queue_size() == 1
    
    def test_process_invalid_json(self, ingest: KafkaConsumerIngest) -> None:
        """Test processing a message with invalid JSON."""
        message = "{invalid json"
        
        success = ingest._process_message(message)
        
        assert success is False
        assert ingest.total_invalid == 1
        assert ingest.total_valid == 0
    
    def test_process_invalid_schema(self, ingest: KafkaConsumerIngest) -> None:
        """Test processing a message that fails schema validation."""
        event_data = {
            "timestamp": "2023-11-20T10:30:00",
            "metric": "",  # Empty metric name is invalid
            "value": 75.5
        }
        message = json.dumps(event_data)
        
        success = ingest._process_message(message)
        
        assert success is False
        assert ingest.total_invalid == 1
        assert ingest.total_valid == 0
    
    def test_process_missing_required_fields(self, ingest: KafkaConsumerIngest) -> None:
        """Test processing a message missing required fields."""
        event_data = {
            "timestamp": "2023-11-20T10:30:00",
            # Missing 'metric' and 'value'
        }
        message = json.dumps(event_data)
        
        success = ingest._process_message(message)
        
        assert success is False
        assert ingest.total_invalid == 1
    
    def test_process_multiple_messages(
        self,
        ingest: KafkaConsumerIngest,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing multiple valid messages."""
        events = [
            {
                "timestamp": "2023-11-20T10:30:00",
                "metric": "cpu",
                "value": 75.5
            },
            {
                "timestamp": "2023-11-20T10:31:00",
                "metric": "memory",
                "value": 85.2
            },
            {
                "timestamp": "2023-11-20T10:32:00",
                "metric": "requests",
                "value": 1500.0
            }
        ]
        
        for event_data in events:
            message = json.dumps(event_data)
            ingest._process_message(message)
        
        assert ingest.total_valid == 3
        assert ingest.total_pushed == 3
        assert persistence.get_queue_size() == 3
    
    def test_process_mixed_valid_invalid(
        self,
        ingest: KafkaConsumerIngest,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing a mix of valid and invalid messages."""
        messages = [
            json.dumps({"timestamp": "2023-11-20T10:30:00", "metric": "cpu", "value": 75.5}),
            "{invalid json}",
            json.dumps({"timestamp": "2023-11-20T10:31:00", "metric": "memory", "value": 85.2}),
            json.dumps({"timestamp": "invalid", "metric": "disk", "value": 50.0}),
            json.dumps({"timestamp": "2023-11-20T10:32:00", "metric": "requests", "value": 1500.0})
        ]
        
        for message in messages:
            ingest._process_message(message)
        
        assert ingest.total_consumed == 0  # Not incremented in _process_message
        assert ingest.total_valid == 3
        assert ingest.total_invalid == 2
        assert ingest.total_pushed == 3
        assert persistence.get_queue_size() == 3
    
    def test_get_stats(self, ingest: KafkaConsumerIngest) -> None:
        """Test getting statistics from ingest service."""
        stats = ingest.get_stats()
        
        assert "total_consumed" in stats
        assert "total_valid" in stats
        assert "total_invalid" in stats
        assert "total_pushed" in stats
        assert "total_failed" in stats
        assert "queue_size" in stats
        assert stats["total_consumed"] == 0
    
    def test_process_event_with_unix_timestamp(
        self,
        ingest: KafkaConsumerIngest,
        persistence: InMemoryPersistence
    ) -> None:
        """Test processing event with Unix timestamp."""
        event_data = {
            "timestamp": 1700478600.0,
            "metric": "cpu",
            "value": 75.5
        }
        message = json.dumps(event_data)
        
        success = ingest._process_message(message)
        
        assert success is True
        assert ingest.total_valid == 1
        assert persistence.get_queue_size() == 1
        
        # Verify the event can be retrieved and parsed
        stored_json = persistence.pop_raw_event(timeout=0)
        assert stored_json is not None
        stored_event = Event.parse_raw(stored_json)
        assert stored_event.metric == "cpu"
        assert stored_event.value == 75.5


class TestSetupLogging:
    """Test cases for logging setup."""
    
    def test_setup_logging_default(self) -> None:
        """Test logging setup with default parameters."""
        setup_logging()
        # Just ensure it doesn't raise an exception
    
    def test_setup_logging_custom_level(self) -> None:
        """Test logging setup with custom level."""
        setup_logging(log_level="DEBUG")
        # Just ensure it doesn't raise an exception
    
    def test_setup_logging_custom_format(self) -> None:
        """Test logging setup with custom format."""
        setup_logging(log_format="%(levelname)s - %(message)s")
        # Just ensure it doesn't raise an exception


class TestIntegrationIngestToPersistence:
    """Integration tests for ingest to persistence flow."""
    
    def test_full_flow_valid_events(self) -> None:
        """Test full flow from ingest to persistence."""
        persistence = InMemoryPersistence()
        
        with patch('anomaly.ingest.Consumer'):
            ingest = KafkaConsumerIngest(
                persistence=persistence,
                kafka_broker="localhost:9092",
                kafka_topic="events",
                group_id="test-group"
            )
        
        # Create and process events
        events = [
            {"timestamp": "2023-11-20T10:30:00", "metric": "cpu", "value": 75.5},
            {"timestamp": "2023-11-20T10:31:00", "metric": "memory", "value": 85.2},
            {"timestamp": "2023-11-20T10:32:00", "metric": "requests", "value": 1500.0}
        ]
        
        for event_data in events:
            message = json.dumps(event_data)
            ingest._process_message(message)
        
        # Verify events are in persistence
        assert persistence.get_queue_size() == 3
        
        # Pop and verify each event
        for expected_event in events:
            stored_json = persistence.pop_raw_event(timeout=1)
            assert stored_json is not None
            stored_event = Event.parse_raw(stored_json)
            assert stored_event.metric == expected_event["metric"]
            assert stored_event.value == expected_event["value"]
        
        # Queue should now be empty
        assert persistence.get_queue_size() == 0
    
    def test_persistence_handles_high_volume(self) -> None:
        """Test that persistence can handle many events."""
        persistence = InMemoryPersistence(max_queue_size=1000)
        
        with patch('anomaly.ingest.Consumer'):
            ingest = KafkaConsumerIngest(
                persistence=persistence,
                kafka_broker="localhost:9092",
                kafka_topic="events",
                group_id="test-group"
            )
        
        # Process many events
        num_events = 500
        for i in range(num_events):
            event_data = {
                "timestamp": f"2023-11-20T10:{i % 60:02d}:00",
                "metric": f"metric_{i % 10}",
                "value": float(i)
            }
            message = json.dumps(event_data)
            ingest._process_message(message)
        
        assert ingest.total_valid == num_events
        assert persistence.get_queue_size() == num_events
