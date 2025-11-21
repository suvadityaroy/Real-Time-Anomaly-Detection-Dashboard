"""Tests for data models."""

import pytest
import time
from datetime import datetime
from pydantic import ValidationError

from anomaly.models import Event, EventBatch, StoredEvent, EventResult, EventRun


class TestEvent:
    """Test cases for Event model."""
    
    def test_event_with_iso_timestamp(self) -> None:
        """Test event creation with ISO8601 timestamp."""
        event = Event(
            timestamp="2023-11-20T10:30:00",
            metric="cpu",
            value=75.5
        )
        assert event.metric == "cpu"
        assert event.value == 75.5
        assert event.timestamp == "2023-11-20T10:30:00"
        assert event.meta is None
    
    def test_event_with_unix_timestamp(self) -> None:
        """Test event creation with Unix epoch timestamp."""
        event = Event(
            timestamp=1700478600.0,
            metric="memory",
            value=85.2
        )
        assert event.metric == "memory"
        assert event.value == 85.2
        assert event.timestamp == 1700478600.0
    
    def test_event_with_metadata(self) -> None:
        """Test event creation with optional metadata."""
        event = Event(
            timestamp="2023-11-20T10:30:00",
            metric="requests",
            value=1500.0,
            meta={"host": "server-1", "region": "us-west"}
        )
        assert event.meta == {"host": "server-1", "region": "us-west"}
    
    def test_event_invalid_timestamp_format(self) -> None:
        """Test that invalid timestamp format raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            Event(
                timestamp="invalid-timestamp",
                metric="cpu",
                value=50.0
            )
        assert "Invalid ISO8601 timestamp" in str(exc_info.value)
    
    def test_event_invalid_timestamp_range(self) -> None:
        """Test that timestamp outside valid range raises error."""
        with pytest.raises(ValidationError) as exc_info:
            Event(
                timestamp=100.0,  # Too early (before year 2000)
                metric="cpu",
                value=50.0
            )
        assert "Invalid Unix timestamp" in str(exc_info.value)
    
    def test_event_empty_metric_name(self) -> None:
        """Test that empty metric name raises validation error."""
        with pytest.raises(ValidationError):
            Event(
                timestamp="2023-11-20T10:30:00",
                metric="",
                value=50.0
            )
    
    def test_get_timestamp_float(self) -> None:
        """Test conversion from ISO timestamp to float."""
        event = Event(
            timestamp="2023-11-20T10:30:00",
            metric="cpu",
            value=50.0
        )
        ts_float = event.get_timestamp_float()
        assert isinstance(ts_float, float)
        assert ts_float > 0
    
    def test_get_timestamp_iso(self) -> None:
        """Test conversion from Unix timestamp to ISO format."""
        event = Event(
            timestamp=1700478600.0,
            metric="cpu",
            value=50.0
        )
        ts_iso = event.get_timestamp_iso()
        assert isinstance(ts_iso, str)
        assert "T" in ts_iso or " " in ts_iso  # ISO format contains date-time separator
    
    def test_event_json_serialization(self) -> None:
        """Test that event can be serialized to JSON."""
        event = Event(
            timestamp="2023-11-20T10:30:00",
            metric="cpu",
            value=75.5,
            meta={"host": "server-1"}
        )
        json_str = event.json()
        assert "cpu" in json_str
        assert "75.5" in json_str
        assert "server-1" in json_str


class TestEventBatch:
    """Test cases for EventBatch model."""
    
    def test_event_batch_creation(self) -> None:
        """Test creating a batch with multiple events."""
        events = [
            Event(timestamp="2023-11-20T10:30:00", metric="cpu", value=75.5),
            Event(timestamp="2023-11-20T10:31:00", metric="memory", value=85.2),
            Event(timestamp="2023-11-20T10:32:00", metric="cpu", value=80.1)
        ]
        batch = EventBatch(events=events, batch_id="batch-001")
        assert len(batch) == 3
        assert batch.batch_id == "batch-001"
        assert batch.created_at > 0
    
    def test_event_batch_empty_raises_error(self) -> None:
        """Test that empty batch raises validation error."""
        with pytest.raises(ValidationError):
            EventBatch(events=[])
    
    def test_event_batch_metrics(self) -> None:
        """Test getting unique metrics from batch."""
        events = [
            Event(timestamp="2023-11-20T10:30:00", metric="cpu", value=75.5),
            Event(timestamp="2023-11-20T10:31:00", metric="memory", value=85.2),
            Event(timestamp="2023-11-20T10:32:00", metric="cpu", value=80.1)
        ]
        batch = EventBatch(events=events)
        metrics = batch.metrics()
        assert metrics == ["cpu", "memory"]  # Sorted and unique
    
    def test_event_batch_length(self) -> None:
        """Test that len() works on batch."""
        events = [
            Event(timestamp="2023-11-20T10:30:00", metric="cpu", value=75.5),
            Event(timestamp="2023-11-20T10:31:00", metric="memory", value=85.2)
        ]
        batch = EventBatch(events=events)
        assert len(batch) == 2


class TestStoredEvent:
    """Test cases for StoredEvent model."""
    
    def test_stored_event_creation(self) -> None:
        """Test creating a stored event."""
        event = Event(timestamp="2023-11-20T10:30:00", metric="cpu", value=75.5)
        stored = StoredEvent(
            event_id="evt-123",
            event=event,
            status="pending"
        )
        assert stored.event_id == "evt-123"
        assert stored.event.metric == "cpu"
        assert stored.status == "pending"
        assert stored.processed_at is None
    
    def test_stored_event_invalid_status(self) -> None:
        """Test that invalid status raises error."""
        event = Event(timestamp="2023-11-20T10:30:00", metric="cpu", value=75.5)
        with pytest.raises(ValidationError) as exc_info:
            StoredEvent(
                event_id="evt-123",
                event=event,
                status="invalid-status"
            )
        assert "Invalid status" in str(exc_info.value)
    
    def test_stored_event_processed_status(self) -> None:
        """Test stored event with processed status."""
        event = Event(timestamp="2023-11-20T10:30:00", metric="cpu", value=75.5)
        now = time.time()
        stored = StoredEvent(
            event_id="evt-123",
            event=event,
            status="processed",
            processed_at=now
        )
        assert stored.status == "processed"
        assert stored.processed_at == now


class TestEventResult:
    """Test cases for EventResult model."""
    
    def test_event_result_creation(self) -> None:
        """Test creating an event result."""
        result = EventResult(
            event_id="evt-123",
            metric="cpu",
            value=95.5,
            is_anomaly=True,
            anomaly_score=0.85,
            detection_method="z-score"
        )
        assert result.event_id == "evt-123"
        assert result.is_anomaly is True
        assert result.anomaly_score == 0.85
        assert result.detection_method == "z-score"
    
    def test_event_result_with_details(self) -> None:
        """Test event result with additional details."""
        result = EventResult(
            event_id="evt-123",
            metric="cpu",
            value=95.5,
            is_anomaly=True,
            anomaly_score=0.85,
            detection_method="z-score",
            details={"z_score": 3.2, "threshold": 3.0}
        )
        assert result.details == {"z_score": 3.2, "threshold": 3.0}
    
    def test_event_result_invalid_score(self) -> None:
        """Test that score outside [0, 1] raises error."""
        with pytest.raises(ValidationError):
            EventResult(
                event_id="evt-123",
                metric="cpu",
                value=95.5,
                is_anomaly=True,
                anomaly_score=1.5,  # Invalid: > 1.0
                detection_method="z-score"
            )


class TestEventRun:
    """Test cases for EventRun model."""
    
    def test_event_run_creation(self) -> None:
        """Test creating an event run."""
        run = EventRun(
            run_id="run-001",
            event_count=100,
            anomaly_count=5,
            status="running"
        )
        assert run.run_id == "run-001"
        assert run.event_count == 100
        assert run.anomaly_count == 5
        assert run.status == "running"
    
    def test_event_run_completed(self) -> None:
        """Test completed event run."""
        now = time.time()
        run = EventRun(
            run_id="run-001",
            event_count=100,
            anomaly_count=5,
            status="completed",
            completed_at=now
        )
        assert run.status == "completed"
        assert run.completed_at == now
    
    def test_event_run_invalid_anomaly_count(self) -> None:
        """Test that anomaly count > event count raises error."""
        with pytest.raises(ValidationError) as exc_info:
            EventRun(
                run_id="run-001",
                event_count=100,
                anomaly_count=150,  # Invalid: more anomalies than events
                status="running"
            )
        assert "cannot exceed event count" in str(exc_info.value)
