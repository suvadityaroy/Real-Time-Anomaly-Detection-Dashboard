"""Data models for the anomaly detection system.

Defines Pydantic models for events, batches, and persistence records.
All models include validation and serialization support.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
import time


class Event(BaseModel):
    """A single metric event.
    
    Represents a time-series data point with a timestamp, metric name,
    value, and optional metadata.
    """
    
    timestamp: str | float = Field(
        ...,
        description="Event timestamp as ISO8601 string or Unix epoch float"
    )
    metric: str = Field(
        ...,
        description="Metric name (e.g., 'cpu', 'memory', 'requests')",
        min_length=1,
        max_length=255
    )
    value: float = Field(
        ...,
        description="Numeric metric value"
    )
    meta: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional metadata dictionary"
    )
    
    @validator("timestamp")
    def validate_timestamp(cls, v: str | float) -> str | float:
        """Validate timestamp format and convert if needed.
        
        Accepts ISO8601 strings or Unix epoch floats. Validates that
        the timestamp represents a valid datetime.
        """
        if isinstance(v, str):
            # Validate ISO8601 format by attempting to parse
            try:
                datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError(
                    f"Invalid ISO8601 timestamp: {v}. "
                    "Expected format: YYYY-MM-DDTHH:MM:SS or Unix epoch"
                )
        elif isinstance(v, (int, float)):
            # Validate Unix timestamp is reasonable (between year 2000 and 2100)
            if v < 946684800 or v > 4102444800:
                raise ValueError(
                    f"Invalid Unix timestamp: {v}. "
                    "Expected value between 946684800 (2000) and 4102444800 (2100)"
                )
        else:
            raise ValueError(
                f"Invalid timestamp type: {type(v)}. Expected str or float"
            )
        return v
    
    @validator("value")
    def validate_value(cls, v: float) -> float:
        """Validate metric value is finite."""
        if not (-1e308 <= v <= 1e308):  # Check for finite values
            raise ValueError(f"Metric value must be finite: {v}")
        return v
    
    def get_timestamp_float(self) -> float:
        """Get timestamp as Unix epoch float.
        
        Returns:
            float: Timestamp in seconds since Unix epoch
        """
        if isinstance(self.timestamp, float):
            return self.timestamp
        # Parse ISO8601 string to Unix timestamp
        dt = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
        return dt.timestamp()
    
    def get_timestamp_iso(self) -> str:
        """Get timestamp as ISO8601 string.
        
        Returns:
            str: Timestamp in ISO8601 format
        """
        if isinstance(self.timestamp, str):
            return self.timestamp
        # Convert Unix timestamp to ISO8601 string
        dt = datetime.fromtimestamp(self.timestamp)
        return dt.isoformat()
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventBatch(BaseModel):
    """A batch of events for processing.
    
    Used to group multiple events together for efficient batch processing.
    """
    
    events: List[Event] = Field(
        ...,
        description="List of events in the batch",
        min_items=1
    )
    batch_id: Optional[str] = Field(
        default=None,
        description="Optional batch identifier"
    )
    created_at: float = Field(
        default_factory=time.time,
        description="Batch creation timestamp (Unix epoch)"
    )
    
    @validator("events")
    def validate_events_not_empty(cls, v: List[Event]) -> List[Event]:
        """Ensure batch contains at least one event."""
        if not v:
            raise ValueError("EventBatch must contain at least one event")
        return v
    
    def __len__(self) -> int:
        """Return number of events in the batch."""
        return len(self.events)
    
    def metrics(self) -> List[str]:
        """Get list of unique metric names in the batch.
        
        Returns:
            List[str]: Sorted list of unique metric names
        """
        return sorted(set(event.metric for event in self.events))


class StoredEvent(BaseModel):
    """A stored event record with processing metadata.
    
    Extends Event with additional fields for tracking processing
    status, timing, and results.
    """
    
    event_id: str = Field(
        ...,
        description="Unique event identifier"
    )
    event: Event = Field(
        ...,
        description="The original event data"
    )
    ingested_at: float = Field(
        default_factory=time.time,
        description="Timestamp when event was ingested (Unix epoch)"
    )
    processed_at: Optional[float] = Field(
        default=None,
        description="Timestamp when event was processed (Unix epoch)"
    )
    status: str = Field(
        default="pending",
        description="Processing status (pending, processed, failed)"
    )
    
    @validator("status")
    def validate_status(cls, v: str) -> str:
        """Validate status is a known value."""
        valid_statuses = {"pending", "processed", "failed"}
        if v not in valid_statuses:
            raise ValueError(
                f"Invalid status: {v}. Must be one of {valid_statuses}"
            )
        return v


class EventResult(BaseModel):
    """Result of anomaly detection processing.
    
    Contains the detection results and metadata for a processed event.
    """
    
    event_id: str = Field(
        ...,
        description="Unique event identifier"
    )
    metric: str = Field(
        ...,
        description="Metric name that was analyzed"
    )
    value: float = Field(
        ...,
        description="Original metric value"
    )
    is_anomaly: bool = Field(
        ...,
        description="Whether the event was detected as an anomaly"
    )
    anomaly_score: float = Field(
        ...,
        description="Anomaly score (0.0 = normal, 1.0 = definitely anomalous)",
        ge=0.0,
        le=1.0
    )
    detection_method: str = Field(
        ...,
        description="Name of the detection algorithm used"
    )
    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional details about the detection"
    )
    processed_at: float = Field(
        default_factory=time.time,
        description="Timestamp when processing completed (Unix epoch)"
    )
    
    @validator("anomaly_score")
    def validate_score(cls, v: float) -> float:
        """Ensure anomaly score is in valid range."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"Anomaly score must be between 0.0 and 1.0, got {v}")
        return v


class EventRun(BaseModel):
    """Represents a processing run containing multiple events.
    
    Used to track batches of events that were processed together.
    """
    
    run_id: str = Field(
        ...,
        description="Unique run identifier"
    )
    started_at: float = Field(
        default_factory=time.time,
        description="Run start timestamp (Unix epoch)"
    )
    completed_at: Optional[float] = Field(
        default=None,
        description="Run completion timestamp (Unix epoch)"
    )
    event_count: int = Field(
        default=0,
        description="Number of events processed",
        ge=0
    )
    anomaly_count: int = Field(
        default=0,
        description="Number of anomalies detected",
        ge=0
    )
    status: str = Field(
        default="running",
        description="Run status (running, completed, failed)"
    )
    
    @validator("status")
    def validate_status(cls, v: str) -> str:
        """Validate status is a known value."""
        valid_statuses = {"running", "completed", "failed"}
        if v not in valid_statuses:
            raise ValueError(
                f"Invalid status: {v}. Must be one of {valid_statuses}"
            )
        return v
    
    @validator("anomaly_count")
    def validate_anomaly_count(cls, v: int, values: Dict[str, Any]) -> int:
        """Ensure anomaly count doesn't exceed event count."""
        event_count = values.get("event_count", 0)
        if v > event_count:
            raise ValueError(
                f"Anomaly count ({v}) cannot exceed event count ({event_count})"
            )
        return v
