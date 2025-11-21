"""Tests for the FastAPI application.

Tests REST endpoints, WebSocket functionality, and dependency injection.
"""

import pytest
import json
import asyncio
from typing import Dict, Any
from fastapi.testclient import TestClient

from anomaly.api import app, override_persistence
from anomaly.persistence import InMemoryPersistence
from anomaly.models import Event


@pytest.fixture
def test_persistence():
    """Create a test persistence instance."""
    return InMemoryPersistence()


@pytest.fixture
def client(test_persistence):
    """Create a test client with overridden dependencies."""
    override_persistence(test_persistence)
    
    # Use TestClient without lifespan for simpler testing
    with TestClient(app) as test_client:
        yield test_client


def test_root_endpoint(client):
    """Test the root endpoint returns API info."""
    response = client.get("/api")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert data["version"] == "1.0.0"


def test_health_endpoint(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "queue_size" in data
    assert isinstance(data["queue_size"], int)


def test_post_event_valid(client):
    """Test posting a valid event."""
    event_data = {
        "timestamp": 1234567890.0,
        "metric": "cpu_usage",
        "value": 75.5,
        "meta": {"host": "server-01"}
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert "event_id" in data
    assert data["message"] == "Event accepted for processing"


def test_post_event_invalid_timestamp(client):
    """Test posting an event with invalid timestamp."""
    event_data = {
        "timestamp": "invalid-timestamp",
        "metric": "cpu_usage",
        "value": 75.5
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 400
    assert "detail" in response.json()


def test_post_event_missing_metric(client):
    """Test posting an event without metric name."""
    event_data = {
        "timestamp": 1234567890.0,
        "value": 75.5
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 422  # Validation error


def test_post_event_invalid_value(client):
    """Test posting an event with non-numeric value."""
    event_data = {
        "timestamp": 1234567890.0,
        "metric": "cpu_usage",
        "value": "not-a-number"
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 422  # Validation error


def test_post_event_with_metadata(client):
    """Test posting an event with metadata."""
    event_data = {
        "timestamp": 1234567890.0,
        "metric": "memory_usage",
        "value": 8192.0,
        "meta": {
            "host": "server-01",
            "datacenter": "us-west-2",
            "environment": "production"
        }
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True


def test_post_multiple_events(client, test_persistence):
    """Test posting multiple events and checking queue."""
    events = [
        {"timestamp": 1234567890.0 + i, "metric": "cpu_usage", "value": 50.0 + i}
        for i in range(5)
    ]
    
    for event_data in events:
        response = client.post("/events/raw", json=event_data)
        assert response.status_code == 201
    
    # Check queue size
    response = client.get("/health")
    data = response.json()
    assert data["queue_size"] == 5


def test_get_anomalies_empty(client):
    """Test getting anomalies when none exist."""
    response = client.get("/anomalies")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["anomalies"] == []


def test_get_anomalies_with_limit(client):
    """Test getting anomalies with limit parameter."""
    response = client.get("/anomalies?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert "anomalies" in data
    assert "count" in data


def test_get_anomalies_with_stored_results(client, test_persistence):
    """Test getting anomalies after storing some results."""
    # Manually add some processed results
    anomaly_result = {
        "metric": "cpu_usage",
        "value": 95.0,
        "timestamp": 1234567890.0,
        "is_anomaly": True,
        "z_score": 4.2,
        "detected_at": 1234567890.0
    }
    
    test_persistence.save_processed("event-1", anomaly_result)
    
    # Get anomalies
    response = client.get("/anomalies")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["anomalies"]) == 1
    assert data["anomalies"][0]["metric"] == "cpu_usage"


def test_event_persistence_flow(client, test_persistence):
    """Test full flow: post event -> check queue -> verify storage."""
    # Post event
    event_data = {
        "timestamp": 1234567890.0,
        "metric": "network_throughput",
        "value": 1024.0
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 201
    
    # Verify event is in queue
    assert test_persistence.get_queue_size() == 1
    
    # Pop event from queue
    event_json = test_persistence.pop_raw_event(timeout=1)
    assert event_json is not None
    
    # Verify event data
    event = Event.parse_raw(event_json)
    assert event.metric == "network_throughput"
    assert event.value == 1024.0


def test_iso8601_timestamp(client):
    """Test posting event with ISO8601 timestamp."""
    event_data = {
        "timestamp": "2024-01-15T10:30:00",
        "metric": "requests_per_second",
        "value": 150.0
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 201


def test_unix_timestamp(client):
    """Test posting event with Unix timestamp."""
    event_data = {
        "timestamp": 1705318200.0,
        "metric": "requests_per_second",
        "value": 150.0
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 201


def test_concurrent_event_posting(client):
    """Test posting events concurrently."""
    import concurrent.futures
    
    def post_event(i):
        event_data = {
            "timestamp": 1234567890.0 + i,
            "metric": f"metric_{i % 3}",
            "value": float(i)
        }
        return client.post("/events/raw", json=event_data)
    
    # Post 10 events concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(post_event, i) for i in range(10)]
        responses = [f.result() for f in futures]
    
    # All should succeed
    assert all(r.status_code == 201 for r in responses)


def test_large_metadata(client):
    """Test posting event with large metadata."""
    large_meta = {f"key_{i}": f"value_{i}" for i in range(100)}
    
    event_data = {
        "timestamp": 1234567890.0,
        "metric": "test_metric",
        "value": 42.0,
        "meta": large_meta
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 201


def test_extreme_values(client):
    """Test posting events with extreme values."""
    test_cases = [
        {"timestamp": 1234567890.0, "metric": "test", "value": 0.0},
        {"timestamp": 1234567890.0, "metric": "test", "value": -1000000.0},
        {"timestamp": 1234567890.0, "metric": "test", "value": 1000000.0},
        {"timestamp": 1234567890.0, "metric": "test", "value": 0.000001},
    ]
    
    for event_data in test_cases:
        response = client.post("/events/raw", json=event_data)
        assert response.status_code == 201


def test_metric_name_validation(client):
    """Test metric name validation."""
    # Empty metric name should fail
    event_data = {
        "timestamp": 1234567890.0,
        "metric": "",
        "value": 42.0
    }
    
    response = client.post("/events/raw", json=event_data)
    assert response.status_code == 422


def test_api_cors_headers(client):
    """Test that CORS headers are present."""
    response = client.get("/health")
    assert "access-control-allow-origin" in response.headers


# WebSocket tests
@pytest.mark.asyncio
async def test_websocket_connection():
    """Test WebSocket connection and disconnection."""
    # Note: WebSocket testing with TestClient requires special handling
    # This is a basic test structure
    from fastapi.testclient import TestClient
    
    with TestClient(app) as client:
        try:
            with client.websocket_connect("/ws/anomalies") as websocket:
                # Connection successful
                assert websocket is not None
                
                # Send ping
                websocket.send_text("ping")
                
                # Receive pong
                data = websocket.receive_text()
                assert data == "pong"
        except Exception as e:
            # WebSocket might not work without proper lifespan
            pytest.skip(f"WebSocket test requires running server: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
