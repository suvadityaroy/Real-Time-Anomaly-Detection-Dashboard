"""FastAPI application for the anomaly detection system.

Provides REST API endpoints for event ingestion, anomaly queries,
and WebSocket streaming of live anomalies.
"""

import logging
import json
import asyncio
import csv
import io
from datetime import datetime
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, status, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import redis.asyncio as aioredis
from pathlib import Path
from confluent_kafka import Producer

from anomaly.config import get_settings, Settings
from anomaly.models import Event
from anomaly.persistence import Persistence, RedisPersistence, InMemoryPersistence


logger = logging.getLogger(__name__)


# Global state
_persistence: Optional[Persistence] = None
_redis_client: Optional[aioredis.Redis] = None
_ws_manager: Optional["WebSocketManager"] = None
_kafka_producer: Optional[Producer] = None


class WebSocketManager:
    """Manages WebSocket connections and broadcasts messages to all clients."""
    
    def __init__(self):
        """Initialize the WebSocket manager."""
        self.active_connections: List[WebSocket] = []
        self.redis_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def connect(self, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection.
        
        Args:
            websocket: The WebSocket connection to accept
        """
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection.
        
        Args:
            websocket: The WebSocket connection to remove
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")
    
    async def broadcast(self, message: str) -> None:
        """Broadcast a message to all connected clients.
        
        Args:
            message: The message to broadcast
        """
        if not self.active_connections:
            return
        
        # Send to all connected clients
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error sending to WebSocket client: {e}")
                disconnected.append(connection)
        
        # Clean up disconnected clients
        for connection in disconnected:
            self.disconnect(connection)
    
    async def start_redis_listener(self, redis_client: aioredis.Redis, channel: str) -> None:
        """Start listening to Redis Pub/Sub and broadcast messages.
        
        Args:
            redis_client: Async Redis client
            channel: Redis Pub/Sub channel to subscribe to
        """
        self._running = True
        logger.info(f"Starting Redis Pub/Sub listener on channel: {channel}")
        
        pubsub = None
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            
            async for message in pubsub.listen():
                if not self._running:
                    break
                
                if message["type"] == "message":
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    
                    logger.debug(f"Received message from Redis: {data}")
                    await self.broadcast(data)
        
        except asyncio.CancelledError:
            logger.info("Redis listener cancelled")
        except Exception as e:
            logger.error(f"Error in Redis listener: {e}")
        finally:
            if pubsub:
                await pubsub.unsubscribe(channel)
                await pubsub.close()
    
    def stop(self) -> None:
        """Stop the Redis listener."""
        self._running = False
        if self.redis_task and not self.redis_task.done():
            self.redis_task.cancel()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI startup/shutdown.
    
    Initializes persistence and WebSocket manager on startup,
    and cleans up on shutdown.
    """
    global _persistence, _redis_client, _ws_manager, _kafka_producer
    
    # Startup
    settings = get_settings()
    logger.info(f"Starting API server in {settings.environment} environment")
    
    # Initialize persistence
    if settings.environment == "testing":
        _persistence = InMemoryPersistence()
        logger.info("Using InMemoryPersistence for testing")
    else:
        _persistence = RedisPersistence(
            redis_url=settings.redis_url,
            raw_events_key=settings.redis_raw_events_key,
            processed_prefix=settings.redis_processed_prefix
        )
        logger.info(f"Connected to Redis at {settings.redis_url}")
    
    # Initialize Kafka producer
    kafka_config = {
        'bootstrap.servers': settings.kafka_broker,
        'client.id': 'api-producer'
    }
    logger.info(f"Initializing Kafka producer with broker: {settings.kafka_broker}")
    _kafka_producer = Producer(kafka_config)
    logger.info(f"Kafka producer initialized successfully")
    
    # Initialize async Redis client for Pub/Sub
    _redis_client = await aioredis.from_url(
        settings.redis_url,
        decode_responses=True
    )
    
    # Initialize WebSocket manager and start Redis listener
    _ws_manager = WebSocketManager()
    _ws_manager.redis_task = asyncio.create_task(
        _ws_manager.start_redis_listener(_redis_client, settings.alerts_channel)
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down API server")
    
    # Stop WebSocket manager
    if _ws_manager:
        _ws_manager.stop()
        if _ws_manager.redis_task:
            await _ws_manager.redis_task
    
    # Flush and close Kafka producer
    if _kafka_producer:
        _kafka_producer.flush(timeout=5)
        logger.info("Kafka producer flushed and closed")
    
    # Close Redis connection
    if _redis_client:
        await _redis_client.close()
    
    # Close persistence
    if isinstance(_persistence, RedisPersistence):
        _persistence.close()


# Create FastAPI app
app = FastAPI(
    title="Real-Time Anomaly Detection System",
    description="REST API and WebSocket endpoints for anomaly detection",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For demo purposes; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for dashboard
dashboard_path = Path(__file__).parent / "dashboard_static"
if dashboard_path.exists():
    app.mount("/static", StaticFiles(directory=str(dashboard_path)), name="static")


# Dependency injection
def get_persistence() -> Persistence:
    """Get the persistence instance.
    
    Returns:
        Persistence: The global persistence instance
        
    Raises:
        HTTPException: If persistence is not initialized
    """
    if _persistence is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Persistence not initialized"
        )
    return _persistence


def get_ws_manager() -> WebSocketManager:
    """Get the WebSocket manager instance.
    
    Returns:
        WebSocketManager: The global WebSocket manager
        
    Raises:
        HTTPException: If WebSocket manager is not initialized
    """
    if _ws_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WebSocket manager not initialized"
        )
    return _ws_manager


# Pydantic models for API
class EventRequest(BaseModel):
    """Request model for posting a single event."""
    
    timestamp: str | float = Field(
        ...,
        description="Event timestamp as ISO8601 string or Unix epoch"
    )
    metric: str = Field(
        ...,
        description="Metric name",
        min_length=1,
        max_length=255
    )
    value: float = Field(
        ...,
        description="Metric value"
    )
    meta: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional metadata"
    )


class EventResponse(BaseModel):
    """Response model for event submission."""
    
    success: bool = Field(..., description="Whether the event was accepted")
    message: str = Field(..., description="Response message")
    event_id: Optional[str] = Field(default=None, description="Generated event ID")


class HealthResponse(BaseModel):
    """Response model for health check."""
    
    status: str = Field(..., description="Service status")
    queue_size: int = Field(..., description="Current queue size")
    environment: str = Field(..., description="Environment name")


class AnomalyResponse(BaseModel):
    """Response model for anomaly query."""
    
    anomalies: List[Dict[str, Any]] = Field(..., description="List of recent anomalies")
    count: int = Field(..., description="Total count of anomalies returned")


# API Endpoints
@app.get("/")
async def root():
    """Root endpoint - redirect to dashboard.
    
    Returns:
        FileResponse: The dashboard HTML file
    """
    dashboard_file = Path(__file__).parent / "dashboard_static" / "index.html"
    if dashboard_file.exists():
        return FileResponse(dashboard_file)
    
    return {
        "message": "Real-Time Anomaly Detection System API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "dashboard": "/static/index.html"
    }


@app.get("/api", response_model=Dict[str, str])
async def api_info() -> Dict[str, str]:
    """API information endpoint.
    
    Returns:
        Dict[str, str]: API info and links
    """
    return {
        "message": "Real-Time Anomaly Detection System API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "dashboard": "/"
    }


@app.get("/health", response_model=HealthResponse)
async def health_check(
    persistence: Persistence = Depends(get_persistence)
) -> HealthResponse:
    """Health check endpoint.
    
    Returns service status and queue size.
    
    Args:
        persistence: Injected persistence instance
        
    Returns:
        HealthResponse: Health status information
    """
    settings = get_settings()
    queue_size = persistence.get_queue_size()
    
    return HealthResponse(
        status="ok",
        queue_size=queue_size,
        environment=settings.environment
    )


@app.post("/events/raw", response_model=EventResponse, status_code=status.HTTP_201_CREATED)
async def post_event(
    event_request: EventRequest,
    persistence: Persistence = Depends(get_persistence)
) -> EventResponse:
    """Accept a single raw event and push to processing queue.
    
    Args:
        event_request: The event data to ingest
        persistence: Injected persistence instance
        
    Returns:
        EventResponse: Success response with event ID
        
    Raises:
        HTTPException: If event validation or storage fails
    """
    global _kafka_producer
    
    try:
        # Validate and create Event model
        event = Event(
            timestamp=event_request.timestamp,
            metric=event_request.metric,
            value=event_request.value,
            meta=event_request.meta
        )
        
        # Generate event ID
        import time
        import hashlib
        event_id = hashlib.sha256(
            f"{event.metric}:{event.get_timestamp_float()}:{event.value}".encode()
        ).hexdigest()[:16]
        
        # Serialize and push to queue (Redis)
        event_json = event.json()
        success = persistence.push_raw_event(event_json)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to push event to queue"
            )
        
        # Also publish to Kafka for real-time processing
        if _kafka_producer:
            settings = get_settings()
            try:
                _kafka_producer.produce(
                    settings.kafka_topic,
                    key=event.metric.encode('utf-8'),
                    value=event_json.encode('utf-8'),
                    callback=lambda err, msg: logger.error(f"Kafka error: {err}") if err else None
                )
                _kafka_producer.poll(0)  # Trigger delivery callbacks
                logger.debug(f"Published event {event_id} to Kafka topic {settings.kafka_topic}")
            except Exception as e:
                logger.warning(f"Failed to publish to Kafka: {e}. Event stored in Redis only.")
        
        logger.info(f"Accepted event {event_id} for metric {event.metric}")
        
        return EventResponse(
            success=True,
            message="Event accepted for processing",
            event_id=event_id
        )
    
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid event data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@app.get("/anomalies", response_model=AnomalyResponse)
async def get_anomalies(
    limit: int = 100,
    persistence: Persistence = Depends(get_persistence)
) -> AnomalyResponse:
    """Get recent anomalies from storage.
    
    Args:
        limit: Maximum number of anomalies to return
        persistence: Injected persistence instance
        
    Returns:
        AnomalyResponse: List of recent anomalies
    """
    anomalies: List[Dict[str, Any]] = []
    
    # If using RedisPersistence, scan for anomaly keys
    if isinstance(persistence, RedisPersistence):
        try:
            # Scan for all processed event keys
            pattern = f"{persistence.processed_prefix}anomaly-*"
            keys = persistence.client.keys(pattern)
            
            # Get all anomaly data
            for key in keys:
                data = persistence.client.hgetall(key)
                if data and data.get("is_anomaly") == "True":
                    anomalies.append({
                        "event_id": key.replace(persistence.processed_prefix, ""),
                        "metric": data.get("metric"),
                        "value": float(data.get("value", 0)),
                        "timestamp": float(data.get("timestamp", 0)),
                        "z_score": float(data.get("z_score", 0)),
                        "detected_at": float(data.get("detected_at", 0))
                    })
            
            # Sort by detected_at descending
            anomalies.sort(key=lambda x: x.get("detected_at", 0), reverse=True)
            anomalies = anomalies[:limit]
            
        except Exception as e:
            logger.error(f"Error fetching anomalies from Redis: {e}")
    
    # If using InMemoryPersistence, check processed results
    elif isinstance(persistence, InMemoryPersistence):
        all_processed = persistence.get_all_processed()
        # Filter for anomalies
        for event_id, result in all_processed.items():
            if result.get("is_anomaly"):
                anomalies.append({
                    "event_id": event_id,
                    "metric": result.get("metric"),
                    "value": result.get("value"),
                    "timestamp": result.get("timestamp"),
                    "z_score": result.get("z_score"),
                    "detected_at": result.get("detected_at")
                })
        
        # Sort by detected_at descending
        anomalies.sort(key=lambda x: x.get("detected_at", 0), reverse=True)
        anomalies = anomalies[:limit]
    
    return AnomalyResponse(
        anomalies=anomalies,
        count=len(anomalies)
    )


def parse_csv_file(content: str) -> List[Dict[str, Any]]:
    """Parse CSV file content into event dictionaries."""
    csv_reader = csv.DictReader(io.StringIO(content))
    required_columns = {'metric', 'value', 'timestamp'}
    if not required_columns.issubset(csv_reader.fieldnames or []):
        raise ValueError(f"CSV must contain columns: {', '.join(required_columns)}")
    return list(csv_reader)


def parse_json_file(content: str) -> List[Dict[str, Any]]:
    """Parse JSON file content into event dictionaries.
    
    Supports two formats:
    1. Array of events: [{"metric": "...", "value": ..., "timestamp": "..."}, ...]
    2. Single event: {"metric": "...", "value": ..., "timestamp": "..."}
    """
    data = json.loads(content)
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return [data]
    else:
        raise ValueError("JSON must be an object or array of objects")


def parse_txt_file(content: str) -> List[Dict[str, Any]]:
    """Parse TXT file content into event dictionaries.
    
    Format: metric,value,timestamp (one per line, comma or tab separated)
    Lines starting with # are ignored as comments.
    """
    events = []
    for line_num, line in enumerate(content.strip().split('\n'), start=1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        # Try comma-separated, then tab-separated
        parts = line.split(',') if ',' in line else line.split('\t')
        if len(parts) < 3:
            raise ValueError(f"Line {line_num}: Expected at least 3 fields (metric,value,timestamp)")
        
        event = {
            'metric': parts[0].strip(),
            'value': parts[1].strip(),
            'timestamp': parts[2].strip()
        }
        if len(parts) > 3:  # Optional meta
            event['meta'] = parts[3].strip()
        events.append(event)
    return events


@app.post("/upload", status_code=201)
async def upload_file(
    file: UploadFile = File(...),
    persistence: Persistence = Depends(get_persistence)
) -> Dict[str, Any]:
    """Upload data file with sensor events for batch processing.
    
    Supported Formats:
    - CSV: metric,value,timestamp (with header row)
    - JSON: Array of event objects or single event object
    - TXT: One event per line (comma or tab separated)
    
    Args:
        file: Uploaded file (CSV, JSON, or TXT)
        persistence: Injected persistence instance
        
    Returns:
        Dict: Upload summary with success/error counts
        
    Raises:
        HTTPException: If file format is invalid or unsupported
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Validate file extension
    filename_lower = file.filename.lower()
    if not any(filename_lower.endswith(ext) for ext in ['.csv', '.json', '.txt']):
        raise HTTPException(
            status_code=400,
            detail="Invalid file format. Supported: CSV, JSON, TXT"
        )
    
    try:
        # Read file content
        contents = await file.read()
        file_content = contents.decode('utf-8')
        
        # Parse based on file type
        if filename_lower.endswith('.csv'):
            events_data = parse_csv_file(file_content)
        elif filename_lower.endswith('.json'):
            events_data = parse_json_file(file_content)
        elif filename_lower.endswith('.txt'):
            events_data = parse_txt_file(file_content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        
        # Process events
        success_count = 0
        error_count = 0
        errors = []
        
        for row_num, row in enumerate(events_data, start=1):
            try:
                # Parse meta if present
                meta = {}
                if 'meta' in row and row['meta']:
                    try:
                        if isinstance(row['meta'], str):
                            meta = json.loads(row['meta'])
                        else:
                            meta = row['meta']
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # Create event
                event = Event(
                    metric=str(row['metric']).strip(),
                    value=float(row['value']),
                    timestamp=str(row['timestamp']).strip(),
                    meta=meta
                )
                
                # Store in Redis
                event_json = event.json()
                if not persistence.push_raw_event(event_json):
                    raise Exception("Failed to push to persistence")
                
                # Publish to Kafka
                if _kafka_producer:
                    try:
                        _kafka_producer.produce(
                            get_settings().kafka_topic,
                            key=event.metric.encode('utf-8'),
                            value=event_json.encode('utf-8')
                        )
                    except Exception as e:
                        logger.error(f"Failed to publish event to Kafka: {e}")
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                errors.append({"row": row_num, "error": str(e)})
                logger.warning(f"Error processing row {row_num}: {e}")
        
        # Flush Kafka producer
        if _kafka_producer:
            _kafka_producer.flush()
        
        response = {
            "success": True,
            "message": "File uploaded and processed",
            "filename": file.filename,
            "total_rows": success_count + error_count,
            "success_count": success_count,
            "error_count": error_count
        }
        
        if errors and len(errors) <= 10:  # Only include errors if <= 10
            response["errors"] = errors
        elif error_count > 0:
            response["errors_note"] = f"Too many errors to display ({error_count} total)"
        
        logger.info(
            f"CSV upload completed: {success_count} success, {error_count} errors from {file.filename}"
        )
        
        return response
        
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid file encoding. Please use UTF-8."
        )
    except Exception as e:
        logger.error(f"Error processing CSV upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process file: {str(e)}"
        )


@app.post("/upload/bulk", status_code=201)
async def upload_bulk(
    files: List[UploadFile] = File(...),
    persistence: Persistence = Depends(get_persistence)
) -> Dict[str, Any]:
    """Upload multiple data files for batch processing.
    
    Args:
        files: List of uploaded files (CSV, JSON, or TXT)
        persistence: Injected persistence instance
        
    Returns:
        Dict: Summary with per-file results and totals
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    total_success = 0
    total_errors = 0
    file_results = []
    
    for file in files:
        try:
            # Process each file individually
            result = await upload_file(file, persistence)
            file_results.append({
                "filename": file.filename,
                "success": True,
                "success_count": result["success_count"],
                "error_count": result["error_count"]
            })
            total_success += result["success_count"]
            total_errors += result["error_count"]
        except HTTPException as e:
            file_results.append({
                "filename": file.filename,
                "success": False,
                "error": str(e.detail)
            })
            logger.error(f"Failed to process file {file.filename}: {e.detail}")
        except Exception as e:
            file_results.append({
                "filename": file.filename,
                "success": False,
                "error": str(e)
            })
            logger.error(f"Failed to process file {file.filename}: {e}", exc_info=True)
    
    return {
        "success": True,
        "message": f"Processed {len(files)} files",
        "total_files": len(files),
        "total_success_count": total_success,
        "total_error_count": total_errors,
        "files": file_results
    }


@app.websocket("/ws/anomalies")
async def websocket_endpoint(
    websocket: WebSocket,
    ws_manager: WebSocketManager = Depends(get_ws_manager)
) -> None:
    """WebSocket endpoint for real-time anomaly streaming.
    
    Clients can connect to receive live anomaly notifications as they
    are detected and published to the Redis Pub/Sub channel.
    
    Args:
        websocket: The WebSocket connection
        ws_manager: Injected WebSocket manager
    """
    await ws_manager.connect(websocket)
    
    try:
        # Keep connection alive and handle incoming messages (ping/pong)
        while True:
            # Wait for any client messages (heartbeat, etc.)
            data = await websocket.receive_text()
            
            # Echo back for ping/pong
            if data == "ping":
                await websocket.send_text("pong")
    
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected normally")
        ws_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        ws_manager.disconnect(websocket)


# Override persistence for testing
def override_persistence(persistence: Persistence) -> None:
    """Override the global persistence instance (for testing).
    
    Args:
        persistence: The persistence instance to use
    """
    global _persistence
    _persistence = persistence


# Main entry point for development
if __name__ == "__main__":
    import uvicorn
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run the server
    uvicorn.run(
        "anomaly.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
