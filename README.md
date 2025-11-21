# 🔍 Real-Time Anomaly Detection System

A production-ready, high-performance streaming anomaly detection system using **pure algorithmic approaches** (no ML). Built for real-time event processing with Kafka, Redis, and FastAPI.

[![CI](https://github.com/yourusername/real-time-anomaly-detection-system/workflows/CI/badge.svg)](https://github.com/yourusername/real-time-anomaly-detection-system/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ✨ Features

- 🚀 **Real-time processing** with sub-second latency
- 📊 **Statistical anomaly detection** using Z-scores (no ML required)
- 🔄 **Sliding window statistics** with Welford's algorithm
- 📡 **Live WebSocket dashboard** for real-time monitoring
- 📤 **Drag-and-drop file upload** with multi-format support (CSV, JSON, TXT)
- 📦 **Bulk data ingestion** - upload multiple files simultaneously
- 📧 **Multi-channel alerts** (Redis Pub/Sub, Email)
- 🐳 **Docker-ready** with full orchestration
- 🧪 **Comprehensive tests** with 100+ test cases
- 🎯 **Production-ready** with graceful shutdown and error handling

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌────────────────┐
│  Kafka Producer │────▶│   Ingest     │────▶│     Redis      │
│  (Events)       │     │   Service    │     │  (Raw Queue)   │
└─────────────────┘     └──────────────┘     └────────┬───────┘
                                                       │
                                                       ▼
┌─────────────────┐     ┌──────────────┐     ┌────────────────┐
│   Dashboard     │◀────│  FastAPI     │◀────│   Processor    │
│  (WebSocket)    │     │    (REST)    │     │   Service      │
└─────────────────┘     └──────────────┘     └────────────────┘
         ▲                      │                      │
         │                      │                      ▼
         │              ┌───────▼──────┐      ┌────────────────┐
         └──────────────│ Redis Pub/Sub│◀─────│ Z-Score        │
                        │  (Alerts)    │      │ Detector       │
                        └──────────────┘      └────────────────┘
```

### Component Flow

1. **Ingestion**: Kafka consumer → validates events → Redis queue
2. **Processing**: Processor → sliding window stats → anomaly detection → alerts
3. **Detection**: Z-score algorithm (configurable threshold, typically 3σ)
4. **Alerting**: Redis Pub/Sub broadcast → WebSocket → Email (rate-limited)
5. **API**: REST endpoints for queries, event submission, health checks

## 📁 Project Structure

```
real-time-anomaly-detection-system/
├─ src/anomaly/                    # Core application code
│  ├─ api.py                       # FastAPI REST + WebSocket server
│  ├─ ingest.py                    # Kafka consumer service
│  ├─ processor.py                 # Anomaly detection engine
│  ├─ window.py                    # Sliding window statistics
│  ├─ detector.py                  # Z-score detection algorithms
│  ├─ alerts.py                    # Multi-channel alerting
│  ├─ email_sender.py              # SMTP email notifications
│  ├─ main.py                      # Service orchestrator
│  ├─ models.py                    # Pydantic data models
│  ├─ persistence.py               # Redis/in-memory storage
│  ├─ config.py                    # Configuration management
│  └─ dashboard_static/
│     └─ index.html                # Live monitoring dashboard
├─ src/tests/                      # Comprehensive test suite
│  ├─ test_api.py                  # API endpoint tests (23 tests)
│  ├─ test_ingest.py               # Ingestion tests (15 tests)
│  ├─ test_window.py               # Window statistics tests (28 tests)
│  ├─ test_detector.py             # Detection algorithm tests (24 tests)
│  └─ test_processor_integration.py # Integration tests (16 tests)
├─ examples/
│  ├─ sample_data.csv              # Sample CSV data (15 events)
│  ├─ sample_data.json             # Sample JSON data (10 events)
│  └─ sample_data.txt              # Sample TXT data (11 events)
├─ .github/workflows/
│  └─ ci.yml                       # GitHub Actions CI/CD
├─ docker-compose.yml              # Full stack orchestration
├─ Dockerfile                      # Multi-stage production image
├─ requirements.txt                # Python dependencies
├─ .env.example                    # Configuration template
└─ README.md                       # This file
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **Docker** and **Docker Compose** (or Podman)
- **4GB RAM** minimum for local stack
- **Git** for cloning the repository

### Option 1: Full Stack with Docker (Recommended)

Start all services with one command:

```bash
# Clone the repository
git clone https://github.com/yourusername/real-time-anomaly-detection-system.git
cd real-time-anomaly-detection-system

# Copy environment template
cp .env.example .env

# Start all services (Kafka, Redis, API, Processor, Ingest)
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f api processor
```

**Access the dashboard**: Open http://localhost:8000 in your browser

**API documentation**: http://localhost:8000/docs

### Option 2: Local Development (API + Processor Only)

For faster development without Kafka:

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Redis (required)
docker-compose up -d redis

# Start API server (with hot reload)
uvicorn anomaly.api:app --reload --host 0.0.0.0 --port 8000

# In another terminal, start processor
python -m anomaly.processor

# Send events via API
curl -X POST http://localhost:8000/events/raw \
  -H "Content-Type: application/json" \
  -d '{"timestamp": 1234567890.0, "metric": "cpu_usage", "value": 75.5}'
```

### Option 3: Individual Services

Run specific services as needed:

```bash
# Start infrastructure only
docker-compose up -d kafka redis zookeeper

# Run processor
python -m anomaly.processor

# Run API server
python -m anomaly.main api --port 8000

# Run ingest (if using Kafka)
python -m anomaly.ingest

# Run all services together
python -m anomaly.main all
```

## ⚙️ Configuration

All configuration is managed via environment variables or `.env` file:

```env
# Redis Configuration
REDIS_URL=redis://localhost:6379/0

# Kafka Configuration (optional - only needed for ingest service)
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=events
KAFKA_GROUP_ID=anomaly-ingest-group

# Processing Configuration
WINDOW_SECONDS=300.0                # 5-minute sliding window
ANOMALY_THRESHOLD=3.0               # Z-score threshold (3 sigma)
ANOMALY_MIN_COUNT=5                 # Min samples before detection
SLEEP_ON_EMPTY=0.1                  # Sleep when queue empty (seconds)

# Alerts Configuration
ALERTS_CHANNEL=anomalies            # Redis Pub/Sub channel

# Email Configuration (optional)
EMAIL_ENABLED=false
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
EMAIL_FROM=alerts@example.com
EMAIL_TO=admin@example.com,ops@example.com
EMAIL_RATE_LIMIT_SECONDS=60.0

# Logging
LOG_LEVEL=INFO
ENVIRONMENT=development
```

### Email Setup (Optional)

To enable email alerts:

1. Set `EMAIL_ENABLED=true`
2. Configure SMTP settings (Gmail example):
   - Enable 2FA in Gmail
   - Generate App Password: https://myaccount.google.com/apppasswords
   - Use app password as `SMTP_PASSWORD`
3. Set recipient emails in `EMAIL_TO` (comma-separated)

## 📊 Usage Examples

### Send Events via API

```bash
# Single event
curl -X POST http://localhost:8000/events/raw \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": 1234567890.0,
    "metric": "cpu_usage",
    "value": 85.5,
    "meta": {"host": "server-01"}
  }'

# Bulk events with timestamps
for i in {1..100}; do
  curl -X POST http://localhost:8000/events/raw \
    -H "Content-Type: application/json" \
    -d "{\"timestamp\": $(date +%s), \"metric\": \"cpu_usage\", \"value\": $((50 + RANDOM % 50))}"
  sleep 0.1
done
```

### Upload Data Files (CSV, JSON, TXT)

```bash
# Upload single CSV file
curl -X POST http://localhost:8000/upload \
  -F "file=@examples/sample_data.csv"

# Upload single JSON file
curl -X POST http://localhost:8000/upload \
  -F "file=@data.json"

# Upload multiple files at once (bulk endpoint)
curl -X POST http://localhost:8000/upload/bulk \
  -F "files=@data1.csv" \
  -F "files=@data2.json" \
  -F "files=@data3.txt"
```

**File Formats Supported**:

**CSV** - Comma-separated values with header:
```csv
metric,value,timestamp
temperature_sensor,50.5,2025-01-21T12:00:00Z
pressure_sensor,101.3,2025-01-21T12:00:00Z
```

**JSON** - Array of events or single event:
```json
[
  {"metric": "cpu_usage", "value": 45.2, "timestamp": "2025-01-21T12:00:00Z"},
  {"metric": "memory_usage", "value": 62.3, "timestamp": "2025-01-21T12:01:00Z"}
]
```

**TXT** - One event per line (comma or tab separated):
```txt
network_latency,12.5,2025-01-21T12:00:00Z
disk_io,45.3,2025-01-21T12:00:00Z
```

**Dashboard Upload**: Open http://localhost:8000 and drag files directly into the upload zone for visual feedback and progress tracking.

### Send Events via Kafka

```bash
# Start Kafka producer
docker-compose exec kafka kafka-console-producer \
  --broker-list kafka:29092 \
  --topic events

# Paste JSON events (one per line)
{"timestamp": 1234567890.0, "metric": "memory_usage", "value": 4096.0}
{"timestamp": 1234567891.0, "metric": "memory_usage", "value": 4200.0}
# Press Ctrl+D to exit
```

### Query Recent Anomalies

```bash
# Get anomalies (REST API)
curl http://localhost:8000/anomalies?limit=10

# Health check
curl http://localhost:8000/health
```

### Monitor via Dashboard

1. Open http://localhost:8000 in browser
2. Dashboard auto-connects to WebSocket
3. Watch live anomalies as they're detected
4. Red highlights show Z-score and deviation

### Python Client Example

```python
import httpx
import time

# Send events
def send_metric(metric_name: str, value: float):
    response = httpx.post(
        "http://localhost:8000/events/raw",
        json={
            "timestamp": time.time(),
            "metric": metric_name,
            "value": value
        }
    )
    return response.json()

# Normal values (mean ~50)
for i in range(20):
    send_metric("cpu_usage", 50 + (i % 10))
    time.sleep(1)

# Anomaly! (value 95 is >3σ from mean)
send_metric("cpu_usage", 95)

# Check dashboard - you should see the anomaly!
```

## 🧪 Development

### Run Tests

```bash
# All tests with coverage
pytest src/tests/ -v --cov=anomaly --cov-report=html

# Specific test files
pytest src/tests/test_api.py -v
pytest src/tests/test_window.py -v
pytest src/tests/test_detector.py -v

# With verbose output
pytest src/tests/ -vv -s

# Fast fail on first error
pytest src/tests/ -x
```

### Code Quality

```bash
# Lint with ruff
ruff check src/

# Format with black
black src/

# Type checking with mypy
mypy src/anomaly --ignore-missing-imports
```

### Hot Reload Development

```bash
# API with auto-reload
uvicorn anomaly.api:app --reload --log-level debug

# Run processor with debug logging
LOG_LEVEL=DEBUG python -m anomaly.processor
```

## 📦 Component Details

### 1. API Service (`api.py`)

FastAPI application with REST + WebSocket endpoints.

**Endpoints**:
- `GET /` - Live dashboard with drag-and-drop upload (HTML)
- `GET /health` - Health check with queue size
- `POST /events/raw` - Submit single event
- `POST /upload` - Upload data file (CSV, JSON, TXT)
- `POST /upload/bulk` - Upload multiple files simultaneously
- `GET /anomalies?limit=100` - Query recent anomalies
- `WS /ws/anomalies` - WebSocket stream of live anomalies
- `GET /docs` - Auto-generated API documentation

**Features**:
- CORS middleware for cross-origin requests
- Dependency injection for persistence
- Async Redis Pub/Sub listener
- WebSocket connection management
- Static file serving for dashboard

### 2. Processor Service (`processor.py`)

Main anomaly detection engine.

**Flow**:
1. Poll Redis queue for raw events
2. Append to per-metric sliding window
3. Compute statistics (mean, variance, stddev)
4. Run Z-score detection
5. Broadcast anomalies to alerts channel

**Features**:
- Graceful shutdown (SIGINT/SIGTERM)
- Backpressure handling (sleep when empty)
- Multi-metric window management
- Thread-safe alert broadcasting

### 3. Sliding Window (`window.py`)

Time-based sliding window with online statistics.

**Algorithm**: Welford's method for numerically stable variance
- O(1) append and purge operations
- Maintains `deque[(timestamp, value)]`
- Auto-purges old values beyond window
- Returns `WindowStats(count, mean, variance, stddev)`

### 4. Detector (`detector.py`)

Statistical anomaly detection algorithms.

**Z-Score Detector**:
- Formula: `z = (value - mean) / stddev`
- Detects if `|z| >= threshold` (default: 3.0)
- Requires minimum sample count (default: 5)

**Percentile Detector** (for non-normal distributions):
- Detects values beyond p99 or below p1
- Useful for skewed distributions

### 5. Alerts (`alerts.py`)

Multi-channel alert broadcasting.

**Channels**:
- **Redis Pub/Sub**: Real-time broadcast to all listeners
- **Email**: Rate-limited SMTP notifications

**Features**:
- Token bucket rate limiting for emails
- Background thread queue for non-blocking send
- Configurable channels and recipients

### 6. Persistence (`persistence.py`)

Dual-backend storage abstraction.

**RedisPersistence** (Production):
- Raw events: Redis list with `RPUSH`/`BLPOP`
- Processed results: Redis hashes
- Pub/Sub for alerts

**InMemoryPersistence** (Testing):
- Thread-safe Python `Queue`
- Dictionary for results
- Fast, no external dependencies

## 🔍 Monitoring & Operations

### Health Checks

```bash
# API health
curl http://localhost:8000/health

# Docker service status
docker-compose ps

# View logs
docker-compose logs -f processor
docker-compose logs -f api
```

### Redis Monitoring

```bash
# Connect to Redis
docker-compose exec redis redis-cli

# Check queue size
LLEN raw_events

# Monitor Pub/Sub channel
SUBSCRIBE anomalies

# View processed events
KEYS event:*
HGETALL event:abc123
```

### Kafka Monitoring (if using ingest)

```bash
# Consumer lag
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group anomaly-ingest-group

# Topic messages
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events --from-beginning
```

### Metrics & Observability

**Current metrics** (logged):
- Events processed per second
- Anomalies detected per window
- Queue depth and backpressure
- WebSocket connection count

## 🔒 Security & Production Notes

### Current State (Development)

⚠️ **Not production-ready without modifications:**

1. **Authentication**: No auth on API endpoints
2. **Rate Limiting**: No request throttling
3. **HTTPS**: Running on HTTP only
4. **CORS**: Allow-all origins (`*`)
5. **Secrets**: Environment variables in plain text

### Production Checklist

- [ ] Add API authentication (JWT, OAuth2, API keys)
- [ ] Implement rate limiting (per IP, per user)
- [ ] Enable HTTPS with TLS certificates
- [ ] Restrict CORS to allowed origins
- [ ] Use secrets manager (AWS Secrets Manager, Vault)
- [ ] Configure firewall rules and network policies
- [ ] Enable audit logging
- [ ] Set resource limits (CPU, memory, connections)
- [ ] Configure backup and disaster recovery
- [ ] Implement horizontal scaling (multiple processor replicas)
- [ ] Add distributed coordination (Redis Cluster, Kafka partitions)
- [ ] Set up monitoring and alerting (PagerDuty, Opsgenie)

### Recommended Production Stack

```
Load Balancer (HTTPS) → API (3 replicas)
                         ↓
                      Redis Cluster (3 nodes)
                         ↓
                      Processor (5 replicas)
                         ↓
                      Kafka Cluster (3 brokers)
```

## 🐛 Troubleshooting

### Dashboard shows "Disconnected"

```bash
# Check API is running
curl http://localhost:8000/health

# Check Redis is accessible
docker-compose exec redis redis-cli ping

# Check WebSocket in browser console
# Should see: "WebSocket connected"
```

### No anomalies detected

```bash
# Verify processor is running
docker-compose logs processor

# Check if events are in queue
docker-compose exec redis redis-cli LLEN raw_events

# Send test anomaly (high value after normal values)
for i in {1..10}; do
  curl -X POST http://localhost:8000/events/raw -d '{"timestamp": '$(date +%s)', "metric": "test", "value": 50}'
done
curl -X POST http://localhost:8000/events/raw -d '{"timestamp": '$(date +%s)', "metric": "test", "value": 200}'
```

### Email alerts not sending

```bash
# Check email is enabled
echo $EMAIL_ENABLED  # Should be "true"

# Test SMTP connection
python -m anomaly.email_sender

# Check logs for errors
docker-compose logs processor | grep -i email
```

---

**Built with ❤️ for real-time data processing**
