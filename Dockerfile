# Dockerfile for Real-Time Anomaly Detection System
# Multi-stage build for optimized production image

FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Production stage
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install runtime dependencies (curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY src/ ./src/
COPY examples/ ./examples/

# Create non-root user for security
RUN useradd -m -u 1000 appuser && \
    cp -r /root/.local /home/appuser/.local && \
    chown -R appuser:appuser /app /home/appuser/.local

USER appuser

# Make sure scripts in .local are usable
ENV PATH=/home/appuser/.local/bin:$PATH

# Set Python path to include src directory
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Expose port for API
EXPOSE 8000

# Healthcheck for API service
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command runs the API server
# Override with docker-compose for other services
CMD ["uvicorn", "anomaly.api:app", "--host", "0.0.0.0", "--port", "8000"]
