"""Configuration management for the anomaly detection system.

Provides typed settings using Pydantic BaseSettings with support for
environment variables and .env files.
"""

import os
from typing import Optional
try:
    from pydantic import BaseSettings, Field, validator  # type: ignore
except ImportError:
    from pydantic_settings import BaseSettings  # type: ignore
    from pydantic import Field, validator  # type: ignore


class Settings(BaseSettings):  # type: ignore
    """Application configuration settings.
    
    All settings can be overridden via environment variables or .env file.
    """
    
    # Kafka Configuration
    kafka_broker: str = Field(
        default="localhost:9092",
        env="KAFKA_BROKER",
        description="Kafka broker address(es)"
    )
    kafka_topic: str = Field(
        default="events",
        env="KAFKA_TOPIC",
        description="Kafka topic to consume events from"
    )
    kafka_group_id: str = Field(
        default="anomaly-ingest-group",
        env="KAFKA_GROUP_ID",
        description="Kafka consumer group ID"
    )
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        env="KAFKA_AUTO_OFFSET_RESET",
        description="Kafka consumer offset reset strategy"
    )
    kafka_session_timeout_ms: int = Field(
        default=10000,
        env="KAFKA_SESSION_TIMEOUT_MS",
        description="Kafka consumer session timeout in milliseconds"
    )
    
    # Redis Configuration
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        env="REDIS_URL",
        description="Redis connection URL"
    )
    redis_raw_events_key: str = Field(
        default="raw_events",
        env="REDIS_RAW_EVENTS_KEY",
        description="Redis list key for raw events queue"
    )
    redis_processed_prefix: str = Field(
        default="event:",
        env="REDIS_PROCESSED_PREFIX",
        description="Redis key prefix for processed event results"
    )
    
    # Processing Configuration
    batch_size: int = Field(
        default=100,
        env="BATCH_SIZE",
        description="Number of events to process in a batch",
        gt=0
    )
    max_queue_size: int = Field(
        default=10000,
        env="MAX_QUEUE_SIZE",
        description="Maximum size of in-memory processing queue",
        gt=0
    )
    poll_timeout: float = Field(
        default=1.0,
        env="POLL_TIMEOUT",
        description="Kafka consumer poll timeout in seconds",
        gt=0
    )
    
    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT",
        description="Log message format"
    )
    
    # Application Configuration
    app_name: str = Field(
        default="anomaly-detection",
        env="APP_NAME",
        description="Application name"
    )
    environment: str = Field(
        default="development",
        env="ENVIRONMENT",
        description="Environment (development, staging, production)"
    )
    
    # Processor Configuration
    window_seconds: float = Field(
        default=300.0,
        env="WINDOW_SECONDS",
        description="Sliding window duration in seconds",
        gt=0
    )
    anomaly_threshold: float = Field(
        default=3.0,
        env="ANOMALY_THRESHOLD",
        description="Z-score threshold for anomaly detection",
        gt=0
    )
    anomaly_min_count: int = Field(
        default=5,
        env="ANOMALY_MIN_COUNT",
        description="Minimum window count before detecting anomalies",
        gt=1
    )
    sleep_on_empty: float = Field(
        default=0.1,
        env="SLEEP_ON_EMPTY",
        description="Sleep duration when event queue is empty (seconds)",
        gt=0
    )
    
    # Alerts Configuration
    alerts_channel: str = Field(
        default="anomalies",
        env="ALERTS_CHANNEL",
        description="Redis Pub/Sub channel for alerts"
    )
    
    # Email Configuration
    email_enabled: bool = Field(
        default=False,
        env="EMAIL_ENABLED",
        description="Enable email notifications"
    )
    smtp_host: Optional[str] = Field(
        default=None,
        env="SMTP_HOST",
        description="SMTP server hostname"
    )
    smtp_port: int = Field(
        default=587,
        env="SMTP_PORT",
        description="SMTP server port"
    )
    smtp_user: Optional[str] = Field(
        default=None,
        env="SMTP_USER",
        description="SMTP username"
    )
    smtp_password: Optional[str] = Field(
        default=None,
        env="SMTP_PASSWORD",
        description="SMTP password"
    )
    email_from: Optional[str] = Field(
        default=None,
        env="EMAIL_FROM",
        description="From email address"
    )
    email_to: str = Field(
        default="",
        env="EMAIL_TO",
        description="Comma-separated list of recipient emails"
    )
    email_rate_limit_seconds: float = Field(
        default=60.0,
        env="EMAIL_RATE_LIMIT_SECONDS",
        description="Minimum seconds between emails",
        gt=0
    )
    
    @validator("log_level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid Python logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper
    
    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of the expected values."""
        valid_envs = {"development", "staging", "production", "testing"}
        v_lower = v.lower()
        if v_lower not in valid_envs:
            raise ValueError(f"Invalid environment: {v}. Must be one of {valid_envs}")
        return v_lower
    
    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create the global settings instance.
    
    This function implements a singleton pattern for settings,
    ensuring only one instance is created and reused.
    
    Returns:
        Settings: The application settings instance
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Force reload settings from environment/file.
    
    Useful for testing or when environment variables change at runtime.
    
    Returns:
        Settings: The newly loaded settings instance
    """
    global _settings
    _settings = Settings()
    return _settings


def load_settings_from_file(filepath: str) -> Settings:
    """Load settings from a specific file path.
    
    Args:
        filepath: Path to the .env file to load
        
    Returns:
        Settings: Settings loaded from the specified file
        
    Raises:
        FileNotFoundError: If the specified file doesn't exist
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Settings file not found: {filepath}")
    
    # Temporarily set the env file location
    original_env_file = Settings.Config.env_file
    try:
        Settings.Config.env_file = filepath
        settings = Settings()
        return settings
    finally:
        Settings.Config.env_file = original_env_file
