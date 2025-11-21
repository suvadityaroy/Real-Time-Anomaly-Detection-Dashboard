"""Main orchestrator for running multiple services.

Provides an entrypoint that can start the processor, API server,
and other components either together or separately.
"""

import logging
import sys
import signal
import threading
import time
from typing import Optional
import multiprocessing as mp

# Import service components
from anomaly.config import get_settings
from anomaly.processor import Processor
from anomaly.persistence import RedisPersistence


logger = logging.getLogger(__name__)


class ServiceOrchestrator:
    """Orchestrates multiple services for local development and demo."""
    
    def __init__(self):
        """Initialize the orchestrator."""
        self.settings = get_settings()
        self.processes: list = []
        self.threads: list = []
        self._shutdown_event = threading.Event()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown_event.set()
    
    def start_processor(self, in_thread: bool = True) -> Optional[threading.Thread]:
        """Start the anomaly processor service.
        
        Args:
            in_thread: If True, run in a separate thread. Otherwise run in current thread.
            
        Returns:
            Optional[threading.Thread]: The thread object if in_thread=True, else None
        """
        def run_processor():
            try:
                logger.info("Starting Processor service...")
                persistence = RedisPersistence(
                    redis_url=self.settings.redis_url,
                    raw_events_key=self.settings.redis_raw_events_key,
                    processed_prefix=self.settings.redis_processed_prefix
                )
                
                processor = Processor(
                    persistence=persistence,
                    window_seconds=self.settings.window_seconds,
                    anomaly_threshold=self.settings.anomaly_threshold,
                    min_count=self.settings.anomaly_min_count,
                    sleep_on_empty=self.settings.sleep_on_empty
                )
                
                processor.run()
            except Exception as e:
                logger.error(f"Processor service error: {e}", exc_info=True)
        
        if in_thread:
            thread = threading.Thread(target=run_processor, name="processor", daemon=True)
            thread.start()
            self.threads.append(thread)
            logger.info("Processor service started in thread")
            return thread
        else:
            run_processor()
            return None
    
    def start_api(self, host: str = "0.0.0.0", port: int = 8000) -> None:
        """Start the FastAPI server using uvicorn.
        
        Args:
            host: Host to bind to
            port: Port to bind to
        """
        try:
            import uvicorn
            logger.info(f"Starting API server on {host}:{port}...")
            
            # Configure uvicorn logging
            log_config = uvicorn.config.LOGGING_CONFIG
            log_config["formatters"]["default"]["fmt"] = self.settings.log_format
            log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(client_addr)s - %(request_line)s - %(status_code)s"
            
            # Run API server
            uvicorn.run(
                "anomaly.api:app",
                host=host,
                port=port,
                log_level=self.settings.log_level.lower(),
                log_config=log_config,
                access_log=True
            )
        except Exception as e:
            logger.error(f"API server error: {e}", exc_info=True)
    
    def start_all(self) -> None:
        """Start all services (processor in thread, API in main thread)."""
        logger.info("Starting all services...")
        
        # Start processor in background thread
        self.start_processor(in_thread=True)
        
        # Give processor time to initialize
        time.sleep(2)
        
        # Start API server in main thread (blocking)
        self.start_api()
    
    def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        logger.info("Services running. Press Ctrl+C to stop.")
        self._shutdown_event.wait()
        logger.info("Shutting down services...")


def run_processor_only() -> None:
    """Run only the processor service."""
    # Configure logging
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format=settings.log_format
    )
    
    logger.info("Starting Processor service (standalone mode)")
    
    orchestrator = ServiceOrchestrator()
    orchestrator.start_processor(in_thread=False)


def run_api_only(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run only the API server.
    
    Args:
        host: Host to bind to
        port: Port to bind to
    """
    # Configure logging
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format=settings.log_format
    )
    
    logger.info("Starting API server (standalone mode)")
    
    orchestrator = ServiceOrchestrator()
    orchestrator.start_api(host=host, port=port)


def run_all() -> None:
    """Run all services together."""
    # Configure logging
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format=settings.log_format
    )
    
    logger.info("Starting all services")
    
    orchestrator = ServiceOrchestrator()
    orchestrator.start_all()


def main() -> None:
    """Main entry point with command-line argument parsing."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Real-Time Anomaly Detection System - Service Orchestrator"
    )
    parser.add_argument(
        "service",
        choices=["all", "processor", "api"],
        help="Service to run: all (processor + api), processor only, or api only"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="API server host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API server port (default: 8000)"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override log level from environment"
    )
    
    args = parser.parse_args()
    
    # Override log level if specified
    if args.log_level:
        import os
        os.environ["LOG_LEVEL"] = args.log_level
    
    # Run requested service
    try:
        if args.service == "all":
            run_all()
        elif args.service == "processor":
            run_processor_only()
        elif args.service == "api":
            run_api_only(host=args.host, port=args.port)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
