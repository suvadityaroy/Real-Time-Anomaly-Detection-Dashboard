"""Persistence layer for event storage and retrieval.

Provides abstractions for storing raw events and processed results.
Includes Redis-based production implementation and in-memory testing backend.
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from queue import Queue, Empty
import threading
import json

import redis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError


logger = logging.getLogger(__name__)


class Persistence(ABC):
    """Abstract base class for persistence backends.
    
    Defines the interface that all persistence implementations must follow.
    Supports pushing raw events to a queue, popping them for processing,
    and storing/retrieving processed results.
    """
    
    @abstractmethod
    def push_raw_event(self, event_json: str) -> bool:
        """Push a raw event to the processing queue.
        
        Args:
            event_json: JSON-serialized event string
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def pop_raw_event(self, timeout: int = 1) -> Optional[str]:
        """Pop a raw event from the processing queue.
        
        Blocks for up to `timeout` seconds waiting for an event.
        
        Args:
            timeout: Maximum seconds to wait for an event
            
        Returns:
            Optional[str]: JSON-serialized event string, or None if timeout
        """
        pass
    
    @abstractmethod
    def save_processed(self, event_id: str, result_dict: Dict[str, Any]) -> bool:
        """Save a processed event result.
        
        Args:
            event_id: Unique event identifier
            result_dict: Dictionary containing processing results
            
        Returns:
            bool: True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_processed(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a processed event result.
        
        Args:
            event_id: Unique event identifier
            
        Returns:
            Optional[Dict[str, Any]]: Result dictionary, or None if not found
        """
        pass
    
    @abstractmethod
    def get_queue_size(self) -> int:
        """Get the current size of the raw events queue.
        
        Returns:
            int: Number of events waiting to be processed
        """
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all stored data (for testing)."""
        pass


class RedisPersistence(Persistence):
    """Redis-based persistence backend for production use.
    
    Uses Redis lists for the event queue (RPUSH/BLPOP) and hashes
    for storing processed results. Provides durability and scalability.
    """
    
    def __init__(
        self,
        redis_url: str,
        raw_events_key: str = "raw_events",
        processed_prefix: str = "event:"
    ) -> None:
        """Initialize Redis persistence.
        
        Args:
            redis_url: Redis connection URL (e.g., redis://localhost:6379/0)
            raw_events_key: Redis key for the raw events list
            processed_prefix: Prefix for processed event hash keys
            
        Raises:
            RedisConnectionError: If connection to Redis fails
        """
        self.redis_url = redis_url
        self.raw_events_key = raw_events_key
        self.processed_prefix = processed_prefix
        
        try:
            self.client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            # Test connection
            self.client.ping()
            logger.info(f"Connected to Redis at {redis_url}")
        except (RedisConnectionError, RedisError) as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def push_raw_event(self, event_json: str) -> bool:
        """Push a raw event to Redis list.
        
        Uses RPUSH to append to the end of the list.
        
        Args:
            event_json: JSON-serialized event string
            
        Returns:
            bool: True if successful, False on error
        """
        try:
            self.client.rpush(self.raw_events_key, event_json)
            logger.debug(f"Pushed event to {self.raw_events_key}")
            return True
        except RedisError as e:
            logger.error(f"Failed to push event to Redis: {e}")
            return False
    
    def pop_raw_event(self, timeout: int = 1) -> Optional[str]:
        """Pop a raw event from Redis list.
        
        Uses BLPOP to block until an event is available or timeout.
        
        Args:
            timeout: Maximum seconds to wait (0 = wait forever)
            
        Returns:
            Optional[str]: JSON-serialized event string, or None if timeout
        """
        try:
            result = self.client.blpop([self.raw_events_key], timeout=timeout)  # type: ignore
            if result:
                _, event_json = result  # BLPOP returns (key, value)
                logger.debug(f"Popped event from {self.raw_events_key}")
                return event_json  # type: ignore
            return None
        except RedisError as e:
            logger.error(f"Failed to pop event from Redis: {e}")
            return None
    
    def save_processed(self, event_id: str, result_dict: Dict[str, Any]) -> bool:
        """Save processed result to Redis hash.
        
        Args:
            event_id: Unique event identifier
            result_dict: Dictionary containing processing results
            
        Returns:
            bool: True if successful, False on error
        """
        key = f"{self.processed_prefix}{event_id}"
        try:
            # Convert nested dicts/lists to JSON strings for Redis
            serialized_dict = {
                k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                for k, v in result_dict.items()
            }
            self.client.hset(key, mapping=serialized_dict)
            logger.debug(f"Saved processed result to {key}")
            return True
        except RedisError as e:
            logger.error(f"Failed to save processed result: {e}")
            return False
    
    def get_processed(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed result from Redis hash.
        
        Args:
            event_id: Unique event identifier
            
        Returns:
            Optional[Dict[str, Any]]: Result dictionary, or None if not found
        """
        key = f"{self.processed_prefix}{event_id}"
        try:
            result = self.client.hgetall(key)  # type: ignore
            if not result:
                return None
            
            # Deserialize JSON strings back to objects
            deserialized: Dict[str, Any] = {}
            for k, v in result.items():  # type: ignore
                try:
                    deserialized[k] = json.loads(v)  # type: ignore
                except json.JSONDecodeError:
                    deserialized[k] = v
            
            logger.debug(f"Retrieved processed result from {key}")
            return deserialized
        except RedisError as e:
            logger.error(f"Failed to get processed result: {e}")
            return None
    
    def get_queue_size(self) -> int:
        """Get the size of the raw events queue.
        
        Returns:
            int: Number of events in the queue, or 0 on error
        """
        try:
            size: int = self.client.llen(self.raw_events_key)  # type: ignore
            return size
        except RedisError as e:
            logger.error(f"Failed to get queue size: {e}")
            return 0
    
    def clear(self) -> None:
        """Clear all data (use with caution!).
        
        Deletes the raw events queue. Does not delete processed results
        to avoid accidentally losing data.
        """
        try:
            self.client.delete(self.raw_events_key)
            logger.warning(f"Cleared {self.raw_events_key}")
        except RedisError as e:
            logger.error(f"Failed to clear data: {e}")
    
    def close(self) -> None:
        """Close the Redis connection."""
        try:
            self.client.close()
            logger.info("Redis connection closed")
        except RedisError as e:
            logger.error(f"Error closing Redis connection: {e}")


class InMemoryPersistence(Persistence):
    """In-memory persistence backend for testing.
    
    Uses Python Queue for thread-safe event queuing and a dictionary
    for storing processed results. No durability, but fast and simple.
    """
    
    def __init__(self, max_queue_size: int = 10000) -> None:
        """Initialize in-memory persistence.
        
        Args:
            max_queue_size: Maximum size of the event queue
        """
        self.raw_events: Queue[str] = Queue(maxsize=max_queue_size)
        self.processed_results: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        logger.info(f"Initialized InMemoryPersistence with max queue size {max_queue_size}")
    
    def push_raw_event(self, event_json: str) -> bool:
        """Push a raw event to the in-memory queue.
        
        Args:
            event_json: JSON-serialized event string
            
        Returns:
            bool: True if successful, False if queue is full
        """
        try:
            self.raw_events.put(event_json, block=False)
            logger.debug("Pushed event to in-memory queue")
            return True
        except Exception as e:
            logger.error(f"Failed to push event to queue: {e}")
            return False
    
    def pop_raw_event(self, timeout: int = 1) -> Optional[str]:
        """Pop a raw event from the in-memory queue.
        
        Args:
            timeout: Maximum seconds to wait for an event
            
        Returns:
            Optional[str]: JSON-serialized event string, or None if timeout
        """
        try:
            event_json = self.raw_events.get(timeout=timeout)
            logger.debug("Popped event from in-memory queue")
            return event_json
        except Empty:
            return None
        except Exception as e:
            logger.error(f"Failed to pop event from queue: {e}")
            return None
    
    def save_processed(self, event_id: str, result_dict: Dict[str, Any]) -> bool:
        """Save processed result to in-memory dictionary.
        
        Args:
            event_id: Unique event identifier
            result_dict: Dictionary containing processing results
            
        Returns:
            bool: True (always successful in memory)
        """
        with self.lock:
            self.processed_results[event_id] = result_dict.copy()
            logger.debug(f"Saved processed result for event {event_id}")
            return True
    
    def get_processed(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed result from in-memory dictionary.
        
        Args:
            event_id: Unique event identifier
            
        Returns:
            Optional[Dict[str, Any]]: Result dictionary, or None if not found
        """
        with self.lock:
            result = self.processed_results.get(event_id)
            if result:
                logger.debug(f"Retrieved processed result for event {event_id}")
                return result.copy()
            return None
    
    def get_queue_size(self) -> int:
        """Get the size of the in-memory queue.
        
        Returns:
            int: Number of events in the queue
        """
        return self.raw_events.qsize()
    
    def clear(self) -> None:
        """Clear all in-memory data."""
        with self.lock:
            # Empty the queue
            while not self.raw_events.empty():
                try:
                    self.raw_events.get_nowait()
                except Empty:
                    break
            # Clear processed results
            self.processed_results.clear()
            logger.info("Cleared all in-memory data")
    
    def get_all_processed(self) -> Dict[str, Dict[str, Any]]:
        """Get all processed results (testing helper).
        
        Returns:
            Dict[str, Dict[str, Any]]: All processed results
        """
        with self.lock:
            return {k: v.copy() for k, v in self.processed_results.items()}
