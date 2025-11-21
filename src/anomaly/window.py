"""Sliding window implementation for time-series statistics.

Provides efficient append and purge operations using a deque, with
accurate online statistics computation using Welford's algorithm.
"""

import logging
from collections import deque, namedtuple
from typing import Deque, Tuple


logger = logging.getLogger(__name__)


# Named tuple for window statistics
WindowStats = namedtuple('WindowStats', ['count', 'mean', 'variance', 'stddev'])


class SlidingWindow:
    """Time-based sliding window for computing statistics.
    
    Maintains a window of (timestamp, value) pairs and provides
    efficient statistics computation using Welford's online algorithm.
    
    The window automatically excludes data points older than the
    specified window duration when computing statistics.
    """
    
    def __init__(self, window_seconds: float) -> None:
        """Initialize a sliding window.
        
        Args:
            window_seconds: Window duration in seconds. Only data points
                within this time range from the current time are included
                in statistics calculations.
                
        Raises:
            ValueError: If window_seconds is not positive
        """
        if window_seconds <= 0:
            raise ValueError(f"window_seconds must be positive, got {window_seconds}")
        
        self.window_seconds = window_seconds
        self.data: Deque[Tuple[float, float]] = deque()
        
        # Welford's algorithm state for online statistics
        self._count = 0
        self._mean = 0.0
        self._m2 = 0.0  # Sum of squared differences from mean
        
        logger.debug(f"Initialized SlidingWindow with {window_seconds}s duration")
    
    def append(self, timestamp: float, value: float) -> None:
        """Add a new data point to the window.
        
        Args:
            timestamp: Unix epoch timestamp in seconds
            value: Numeric value to add
            
        Raises:
            ValueError: If timestamp or value is not finite
        """
        # Validate inputs
        if not (-1e308 <= timestamp <= 1e308):
            raise ValueError(f"Invalid timestamp: {timestamp}")
        if not (-1e308 <= value <= 1e308):
            raise ValueError(f"Invalid value: {value}")
        
        self.data.append((timestamp, value))
        
        # Update online statistics using Welford's algorithm
        self._count += 1
        delta = value - self._mean
        self._mean += delta / self._count
        delta2 = value - self._mean
        self._m2 += delta * delta2
        
        logger.debug(
            f"Appended: timestamp={timestamp}, value={value}, "
            f"count={self._count}, mean={self._mean:.2f}"
        )
    
    def purge_old(self, current_time: float) -> None:
        """Remove data points older than the window duration.
        
        Removes all data points with timestamps older than
        (current_time - window_seconds) from the left side of the deque.
        Updates online statistics accordingly.
        
        Args:
            current_time: Current Unix epoch timestamp in seconds
        """
        cutoff_time = current_time - self.window_seconds
        purged_count = 0
        
        # Remove old entries from the left (oldest first)
        while self.data and self.data[0][0] < cutoff_time:
            timestamp, value = self.data.popleft()
            
            # Update online statistics (removing a value)
            if self._count > 0:
                delta = value - self._mean
                self._mean -= delta / self._count if self._count > 1 else self._mean
                delta2 = value - self._mean
                self._m2 -= delta * delta2
                self._count -= 1
                
                # Handle numerical errors (M2 should never be negative)
                if self._m2 < 0:
                    self._m2 = 0.0
            
            purged_count += 1
        
        if purged_count > 0:
            logger.debug(
                f"Purged {purged_count} old entries, "
                f"remaining count={self._count}"
            )
    
    def stats(self, current_time: float) -> WindowStats:
        """Compute statistics for data within the window.
        
        First purges old data, then computes statistics using the
        maintained Welford's algorithm state.
        
        Args:
            current_time: Current Unix epoch timestamp in seconds
            
        Returns:
            WindowStats: Named tuple with count, mean, variance, and stddev
        """
        # Purge old data first
        self.purge_old(current_time)
        
        # Compute statistics from Welford's state
        count = self._count
        mean = self._mean if count > 0 else 0.0
        
        # Variance and stddev
        if count < 2:
            variance = 0.0
            stddev = 0.0
        else:
            variance = self._m2 / (count - 1)  # Sample variance (Bessel's correction)
            stddev = variance ** 0.5
        
        stats = WindowStats(
            count=count,
            mean=mean,
            variance=variance,
            stddev=stddev
        )
        
        logger.debug(
            f"Window stats: count={count}, mean={mean:.2f}, "
            f"variance={variance:.2f}, stddev={stddev:.2f}"
        )
        
        return stats
    
    def clear(self) -> None:
        """Clear all data from the window and reset statistics."""
        self.data.clear()
        self._count = 0
        self._mean = 0.0
        self._m2 = 0.0
        logger.debug("Window cleared")
    
    def __len__(self) -> int:
        """Return the current number of data points in the window."""
        return len(self.data)
    
    def __repr__(self) -> str:
        """Return string representation of the window."""
        return (
            f"SlidingWindow(window_seconds={self.window_seconds}, "
            f"count={self._count}, mean={self._mean:.2f})"
        )


class MultiMetricWindow:
    """Manager for multiple sliding windows, one per metric.
    
    Provides convenient access to per-metric windows with automatic
    creation and lifecycle management.
    """
    
    def __init__(self, window_seconds: float) -> None:
        """Initialize multi-metric window manager.
        
        Args:
            window_seconds: Default window duration for all metrics
        """
        self.window_seconds = window_seconds
        self.windows: dict[str, SlidingWindow] = {}
        logger.info(f"Initialized MultiMetricWindow with {window_seconds}s windows")
    
    def get_window(self, metric: str) -> SlidingWindow:
        """Get or create a window for the specified metric.
        
        Args:
            metric: Metric name
            
        Returns:
            SlidingWindow: Window for the metric
        """
        if metric not in self.windows:
            self.windows[metric] = SlidingWindow(self.window_seconds)
            logger.debug(f"Created new window for metric: {metric}")
        return self.windows[metric]
    
    def append(self, metric: str, timestamp: float, value: float) -> None:
        """Append a value to the window for the specified metric.
        
        Args:
            metric: Metric name
            timestamp: Unix epoch timestamp
            value: Numeric value
        """
        window = self.get_window(metric)
        window.append(timestamp, value)
    
    def stats(self, metric: str, current_time: float) -> WindowStats:
        """Get statistics for the specified metric.
        
        Args:
            metric: Metric name
            current_time: Current Unix epoch timestamp
            
        Returns:
            WindowStats: Statistics for the metric window
        """
        window = self.get_window(metric)
        return window.stats(current_time)
    
    def purge_all(self, current_time: float) -> None:
        """Purge old data from all metric windows.
        
        Args:
            current_time: Current Unix epoch timestamp
        """
        for metric, window in self.windows.items():
            window.purge_old(current_time)
    
    def clear_all(self) -> None:
        """Clear all metric windows."""
        for window in self.windows.values():
            window.clear()
        logger.info("Cleared all metric windows")
    
    def get_metrics(self) -> list[str]:
        """Get list of all tracked metrics.
        
        Returns:
            list[str]: Sorted list of metric names
        """
        return sorted(self.windows.keys())
    
    def __len__(self) -> int:
        """Return the number of tracked metrics."""
        return len(self.windows)
    
    def __repr__(self) -> str:
        """Return string representation of the manager."""
        return (
            f"MultiMetricWindow(window_seconds={self.window_seconds}, "
            f"metrics={len(self.windows)})"
        )
