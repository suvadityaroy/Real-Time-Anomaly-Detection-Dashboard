"""Anomaly detection algorithms.

Provides statistical anomaly detection using z-score and other methods.
No machine learning - pure algorithmic approaches.
"""

import logging
import time
from typing import Optional, Dict, Any

from anomaly.window import SlidingWindow, WindowStats


logger = logging.getLogger(__name__)


class ZScoreDetector:
    """Z-score based anomaly detector.
    
    Detects anomalies by computing the z-score (number of standard
    deviations from the mean) for each value. Values with |z| >= threshold
    are flagged as anomalies.
    
    This is a simple but effective statistical method that works well
    for normally-distributed data.
    """
    
    def __init__(self, threshold: float = 3.0, min_count: int = 5) -> None:
        """Initialize z-score detector.
        
        Args:
            threshold: Z-score threshold for anomaly detection.
                Typical values are 2.0-3.0 (2-3 standard deviations).
            min_count: Minimum number of data points required in the
                window before anomalies can be detected. This prevents
                false positives during cold start.
                
        Raises:
            ValueError: If threshold or min_count are invalid
        """
        if threshold <= 0:
            raise ValueError(f"threshold must be positive, got {threshold}")
        if min_count < 2:
            raise ValueError(f"min_count must be >= 2, got {min_count}")
        
        self.threshold = threshold
        self.min_count = min_count
        
        # Statistics tracking
        self.total_checked = 0
        self.total_anomalies = 0
        
        logger.info(
            f"Initialized ZScoreDetector: threshold={threshold}, "
            f"min_count={min_count}"
        )
    
    def detect(
        self,
        metric: str,
        window: SlidingWindow,
        value: float,
        timestamp: float
    ) -> Optional[Dict[str, Any]]:
        """Detect if a value is an anomaly based on window statistics.
        
        Computes z-score: z = (value - mean) / stddev
        
        Args:
            metric: Name of the metric being checked
            window: Sliding window containing historical data
            value: Current value to check for anomaly
            timestamp: Timestamp of the current value
            
        Returns:
            Optional[Dict]: Anomaly details if detected, None otherwise.
                Dictionary contains: metric, value, mean, stddev, z_score,
                threshold, timestamp, is_anomaly (always True)
        """
        self.total_checked += 1
        
        # Get window statistics
        stats = window.stats(timestamp)
        
        # Check if we have enough data
        if stats.count < self.min_count:
            logger.debug(
                f"Skipping anomaly detection for {metric}: "
                f"insufficient data ({stats.count} < {self.min_count})"
            )
            return None
        
        # Handle zero standard deviation
        if stats.stddev == 0:
            logger.debug(
                f"Skipping anomaly detection for {metric}: "
                f"zero standard deviation (constant values)"
            )
            return None
        
        # Compute z-score
        z_score = (value - stats.mean) / stats.stddev
        
        # Check if anomaly
        is_anomaly = abs(z_score) >= self.threshold
        
        if is_anomaly:
            self.total_anomalies += 1
            anomaly_dict = {
                'metric': metric,
                'value': value,
                'mean': stats.mean,
                'stddev': stats.stddev,
                'variance': stats.variance,
                'z_score': z_score,
                'threshold': self.threshold,
                'timestamp': timestamp,
                'is_anomaly': True,
                'detection_method': 'z-score',
                'window_count': stats.count,
                'detected_at': time.time()
            }
            
            logger.warning(
                f"ANOMALY DETECTED: {metric}={value:.2f}, "
                f"mean={stats.mean:.2f}, stddev={stats.stddev:.2f}, "
                f"z={z_score:.2f} (threshold={self.threshold})"
            )
            
            return anomaly_dict
        else:
            logger.debug(
                f"Normal value for {metric}: value={value:.2f}, "
                f"mean={stats.mean:.2f}, z={z_score:.2f}"
            )
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detector statistics.
        
        Returns:
            Dict: Statistics including total_checked and total_anomalies
        """
        return {
            'total_checked': self.total_checked,
            'total_anomalies': self.total_anomalies,
            'threshold': self.threshold,
            'min_count': self.min_count
        }
    
    def reset_stats(self) -> None:
        """Reset detector statistics."""
        self.total_checked = 0
        self.total_anomalies = 0
        logger.info("Detector statistics reset")
    
    def __repr__(self) -> str:
        """Return string representation of the detector."""
        return (
            f"ZScoreDetector(threshold={self.threshold}, "
            f"min_count={self.min_count}, "
            f"anomalies={self.total_anomalies}/{self.total_checked})"
        )


class PercentileDetector:
    """Percentile-based anomaly detector.
    
    Detects anomalies by checking if values fall outside a specified
    percentile range (e.g., below 1st or above 99th percentile).
    
    This method is more robust to non-normal distributions than z-score.
    """
    
    def __init__(
        self,
        lower_percentile: float = 1.0,
        upper_percentile: float = 99.0,
        min_count: int = 20
    ) -> None:
        """Initialize percentile detector.
        
        Args:
            lower_percentile: Lower percentile threshold (0-100)
            upper_percentile: Upper percentile threshold (0-100)
            min_count: Minimum number of data points required
            
        Raises:
            ValueError: If percentiles are invalid
        """
        if not (0 <= lower_percentile < upper_percentile <= 100):
            raise ValueError(
                f"Invalid percentiles: lower={lower_percentile}, "
                f"upper={upper_percentile}"
            )
        if min_count < 10:
            raise ValueError(f"min_count must be >= 10, got {min_count}")
        
        self.lower_percentile = lower_percentile
        self.upper_percentile = upper_percentile
        self.min_count = min_count
        
        self.total_checked = 0
        self.total_anomalies = 0
        
        logger.info(
            f"Initialized PercentileDetector: "
            f"range=[{lower_percentile}, {upper_percentile}], "
            f"min_count={min_count}"
        )
    
    def detect(
        self,
        metric: str,
        window: SlidingWindow,
        value: float,
        timestamp: float
    ) -> Optional[Dict[str, Any]]:
        """Detect if a value is an anomaly based on percentiles.
        
        Args:
            metric: Name of the metric being checked
            window: Sliding window containing historical data
            value: Current value to check for anomaly
            timestamp: Timestamp of the current value
            
        Returns:
            Optional[Dict]: Anomaly details if detected, None otherwise
        """
        self.total_checked += 1
        
        # Get window statistics
        stats = window.stats(timestamp)
        
        # Check if we have enough data
        if stats.count < self.min_count:
            logger.debug(
                f"Skipping anomaly detection for {metric}: "
                f"insufficient data ({stats.count} < {self.min_count})"
            )
            return None
        
        # Extract values from window (need to sort for percentiles)
        values = sorted([v for _, v in window.data])
        
        # Compute percentile thresholds
        lower_idx = int(len(values) * self.lower_percentile / 100)
        upper_idx = int(len(values) * self.upper_percentile / 100)
        
        lower_bound = values[lower_idx] if lower_idx < len(values) else values[0]
        upper_bound = values[upper_idx] if upper_idx < len(values) else values[-1]
        
        # Check if anomaly
        is_anomaly = value < lower_bound or value > upper_bound
        
        if is_anomaly:
            self.total_anomalies += 1
            anomaly_dict = {
                'metric': metric,
                'value': value,
                'mean': stats.mean,
                'stddev': stats.stddev,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'lower_percentile': self.lower_percentile,
                'upper_percentile': self.upper_percentile,
                'timestamp': timestamp,
                'is_anomaly': True,
                'detection_method': 'percentile',
                'window_count': stats.count,
                'detected_at': time.time()
            }
            
            logger.warning(
                f"ANOMALY DETECTED: {metric}={value:.2f}, "
                f"bounds=[{lower_bound:.2f}, {upper_bound:.2f}]"
            )
            
            return anomaly_dict
        else:
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get detector statistics."""
        return {
            'total_checked': self.total_checked,
            'total_anomalies': self.total_anomalies,
            'lower_percentile': self.lower_percentile,
            'upper_percentile': self.upper_percentile,
            'min_count': self.min_count
        }
    
    def __repr__(self) -> str:
        """Return string representation of the detector."""
        return (
            f"PercentileDetector("
            f"range=[{self.lower_percentile}, {self.upper_percentile}], "
            f"anomalies={self.total_anomalies}/{self.total_checked})"
        )
