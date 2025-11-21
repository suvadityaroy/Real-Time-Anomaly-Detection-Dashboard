"""Tests for anomaly detection algorithms."""

import pytest
import time

from anomaly.window import SlidingWindow
from anomaly.detector import ZScoreDetector, PercentileDetector


class TestZScoreDetector:
    """Test cases for ZScoreDetector."""
    
    def test_initialization(self) -> None:
        """Test detector initialization."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        
        assert detector.threshold == 3.0
        assert detector.min_count == 5
        assert detector.total_checked == 0
        assert detector.total_anomalies == 0
    
    def test_initialization_invalid_params(self) -> None:
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError) as exc_info:
            ZScoreDetector(threshold=-1.0)
        assert "must be positive" in str(exc_info.value)
        
        with pytest.raises(ValueError) as exc_info:
            ZScoreDetector(threshold=3.0, min_count=1)
        assert "must be >= 2" in str(exc_info.value)
    
    def test_insufficient_data(self) -> None:
        """Test that no anomaly is detected with insufficient data."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add only 3 values (less than min_count)
        for i in range(3):
            window.append(base_time + i, 50.0 + i)
        
        result = detector.detect("cpu", window, 100.0, base_time + 10)
        
        assert result is None
        assert detector.total_checked == 1
        assert detector.total_anomalies == 0
    
    def test_zero_stddev(self) -> None:
        """Test that no anomaly is detected when stddev is zero."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add constant values (stddev = 0)
        for i in range(10):
            window.append(base_time + i, 50.0)
        
        result = detector.detect("cpu", window, 50.0, base_time + 10)
        
        assert result is None
    
    def test_normal_value_no_anomaly(self) -> None:
        """Test that normal values are not flagged as anomalies."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add values with mean=50, stddev≈14.1
        values = [30.0, 40.0, 50.0, 60.0, 70.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        # Test a value close to mean (z ≈ 0.7)
        result = detector.detect("cpu", window, 60.0, base_time + 10)
        
        assert result is None
        assert detector.total_checked == 1
        assert detector.total_anomalies == 0
    
    def test_anomaly_detected_high(self) -> None:
        """Test that high outliers are detected as anomalies."""
        detector = ZScoreDetector(threshold=2.5, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add values: mean=50, stddev≈14.1
        values = [30.0, 40.0, 50.0, 60.0, 70.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        # Test a high outlier (z ≈ 3.5)
        result = detector.detect("cpu", window, 100.0, base_time + 10)
        
        assert result is not None
        assert result['is_anomaly'] is True
        assert result['metric'] == "cpu"
        assert result['value'] == 100.0
        assert abs(result['z_score']) > 2.5
        assert result['detection_method'] == 'z-score'
        assert detector.total_anomalies == 1
    
    def test_anomaly_detected_low(self) -> None:
        """Test that low outliers are detected as anomalies."""
        detector = ZScoreDetector(threshold=2.5, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add values: mean=50, stddev≈14.1
        values = [30.0, 40.0, 50.0, 60.0, 70.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        # Test a low outlier (z ≈ -3.5)
        result = detector.detect("cpu", window, 0.0, base_time + 10)
        
        assert result is not None
        assert result['is_anomaly'] is True
        assert abs(result['z_score']) > 2.5
        assert detector.total_anomalies == 1
    
    def test_anomaly_at_exact_threshold(self) -> None:
        """Test behavior at exact threshold boundary."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Create window with known statistics
        # mean=50, stddev≈15.811
        values = [20.0, 35.0, 50.0, 65.0, 80.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        stats = window.stats(base_time + 10)
        
        # Value exactly at +3 stddev
        test_value = stats.mean + 3.0 * stats.stddev
        result = detector.detect("cpu", window, test_value, base_time + 10)
        
        # Should be detected (>= threshold)
        assert result is not None
        assert abs(result['z_score']) >= 2.99  # Account for floating point
    
    def test_multiple_detections(self) -> None:
        """Test multiple anomaly detections."""
        detector = ZScoreDetector(threshold=2.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Build baseline window
        values = [40.0, 45.0, 50.0, 55.0, 60.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        # Test multiple outliers
        anomaly1 = detector.detect("cpu", window, 100.0, base_time + 10)
        anomaly2 = detector.detect("cpu", window, 5.0, base_time + 11)
        normal = detector.detect("cpu", window, 52.0, base_time + 12)
        
        assert anomaly1 is not None
        assert anomaly2 is not None
        assert normal is None
        assert detector.total_checked == 3
        assert detector.total_anomalies == 2
    
    def test_anomaly_dict_structure(self) -> None:
        """Test that anomaly dictionary contains all required fields."""
        detector = ZScoreDetector(threshold=2.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        values = [40.0, 45.0, 50.0, 55.0, 60.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        result = detector.detect("cpu", window, 100.0, base_time + 10)
        
        assert result is not None
        # Check all required fields
        required_fields = [
            'metric', 'value', 'mean', 'stddev', 'variance',
            'z_score', 'threshold', 'timestamp', 'is_anomaly',
            'detection_method', 'window_count', 'detected_at'
        ]
        for field in required_fields:
            assert field in result
        
        assert result['metric'] == 'cpu'
        assert result['value'] == 100.0
        assert result['is_anomaly'] is True
        assert result['detection_method'] == 'z-score'
        assert result['threshold'] == 2.0
        assert result['window_count'] == 5
    
    def test_get_stats(self) -> None:
        """Test getting detector statistics."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        values = [40.0, 50.0, 60.0, 70.0, 80.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        detector.detect("cpu", window, 100.0, base_time + 10)
        detector.detect("cpu", window, 60.0, base_time + 11)
        
        stats = detector.get_stats()
        
        assert stats['total_checked'] == 2
        assert stats['total_anomalies'] == 1
        assert stats['threshold'] == 3.0
        assert stats['min_count'] == 5
    
    def test_reset_stats(self) -> None:
        """Test resetting detector statistics."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        values = [40.0, 50.0, 60.0, 70.0, 80.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        detector.detect("cpu", window, 100.0, base_time + 10)
        
        assert detector.total_checked == 1
        assert detector.total_anomalies == 1
        
        detector.reset_stats()
        
        assert detector.total_checked == 0
        assert detector.total_anomalies == 0
    
    def test_repr(self) -> None:
        """Test string representation."""
        detector = ZScoreDetector(threshold=3.0, min_count=5)
        
        repr_str = repr(detector)
        assert "ZScoreDetector" in repr_str
        assert "3.0" in repr_str
        assert "5" in repr_str


class TestPercentileDetector:
    """Test cases for PercentileDetector."""
    
    def test_initialization(self) -> None:
        """Test detector initialization."""
        detector = PercentileDetector(
            lower_percentile=5.0,
            upper_percentile=95.0,
            min_count=20
        )
        
        assert detector.lower_percentile == 5.0
        assert detector.upper_percentile == 95.0
        assert detector.min_count == 20
    
    def test_initialization_invalid_params(self) -> None:
        """Test that invalid parameters raise errors."""
        with pytest.raises(ValueError):
            PercentileDetector(lower_percentile=95.0, upper_percentile=5.0)
        
        with pytest.raises(ValueError):
            PercentileDetector(lower_percentile=-1.0)
        
        with pytest.raises(ValueError):
            PercentileDetector(min_count=5)  # Too low
    
    def test_insufficient_data(self) -> None:
        """Test that no anomaly is detected with insufficient data."""
        detector = PercentileDetector(min_count=20)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add only 10 values
        for i in range(10):
            window.append(base_time + i, float(i))
        
        result = detector.detect("cpu", window, 100.0, base_time + 20)
        
        assert result is None
    
    def test_normal_value_no_anomaly(self) -> None:
        """Test that values within percentile range are not anomalies."""
        detector = PercentileDetector(
            lower_percentile=10.0,
            upper_percentile=90.0,
            min_count=20
        )
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add 30 values from 0 to 29
        for i in range(30):
            window.append(base_time + i, float(i))
        
        # Value in middle should not be anomaly
        result = detector.detect("cpu", window, 15.0, base_time + 40)
        
        assert result is None
    
    def test_anomaly_detected_high(self) -> None:
        """Test that high outliers are detected."""
        detector = PercentileDetector(
            lower_percentile=5.0,
            upper_percentile=95.0,
            min_count=20
        )
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add values from 0 to 29
        for i in range(30):
            window.append(base_time + i, float(i))
        
        # Very high value should be anomaly
        result = detector.detect("cpu", window, 100.0, base_time + 40)
        
        assert result is not None
        assert result['is_anomaly'] is True
        assert result['detection_method'] == 'percentile'
    
    def test_anomaly_detected_low(self) -> None:
        """Test that low outliers are detected."""
        detector = PercentileDetector(
            lower_percentile=5.0,
            upper_percentile=95.0,
            min_count=20
        )
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add values from 10 to 39
        for i in range(10, 40):
            window.append(base_time + i, float(i))
        
        # Very low value should be anomaly
        result = detector.detect("cpu", window, 0.0, base_time + 50)
        
        assert result is not None
        assert result['is_anomaly'] is True
    
    def test_anomaly_dict_structure(self) -> None:
        """Test that anomaly dictionary contains required fields."""
        detector = PercentileDetector(min_count=20)
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        for i in range(30):
            window.append(base_time + i, float(i))
        
        result = detector.detect("cpu", window, 100.0, base_time + 40)
        
        assert result is not None
        required_fields = [
            'metric', 'value', 'mean', 'stddev', 'lower_bound',
            'upper_bound', 'lower_percentile', 'upper_percentile',
            'timestamp', 'is_anomaly', 'detection_method'
        ]
        for field in required_fields:
            assert field in result
    
    def test_get_stats(self) -> None:
        """Test getting detector statistics."""
        detector = PercentileDetector(min_count=20)
        stats = detector.get_stats()
        
        assert 'total_checked' in stats
        assert 'total_anomalies' in stats
        assert 'lower_percentile' in stats
        assert 'upper_percentile' in stats
