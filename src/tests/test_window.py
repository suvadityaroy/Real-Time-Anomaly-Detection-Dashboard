"""Tests for sliding window implementation."""

import pytest
import time
import statistics

from anomaly.window import SlidingWindow, MultiMetricWindow, WindowStats


class TestSlidingWindow:
    """Test cases for SlidingWindow class."""
    
    def test_initialization(self) -> None:
        """Test window initialization."""
        window = SlidingWindow(window_seconds=60.0)
        assert window.window_seconds == 60.0
        assert len(window) == 0
        assert window._count == 0
        assert window._mean == 0.0
        assert window._m2 == 0.0
    
    def test_initialization_invalid_duration(self) -> None:
        """Test that negative or zero duration raises error."""
        with pytest.raises(ValueError) as exc_info:
            SlidingWindow(window_seconds=-10.0)
        assert "must be positive" in str(exc_info.value)
        
        with pytest.raises(ValueError):
            SlidingWindow(window_seconds=0.0)
    
    def test_append_single_value(self) -> None:
        """Test appending a single value."""
        window = SlidingWindow(window_seconds=60.0)
        timestamp = time.time()
        
        window.append(timestamp, 10.0)
        
        assert len(window) == 1
        assert window._count == 1
        assert window._mean == 10.0
    
    def test_append_multiple_values(self) -> None:
        """Test appending multiple values and computing mean."""
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        assert len(window) == 5
        assert window._count == 5
        # Mean of [10, 20, 30, 40, 50] is 30
        assert abs(window._mean - 30.0) < 0.001
    
    def test_stats_computation(self) -> None:
        """Test statistics computation with known values."""
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Values: [10, 20, 30, 40, 50]
        # Mean: 30, Variance: 250, StdDev: 15.811...
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        stats = window.stats(base_time + 10)
        
        assert stats.count == 5
        assert abs(stats.mean - 30.0) < 0.001
        # Sample variance with Bessel's correction
        expected_variance = statistics.variance(values)
        assert abs(stats.variance - expected_variance) < 0.001
        assert abs(stats.stddev - expected_variance ** 0.5) < 0.001
    
    def test_welford_algorithm_correctness(self) -> None:
        """Test that Welford's algorithm matches standard calculation."""
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Use a variety of values
        values = [5.3, 12.7, 8.9, 15.2, 11.1, 6.8, 14.5, 9.3, 13.6, 7.4]
        
        for i, value in enumerate(values):
            window.append(base_time + i, value)
        
        stats = window.stats(base_time + 20)
        
        # Compare with Python's statistics module
        expected_mean = statistics.mean(values)
        expected_variance = statistics.variance(values)
        expected_stddev = statistics.stdev(values)
        
        assert abs(stats.mean - expected_mean) < 0.001
        assert abs(stats.variance - expected_variance) < 0.001
        assert abs(stats.stddev - expected_stddev) < 0.001
    
    def test_purge_old_data(self) -> None:
        """Test purging old data outside the window."""
        window = SlidingWindow(window_seconds=10.0)
        base_time = 1000.0
        
        # Add values spread over 20 seconds
        for i in range(20):
            window.append(base_time + i, float(i))
        
        assert len(window) == 20
        
        # Purge with current time = base + 20
        # Only last 10 seconds should remain (timestamps 10-19)
        window.purge_old(base_time + 20)
        
        assert len(window) == 10
        # Remaining values should be 10-19, mean = 14.5
        stats = window.stats(base_time + 20)
        assert stats.count == 10
        expected_mean = sum(range(10, 20)) / 10
        assert abs(stats.mean - expected_mean) < 0.001
    
    def test_stats_with_purge(self) -> None:
        """Test that stats automatically purges old data."""
        window = SlidingWindow(window_seconds=5.0)
        base_time = 1000.0
        
        # Add old values
        window.append(base_time, 10.0)
        window.append(base_time + 1, 20.0)
        
        # Add recent values
        window.append(base_time + 10, 30.0)
        window.append(base_time + 11, 40.0)
        
        # Stats at base + 12 should only include last two values
        stats = window.stats(base_time + 12)
        
        assert stats.count == 2
        assert abs(stats.mean - 35.0) < 0.001  # Mean of [30, 40]
    
    def test_empty_window_stats(self) -> None:
        """Test stats on empty window."""
        window = SlidingWindow(window_seconds=60.0)
        stats = window.stats(time.time())
        
        assert stats.count == 0
        assert stats.mean == 0.0
        assert stats.variance == 0.0
        assert stats.stddev == 0.0
    
    def test_single_value_stats(self) -> None:
        """Test stats with single value (no variance)."""
        window = SlidingWindow(window_seconds=60.0)
        timestamp = time.time()
        
        window.append(timestamp, 42.0)
        stats = window.stats(timestamp + 1)
        
        assert stats.count == 1
        assert stats.mean == 42.0
        assert stats.variance == 0.0
        assert stats.stddev == 0.0
    
    def test_constant_values(self) -> None:
        """Test window with all identical values."""
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        # All values are 100.0
        for i in range(10):
            window.append(base_time + i, 100.0)
        
        stats = window.stats(base_time + 20)
        
        assert stats.count == 10
        assert abs(stats.mean - 100.0) < 0.001
        assert abs(stats.variance) < 0.001  # Should be ~0
        assert abs(stats.stddev) < 0.001
    
    def test_clear(self) -> None:
        """Test clearing the window."""
        window = SlidingWindow(window_seconds=60.0)
        base_time = time.time()
        
        for i in range(5):
            window.append(base_time + i, float(i))
        
        assert len(window) == 5
        
        window.clear()
        
        assert len(window) == 0
        assert window._count == 0
        assert window._mean == 0.0
        assert window._m2 == 0.0
    
    def test_append_invalid_values(self) -> None:
        """Test that invalid values raise errors."""
        window = SlidingWindow(window_seconds=60.0)
        
        # Invalid timestamp
        with pytest.raises(ValueError):
            window.append(float('inf'), 10.0)
        
        # Invalid value
        with pytest.raises(ValueError):
            window.append(1000.0, float('nan'))
    
    def test_repr(self) -> None:
        """Test string representation."""
        window = SlidingWindow(window_seconds=60.0)
        window.append(1000.0, 10.0)
        window.append(1001.0, 20.0)
        
        repr_str = repr(window)
        assert "SlidingWindow" in repr_str
        assert "60.0" in repr_str


class TestMultiMetricWindow:
    """Test cases for MultiMetricWindow class."""
    
    def test_initialization(self) -> None:
        """Test multi-metric window initialization."""
        manager = MultiMetricWindow(window_seconds=60.0)
        
        assert manager.window_seconds == 60.0
        assert len(manager) == 0
        assert len(manager.get_metrics()) == 0
    
    def test_get_window_creates_new(self) -> None:
        """Test that get_window creates new window on first access."""
        manager = MultiMetricWindow(window_seconds=60.0)
        
        window = manager.get_window("cpu")
        
        assert window is not None
        assert window.window_seconds == 60.0
        assert len(manager) == 1
        assert "cpu" in manager.get_metrics()
    
    def test_get_window_reuses_existing(self) -> None:
        """Test that get_window returns same window on repeated access."""
        manager = MultiMetricWindow(window_seconds=60.0)
        
        window1 = manager.get_window("cpu")
        window2 = manager.get_window("cpu")
        
        assert window1 is window2
        assert len(manager) == 1
    
    def test_append_to_metric(self) -> None:
        """Test appending values to specific metrics."""
        manager = MultiMetricWindow(window_seconds=60.0)
        base_time = time.time()
        
        manager.append("cpu", base_time, 75.0)
        manager.append("cpu", base_time + 1, 80.0)
        manager.append("memory", base_time, 85.0)
        
        assert len(manager) == 2
        assert set(manager.get_metrics()) == {"cpu", "memory"}
        
        cpu_window = manager.get_window("cpu")
        assert len(cpu_window) == 2
        
        memory_window = manager.get_window("memory")
        assert len(memory_window) == 1
    
    def test_stats_per_metric(self) -> None:
        """Test getting statistics per metric."""
        manager = MultiMetricWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add CPU values
        for i in range(5):
            manager.append("cpu", base_time + i, 70.0 + i)
        
        # Add memory values
        for i in range(3):
            manager.append("memory", base_time + i, 80.0 + i * 2)
        
        cpu_stats = manager.stats("cpu", base_time + 10)
        memory_stats = manager.stats("memory", base_time + 10)
        
        assert cpu_stats.count == 5
        assert abs(cpu_stats.mean - 72.0) < 0.001
        
        assert memory_stats.count == 3
        assert abs(memory_stats.mean - 82.0) < 0.001
    
    def test_purge_all_metrics(self) -> None:
        """Test purging old data from all metrics."""
        manager = MultiMetricWindow(window_seconds=10.0)
        base_time = 1000.0
        
        # Add old and new data to multiple metrics
        manager.append("cpu", base_time, 50.0)
        manager.append("cpu", base_time + 15, 60.0)
        manager.append("memory", base_time, 70.0)
        manager.append("memory", base_time + 15, 80.0)
        
        manager.purge_all(base_time + 20)
        
        # Only recent data should remain
        cpu_stats = manager.stats("cpu", base_time + 20)
        memory_stats = manager.stats("memory", base_time + 20)
        
        assert cpu_stats.count == 1
        assert memory_stats.count == 1
    
    def test_clear_all_metrics(self) -> None:
        """Test clearing all metric windows."""
        manager = MultiMetricWindow(window_seconds=60.0)
        base_time = time.time()
        
        manager.append("cpu", base_time, 50.0)
        manager.append("memory", base_time, 70.0)
        
        assert len(manager) == 2
        
        manager.clear_all()
        
        # Metrics still exist but windows are empty
        assert len(manager) == 2
        cpu_window = manager.get_window("cpu")
        assert len(cpu_window) == 0
    
    def test_get_metrics_sorted(self) -> None:
        """Test that get_metrics returns sorted list."""
        manager = MultiMetricWindow(window_seconds=60.0)
        base_time = time.time()
        
        # Add in non-alphabetical order
        manager.append("zebra", base_time, 1.0)
        manager.append("alpha", base_time, 2.0)
        manager.append("delta", base_time, 3.0)
        
        metrics = manager.get_metrics()
        assert metrics == ["alpha", "delta", "zebra"]
    
    def test_repr(self) -> None:
        """Test string representation."""
        manager = MultiMetricWindow(window_seconds=60.0)
        manager.append("cpu", time.time(), 50.0)
        
        repr_str = repr(manager)
        assert "MultiMetricWindow" in repr_str
        assert "60.0" in repr_str
