"""Tests for configuration classes."""

from bulkman import BulkheadConfig, ExecutionResult


class TestBulkheadConfig:
    """Test BulkheadConfig class."""

    def test_config_defaults(self):
        """Test config has reasonable defaults."""
        config = BulkheadConfig(name="test")
        assert config.name == "test"
        assert config.max_concurrent_calls == 10
        assert config.max_queue_size == 100
        assert config.timeout_seconds is None
        assert config.failure_threshold == 5
        assert config.success_threshold == 3
        assert config.isolation_duration == 30.0
        assert config.circuit_breaker_enabled is True
        assert config.health_check_interval == 5.0

    def test_config_custom_values(self):
        """Test config accepts custom values."""
        config = BulkheadConfig(
            name="custom",
            max_concurrent_calls=20,
            max_queue_size=50,
            timeout_seconds=10.0,
            failure_threshold=3,
            success_threshold=2,
            isolation_duration=60.0,
            circuit_breaker_enabled=False,
            health_check_interval=10.0,
        )
        assert config.name == "custom"
        assert config.max_concurrent_calls == 20
        assert config.max_queue_size == 50
        assert config.timeout_seconds == 10.0
        assert config.failure_threshold == 3
        assert config.success_threshold == 2
        assert config.isolation_duration == 60.0
        assert config.circuit_breaker_enabled is False
        assert config.health_check_interval == 10.0


class TestExecutionResult:
    """Test ExecutionResult class."""

    def test_execution_result_success(self):
        """Test successful execution result."""
        result = ExecutionResult(
            success=True,
            result=42,
            error=None,
            execution_time=0.5,
            bulkhead_name="test",
            queued_time=0.1,
            execution_id="123",
        )
        assert result.success is True
        assert result.result == 42
        assert result.error is None
        assert result.execution_time == 0.5
        assert result.bulkhead_name == "test"
        assert result.queued_time == 0.1
        assert result.execution_id == "123"

    def test_execution_result_failure(self):
        """Test failed execution result."""
        error = ValueError("Test error")
        result = ExecutionResult(
            success=False,
            result=None,
            error=error,
            execution_time=0.5,
            bulkhead_name="test",
        )
        assert result.success is False
        assert result.result is None
        assert result.error is error
        assert result.execution_time == 0.5
        assert result.bulkhead_name == "test"
