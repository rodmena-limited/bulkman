"""Tests for exception classes."""

from bulkman import (
    BulkheadCircuitOpenError,
    BulkheadError,
    BulkheadFullError,
    BulkheadIsolationError,
    BulkheadTimeoutError,
)


class TestExceptions:
    """Test exception hierarchy."""

    def test_bulkhead_error_is_exception(self):
        """Test BulkheadError is an Exception."""
        error = BulkheadError("Test")
        assert isinstance(error, Exception)

    def test_bulkhead_isolation_error(self):
        """Test BulkheadIsolationError inherits from BulkheadError."""
        error = BulkheadIsolationError("Test")
        assert isinstance(error, BulkheadError)
        assert isinstance(error, Exception)

    def test_bulkhead_timeout_error(self):
        """Test BulkheadTimeoutError inherits from BulkheadError."""
        error = BulkheadTimeoutError("Test")
        assert isinstance(error, BulkheadError)
        assert isinstance(error, Exception)

    def test_bulkhead_full_error(self):
        """Test BulkheadFullError inherits from BulkheadError."""
        error = BulkheadFullError("Test")
        assert isinstance(error, BulkheadError)
        assert isinstance(error, Exception)

    def test_bulkhead_circuit_open_error(self):
        """Test BulkheadCircuitOpenError inherits from BulkheadError."""
        error = BulkheadCircuitOpenError("Test")
        assert isinstance(error, BulkheadError)
        assert isinstance(error, Exception)

    def test_exception_messages(self):
        """Test exceptions carry messages properly."""
        msg = "Custom error message"

        error1 = BulkheadError(msg)
        assert str(error1) == msg

        error2 = BulkheadTimeoutError(msg)
        assert str(error2) == msg

        error3 = BulkheadCircuitOpenError(msg)
        assert str(error3) == msg
