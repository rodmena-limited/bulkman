"""Tests for state definitions."""

from bulkman import BulkheadState


class TestBulkheadState:
    """Test BulkheadState enum."""

    def test_state_values(self):
        """Test state enum values."""
        assert BulkheadState.HEALTHY.value == "healthy"
        assert BulkheadState.DEGRADED.value == "degraded"
        assert BulkheadState.ISOLATED.value == "isolated"
        assert BulkheadState.FAILED.value == "failed"

    def test_state_members(self):
        """Test all state members exist."""
        states = [member.name for member in BulkheadState]
        assert "HEALTHY" in states
        assert "DEGRADED" in states
        assert "ISOLATED" in states
        assert "FAILED" in states

    def test_state_equality(self):
        """Test state equality."""
        assert BulkheadState.HEALTHY == BulkheadState.HEALTHY
        assert BulkheadState.HEALTHY != BulkheadState.DEGRADED
