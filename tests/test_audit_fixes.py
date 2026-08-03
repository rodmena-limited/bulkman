"""Regression tests for audit fixes.

Covers: circuit breaker off by default, async queue capacity enforcement,
timeout stats integrity, async shutdown, sync bridge shutdown/parity/capacity,
and config validation.  These are additions; existing tests were not modified.
"""

import random
import threading
import time

import anyio
import pytest
from resilient_circuit.storage import InMemoryStorage

from bulkman import (
    Bulkhead,
    BulkheadCircuitOpenError,
    BulkheadConfig,
    BulkheadError,
    BulkheadFullError,
    BulkheadShutdownError,
    BulkheadState,
    BulkheadThreading,
    BulkheadTimeoutError,
)
from bulkman.sync_bridge import BulkheadSync


class TestCircuitBreakerOffByDefault:
    """Circuit breaker must be opt-in, not the default."""

    async def test_async_bulkhead_default_no_circuit_breaker(self):
        bulkhead = Bulkhead(BulkheadConfig(name="default_cb"))
        assert bulkhead._circuit_breaker is None
        assert bulkhead.config.circuit_breaker_enabled is False

    async def test_async_bulkhead_stats_report_cb_disabled(self):
        bulkhead = Bulkhead(BulkheadConfig(name="default_cb"))
        await bulkhead.execute(lambda: 1)
        stats = await bulkhead.get_stats()
        assert stats["circuit_breaker_enabled"] is False
        assert "circuit_status" not in stats

    async def test_default_config_state_healthy(self):
        bulkhead = Bulkhead(BulkheadConfig(name="default_cb"))
        assert await bulkhead.get_state() == BulkheadState.HEALTHY
        assert await bulkhead.is_healthy() is True

    def test_threading_default_no_circuit_breaker(self):
        bulkhead = BulkheadThreading(BulkheadConfig(name="default_cb"))
        assert bulkhead._circuit_breaker is None
        bulkhead.shutdown(wait=False)


class TestAsyncQueueCapacity:
    """Async Bulkhead must enforce max_queue_size (BulkheadFullError)."""

    async def test_rejects_beyond_capacity(self):
        config = BulkheadConfig(
            name="q_reject",
            max_concurrent_calls=1,
            max_queue_size=0,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)
        held = anyio.Event()
        release = anyio.Event()

        async def hold():
            held.set()
            await release.wait()

        async with anyio.create_task_group() as nursery:
            nursery.start_soon(lambda: bulkhead.execute(hold))
            await held.wait()
            with pytest.raises(BulkheadFullError):
                await bulkhead.execute(lambda: 1)
            release.set()

    async def test_admits_after_capacity_released(self):
        config = BulkheadConfig(
            name="q_release",
            max_concurrent_calls=1,
            max_queue_size=0,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)
        held = anyio.Event()
        release = anyio.Event()

        async def hold():
            held.set()
            await release.wait()

        async with anyio.create_task_group() as nursery:
            nursery.start_soon(lambda: bulkhead.execute(hold))
            await held.wait()
            with pytest.raises(BulkheadFullError):
                await bulkhead.execute(lambda: 1)
            release.set()
            await anyio.sleep(0.01)  # let the slot drain
            result = await bulkhead.execute(lambda: 42)
            assert result.success is True
            assert result.result == 42

    async def test_waits_are_limited_by_queue_size(self):
        config = BulkheadConfig(
            name="q_limited",
            max_concurrent_calls=1,
            max_queue_size=2,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)
        release = anyio.Event()

        async def hold():
            await release.wait()

        async with anyio.create_task_group() as nursery:
            # 1 running + 2 queued = full
            for _ in range(3):
                nursery.start_soon(lambda: bulkhead.execute(hold))
            await anyio.sleep(0.05)
            with pytest.raises(BulkheadFullError):
                await bulkhead.execute(lambda: 1)
            release.set()


class TestTimeoutStatsIntegrity:
    """A timed-out task must not corrupt another task's active counter."""

    async def test_waiting_timeout_does_not_corrupt_stats(self):
        """A caller cancelled while waiting must not decrement a running task."""
        config = BulkheadConfig(
            name="timeout_stats",
            max_concurrent_calls=1,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)
        started = anyio.Event()
        release = anyio.Event()

        async def long_task():
            started.set()
            await release.wait()
            return "done"

        async def slow():
            await anyio.sleep(1.0)

        async with anyio.create_task_group() as nursery:
            nursery.start_soon(lambda: bulkhead.execute(long_task))
            await started.wait()
            with anyio.move_on_after(0.05) as scope:
                await bulkhead.execute(slow)
            assert scope.cancelled_caught
            stats = await bulkhead.get_stats()
            assert stats["active_tasks"] == 1, "timeout corrupted active_tasks"
            # The cancelled caller never became active, so nothing to release
            assert stats["rejected_executions"] == 0
            release.set()

        # Slot released after the running task finished: capacity restored
        result = await bulkhead.execute(lambda: 42)
        assert result.success is True

    async def test_active_tasks_returns_to_zero(self):
        """After the bulkhead's own timeout fires mid-execution, counters drain."""
        config = BulkheadConfig(
            name="timeout_stats2",
            max_concurrent_calls=1,
            timeout_seconds=0.05,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)

        async def slow():
            await anyio.sleep(1.0)

        with pytest.raises(BulkheadTimeoutError):
            await bulkhead.execute(slow)
        stats = await bulkhead.get_stats()
        assert stats["active_tasks"] == 0
        assert stats["total_executions"] == 1


class TestAsyncShutdown:
    """Async Bulkhead.shutdown must exist and be idempotent."""

    async def test_shutdown_idempotent(self):
        bulkhead = Bulkhead(BulkheadConfig(name="shutdown", circuit_breaker_enabled=True))
        await bulkhead.execute(lambda: 1)
        await bulkhead.shutdown()
        await bulkhead.shutdown()  # second call must not raise

    async def test_execute_after_shutdown(self):
        """Shutdown is terminal: execute() raises a typed BulkheadError."""
        bulkhead = Bulkhead(BulkheadConfig(name="shutdown2", circuit_breaker_enabled=True))
        await bulkhead.shutdown()
        with pytest.raises(BulkheadShutdownError):
            await bulkhead.execute(lambda: 7)

        async def async_func():
            return 7

        with pytest.raises(BulkheadShutdownError):
            await bulkhead.execute(async_func)


class TestSyncBridgeShutdown:
    """BulkheadSync.shutdown must not raise TypeError/AttributeError."""

    def test_shutdown_works_and_is_idempotent(self):
        bulkhead = BulkheadSync(BulkheadConfig(name="sb_shutdown", circuit_breaker_enabled=False))
        future = bulkhead.execute(lambda: 1)
        assert future.result(timeout=5.0).success is True
        bulkhead.shutdown()
        bulkhead.shutdown()  # idempotent

    def test_execute_after_shutdown_raises(self):
        bulkhead = BulkheadSync(BulkheadConfig(name="sb_shutdown2", circuit_breaker_enabled=False))
        bulkhead.shutdown()
        with pytest.raises(BulkheadShutdownError):
            bulkhead.execute(lambda: 1)


class TestSyncBridgeParity:
    """BulkheadSync must expose the same observability API as BulkheadThreading."""

    def test_state_health_stats_parity(self):
        bulkhead = BulkheadSync(BulkheadConfig(name="sb_parity", circuit_breaker_enabled=False))
        future = bulkhead.execute(lambda: 1)
        future.result(timeout=5.0)
        assert bulkhead.get_state() == BulkheadState.HEALTHY
        assert bulkhead.is_healthy() is True
        stats = bulkhead.get_stats()
        assert stats["total_executions"] == 1
        bulkhead.reset_stats()
        assert bulkhead.get_stats()["total_executions"] == 0
        bulkhead.shutdown()

    def test_capacity_reject_and_release(self):
        bulkhead = BulkheadSync(
            BulkheadConfig(
                name="sb_capacity",
                max_concurrent_calls=1,
                max_queue_size=0,
                circuit_breaker_enabled=False,
            )
        )
        hold = threading.Event()

        def blocking():
            hold.wait(timeout=5.0)
            return "done"

        future1 = bulkhead.execute(blocking)
        time.sleep(0.05)
        with pytest.raises(BulkheadFullError):
            bulkhead.execute(lambda: 1)
        hold.set()
        assert future1.result(timeout=5.0).success is True
        # Slot released: capacity restored
        future3 = bulkhead.execute(lambda: 2)
        assert future3.result(timeout=5.0).success is True
        bulkhead.shutdown()


class TestShutdownAccounting:
    """Shutdown must not leak capacity or crash concurrent callers."""

    def test_sync_bridge_inflight_released_on_shutdown_cancel(self):
        """Executor-cancelled tasks must release their in-flight slots (R2-A)."""
        bulkhead = BulkheadSync(
            BulkheadConfig(
                name="sb_leak",
                max_concurrent_calls=1,
                max_queue_size=5,
                circuit_breaker_enabled=False,
            )
        )
        hold = threading.Event()

        def blocking():
            hold.wait(timeout=30.0)
            return "done"

        for _ in range(6):
            bulkhead.execute(blocking)
        time.sleep(0.2)
        assert bulkhead._in_flight_count == 6

        bulkhead.shutdown(wait=False)  # cancels the 5 queued work items
        time.sleep(0.2)
        assert bulkhead._in_flight_count == 1, "queued tasks leaked in-flight slots"

        hold.set()
        time.sleep(0.3)
        assert bulkhead._in_flight_count == 0, "running task did not release its slot"

    def test_threading_shutdown_honors_timeout(self):
        """shutdown(wait=True, timeout=...) must return within the timeout (R2-C)."""
        bulkhead = BulkheadThreading(
            BulkheadConfig(name="th_timeout", circuit_breaker_enabled=False)
        )
        bulkhead.execute(lambda: time.sleep(5.0))

        start = time.monotonic()
        bulkhead.shutdown(wait=True, timeout=0.1)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"shutdown blocked {elapsed:.1f}s despite timeout=0.1"

    def test_sync_bridge_shutdown_honors_timeout(self):
        """BulkheadSync.shutdown(wait=True, timeout=...) must return in time (R2-C)."""
        bulkhead = BulkheadSync(BulkheadConfig(name="sb_timeout", circuit_breaker_enabled=False))
        bulkhead.execute(lambda: time.sleep(5.0))
        time.sleep(0.1)  # let the task start running

        start = time.monotonic()
        bulkhead.shutdown(wait=True, timeout=0.1)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"shutdown blocked {elapsed:.1f}s despite timeout=0.1"

    def test_threading_shutdown_race_no_attribute_error(self):
        """shutdown() racing _check_circuit() must not crash (R2-B)."""
        bulkhead = BulkheadThreading(BulkheadConfig(name="th_race", circuit_breaker_enabled=True))
        inner = bulkhead._circuit_lock
        gate = threading.Event()

        class OncePausingLock:
            """Passes the first acquisition; blocks subsequent ones on gate."""

            def __init__(self):
                self.blocked = False

            def __enter__(self):
                if not self.blocked:
                    self.blocked = True
                    gate.wait(timeout=5.0)
                return inner.__enter__()

            def __exit__(self, *args):
                return inner.__exit__(*args)

        bulkhead._circuit_lock = OncePausingLock()
        outcome = {}

        def t_execute():
            try:
                bulkhead.execute(lambda: 1)
                outcome["out"] = "ok"
            except Exception as e:
                outcome["out"] = type(e).__name__

        t = threading.Thread(target=t_execute)
        t.start()
        time.sleep(0.2)  # T parked after the breaker None-check, before the lock
        bulkhead.shutdown(wait=False)
        gate.set()
        t.join(timeout=5.0)

        assert not t.is_alive(), "executor thread deadlocked"
        assert outcome.get("out") != "AttributeError", "TOCTOU crash on shutdown race"

    async def test_async_shutdown_race_no_attribute_error(self):
        """Async shutdown() racing _check_circuit() must not crash (R2-B)."""
        bulkhead = Bulkhead(BulkheadConfig(name="a_race", circuit_breaker_enabled=True))
        inner = bulkhead._circuit_lock
        gate = threading.Event()

        class OncePausingLock:
            def __init__(self):
                self.blocked = False

            def __enter__(self):
                if not self.blocked:
                    self.blocked = True
                    gate.wait(timeout=5.0)
                return inner.__enter__()

            def __exit__(self, *args):
                return inner.__exit__(*args)

        bulkhead._circuit_lock = OncePausingLock()
        outcome = {}

        async def t_execute():
            try:
                result = await bulkhead.execute(lambda: 1)
                outcome["out"] = "ok" if result.success else "failed"
            except Exception as e:
                outcome["out"] = type(e).__name__

        async with anyio.create_task_group() as nursery:
            nursery.start_soon(t_execute)
            await anyio.sleep(0.2)  # parked after the breaker check, before the lock
            await bulkhead.shutdown()
            gate.set()

        assert outcome.get("out") != "AttributeError", "TOCTOU crash on shutdown race"

    async def test_async_stats_include_in_flight(self):
        """Async get_stats exposes in_flight_count for parity with threading."""
        config = BulkheadConfig(
            name="stats_inflight",
            max_concurrent_calls=1,
            max_queue_size=1,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)
        held = anyio.Event()
        release = anyio.Event()

        async def hold():
            held.set()
            await release.wait()

        async with anyio.create_task_group() as nursery:
            nursery.start_soon(lambda: bulkhead.execute(hold))
            await held.wait()
            nursery.start_soon(lambda: bulkhead.execute(hold))  # queued
            await anyio.sleep(0.05)
            stats = await bulkhead.get_stats()
            assert stats["in_flight_count"] == 2
            release.set()


class TestRound3SyncFunctionSemantics:
    """Sync functions must honor timeout and concurrency config on async."""

    async def test_sync_func_timeout_fires_promptly(self):
        """A sync function past the timeout must fail at ~timeout, not after."""
        import time as _time

        config = BulkheadConfig(
            name="r3_timeout",
            timeout_seconds=0.1,
            circuit_breaker_enabled=False,
        )
        bulkhead = Bulkhead(config)
        t0 = _time.monotonic()
        with pytest.raises(BulkheadTimeoutError):
            await bulkhead.execute(lambda: _time.sleep(1.0))
        elapsed = _time.monotonic() - t0
        assert elapsed < 0.5, f"timeout took {elapsed:.2f}s for a sync function"
        stats = await bulkhead.get_stats()
        assert stats["active_tasks"] == 0
        assert stats["in_flight_count"] == 0

    async def test_sync_concurrency_matches_config(self):
        """The dedicated pool must allow max_concurrent_calls sync executions."""
        import threading as _threading
        import time as _time

        active = 0
        peak = 0
        lock = _threading.Lock()

        def sync_work():
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            _time.sleep(0.3)
            with lock:
                active -= 1

        bulkhead = Bulkhead(
            BulkheadConfig(
                name="r3_pool",
                max_concurrent_calls=50,
                circuit_breaker_enabled=False,
            )
        )
        async with anyio.create_task_group() as nursery:
            for _ in range(50):
                nursery.start_soon(lambda: bulkhead.execute(sync_work))
        # The anyio shared pool caps at 40; the dedicated pool must exceed it
        assert peak >= 41, f"peak concurrent sync executions was {peak}"

    async def test_worker_baseexception_is_contained(self):
        """A worker-thread KeyboardInterrupt must not crash the caller."""
        bulkhead = Bulkhead(BulkheadConfig(name="r3_ki", circuit_breaker_enabled=False))
        result = await bulkhead.execute(lambda: (_ for _ in ()).throw(KeyboardInterrupt("x")))
        assert result.success is False
        assert isinstance(result.error, BulkheadError)
        assert "KeyboardInterrupt" in str(result.error)
        # The bulkhead still works afterwards
        result2 = await bulkhead.execute(lambda: 1)
        assert result2.success is True

    def test_bridge_worker_baseexception_resolves_and_portal_survives(self):
        """A worker BaseException must resolve the future and keep the portal alive."""
        bulkhead1 = BulkheadSync(BulkheadConfig(name="r3_ki_b", circuit_breaker_enabled=False))
        future = bulkhead1.execute(lambda: (_ for _ in ()).throw(KeyboardInterrupt("x")))
        result = future.result(timeout=5.0)
        assert result.success is False
        assert isinstance(result.error, BulkheadError)
        assert "KeyboardInterrupt" in str(result.error)
        bulkhead1.shutdown()

        # A fresh BulkheadSync proves the shared portal thread survived
        bulkhead2 = BulkheadSync(BulkheadConfig(name="r3_ki_c", circuit_breaker_enabled=False))
        result2 = bulkhead2.execute(lambda: 42).result(timeout=5.0)
        assert result2.success is True
        assert result2.result == 42
        bulkhead2.shutdown()


def _stress_sync_task(rng, i):
    if rng.random() < 0.2:
        time.sleep(0.05)
    if rng.random() < 0.1:
        raise ValueError(f"sync fail {i}")
    return i


async def _stress_async_task(rng, i):
    await anyio.sleep(0.02)
    if rng.random() < 0.1:
        raise ValueError(f"async fail {i}")
    return i


async def _stress_run_one(bulkhead, rng, i):
    func = _stress_sync_task if i % 2 == 0 else _stress_async_task
    try:
        await bulkhead.execute(func, rng, i)
    except Exception:
        pass


async def _stress_statter(bulkhead):
    for _ in range(30):
        await bulkhead.get_stats()
        await anyio.sleep(0.01)


def _assert_stress_consistent(stats: dict) -> None:
    assert stats["total_executions"] == (
        stats["successful_executions"] + stats["failed_executions"]
    ), stats
    assert stats["active_tasks"] == 0, stats
    assert stats["in_flight_count"] == 0, stats
    assert stats["total_executions"] > 0, stats


class TestRound3StressAccounting:
    """Counters must stay consistent under heavy mixed load."""

    async def test_stress_accounting_consistent(self):
        rng = random.Random(42)
        bulkhead = Bulkhead(
            BulkheadConfig(
                name="r3_stress",
                max_concurrent_calls=5,
                max_queue_size=10,
                timeout_seconds=0.2,
                circuit_breaker_enabled=False,
            )
        )
        async with anyio.create_task_group() as nursery:
            nursery.start_soon(_stress_statter, bulkhead)
            for i in range(300):
                nursery.start_soon(_stress_run_one, bulkhead, rng, i)

        _assert_stress_consistent(await bulkhead.get_stats())


class TestBulkheadShutdownError:
    """The typed shutdown error must be distinguishable from task errors."""

    def test_is_a_bulkhead_error(self):
        assert issubclass(BulkheadShutdownError, BulkheadError)
        assert not issubclass(BulkheadShutdownError, RuntimeError)

    def test_threading_execute_after_shutdown_is_typed(self):
        bulkhead = BulkheadThreading(BulkheadConfig(name="sd_err_t", circuit_breaker_enabled=False))
        bulkhead.shutdown(wait=False)
        with pytest.raises(BulkheadShutdownError):
            bulkhead.execute(lambda: 1)


class TestRound4TimeoutStormAndHalfOpen:
    """Closes round-3 uncertainties: timeout storms and the half-open path."""

    async def test_sync_timeout_storm_recovers(self):
        """Many concurrent sync timeouts: prompt failures, drained counters, recovery."""
        import time as _time

        bulkhead = Bulkhead(
            BulkheadConfig(
                name="r4_storm",
                max_concurrent_calls=10,
                timeout_seconds=0.1,
                circuit_breaker_enabled=False,
            )
        )
        start = _time.monotonic()
        results = []

        async def run_one():
            try:
                await bulkhead.execute(lambda: _time.sleep(2.0))
                results.append(("ok", _time.monotonic() - start))
            except BulkheadTimeoutError:
                results.append(("timeout", _time.monotonic() - start))

        async with anyio.create_task_group() as nursery:
            for _ in range(30):
                nursery.start_soon(run_one)

        # All 30 callers must have failed promptly with the timeout
        assert len(results) == 30
        assert all(kind == "timeout" for kind, _ in results), results
        assert all(elapsed < 0.5 for _, elapsed in results), results

        stats = await bulkhead.get_stats()
        assert stats["active_tasks"] == 0, stats
        assert stats["in_flight_count"] == 0, stats

        # Async functions recover immediately (no thread pool involved)
        async def quick_async():
            return 7

        result = await bulkhead.execute(quick_async)
        assert result.success is True
        assert result.result == 7

        # Sync functions recover once the abandoned threads finish - the
        # pool is bounded by max_concurrent_calls, same contract as
        # BulkheadThreading (threads cannot be killed).
        await anyio.sleep(2.2)
        result = await bulkhead.execute(lambda: 42)
        assert result.success is True
        assert result.result == 42

        await bulkhead.shutdown()  # cancel the never-started queued work

    async def test_half_open_success_path_closes_circuit(self):
        """Open -> cooldown -> half-open probe succeeds -> circuit closes."""

        config = BulkheadConfig(
            name="r4_half_open",
            failure_threshold=2,
            success_threshold=3,
            isolation_duration=0.05,
            circuit_breaker_enabled=True,
        )
        bulkhead = Bulkhead(config, circuit_storage=InMemoryStorage())

        def failing():
            raise ValueError("boom")

        # Two consecutive failures open the circuit
        assert (await bulkhead.execute(failing)).success is False
        assert (await bulkhead.execute(failing)).success is False
        assert await bulkhead.get_state() == BulkheadState.ISOLATED

        # Open: requests blocked
        with pytest.raises(BulkheadCircuitOpenError):
            await bulkhead.execute(lambda: 1)

        # After cooldown the next call is allowed through as a half-open probe
        await anyio.sleep(0.4)
        assert (await bulkhead.execute(lambda: 1)).success is True
        assert await bulkhead.get_state() == BulkheadState.DEGRADED

        # The success window (success_threshold probes) closes the circuit
        assert (await bulkhead.execute(lambda: 2)).success is True
        assert (await bulkhead.execute(lambda: 3)).success is True
        assert await bulkhead.get_state() == BulkheadState.HEALTHY

    def test_threading_half_open_success_path_closes_circuit(self):
        """Same half-open -> closed journey on the threading implementation."""
        import time as _time

        config = BulkheadConfig(
            name="r4_half_open_t",
            failure_threshold=2,
            success_threshold=3,
            isolation_duration=0.05,
            circuit_breaker_enabled=True,
        )
        bulkhead = BulkheadThreading(config, circuit_storage=InMemoryStorage())
        try:

            def failing():
                raise ValueError("boom")

            assert bulkhead.execute(failing).result(timeout=5.0).success is False
            assert bulkhead.execute(failing).result(timeout=5.0).success is False
            assert bulkhead.get_state() == BulkheadState.ISOLATED

            with pytest.raises(BulkheadCircuitOpenError):
                bulkhead.execute(lambda: 1)

            _time.sleep(0.4)
            assert bulkhead.execute(lambda: 1).result(timeout=5.0).success is True
            assert bulkhead.get_state() == BulkheadState.DEGRADED

            assert bulkhead.execute(lambda: 2).result(timeout=5.0).success is True
            assert bulkhead.execute(lambda: 3).result(timeout=5.0).success is True
            assert bulkhead.get_state() == BulkheadState.HEALTHY
        finally:
            bulkhead.shutdown(wait=False)


class TestRound5NoTrust:
    """No-trust regression tests: ledger exactness, distributed state."""

    async def test_mid_execution_timeout_counts_as_failure(self):
        """A task that started and was cut by the timeout records a failure."""
        bulkhead = Bulkhead(
            BulkheadConfig(
                name="r5_mid_timeout",
                max_concurrent_calls=1,
                timeout_seconds=0.1,
                circuit_breaker_enabled=False,
            )
        )

        async def slow():
            await anyio.sleep(1.0)

        with pytest.raises(BulkheadTimeoutError):
            await bulkhead.execute(slow)
        stats = await bulkhead.get_stats()
        assert stats["total_executions"] == 1, stats
        assert stats["failed_executions"] == 1, stats
        assert stats["successful_executions"] == 0, stats
        assert stats["rejected_executions"] == 0, stats
        assert stats["active_tasks"] == 0, stats
        assert stats["in_flight_count"] == 0, stats

    async def test_waiting_timeout_counts_as_rejected(self):
        """A task that timed out before starting counts as rejected, not failed."""
        bulkhead = Bulkhead(
            BulkheadConfig(
                name="r5_wait_timeout",
                max_concurrent_calls=1,
                timeout_seconds=0.1,
                circuit_breaker_enabled=False,
            )
        )

        async with bulkhead._semaphore:  # hold the only permit
            with pytest.raises(BulkheadTimeoutError):
                await bulkhead.execute(lambda: 1)

        stats = await bulkhead.get_stats()
        assert stats["total_executions"] == 0, stats
        assert stats["rejected_executions"] == 1, stats
        assert stats["failed_executions"] == 0, stats
        assert stats["active_tasks"] == 0, stats

    async def test_ledger_balances_after_mixed_storm(self):
        """total == success + failed after any mix of outcomes drains."""
        import random as _random

        rng = _random.Random(99)
        bulkhead = Bulkhead(
            BulkheadConfig(
                name="r5_ledger",
                max_concurrent_calls=3,
                max_queue_size=3,
                timeout_seconds=0.05,
                circuit_breaker_enabled=False,
            )
        )

        async def async_work(i):
            await anyio.sleep(0.02)
            if rng.random() < 0.2:
                raise ValueError(f"fail {i}")

        def sync_work(i):
            time.sleep(0.02)
            if rng.random() < 0.2:
                raise ValueError(f"fail {i}")

        async def run_one(i):
            func = async_work if i % 2 == 0 else sync_work
            try:
                await bulkhead.execute(func, i)
            except (BulkheadTimeoutError, BulkheadFullError, BulkheadError):
                pass

        async with anyio.create_task_group() as tg:
            for i in range(60):
                tg.start_soon(run_one, i)

        deadline = time.monotonic() + 10
        while True:
            stats = await bulkhead.get_stats()
            if stats["active_tasks"] == 0 and stats["in_flight_count"] == 0:
                break
            assert time.monotonic() < deadline, stats
            await anyio.sleep(0.005)

        assert stats["total_executions"] == (
            stats["successful_executions"] + stats["failed_executions"]
        ), stats
        assert stats["active_tasks"] == 0 and stats["in_flight_count"] == 0, stats

    async def test_distributed_state_shares_open(self, postgres_storage):
        """Circuit state opened through one instance blocks another instance."""
        config = BulkheadConfig(
            name="r5_dist_shared",
            failure_threshold=1,
            isolation_duration=60.0,
            circuit_breaker_enabled=True,
        )
        instance_a = Bulkhead(config, circuit_storage=postgres_storage)

        # A's single failure opens the shared circuit (threshold 1)
        assert (await instance_a.execute(lambda: 1 / 0)).success is False
        assert await instance_a.get_state() == BulkheadState.ISOLATED

        # A new instance (restart semantics) loads the persisted OPEN state
        # and blocks - this is the distributed contract: state is shared at
        # construction time, not live-propagated between running instances.
        instance_b = Bulkhead(config, circuit_storage=postgres_storage)
        with pytest.raises(BulkheadCircuitOpenError):
            await instance_b.execute(lambda: 1)

        # and the original instance is blocked too
        with pytest.raises(BulkheadCircuitOpenError):
            await instance_a.execute(lambda: 1)

    async def test_stale_closed_does_not_clobber_shared_open(self, postgres_storage):
        """A local CLOSED process must not overwrite another's persisted OPEN.

        Reproduces the multiprocess lost-update: bulkhead A opens and persists
        OPEN; bulkhead B (constructed earlier, still CLOSED locally) records
        successes; without the guard B's blind write would flip the shared
        row to CLOSED and a restarted instance would admit calls again.
        """
        config = BulkheadConfig(
            name="r5_no_clobber",
            failure_threshold=5,
            isolation_duration=60.0,
            circuit_breaker_enabled=True,
        )
        instance_b = Bulkhead(config, circuit_storage=postgres_storage)  # loaded CLOSED

        instance_a = Bulkhead(config, circuit_storage=postgres_storage)
        for _ in range(5):  # 5 consecutive failures -> OPEN persisted
            assert (await instance_a.execute(lambda: 1 / 0)).success is False
        assert await instance_a.get_state() == BulkheadState.ISOLATED

        # B stays CLOSED locally (3/5 < 4/5) and records successes
        for _ in range(3):
            assert (await instance_b.execute(lambda: 1 / 0)).success is False
        for _ in range(2):
            assert (await instance_b.execute(lambda: 42)).success is True

        # The shared row must still be OPEN: B's stale CLOSED was not written
        row = postgres_storage.get_state("r5_no_clobber")
        assert row is not None
        assert row["state"] == "OPEN", f"stale CLOSED clobbered OPEN: {row}"

        # A restarted instance must still block
        instance_c = Bulkhead(config, circuit_storage=postgres_storage)
        with pytest.raises(BulkheadCircuitOpenError):
            await instance_c.execute(lambda: 1)


class TestConfigValidation:
    """Invalid configuration values must fail fast instead of crashing at use."""

    def test_zero_concurrency_rejected(self):
        with pytest.raises(ValueError):
            BulkheadConfig(name="x", max_concurrent_calls=0)

    def test_zero_failure_threshold_rejected(self):
        with pytest.raises(ValueError):
            BulkheadConfig(name="x", failure_threshold=0)

    def test_negative_queue_rejected(self):
        with pytest.raises(ValueError):
            BulkheadConfig(name="x", max_queue_size=-1)

    def test_negative_timeout_rejected(self):
        with pytest.raises(ValueError):
            BulkheadConfig(name="x", timeout_seconds=-1.0)

    def test_zero_timeout_rejected(self):
        # 0.0 means "instant timeout" (async) vs "no timeout" (threading);
        # the ambiguity is rejected.
        with pytest.raises(ValueError):
            BulkheadConfig(name="x", timeout_seconds=0.0)


class TestSuccessLimitSemantics:
    """The half-open probe window must be sized by success_threshold."""

    async def test_success_limit_uses_success_threshold(self):
        from fractions import Fraction

        config = BulkheadConfig(
            name="success_limit",
            success_threshold=3,
            circuit_breaker_enabled=True,
        )
        bulkhead = Bulkhead(config)
        assert bulkhead._circuit_breaker.success_limit == Fraction(1, 3)

    def test_threading_success_limit_uses_success_threshold(self):
        from fractions import Fraction

        config = BulkheadConfig(
            name="success_limit_t",
            success_threshold=3,
            circuit_breaker_enabled=True,
        )
        bulkhead = BulkheadThreading(config)
        assert bulkhead._circuit_breaker.success_limit == Fraction(1, 3)
        bulkhead.shutdown(wait=False)
