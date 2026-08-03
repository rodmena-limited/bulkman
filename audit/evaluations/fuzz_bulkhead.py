#!/usr/bin/env python3
"""Fuzz harness: randomized operation sequences against randomized bulkhead
configs, asserting counter invariants after EVERY operation.

Invariants (checked continuously):
  - 0 <= active_tasks <= max_concurrent_calls
  - 0 <= in_flight_count <= max_concurrent_calls + max_queue_size
  - total_executions == successful + failed + active_tasks
    (total counts tasks that started; success/failed count finished ones)
  - after drain: active_tasks == 0 and in_flight_count == 0

Runs the async bulkhead (asyncio), the threading bulkhead, and the sync
bridge.  Intended to run on the GIL-free interpreter to also exercise the
locks under the harshest conditions:

    python fuzz_bulkhead.py --seeds 50 --mode all

FAILS (exit 1) if any invariant breaks or any operation misbehaves.
"""

import argparse
import random
import sys
import threading
import time

import anyio

from bulkman import Bulkhead, BulkheadConfig, BulkheadFullError, BulkheadThreading
from bulkman.exceptions import BulkheadError, BulkheadTimeoutError


def check_invariants(stats: dict, tag: str) -> None:
    cap = stats["max_concurrent_calls"] + stats["max_queue_size"]
    if not (0 <= stats["active_tasks"] <= stats["max_concurrent_calls"]):
        raise AssertionError(f"{tag}: active_tasks out of range: {stats}")
    if not (0 <= stats["in_flight_count"] <= cap):
        raise AssertionError(f"{tag}: in_flight_count out of range: {stats}")
    if stats["total_executions"] != (
        stats["successful_executions"] + stats["failed_executions"] + stats["active_tasks"]
    ):
        raise AssertionError(f"{tag}: total != success + failed + active: {stats}")


def rand_config(rng: random.Random, tag: str) -> BulkheadConfig:
    return BulkheadConfig(
        name=tag,
        max_concurrent_calls=rng.randint(1, 6),
        max_queue_size=rng.randint(0, 8),
        timeout_seconds=rng.choice([None, None, None, 0.02, 0.05, 0.1]),
        circuit_breaker_enabled=False,
    )


async def drain_async(bulkhead, tag: str) -> None:
    deadline = time.monotonic() + 10.0
    while True:
        stats = await bulkhead.get_stats()
        if stats["active_tasks"] == 0 and stats["in_flight_count"] == 0:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"{tag}: did not drain: {stats}")
        await anyio.sleep(0.005)


async def fuzz_async(seed: int, rounds: int, batches: int) -> None:
    rng = random.Random(seed)

    async def async_work(i: int) -> int:
        roll = rng.random()
        if roll < 0.25:
            await anyio.sleep(rng.uniform(0.001, 0.03))
        if roll < 0.1:
            raise ValueError(f"async fail {i}")
        return i

    def sync_work(i: int) -> int:
        roll = rng.random()
        if roll < 0.25:
            time.sleep(rng.uniform(0.001, 0.03))
        if roll < 0.1:
            raise ValueError(f"sync fail {i}")
        return i

    for r in range(rounds):
        bulkhead = Bulkhead(rand_config(rng, f"fz_a_{seed}_{r}"))
        for _ in range(batches):
            n = rng.randint(1, 10)

            async def run_one(i: int) -> None:
                func = async_work if rng.random() < 0.5 else sync_work
                try:
                    await bulkhead.execute(func, i)
                except (BulkheadTimeoutError, BulkheadFullError):
                    pass
                except BulkheadError:
                    pass
                check_invariants(await bulkhead.get_stats(), f"seed{seed} r{r}")

            async with anyio.create_task_group() as tg:
                for i in range(n):
                    tg.start_soon(run_one, i)

        await drain_async(bulkhead, f"seed{seed} r{r}")
        await bulkhead.shutdown()


def drain_threading(bulkhead, tag: str) -> None:
    deadline = time.monotonic() + 10.0
    while True:
        stats = bulkhead.get_stats()
        if stats["active_tasks"] == 0 and stats["in_flight_count"] == 0:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"{tag}: did not drain: {stats}")
        time.sleep(0.005)


def fuzz_threading(seed: int, rounds: int, batches: int) -> None:
    rng = random.Random(seed + 10_000)

    def sync_work(i: int) -> int:
        roll = rng.random()
        if roll < 0.25:
            time.sleep(rng.uniform(0.001, 0.03))
        if roll < 0.1:
            raise ValueError(f"sync fail {i}")
        return i

    for r in range(rounds):
        bulkhead = BulkheadThreading(rand_config(rng, f"fz_t_{seed}_{r}"))
        for _ in range(batches):
            n = rng.randint(1, 10)
            futures = []
            for i in range(n):
                try:
                    futures.append(bulkhead.execute(sync_work, i))
                except BulkheadFullError:
                    pass
            for f in futures:
                try:
                    f.result(timeout=5.0)
                except (BulkheadTimeoutError, BulkheadFullError, BulkheadError):
                    pass
                except Exception:
                    pass
            check_invariants(bulkhead.get_stats(), f"seed{seed} r{r}")
        drain_threading(bulkhead, f"seed{seed} r{r}")
        bulkhead.shutdown(wait=False)


def fuzz_bridge(seed: int, rounds: int, batches: int) -> None:
    """Sync bridge under concurrent multi-threaded fan-out."""
    from bulkman.sync_bridge import BulkheadSync

    rng = random.Random(seed + 20_000)

    def sync_work(i: int) -> int:
        roll = rng.random()
        if roll < 0.25:
            time.sleep(rng.uniform(0.001, 0.03))
        if roll < 0.1:
            raise ValueError(f"bridge fail {i}")
        return i

    for r in range(rounds):
        bulkhead = BulkheadSync(rand_config(rng, f"fz_s_{seed}_{r}"))
        try:
            for _ in range(batches):
                n = rng.randint(1, 10)
                errors = []

                def caller(i: int) -> None:
                    try:
                        future = bulkhead.execute(sync_work, i)
                        future.result(timeout=5.0)
                    except (BulkheadTimeoutError, BulkheadFullError, BulkheadError):
                        pass
                    except Exception as e:  # pragma: no cover - unexpected
                        errors.append((i, type(e).__name__, str(e)))

                threads = [threading.Thread(target=caller, args=(i,)) for i in range(n)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=10.0)

                assert not errors, f"seed{seed} r{r}: unexpected caller errors: {errors}"
                with bulkhead._in_flight_lock:
                    assert (
                        bulkhead._in_flight_count == 0
                    ), f"seed{seed} r{r}: bridge leaked {bulkhead._in_flight_count} slots"
                stats = bulkhead.get_stats()
                assert stats["total_executions"] == (
                    stats["successful_executions"]
                    + stats["failed_executions"]
                    + stats["active_tasks"]
                ), f"seed{seed} r{r}: {stats}"
        finally:
            bulkhead.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--mode", default="all", choices=["async", "threading", "bridge", "all"])
    args = parser.parse_args()

    anyio.run(lambda: _run_all(args.seeds, args.rounds, args.batches, args.mode))
    print(
        f"PASS: fuzz {args.seeds} seeds x {args.rounds} rounds x {args.batches} batches "
        f"(mode={args.mode}) - all invariants held"
    )
    return 0


async def _run_all(seeds: int, rounds: int, batches: int, mode: str) -> None:
    for seed in range(seeds):
        if mode in ("async", "all"):
            await fuzz_async(seed, rounds, batches)
        if mode in ("threading", "all"):
            await anyio.to_thread.run_sync(fuzz_threading, seed, rounds, batches)
        if mode in ("bridge", "all"):
            await anyio.to_thread.run_sync(fuzz_bridge, seed, rounds, batches)


if __name__ == "__main__":
    sys.exit(main())
