"""One weight budget shared by every local Hyperliquid consumer on this IP.

Hyperliquid meters ``/info`` per source IP, not per process.  ``InfoClient``'s
token bucket is class-level, which makes it exactly one process wide, so three
co-resident consumers each politely staying inside 1200 weight/min still put
3600 weight/min on the wire and the exchange answers all three with 429s.

That is the measured situation.  On 2026-08-18 the Build-4 runtime, a wallet
proof engine on :8014 and a copy-engine SSOT script were all reading
``api.hyperliquid.xyz/info`` from this machine; only the runtime had any
limiter at all, and its telemetry showed 32,777 lifetime 429s while insisting
it was inside budget.

So the budget moves out of the process and into a file.

Design constraints this deliberately satisfies:

* **Atomic across processes.**  A single exclusive lock guards a read-modify-
  write of the window file.  No participant can observe a partial reservation.
* **Crash-safe.**  Reservations are timestamped and expire on their own after
  ``WINDOW_S``; nothing has to be released.  A participant killed mid-flight
  leaves at most one window's worth of phantom weight, which drains by itself.
  The lock is an OS file lock, which Windows releases when the owning handle
  closes -- including on abnormal termination -- so a dead process cannot hold
  the budget shut.
* **Bounded.**  Lock acquisition and total wait are both bounded.  The
  execution path never blocks indefinitely on a peer; if the budget cannot be
  obtained in time the caller is told so and applies its own fail-closed rule,
  which is a normal ``TRUTH_WAIT``, not a hang.
* **No new infrastructure.**  Standard library only -- no Redis, no broker, no
  daemon, no console window.
* **Windows-first.**  ``msvcrt.locking`` on Windows, ``fcntl.flock`` elsewhere.

This module performs no sends, opens no sockets, and holds no signing material.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import struct
import tempfile
import threading
import time
import zlib
from typing import Iterator

# The exchange meters a 60-second window per IP.
WINDOW_S = 60.0

# Set this to a path shared by every local consumer to turn the shared budget
# on.  Unset, each process keeps its own class-level bucket and nothing here
# runs -- so importing this module can never change existing behaviour, and a
# test suite is unaffected unless it opts in.
ENV_PATH = "HL_IP_BUDGET_PATH"

# Total IP allowance to share out.  Overridable so a machine running other
# Hyperliquid clients can be told the truth about what is actually available.
ENV_LIMIT = "HL_IP_BUDGET_WEIGHT_PER_MIN"
DEFAULT_LIMIT_WEIGHT_PER_MIN = 1200.0

# How long any participant may wait for the file lock itself.  The lock is held
# only for a prune-and-append, so contention is measured in microseconds; this
# bound exists purely so a pathological peer cannot stall the execution path.
LOCK_TIMEOUT_S = 2.0

_RECORD = struct.Struct("<dd")  # (epoch_seconds, weight)

# The window file carries its own integrity so a damaged one is REJECTED rather
# than silently believed short.  Magic and version make the pre-header format
# recognisable as untrustworthy instead of being misread as a valid short
# window; the count and CRC make truncation, extension and corruption all
# detectable.
_MAGIC = b"HLIP"
_FORMAT_VERSION = 1
_HEADER = struct.Struct("<4sHII")  # magic, version, record count, crc32(body)
_MAX_RECORDS = 4096


class SharedBudgetUnavailable(RuntimeError):
    """The shared budget could not be consulted within its bound.

    Deliberately distinct from "the budget is exhausted": one means the
    coordinator is unreachable, the other means the IP is genuinely spent.
    Callers treat both as "not now", but only the second is a rate-limit fact.
    """


def _try_lock_file(handle) -> bool:
    """One non-blocking attempt at the exclusive lock.

    Deliberately non-blocking on BOTH platforms.  ``msvcrt.LK_LOCK`` retries
    once a second, ten times, before giving up -- roughly ten seconds during
    which our own two-second deadline would have been silently ignored, and ten
    seconds an execution truth read cannot afford to spend waiting on a peer's
    bookkeeping.  ``fcntl.LOCK_EX`` without ``LOCK_NB`` has no bound at all.
    The caller owns the deadline; this function only ever tries once.
    """
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    else:  # pragma: no cover - production is Windows
        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False


def _unlock_file(handle) -> None:
    if os.name == "nt":
        import msvcrt

        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    else:  # pragma: no cover - production is Windows
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class IpWeightBudget:
    """A rolling weight window in a file, shared by every participant.

    ``reserve`` is the only mutating operation and it is all-or-nothing: it
    either records the weight and returns True, or records nothing and returns
    False.  There is no release -- reservations expire with the window, which
    is what makes a crashed participant harmless.
    """

    def __init__(
        self,
        path: str,
        *,
        limit_weight_per_min: float = DEFAULT_LIMIT_WEIGHT_PER_MIN,
        window_s: float = WINDOW_S,
    ) -> None:
        self.path = str(path)
        self.limit = float(limit_weight_per_min)
        self.window_s = float(window_s)
        # Guards this process's own use of the handle; the file lock guards the
        # file against other processes.  Both are needed: a threading lock does
        # not cross processes and a file lock is per-handle, not per-thread.
        self._local_lock = threading.Lock()
        # A dedicated lock file: the data file gets replaced atomically, and a
        # lock held on a replaced inode protects nothing.
        self.lock_path = self.path + ".lock"
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        """Hold the machine-wide lock, or raise inside the caller's bound.

        The lock lives on its own file rather than on the data file, so the
        data file is free to be replaced atomically underneath it.  The OS
        releases the lock when the handle closes -- including on abnormal
        termination -- so a killed participant cannot hold the budget shut.
        """
        deadline = time.monotonic() + LOCK_TIMEOUT_S
        remaining = max(0.0, deadline - time.monotonic())
        if not self._local_lock.acquire(timeout=remaining):
            raise SharedBudgetUnavailable("in-process budget lock timed out")
        try:
            handle = None
            try:
                while True:
                    if handle is None:
                        try:
                            handle = open(self.lock_path, "a+b")
                        except OSError as exc:
                            if time.monotonic() >= deadline:
                                raise SharedBudgetUnavailable(
                                    f"cannot open shared budget lock: {exc}"
                                ) from exc
                            time.sleep(0.005)
                            continue
                    handle.seek(0)
                    if _try_lock_file(handle):
                        break
                    if time.monotonic() >= deadline:
                        raise SharedBudgetUnavailable(
                            f"shared budget lock busy for {LOCK_TIMEOUT_S}s"
                        )
                    time.sleep(0.005)
                try:
                    yield None
                finally:
                    _unlock_file(handle)
            finally:
                if handle is not None:
                    handle.close()
        finally:
            self._local_lock.release()

    def _read_records(self) -> tuple[list[tuple[float, float]], bool]:
        """Read the committed window and say whether it can be believed.

        Returns ``(records, trustworthy)``.  Rounding a damaged file down to
        whole records was the wrong instinct: it turns five 20-weight
        reservations into 80 and hands the missing 20 back out to be spent
        twice.  Under-counting spend is precisely the failure this file exists
        to prevent, so anything that does not verify -- wrong magic, wrong
        version, wrong length, bad checksum, a non-finite or negative value --
        is reported as untrustworthy and the caller applies the conservative
        rule.

        An absent or zero-length file is a legitimately empty window: nothing
        has been published yet.  A NON-empty file that fails verification is
        not, and includes the pre-header format, which self-heals within one
        window rather than requiring anyone to delete anything.
        """
        try:
            with open(self.path, "rb") as handle:
                blob = handle.read()
        except FileNotFoundError:
            return [], True
        except OSError as exc:
            raise SharedBudgetUnavailable(f"cannot read shared budget: {exc}") from exc
        if not blob:
            return [], True
        if len(blob) < _HEADER.size:
            return [], False
        try:
            magic, version, count, checksum = _HEADER.unpack_from(blob, 0)
        except struct.error:
            return [], False
        if magic != _MAGIC or version != _FORMAT_VERSION:
            return [], False
        body = blob[_HEADER.size:]
        if len(body) != count * _RECORD.size:
            return [], False
        if zlib.crc32(body) & 0xFFFFFFFF != checksum:
            return [], False
        records: list[tuple[float, float]] = []
        for offset in range(0, len(body), _RECORD.size):
            ts, weight = _RECORD.unpack_from(body, offset)
            if not (math.isfinite(ts) and math.isfinite(weight)):
                return [], False
            if weight < 0.0:
                return [], False
            records.append((ts, weight))
        return records, True

    def _write_records(self, records: list[tuple[float, float]]) -> None:
        """Publish the window atomically.

        Write-then-replace, never truncate-then-write.  A process killed at any
        point leaves either the previous committed window or the new one, never
        an empty file that would read as "nobody has spent anything" and let
        every remaining participant overspend the IP at once.
        """
        body = b"".join(_RECORD.pack(t, w) for t, w in records)
        blob = _HEADER.pack(
            _MAGIC, _FORMAT_VERSION, len(records), zlib.crc32(body) & 0xFFFFFFFF
        ) + body
        directory = os.path.dirname(self.path) or "."
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".ipbudget-", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _live(self, records: list[tuple[float, float]], now: float):
        cutoff = now - self.window_s
        # A record stamped in the future is a clock-skewed peer, not licence to
        # ignore it: it is kept and counted, so skew can only cost headroom.
        return [(t, w) for t, w in records if t >= cutoff][-_MAX_RECORDS:]

    def spent(self, now: float | None = None) -> float:
        """Weight reserved by every participant in the current window."""
        now = time.time() if now is None else now
        try:
            with self._locked():
                records, trustworthy = self._read_records()
                live = self._live(records, now)
        except SharedBudgetUnavailable:
            return float("nan")
        if not trustworthy:
            # We cannot prove what has been spent, so we do not get to claim it
            # was little.
            return self.limit
        return sum(w for _, w in live)

    def reserve(self, weight: float, *, floor: float = 0.0) -> bool:
        """Atomically reserve ``weight`` if the window can carry it.

        ``floor`` is weight the caller must leave unclaimed -- the execution
        reserve.  Background work passes it; execution work does not.  Returns
        False without recording anything when the reservation would not fit.
        """
        weight = float(weight)
        now = time.time()
        with self._locked():
            raw, trustworthy = self._read_records()
            if not trustworthy:
                # A window we cannot verify is treated as FULL, not as empty.
                # The alternative -- believing a damaged file's smaller number
                # -- hands already-spent capacity back out to be spent again,
                # which is the overspend this file exists to stop.  Refuse this
                # reservation and republish a valid window carrying the whole
                # limit: it drains on its own within WINDOW_S, so the budget
                # self-heals and nobody has to delete a file by hand.
                self._write_records([(now, self.limit)])
                return False
            records = self._live(raw, now)
            spent = sum(w for _, w in records)
            if spent + weight + float(floor) > self.limit:
                return False
            records.append((now, weight))
            self._write_records(records)
            return True

    def diagnostics(self, now: float | None = None) -> dict:
        """Readable state for the HEALTH display and for tests."""
        now = time.time() if now is None else now
        try:
            with self._locked():
                records, trustworthy = self._read_records()
            live = self._live(records, now)
            spent = sum(w for _, w in live) if trustworthy else self.limit
            reachable = True
            detail = "" if trustworthy else "window unverifiable; treated as full"
        except SharedBudgetUnavailable as exc:
            live, spent, reachable, detail = [], 0.0, False, str(exc)
        return {
            "schema": "ip_weight_budget.v1",
            "path": self.path,
            "reachable": reachable,
            "detail": detail,
            "limit_weight_per_min": self.limit,
            "window_s": self.window_s,
            "reserved_weight_last_60s": round(spent, 2),
            "headroom_weight": round(max(0.0, self.limit - spent), 2),
            "reservation_count_last_60s": len(live),
        }


_INSTANCE: IpWeightBudget | None = None
_INSTANCE_LOCK = threading.Lock()


def shared_budget() -> IpWeightBudget | None:
    """The process-wide handle to the shared budget, or None if not configured.

    Absent ``HL_IP_BUDGET_PATH`` this returns None and every caller falls back
    to its own limiter -- the pre-existing behaviour, unchanged.
    """
    global _INSTANCE
    path = os.environ.get(ENV_PATH, "").strip()
    if not path:
        return None
    with _INSTANCE_LOCK:
        if _INSTANCE is None or _INSTANCE.path != path:
            try:
                limit = float(
                    os.environ.get(ENV_LIMIT, "") or DEFAULT_LIMIT_WEIGHT_PER_MIN
                )
            except ValueError:
                limit = DEFAULT_LIMIT_WEIGHT_PER_MIN
            _INSTANCE = IpWeightBudget(path, limit_weight_per_min=limit)
        return _INSTANCE


def reset_shared_budget_for_tests() -> None:
    global _INSTANCE
    with _INSTANCE_LOCK:
        _INSTANCE = None


def default_budget_path() -> str:
    """A machine-wide default location, used by the launcher wiring."""
    base = os.environ.get("PROGRAMDATA") or tempfile.gettempdir()
    return os.path.join(base, "HyperliquidProduction", "ip_weight_budget.bin")
