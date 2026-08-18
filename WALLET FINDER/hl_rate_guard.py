"""Per-process Hyperliquid request budget for the WALLET FINDER products.

Three independent products read ``api.hyperliquid.xyz/info`` from this machine:
the Build-4 copy runtime, this directory's SSOT tracker, and the :8014 proof
engine.  They are deliberately NOT coupled -- separate state, separate cursors,
separate journals, separate business logic.  The one thing they cannot keep
separate is the exchange's rate limit, because Hyperliquid meters ``/info`` per
source IP.  On 2026-08-18 all three were inside their own idea of "polite" and
the IP was carrying roughly 150% of the 1200 weight/min allowance.

So this module gives each process two independent brakes:

1. **A fixed local ceiling** (``HL_IP_PROCESS_WEIGHT_PER_MIN``).  It works with
   no coordination at all.  Sized so the SUM of all three ceilings leaves
   substantial reserve below the real IP limit, which is the fallback the
   operator asked for if shared coordination ever proves fragile.
2. **The shared IP coordinator** (``hl_ip_budget``), when
   ``HL_IP_BUDGET_PATH`` is set.  This is the ONLY thing shared between the
   three products.  It carries weight and timestamps -- no trade state, no
   cursors, no ownership, no journals, no cached exchange truth.

Both must grant before a request goes out, and the fixed ceiling is checked
first so an unreachable coordinator can never raise this process's traffic.

Analysis and tracking work is background work.  It yields: ``acquire`` returns
False rather than blocking for long, and the caller skips that cycle.  Skipping
a background read is always correct -- the next cycle picks it up -- whereas
delaying the Build-4 execution path is not.  That is why the coordinator's
execution reserve exists and why nothing here ever claims it.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Optional

try:
    from hl_ip_budget import SharedBudgetUnavailable, shared_budget
except Exception:  # pragma: no cover - the guard must never break a product
    SharedBudgetUnavailable = RuntimeError  # type: ignore[assignment,misc]

    def shared_budget():  # type: ignore[misc]
        return None


WINDOW_S = 60.0

# Weight this process may spend per minute on its own authority.  Defaults are
# chosen so SSOT + 8014 together stay well inside the slice the copy runtime
# does not need, even with the coordinator switched off entirely:
#
#     Build-4 background   600      (its declared background target)
#     Build-4 execution    600      (reserved, mostly unspent)
#     SSOT                 150      (this default)
#     8014                 100      (this default)
#     ------------------------
#     steady state 600 + 150 + 100 = 850 of 1200, leaving 350 spare for the
#     execution path to burst into.  Build-4's own execution reserve sits
#     inside that spare, so no product has to trust another to behave.
ENV_PROCESS_LIMIT = "HL_IP_PROCESS_WEIGHT_PER_MIN"

# Documented Hyperliquid /info weights.  Anything unlisted bills at the
# expensive default: an unknown request that turns out to be cheap costs a
# little headroom, an unknown request assumed cheap costs a 429.
_CHEAP = 2.0
_DEFAULT = 20.0
_EXPENSIVE = 60.0
_WEIGHTS = {
    "clearinghouseState": _CHEAP,
    "spotClearinghouseState": _CHEAP,
    "orderStatus": _CHEAP,
    "userRole": _EXPENSIVE,
}


def weight_for(request_type: str) -> float:
    return _WEIGHTS.get(str(request_type or ""), _DEFAULT)


class RateGuard:
    """A rolling-window ceiling for one process, plus the shared IP budget."""

    def __init__(self, name: str, default_limit: float) -> None:
        self.name = str(name)
        try:
            self.limit = float(os.environ.get(ENV_PROCESS_LIMIT, "") or default_limit)
        except ValueError:
            self.limit = float(default_limit)
        self._lock = threading.Lock()
        self._window: list = []
        self.granted = 0
        self.refused_local = 0
        self.refused_shared = 0
        self.coordinator_errors = 0

    def _spent(self, now: float) -> float:
        cutoff = now - WINDOW_S
        self._window = [(t, w) for t, w in self._window if t >= cutoff]
        return sum(w for _, w in self._window)

    def acquire(self, request_type: str, *, timeout_s: float = 0.0) -> bool:
        """Try to claim budget for one request.  False means skip this one.

        ``timeout_s`` is a bound, not a target: background work should pass 0
        and simply try again next cycle.  Nothing here blocks the caller
        indefinitely, and nothing here retries on the caller's behalf.
        """
        weight = weight_for(request_type)
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        while True:
            with self._lock:
                now = time.time()
                granted = self._spent(now) + weight <= self.limit
            if not granted:
                self.refused_local += 1
            else:
                budget = shared_budget()
                if budget is not None:
                    try:
                        if not budget.reserve(weight):
                            self.refused_shared += 1
                            granted = False
                    except SharedBudgetUnavailable:
                        # The coordinator is a courtesy to the other products,
                        # not this one's safety net -- the fixed ceiling above
                        # already bounds us.  Record and proceed.
                        self.coordinator_errors += 1
                    except Exception:
                        self.coordinator_errors += 1
            if granted:
                with self._lock:
                    self._window.append((time.time(), weight))
                self.granted += 1
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.25, max(0.01, remaining)))

    def diagnostics(self) -> dict:
        with self._lock:
            spent = self._spent(time.time())
        budget = shared_budget()
        return {
            "schema": "hl_rate_guard.v1",
            "process": self.name,
            "process_limit_weight_per_min": self.limit,
            "process_weight_last_60s": round(spent, 2),
            "process_headroom_weight": round(max(0.0, self.limit - spent), 2),
            "granted": self.granted,
            "refused_local": self.refused_local,
            "refused_shared": self.refused_shared,
            "coordinator_errors": self.coordinator_errors,
            "shared_ip_budget": (
                budget.diagnostics()
                if budget is not None
                else {"reachable": False, "detail": "not configured"}
            ),
        }


_GUARDS = {}
_GUARDS_LOCK = threading.Lock()


def guard(name: str, default_limit: float) -> RateGuard:
    with _GUARDS_LOCK:
        existing = _GUARDS.get(name)
        if existing is None:
            existing = RateGuard(name, default_limit)
            _GUARDS[name] = existing
        return existing


def reset_for_tests(name: Optional[str] = None) -> None:
    with _GUARDS_LOCK:
        if name is None:
            _GUARDS.clear()
        else:
            _GUARDS.pop(name, None)
