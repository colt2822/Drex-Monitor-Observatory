"""Read-only Drex telemetry collector. Stdlib only; never writes, never calls the network.

Sources (both opened mode=ro + query_only; paths come from the environment):
  * router DB   events/workers   -> per-request router-side telemetry
  * NOVA DB     sandbox_specialist_calls / sandbox_route_expansions / sandbox_attempts /
                mission_events / mission_recoveries / sandbox_missions -> harness + lifecycle
A metric that no source records is reported as None (rendered N/A / NOT RECORDED).
"""
from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from urllib.parse import quote

# No machine-specific defaults: an unset path is reported as "not configured" and the UI shows empty states.
NOVA_DB = os.environ.get("DREX_MONITOR_NOVA_DB", "")
ROUTER_DB = os.environ.get("DREX_MONITOR_ROUTER_DB", "")
STALL_SECONDS = int(os.environ.get("DREX_MONITOR_STALL_SECONDS", "600"))
TERMINAL_EVENTS = ("MISSION_PASS", "MISSION_FAIL", "MISSION_NEEDS_OPERATOR", "MISSION_CANCELLED")
NOT_RECORDED = None

# ── redaction ───────────────────────────────────────────────────────────────
_REDACT = [
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+"),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{10,}|github_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"apify_api_[A-Za-z0-9]+"),
    re.compile(r"(?i)\b(authorization|api[_-]?key|token|secret|password|passwd)\b\s*[:=]\s*\S+"),
]


def redact(text: str) -> str:
    for pattern in _REDACT:
        text = pattern.sub("[REDACTED]", text)
    return text


def redact_deep(value):
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {redact(str(k)): redact_deep(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_deep(v) for v in value]
    return value


# ── stats ───────────────────────────────────────────────────────────────────
def percentile(values, p: float):
    """Nearest-rank percentile (no interpolation): smallest value with >= p% of data at/below it."""
    data = sorted(v for v in values if v is not None)
    if not data:
        return None
    rank = max(1, math.ceil(p / 100.0 * len(data)))
    return data[min(rank, len(data)) - 1]


def dist(values) -> dict:
    data = [v for v in values if v is not None]
    return {"n": len(data),
            "avg": (sum(data) / len(data)) if data else None,
            "min": min(data) if data else None, "max": max(data) if data else None,
            "p50": percentile(data, 50), "p95": percentile(data, 95), "p99": percentile(data, 99)}


def rate(num: int, den: int):
    return (num / den) if den else None


def parse_ts(value):
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None


def ro_connect(path: str, timeout: float = 1.0) -> sqlite3.Connection:
    """Strictly read-only. mode=ro (not immutable: the NOVA DB is WAL); query_only as a second guard."""
    if not path:
        raise sqlite3.OperationalError("path not configured")
    conn = sqlite3.connect(f"file:{quote(path)}?mode=ro", uri=True, timeout=timeout)
    conn.execute("PRAGMA query_only=1")
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    return conn


# ── router-side requests (incremental) ──────────────────────────────────────
_SERVER_TIMING = re.compile(r"([a-z_]+);dur=([0-9.]+)")


class RouterRequests:
    """Rebuilds per-request records from router events, reading only rows with id > last_seen."""

    def __init__(self, path: str = ROUTER_DB):
        self.path = path
        self.last_id = 0
        self.requests: list[dict] = []
        self._open: dict | None = None
        self._pending_decision: dict | None = None
        self._prev_tokens: tuple | None = None
        self.rejected = 0
        self.lock = threading.Lock()

    def refresh(self):
        with self.lock:
            conn = ro_connect(self.path)
            try:
                rows = conn.execute(
                    "SELECT id, kind, payload, created_at FROM events WHERE worker_id='drex' AND id>? ORDER BY id",
                    (self.last_id,)).fetchall()
            finally:
                conn.close()
            for row_id, kind, payload, created_at in rows:
                self.last_id = row_id
                try:
                    data = json.loads(payload) if payload else {}
                except ValueError:
                    data = {}
                self._consume(kind, data if isinstance(data, dict) else {}, created_at)
            return self

    def _consume(self, kind, data, ts):
        if kind == "routing_decision":
            if self._pending_decision is not None and self._open is None:
                self.rejected += 1  # decision with no start = router refused (503/409)
            self._pending_decision = {"job_id": data.get("job_id"), "job_size": data.get("job_size"),
                                      "selected": data.get("selected_worker"), "ts": ts}
            return
        if kind == "job_started":
            self._open = {"job_id": data.get("job_id"), "t_start": ts,
                          "job_size": (self._pending_decision or {}).get("job_size")}
            self._pending_decision = None
            return
        if self._open is None:
            return  # terminal/other event with no matching start; cannot be attributed
        snap = data
        est = snap.get("local_usage_estimate") or {}
        ok = kind == "job_finished"
        tokens_in, tokens_out = est.get("input_tokens"), est.get("output_tokens")
        d_in = d_out = None
        if ok and self._prev_tokens and isinstance(tokens_in, int) and isinstance(self._prev_tokens[0], int):
            d_in = tokens_in - self._prev_tokens[0]
            d_out = (tokens_out - self._prev_tokens[1]) if isinstance(tokens_out, int) and isinstance(self._prev_tokens[1], int) else None
            d_in = d_in if d_in >= 0 else None     # counter reset -> not recorded
            d_out = d_out if d_out is not None and d_out >= 0 else None
        if isinstance(tokens_in, int):
            self._prev_tokens = (tokens_in, tokens_out)
        headers = est.get("last_provider_response_headers") or {}
        timing = {k: float(v) for k, v in _SERVER_TIMING.findall(str(headers.get("server-timing", "")))} if ok else {}
        latency = snap.get("rolling_latency_ms")
        self.requests.append({
            "job_id": self._open["job_id"], "t_start": self._open["t_start"], "t_end": ts,
            "ok": ok, "kind": "ok" if ok else kind,
            "latency_ms": float(latency) if isinstance(latency, (int, float)) else None,
            "wall_ms": (ts - self._open["t_start"]) * 1000.0,
            "job_size": self._open["job_size"], "model": snap.get("model"),
            "in_tokens": d_in, "out_tokens": d_out,
            "provider_ms": timing.get("ndm"),
            "health": snap.get("health"), "usage_state": snap.get("usage_state"),
        })
        self._open = None


# ── harness/lifecycle side ──────────────────────────────────────────────────
def load_specialist_calls(conn, since_iso: str | None = None) -> list[dict]:
    sql = ("SELECT task_id, attempt, mission_id, status, error_class, retryable, started_at, completed_at, "
           "length(request_json), length(result_json), json_extract(result_json,'$.model'), "
           "json_extract(result_json,'$.structured_output.route_class') "
           "FROM sandbox_specialist_calls WHERE role='DREX' ORDER BY started_at")
    out = []
    for r in conn.execute(sql):
        s, c = parse_ts(r[6]), parse_ts(r[7])
        out.append({"task_id": r[0], "attempt": r[1], "mission_id": r[2], "status": r[3], "error_class": r[4],
                    "retryable": bool(r[5]), "t_start": s, "t_end": c,
                    "latency_ms": (c - s) * 1000.0 if s is not None and c is not None else None,
                    "req_chars": r[8], "res_chars": r[9], "model": r[10], "route_class": r[11]})
    return out


def load_routing(conn, calls: list[dict], now: float) -> list[dict]:
    """One row per Drex-routed mission (mission_id correlation; codex_task_id is a planner task
    that never appears in sandbox_attempts, so it is deliberately not used for worker joins)."""
    done = {}
    for c in calls:
        if c["status"] == "COMPLETED":
            done[c["task_id"]] = c
    exp = conn.execute("SELECT mission_id, drex_task_id, state, route_class, created_at, updated_at, "
                       "codex_task_id, downstream_task_ids_json FROM sandbox_route_expansions").fetchall()
    missions = {}
    for mid, tid, state, rclass, created, updated, codex_tid, down in exp:
        call = done.get(tid)
        missions[mid] = {"mission_id": mid, "drex_task_id": tid, "state": state, "route_class": rclass,
                         "drex_done": call["t_end"] if call else None, "model": call["model"] if call else None,
                         "dispatched_at": parse_ts(updated),
                         "downstream_count": len(json.loads(down or "[]")) + (1 if codex_tid else 0)}
    failed_only = {}
    for c in calls:
        if c["task_id"] not in done and c["mission_id"] not in missions:
            f = failed_only.setdefault(c["mission_id"], {"mission_id": c["mission_id"], "drex_task_id": c["task_id"],
                                                          "state": "DREX_FAILED", "route_class": None, "model": c["model"],
                                                          "drex_done": c["t_end"], "dispatched_at": None,
                                                          "downstream_count": 0, "attempts": 0,
                                                          "error_class": c["error_class"]})
            f["attempts"] += 1
            f["drex_done"] = c["t_end"] or f["drex_done"]
    ids = list(missions)
    attempts = {}
    events = {}
    recov = {}
    retry_q = {}
    status = {}
    if ids:
        marks = ",".join("?" * len(ids))
        for mid, wid, prov, launched, outcome, st in conn.execute(
                f"SELECT mission_id, worker_id, observed_provider, launched_at, outcome_class, state FROM sandbox_attempts "
                f"WHERE mission_id IN ({marks}) AND launched_at IS NOT NULL ORDER BY launched_at", ids):
            attempts.setdefault(mid, []).append({"worker": wid, "provider": prov, "t": parse_ts(launched),
                                                 "outcome": outcome, "state": st})
        for mid, et, ts, facts in conn.execute(
                f"SELECT mission_id, event_type, timestamp, facts_json FROM mission_events WHERE mission_id IN ({marks}) "
                f"AND event_type IN ('MISSION_PASS','MISSION_FAIL','MISSION_NEEDS_OPERATOR','MISSION_CANCELLED','PLAN_CREATED',"
                f"'MISSION_SOFTWARE_RETRY_QUEUED','TASK_FAIL','TASK_VERIFIER_FAILED') ORDER BY sequence", ids):
            events.setdefault(mid, []).append((et, parse_ts(ts), facts))
        for mid, n in conn.execute(f"SELECT mission_id, count(*) FROM mission_recoveries WHERE mission_id IN ({marks}) GROUP BY 1", ids):
            recov[mid] = n
        for mid, st in conn.execute(f"SELECT mission_id, status FROM sandbox_missions WHERE mission_id IN ({marks})", ids):
            status[mid] = st
    rows = []
    for mid, m in missions.items():
        evs = events.get(mid, [])
        done_t = m["drex_done"]
        launches = [a for a in attempts.get(mid, []) if done_t is None or (a["t"] or 0) >= done_t]
        first = launches[0] if launches else None
        terminal = next((e for e in reversed(evs) if e[0] in TERMINAL_EVENTS), None)
        plan = next((e for e in evs if e[0] == "PLAN_CREATED"), None)
        fail_cat = None
        if terminal and terminal[0] == "MISSION_FAIL":
            try:
                fail_cat = json.loads(terminal[2] or "{}").get("error_category")
            except ValueError:
                pass
        age = (now - done_t) if done_t else None
        if first:
            cls = "LAUNCHED"
        elif terminal and terminal[0] == "MISSION_CANCELLED":
            cls = "CANCELLED"
        elif terminal and terminal[0] == "MISSION_FAIL":
            cls = "PLAN_REJECTED" if fail_cat == "POLICY_BLOCKED" else "MISSION_FAILED_NO_WORKER"
        elif terminal and terminal[0] == "MISSION_NEEDS_OPERATOR":
            cls = "NEEDS_OPERATOR_NO_WORKER"
        elif terminal:
            cls = "TERMINAL_NO_WORKER"
        elif age is not None and age > STALL_SECONDS:
            cls = "STALLED"
        else:
            cls = "IN_PROGRESS"
        after = [e for e in evs if done_t and e[1] and e[1] >= done_t]
        rows.append({**m, "mission_status": status.get(mid),
                     "outcome": cls, "fail_category": fail_cat,
                     "worker": first["worker"] if first else None, "provider": first["provider"] if first else None,
                     "attempt_outcome": first["outcome"] if first else None,
                     "to_dispatch_ms": ((m["dispatched_at"] - done_t) * 1000.0) if m["dispatched_at"] and done_t else None,
                     "to_worker_ms": ((first["t"] - done_t) * 1000.0) if first and done_t else None,
                     "to_plan_ms": ((plan[1] - done_t) * 1000.0) if plan and plan[1] and done_t and plan[1] >= done_t else None,
                     "terminal": terminal[0] if terminal else None,
                     "task_failures": sum(1 for e in after if e[0] in ("TASK_FAIL", "TASK_VERIFIER_FAILED")),
                     "retries_queued": sum(1 for e in after if e[0] == "MISSION_SOFTWARE_RETRY_QUEUED"),
                     "recoveries": recov.get(mid, 0)})
    rows.extend({**f, "mission_status": None, "outcome": "DREX_FAILED", "worker": None,
                 "provider": None, "to_dispatch_ms": None, "to_worker_ms": None, "to_plan_ms": None,
                 "terminal": None, "task_failures": 0, "retries_queued": 0, "recoveries": 0,
                 "fail_category": f["error_class"]} for f in failed_only.values())
    return rows


# ── views ───────────────────────────────────────────────────────────────────
def _req_stats(reqs: list[dict]) -> dict:
    ok = [r for r in reqs if r["ok"]]
    kinds: dict[str, int] = {}
    for r in reqs:
        if not r["ok"]:
            kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    lat = dist([r["latency_ms"] for r in ok])
    return {"total": len(reqs), "success": len(ok), "failed": len(reqs) - len(ok),
            "success_rate": rate(len(ok), len(reqs)), "failure_kinds": kinds,
            "latency_ms": lat,
            "provider_ms": dist([r["provider_ms"] for r in ok]),
            "in_tokens": dist([r["in_tokens"] for r in ok]),
            "out_tokens": dist([r["out_tokens"] for r in ok]),
            "timeouts": NOT_RECORDED, "retries_router": NOT_RECORDED}


def _call_stats(calls: list[dict]) -> dict:
    ok = [c for c in calls if c["status"] == "COMPLETED"]
    return {"total": len(calls), "success": len(ok), "failed": len(calls) - len(ok),
            "success_rate": rate(len(ok), len(calls)),
            "retries": sum(1 for c in calls if (c["attempt"] or 1) > 1),
            "error_classes": _count(c["error_class"] for c in calls if c["status"] != "COMPLETED"),
            "timeouts": sum(1 for c in calls if c["error_class"] == "PROVIDER_TIMEOUT"),
            "latency_ms": dist([c["latency_ms"] for c in ok]),
            "latency_all_ms": dist([c["latency_ms"] for c in calls]),
            "req_chars": dist([c["req_chars"] for c in calls]),
            "res_chars": dist([c["res_chars"] for c in ok])}


def _count(items) -> dict:
    out: dict = {}
    for i in items:
        out[str(i)] = out.get(str(i), 0) + 1
    return out


def _routing_stats(rows: list[dict]) -> dict:
    decided = [r for r in rows if r["outcome"] != "DREX_FAILED"]
    resolved = [r for r in decided if r["outcome"] != "IN_PROGRESS"]
    launched = [r for r in decided if r["outcome"] == "LAUNCHED"]
    dispatched = [r for r in decided if r["state"] in ("CODEX_DISPATCHED", "EXPANDED")]
    return {"decisions": len(decided), "dispatched": len(dispatched),
            "route_to_codex_rate": rate(len(dispatched), len(decided)),
            "route_to_worker_rate": rate(len(launched), len(resolved)),
            "launched": len(launched), "stalls": sum(1 for r in decided if r["outcome"] == "STALLED"),
            "to_dispatch_ms": dist([r["to_dispatch_ms"] for r in decided]),
            "to_worker_ms": dist([r["to_worker_ms"] for r in launched])}


class Monitor:
    def __init__(self, nova_db: str = NOVA_DB, router_db: str = ROUTER_DB, ttl: float = 10.0):
        self.nova_db, self.router = nova_db, RouterRequests(router_db)
        self.router_db, self.ttl = router_db, ttl
        self._cache: tuple[float, dict] | None = None
        self._lock = threading.Lock()

    def snapshot(self, now: float | None = None) -> dict:
        with self._lock:
            t = time.time() if now is None else now
            if self._cache and now is None and t - self._cache[0] < self.ttl:
                return self._cache[1]
            snap = redact_deep(self._build(t))
            self._cache = (t, snap)
            return snap

    def _worker_snapshot(self):
        try:
            conn = ro_connect(self.router_db)
            try:
                row = conn.execute("SELECT payload, updated_at FROM workers WHERE worker_id='drex'").fetchone()
            finally:
                conn.close()
            return (json.loads(row[0]), row[1]) if row else (None, None)
        except (sqlite3.Error, ValueError):
            return None, None

    def _build(self, now: float) -> dict:
        errors = []
        try:
            self.router.refresh()
        except sqlite3.Error as exc:
            errors.append(f"router DB unavailable ({exc})" if not self.router_db else f"router DB unreadable: {type(exc).__name__}")
        reqs = list(self.router.requests)
        calls, routing = [], []
        try:
            conn = ro_connect(self.nova_db)
            try:
                calls = load_specialist_calls(conn)
                routing = load_routing(conn, calls, now)
            finally:
                conn.close()
        except sqlite3.Error as exc:
            errors.append(f"NOVA DB unavailable ({exc})" if not self.nova_db else f"NOVA DB unreadable: {type(exc).__name__}")
        worker, worker_ts = self._worker_snapshot()
        last_router = reqs[-1]["t_end"] if reqs else None
        windows = {}
        for name, secs in (("1h", 3600), ("24h", 86400), ("all", None)):
            cut = now - secs if secs else 0
            windows[name] = {"router": _req_stats([r for r in reqs if r["t_end"] >= cut]),
                             "harness": _call_stats([c for c in calls if (c["t_end"] or c["t_start"] or 0) >= cut])}
        last_ok = next((r for r in reversed(reqs) if r["ok"]), None)
        cfg_model = (worker or {}).get("model") or (last_ok or {}).get("model")
        overview = {
            "model": {"configured_alias": cfg_model, "observed_returned": NOT_RECORDED,
                      "note": "The harness requests the alias 'drex-latest' and the router does not persist the provider's "
                              "returned model. The resolved drex-vX.Y version is not recorded anywhere."},
            "health": {"worker_health": (worker or {}).get("health"), "usage_state": (worker or {}).get("usage_state"),
                       "busy": (worker or {}).get("busy"), "last_error": (worker or {}).get("last_error"),
                       "cooldown_until": (worker or {}).get("cooldown_until"),
                       "last_router_event": iso(last_router), "last_router_event_age_s": (now - last_router) if last_router else None,
                       "worker_snapshot_age_s": (now - worker_ts) if worker_ts else None,
                       "router_requests_refused": self.router.rejected},
            "windows": windows,
            "post_drex": {"stalls": sum(1 for r in routing if r["outcome"] == "STALLED"), "stall_threshold_s": STALL_SECONDS},
            "scope_note": "Router totals include every caller of /api/drex/decision (advisor, triage, harness). "
                          "Harness totals are the mission-specialist subset.",
        }
        ok_reqs = [r for r in reqs if r["ok"]]
        latency = {
            "router_adapter_ms": dist([r["latency_ms"] for r in ok_reqs]),
            "provider_server_timing_ms": dist([r["provider_ms"] for r in ok_reqs]),
            "harness_end_to_end_ms": dist([c["latency_ms"] for c in calls if c["status"] == "COMPLETED"]),
            "slowest_router": [_slim(r) for r in sorted(ok_reqs, key=lambda r: -(r["latency_ms"] or 0))[:10]],
            "slowest_harness": [_slim_call(c) for c in sorted((c for c in calls if c["latency_ms"] is not None),
                                                              key=lambda c: -c["latency_ms"])[:10]],
            "by_model": {m: {"router_adapter_ms": dist([r["latency_ms"] for r in ok_reqs if (r["model"] or "UNKNOWN") == m]),
                             "harness_ms": dist([c["latency_ms"] for c in calls if c["status"] == "COMPLETED" and (c["model"] or "UNKNOWN") == m])}
                         for m in sorted({(r["model"] or "UNKNOWN") for r in reqs} | {(c["model"] or "UNKNOWN") for c in calls})},
            "by_route_class": {k: dist([c["latency_ms"] for c in calls if c["status"] == "COMPLETED" and (c["route_class"] or "UNKNOWN") == k])
                               for k in sorted({(c["route_class"] or "UNKNOWN") for c in calls if c["status"] == "COMPLETED"})},
            "by_job_size": {k: dist([r["latency_ms"] for r in ok_reqs if (r["job_size"] or "UNKNOWN") == k])
                            for k in sorted({(r["job_size"] or "UNKNOWN") for r in ok_reqs})},
            "note": "router_adapter_ms = provider call as timed by the router; provider_server_timing_ms = 'ndm' duration reported by "
                    "the provider; harness_end_to_end_ms = harness->router->provider. Direct-provider-only (bypassing NOVA) is not recorded.",
        }
        routing_view = {
            "stats": _routing_stats(routing),
            "by_route_class": _count(r["route_class"] or "DREX_FAILED" for r in routing),
            "by_outcome": _count(r["outcome"] for r in routing),
            "by_worker": _count(r["worker"] or "NO_WORKER" for r in routing),
            "by_provider": _count(r["provider"] or "NOT RECORDED" for r in routing),
            "by_state": _count(r["state"] for r in routing),
            "drex_failures": {"harness_calls_failed": sum(1 for c in calls if c["status"] != "COMPLETED"),
                              "by_error_class": _count(c["error_class"] for c in calls if c["status"] != "COMPLETED"),
                              "router_failure_kinds": _count(r["kind"] for r in reqs if not r["ok"])},
            "downstream": {"failed_missions": sum(1 for r in routing if r["outcome"] in ("MISSION_FAILED_NO_WORKER", "PLAN_REJECTED")),
                           "task_failures_after_drex": sum(r["task_failures"] for r in routing),
                           "retries_queued_after_drex": sum(r["retries_queued"] for r in routing),
                           "recoveries_after_drex": sum(r["recoveries"] for r in routing),
                           "launched_attempt_outcomes": _count(r["attempt_outcome"] for r in routing if r["outcome"] == "LAUNCHED")},
            "no_worker": [_slim_route(r) for r in routing if r["outcome"] not in ("LAUNCHED", "IN_PROGRESS")][-25:],
            "recent": [_slim_route(r) for r in sorted(routing, key=lambda r: r["drex_done"] or 0)[-25:]][::-1],
            "note": f"Worker launch = first sandbox_attempts.launched_at for the same mission_id at/after Drex completion. STALLED = "
                    f"no launch and no terminal mission event {STALL_SECONDS}s after the Drex decision. Cancelled, plan-rejected and "
                    f"operator-stopped missions are classified separately and are not stalls.",
        }
        bench = _benchmarks(reqs, calls, routing)
        return {"generated_at": iso(now), "errors": errors, "overview": overview, "latency": latency,
                "routing": routing_view, "benchmarks": bench}


def _slim(r):
    return {"job_id": r["job_id"], "at": iso(r["t_end"]), "latency_ms": r["latency_ms"], "provider_ms": r["provider_ms"],
            "in_tokens": r["in_tokens"], "job_size": r["job_size"], "model": r["model"]}


def _slim_call(c):
    return {"task_id": c["task_id"], "mission_id": c["mission_id"], "attempt": c["attempt"], "status": c["status"],
            "error_class": c["error_class"], "at": iso(c["t_end"]), "latency_ms": c["latency_ms"],
            "route_class": c["route_class"], "req_chars": c["req_chars"]}


def _slim_route(r):
    return {"mission_id": r["mission_id"], "drex_task_id": r["drex_task_id"],
            "route_class": r["route_class"], "state": r["state"], "outcome": r["outcome"], "fail_category": r["fail_category"],
            "worker": r["worker"], "provider": r["provider"], "terminal": r["terminal"], "mission_status": r["mission_status"],
            "drex_at": iso(r["drex_done"]), "to_dispatch_ms": r["to_dispatch_ms"], "to_worker_ms": r["to_worker_ms"],
            "task_failures": r["task_failures"], "retries_queued": r["retries_queued"], "recoveries": r["recoveries"]}


def _bench_block(reqs, calls, routing):
    ts = [x for x in [r["t_end"] for r in reqs] + [c["t_end"] for c in calls] if x]
    rs, cs = _req_stats(reqs), _call_stats(calls)
    rt = _routing_stats(routing)
    return {"window": [iso(min(ts)), iso(max(ts))] if ts else [None, None],
            "router": {"requests": rs["total"], "success_rate": rs["success_rate"], "latency_ms": rs["latency_ms"],
                       "in_tokens": rs["in_tokens"], "failure_kinds": rs["failure_kinds"]},
            "harness": {"requests": cs["total"], "success_rate": cs["success_rate"], "latency_ms": cs["latency_ms"],
                        "req_chars": cs["req_chars"], "retries": cs["retries"], "timeouts": cs["timeouts"]},
            "routing": {"decisions": rt["decisions"], "route_to_codex_rate": rt["route_to_codex_rate"],
                        "route_to_worker_rate": rt["route_to_worker_rate"], "to_worker_ms": rt["to_worker_ms"],
                        "stalls": rt["stalls"]},
            "timeouts_router": NOT_RECORDED, "cost_authoritative": NOT_RECORDED}


def _benchmarks(reqs, calls, routing) -> dict:
    models = sorted({(r["model"] or "UNKNOWN") for r in reqs} | {(c["model"] or "UNKNOWN") for c in calls}
                    | {(r["model"] or "UNKNOWN") for r in routing})
    out = []
    for m in models:
        rq = [r for r in reqs if (r["model"] or "UNKNOWN") == m]
        cl = [c for c in calls if (c["model"] or "UNKNOWN") == m]
        rt = [r for r in routing if (r["model"] or "UNKNOWN") == m]
        days = sorted({iso(x)[:10] for x in [r["t_end"] for r in rq] + [c["t_end"] for c in cl] if x})
        by_day = {}
        for d in days:
            by_day[d] = _bench_block([r for r in rq if iso(r["t_end"])[:10] == d],
                                     [c for c in cl if c["t_end"] and iso(c["t_end"])[:10] == d],
                                     [r for r in rt if r["drex_done"] and iso(r["drex_done"])[:10] == d])
        out.append({"model": m, "model_kind": "requested alias (resolved version NOT RECORDED)" if m == "drex-latest" else "as recorded",
                    "total": _bench_block(rq, cl, rt), "by_day": by_day})
    return {"models": out, "note": "Model labels are the recorded request/config value; Drex's resolved drex-vX.Y is not persisted by "
                                   "router or harness, so a silent upstream version change is invisible here. Cost: router prices are a "
                                   "hardcoded LOCAL_ESTIMATE, not authoritative -> N/A."}
