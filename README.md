# Drex Observatory

A small, read-only dashboard for monitoring **Drex**, the routing/decision model used inside a NOVA deployment. It shows model/runtime status, routing activity, latency, success/failure and fallback behavior, and per-day benchmark history. Powered by NOVA.

Standard library only (Python 3.10+). No build step, no front-end framework, no external requests, no telemetry.

## How it works

```
Drex / routing data (SQLite, read-only)
  -> collector.py
  -> read-only JSON API (server.py)
  -> Observatory UI (static/)
```

## What it monitors

Two SQLite databases, both opened read-only (`mode=ro` plus `PRAGMA query_only=1`):

- **Router DB**: tables `events`, `workers`. Per-request, router-side telemetry for the `drex` worker.
- **NOVA DB**: tables `sandbox_specialist_calls`, `sandbox_route_expansions`, `sandbox_attempts`, `mission_events`, `mission_recoveries`, `sandbox_missions`. Harness-side calls and what happened to each routed mission afterwards.

Anything the sources do not record is shown as `N/A` or `NOT RECORDED`. Nothing is estimated or invented. If a database is missing or unreadable, the UI shows the reason and empty states.

## Run

```sh
export DREX_MONITOR_ROUTER_DB=/path/to/router.db
export DREX_MONITOR_NOVA_DB=/path/to/nova.db
python3 server.py
```

With neither variable set, the server still starts and every view shows an empty state with the reason.

Open the URL it prints (default `http://127.0.0.1:4010`; the next free port is used if busy).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DREX_MONITOR_ROUTER_DB` | unset | Router SQLite file |
| `DREX_MONITOR_NOVA_DB` | unset | NOVA SQLite file |
| `DREX_MONITOR_HOST` | `127.0.0.1` | Bind address. Keep it on loopback: there is no authentication |
| `DREX_MONITOR_PORT` | `4010` | First port to try (scans up to 49 higher ports) |
| `DREX_MONITOR_STALL_SECONDS` | `600` | Age after which a routed mission with no worker launch counts as stalled |

## HTTP API

GET only; every other method returns 405. Requests whose `Host` header is not loopback or the configured host are rejected.

`/`, `/healthz`, `/api/overview`, `/api/latency`, `/api/routing`, `/api/benchmarks`. API responses are `{generated_at, errors, data}`. Snapshots are cached for 10 s. Known secret patterns are redacted from output.

## Test

```sh
python3 -m unittest discover -s tests
```

Tests build throwaway SQLite fixtures; no real data is needed.

## Status

Working, early. The collector depends on the NOVA/router schema above and is not a general-purpose Drex client.

## License

[MIT](LICENSE)
