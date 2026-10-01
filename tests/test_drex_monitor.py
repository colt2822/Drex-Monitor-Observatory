import hashlib, json, os, sqlite3, sys, tempfile, time, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collector as c

NOW = 1_800_000_000.0
def iso(t): return c.iso(t)

def snap(model="drex-latest", inp=0, out=0, elapsed=300, health="HEALTHY"):
    return json.dumps({"model": model, "health": health, "usage_state": "UNKNOWN", "rolling_latency_ms": elapsed,
        "local_usage_estimate": {"input_tokens": inp, "output_tokens": out,
        "last_provider_response_headers": {"server-timing": "auth;dur=8, ndm;dur=40.5"}}})

def make_router(path):
    db = sqlite3.connect(path)
    db.execute("create table events (id integer primary key, worker_id text, kind text, payload text, created_at real not null)")
    db.execute("create table workers (worker_id text primary key, payload text not null, updated_at real not null)")
    ev = [("routing_decision", {"job_id": "a", "job_size": "MEDIUM"}), ("job_started", {"job_id": "a"}), ("job_finished", None),
          ("routing_decision", {"job_id": "b", "job_size": "SMALL"}), ("job_started", {"job_id": "b"}), ("invalid_response", None),
          ("routing_decision", {"job_id": "c"}), ("job_started", {"job_id": "c"}), ("job_finished", None)]
    snaps = iter([snap(inp=100, out=10, elapsed=200), snap(inp=100, out=10, elapsed=900), snap(inp=350, out=30, elapsed=400)])
    for i, (k, p) in enumerate(ev, 1):
        db.execute("insert into events values (?,?,?,?,?)", (i, "drex", k, json.dumps(p if p is not None else json.loads(next(snaps))), NOW - 100 + i))
    db.execute("insert into workers values ('drex', ?, ?)", (snap(), NOW))
    db.commit(); db.close()

def make_nova(path):
    db = sqlite3.connect(path)
    db.executescript("""
    create table sandbox_specialist_calls (task_id, attempt, mission_id, role, request_json, request_digest, status, result_json, error_class, retryable, owner_token, claimed_at, started_at, completed_at);
    create table sandbox_route_expansions (mission_id, plan_id, drex_task_id, state, route_class, downstream_task_ids_json, codex_task_id, created_at, updated_at);
    create table sandbox_attempts (task_id, attempt, mission_id, worker_id, observed_provider, launched_at, outcome_class, state);
    create table mission_events (mission_id, sequence, event_type, timestamp, facts_json);
    create table mission_recoveries (mission_id);
    create table sandbox_missions (mission_id, status);""")
    def call(task, att, mid, status, err, s, e, model="drex-latest", rc="STANDARD"):
        res = json.dumps({"model": model, "structured_output": {"route_class": rc} if status == "COMPLETED" else {}})
        db.execute("insert into sandbox_specialist_calls values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (task, att, mid, "DREX", "x" * 1000, "", status, res, err, 1 if err else 0, "", iso(s), iso(s), iso(e)))
    T = NOW - 10000
    call("t1", 1, "m1", "COMPLETED", None, T, T + 1)          # launched
    call("t2", 1, "m2", "COMPLETED", None, T, T + 2)          # stalled (no event, old)
    call("t3", 1, "m3", "COMPLETED", None, T, T + 1)          # plan rejected
    call("t4", 1, "m4", "FAILED", "PROVIDER_UNAVAILABLE", T, T + 1)   # drex failure, then retry
    call("t4", 2, "m4", "FAILED", "PROVIDER_UNAVAILABLE", T + 5, T + 6)
    call("t5", 1, "m5", "COMPLETED", None, NOW - 5, NOW - 4)   # in progress (fresh)
    for mid, tid in (("m1", "t1"), ("m2", "t2"), ("m3", "t3"), ("m5", "t5")):
        db.execute("insert into sandbox_route_expansions values (?,?,?,?,?,?,?,?,?)", (mid, "p", tid, "CODEX_DISPATCHED", "STANDARD", "[]", "cx", iso(T), iso(T + 3)))
    db.execute("insert into sandbox_attempts values ('w1',1,'m1','codex-cli','codex-cli',?, 'PASS','RECEIPT_SEALED')", (iso(T + 60),))
    db.execute("insert into mission_events values ('m3',1,'MISSION_FAIL',?,?)", (iso(T + 30), '{"error_category": "POLICY_BLOCKED"}'))
    db.commit(); db.close()


class Base(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.r, self.n = os.path.join(self.d, "router.db"), os.path.join(self.d, "nova.db")
        make_router(self.r); make_nova(self.n)


class Stats(unittest.TestCase):
    def test_percentile_nearest_rank(self):
        v = list(range(1, 101))
        self.assertEqual((c.percentile(v, 50), c.percentile(v, 95), c.percentile(v, 99)), (50, 95, 99))
        self.assertEqual(c.percentile([5], 99), 5)
        self.assertIsNone(c.percentile([], 50))
        self.assertIsNone(c.percentile([None], 50))
        self.assertEqual(c.percentile([3, 1, 2], 50), 2)

    def test_redaction(self):
        key = "sk-" + "A" * 43
        for s in (key, "Authorization: Bearer abc.def-123", "ghp_" + "x" * 30, "sk-abcdefgh1234", "apify_api_Zz9Zz9", "api_key=hunter2", "token: abc"):
            self.assertNotIn("hunter2", c.redact(s)); self.assertIn("[REDACTED]", c.redact(s))
        self.assertNotIn(key, json.dumps(c.redact_deep({"a": [key], key: "v"})))
        self.assertEqual(c.redact("plain text 123"), "plain text 123")


class RouterTests(Base):
    def test_pairing_and_invariant(self):
        rr = c.RouterRequests(self.r).refresh()
        self.assertEqual([x["job_id"] for x in rr.requests], ["a", "b", "c"])
        self.assertEqual([x["ok"] for x in rr.requests], [True, False, True])
        self.assertEqual(rr.requests[1]["kind"], "invalid_response")
        self.assertEqual(rr.requests[0]["in_tokens"], None)       # first snapshot: no baseline
        self.assertEqual(rr.requests[2]["in_tokens"], 250)        # delta across failure
        self.assertEqual(rr.requests[2]["provider_ms"], 40.5)
        self.assertEqual(rr.requests[0]["latency_ms"], 200)

    def test_incremental(self):
        rr = c.RouterRequests(self.r).refresh(); n = len(rr.requests); rr.refresh()
        self.assertEqual(len(rr.requests), n)
        db = sqlite3.connect(self.r)
        for i, (k, p) in enumerate([("routing_decision", {"job_id": "d"}), ("job_started", {"job_id": "d"}), ("job_finished", json.loads(snap(inp=400, out=40)))], 100):
            db.execute("insert into events values (?,?,?,?,?)", (i, "drex", k, json.dumps(p), NOW))
        db.commit(); db.close(); rr.refresh()
        self.assertEqual(len(rr.requests), n + 1)
        self.assertEqual(rr.requests[-1]["in_tokens"], 50)


class RoutingTests(Base):
    def test_lifecycle_correlation(self):
        conn = c.ro_connect(self.n); calls = c.load_specialist_calls(conn); rows = {r["mission_id"]: r for r in c.load_routing(conn, calls, NOW)}
        self.assertEqual(rows["m1"]["outcome"], "LAUNCHED")
        self.assertNotIn("opportunity_id", rows["m1"])
        self.assertAlmostEqual(rows["m1"]["to_worker_ms"], 59000, delta=5)
        self.assertAlmostEqual(rows["m1"]["to_dispatch_ms"], 2000, delta=5)
        self.assertEqual(rows["m2"]["outcome"], "STALLED")
        self.assertEqual(rows["m3"]["outcome"], "PLAN_REJECTED")   # not a stall
        self.assertEqual(rows["m4"]["outcome"], "DREX_FAILED")     # Drex failure != downstream failure
        self.assertEqual(rows["m4"]["attempts"], 2)
        self.assertEqual(rows["m5"]["outcome"], "IN_PROGRESS")
        self.assertIsNone(rows["m2"]["to_worker_ms"])              # never fabricated

    def test_views_and_missing_metrics(self):
        s = c.Monitor(self.n, self.r).snapshot(NOW)
        self.assertIsNone(s["overview"]["model"]["observed_returned"])
        self.assertIsNone(s["overview"]["windows"]["all"]["router"]["timeouts"])
        self.assertEqual(s["overview"]["windows"]["all"]["router"]["failed"], 1)
        self.assertEqual(s["overview"]["windows"]["all"]["harness"]["retries"], 1)
        self.assertEqual(s["routing"]["stats"]["stalls"], 1)
        self.assertIsNone(s["benchmarks"]["models"][0]["total"]["cost_authoritative"])
        self.assertEqual(s["errors"], [])

    def test_missing_db_degrades(self):
        s = c.Monitor("/nonexistent/nova.db", "/nonexistent/router.db").snapshot(NOW)
        self.assertEqual(len(s["errors"]), 2)

    def test_unconfigured_paths_degrade(self):
        s = c.Monitor("", "").snapshot(NOW)
        self.assertEqual(len(s["errors"]), 2)
        self.assertEqual(s["overview"]["windows"]["all"]["router"]["total"], 0)
        self.assertEqual(s["routing"]["recent"], [])


class ReadOnlyTests(Base):
    def _hash(self, p):
        with open(p, "rb") as f: return hashlib.sha256(f.read()).hexdigest()

    def test_write_rejected(self):
        for p in (self.r, self.n):
            conn = c.ro_connect(p)
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("create table x(a)")
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("delete from " + ("events" if p == self.r else "sandbox_missions"))

    def test_no_db_changes(self):
        before = (self._hash(self.r), self._hash(self.n))
        m = c.Monitor(self.n, self.r); m.snapshot(NOW); m.snapshot(NOW + 60)
        self.assertEqual(before, (self._hash(self.r), self._hash(self.n)))

    def test_wal_db_unchanged(self):
        w = os.path.join(self.d, "wal.db"); db = sqlite3.connect(w); db.execute("pragma journal_mode=wal"); db.execute("create table t(a)"); db.execute("insert into t values (1)"); db.commit()
        before = self._hash(w)
        conn = c.ro_connect(w); self.assertEqual(conn.execute("select a from t").fetchall(), [(1,)]); conn.close()
        self.assertEqual(before, self._hash(w)); db.close()


if __name__ == "__main__":
    unittest.main()
