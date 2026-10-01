"use strict";
// All dynamic content goes through textContent; nothing from the data is ever parsed as HTML.
const VIEWS = ["overview", "latency", "routing", "benchmarks"];
const LABEL = {overview: "Overview", latency: "Latency", routing: "Routing", benchmarks: "Benchmarks"};
const NA = "N/A";
function el(tag, text, cls) { const e = document.createElement(tag); if (text !== undefined) e.textContent = text; if (cls) e.className = cls; return e; }
function ms(v) { if (v == null) return NA; return v >= 1000 ? (v / 1000).toFixed(2) + " s" : Math.round(v) + " ms"; }
function pct(v) { return v == null ? NA : (v * 100).toFixed(1) + "%"; }
function num(v, d) { return v == null ? NA : (typeof v === "number" ? (Number.isInteger(v) ? String(v) : v.toFixed(d === undefined ? 1 : d)) : String(v)); }
function nr(v) { return v == null ? "NOT RECORDED" : String(v); }
function stat(label, value, cls) { const c = el("div", undefined, "stat"); c.append(el("dt", label), el("dd", value, cls)); return c; }
function grid(items) { const g = el("dl", undefined, "stats"); items.forEach(i => g.append(stat(...i))); return g; }
function table(head, rows) {
  if (!rows.length) return el("p", "No data recorded.", "empty");
  const w = el("div", undefined, "scroll"), t = el("table"), tr = el("tr");
  head.forEach(h => tr.append(el("th", h))); t.append(tr);
  rows.forEach(r => { const row = el("tr"); r.forEach(c => { const td = el("td", c == null ? NA : String(c)); if (c == null) td.className = "na"; row.append(td); }); t.append(row); });
  w.append(t); return w;
}
function kv(obj) { return table(["key", "count"], Object.entries(obj || {}).map(([k, v]) => [k, v])); }
function note(t) { return el("p", t, "note"); }
function distRow(name, d) { return [name, d.n, ms(d.avg), ms(d.p50), ms(d.p95), ms(d.p99), ms(d.max)]; }
const DH = ["series", "n", "avg", "p50", "p95", "p99", "max"];

function overview(d, out) {
  const r = d.windows.all.router, h = d.windows.all.harness, m = d.model, hl = d.health;
  out.append(grid([
    ["Drex model returned by provider", nr(m.observed_returned), "warn"], ["configured/requested alias", nr(m.configured_alias)],
    ["router health", nr(hl.worker_health), hl.worker_health === "HEALTHY" ? "ok" : "bad"], ["usage state", nr(hl.usage_state)],
    ["last router Drex event age", hl.last_router_event_age_s == null ? NA : Math.round(hl.last_router_event_age_s) + " s"],
    ["post-Drex stalls", d.post_drex.stalls, d.post_drex.stalls ? "bad" : "ok"]]));
  out.append(note(m.note)); out.append(note(d.scope_note));
  for (const [title, key] of [["Router (all callers)", "router"], ["Harness (mission specialist subset)", "harness"]]) {
    out.append(el("h2", title));
    const cols = ["window", "total", "success", "failed", "success rate", "timeouts", "retries", "avg", "p50", "p95", "p99", "n(latency)"];
    out.append(table(cols, ["1h", "24h", "all"].map(w => { const s = d.windows[w][key], l = s.latency_ms;
      return [w, s.total, s.success, s.failed, pct(s.success_rate),
        key === "router" ? "NOT RECORDED" : s.timeouts + " (PROVIDER_TIMEOUT class)", key === "router" ? "NOT RECORDED" : s.retries,
        ms(l.avg), ms(l.p50), ms(l.p95), ms(l.p99), l.n]; })));
  }
  out.append(el("h2", "Input / output size (all time)"));
  out.append(table(["series", "n", "avg", "p50", "p95", "max"], [
    ["router input tokens/request (provider-reported delta)", r.in_tokens.n, num(r.in_tokens.avg), num(r.in_tokens.p50), num(r.in_tokens.p95), num(r.in_tokens.max)],
    ["router output tokens/request", r.out_tokens.n, num(r.out_tokens.avg), num(r.out_tokens.p50), num(r.out_tokens.p95), num(r.out_tokens.max)],
    ["harness request JSON chars (proxy for state size)", h.req_chars.n, num(h.req_chars.avg, 0), num(h.req_chars.p50, 0), num(h.req_chars.p95, 0), num(h.req_chars.max, 0)],
    ["harness result JSON chars (decision size)", h.res_chars.n, num(h.res_chars.avg, 0), num(h.res_chars.p50, 0), num(h.res_chars.p95, 0), num(h.res_chars.max, 0)]]));
  out.append(el("h2", "Failure breakdown (all time)"));
  out.append(kv({...Object.fromEntries(Object.entries(r.failure_kinds).map(([k, v]) => ["router: " + k, v])), ...Object.fromEntries(Object.entries(h.error_classes).map(([k, v]) => ["harness: " + k, v]))}));
}
function latency(d, out) {
  out.append(table(DH, [distRow("router adapter (provider call as timed by router)", d.router_adapter_ms),
    distRow("provider server-timing 'ndm'", d.provider_server_timing_ms), distRow("harness end-to-end (harness→router→provider)", d.harness_end_to_end_ms)]));
  out.append(note(d.note)); out.append(note("Percentiles are nearest-rank. With small n, p95/p99 approach the max — read n."));
  out.append(el("h2", "By model (recorded label)"));
  out.append(table(DH, Object.entries(d.by_model).flatMap(([k, v]) => [distRow(k + " — router", v.router_adapter_ms), distRow(k + " — harness", v.harness_ms)])));
  out.append(el("h2", "By route class (harness, decision result class)"));
  out.append(table(DH, Object.entries(d.by_route_class).map(([k, v]) => distRow(k, v))));
  out.append(el("h2", "By router job size"));
  out.append(table(DH, Object.entries(d.by_job_size).map(([k, v]) => distRow(k, v))));
  out.append(el("h2", "Slowest router requests"));
  out.append(table(["job", "at", "latency", "provider ndm", "in tokens", "size"], d.slowest_router.map(r => [r.job_id, r.at, ms(r.latency_ms), ms(r.provider_ms), r.in_tokens, r.job_size])));
  out.append(el("h2", "Slowest harness calls"));
  out.append(table(["task", "mission", "attempt", "status", "error", "at", "latency", "route class"], d.slowest_harness.map(c => [c.task_id, c.mission_id, c.attempt, c.status, c.error_class, c.at, ms(c.latency_ms), c.route_class])));
}
function routing(d, out) {
  const s = d.stats;
  out.append(grid([["Drex decisions (completed)", s.decisions], ["downstream dispatches", s.dispatched], ["worker launched", s.launched],
    ["route→codex rate", pct(s.route_to_codex_rate)], ["route→worker rate", pct(s.route_to_worker_rate)], ["post-Drex stalls", s.stalls, s.stalls ? "bad" : "ok"],
    ["Drex→CODEX_DISPATCHED p50", ms(s.to_dispatch_ms.p50)], ["Drex→worker launch p50", ms(s.to_worker_ms.p50)]]));
  out.append(note(d.note));
  out.append(table(DH, [distRow("Drex→CODEX_DISPATCHED (expansion row updated_at)", s.to_dispatch_ms), distRow("Drex→worker launch (launched only; includes planning)", s.to_worker_ms)]));
  out.append(el("h2", "Decision / result class")); out.append(kv(d.by_route_class));
  out.append(el("h2", "Expansion state")); out.append(kv(d.by_state));
  out.append(el("h2", "Post-Drex outcome per mission")); out.append(kv(d.by_outcome));
  out.append(el("h2", "Downstream worker")); out.append(kv(d.by_worker));
  out.append(el("h2", "Downstream provider (observed)")); out.append(kv(d.by_provider));
  out.append(el("h2", "Drex-side failures (not downstream)")); out.append(kv({...Object.fromEntries(Object.entries(d.drex_failures.by_error_class).map(([k, v]) => ["harness " + k, v])), ...Object.fromEntries(Object.entries(d.drex_failures.router_failure_kinds).map(([k, v]) => ["router " + k, v]))}));
  out.append(el("h2", "Downstream lifecycle (after a successful Drex decision)"));
  const dn = d.downstream;
  out.append(kv({"failed missions (rejected/failed, no worker)": dn.failed_missions, "task failures after Drex": dn.task_failures_after_drex, "retries queued after Drex": dn.retries_queued_after_drex, "mission recoveries": dn.recoveries_after_drex}));
  out.append(kv(dn.launched_attempt_outcomes));
  const cols = ["mission", "class", "state", "outcome", "worker", "drex at", "→dispatch", "→worker"];
  const row = r => [r.mission_id, r.route_class, r.state, r.outcome + (r.fail_category ? " (" + r.fail_category + ")" : ""), r.worker, r.drex_at, ms(r.to_dispatch_ms), ms(r.to_worker_ms)];
  out.append(el("h2", "Decision with no downstream worker (recent)")); out.append(table(cols, d.no_worker.map(row)));
  out.append(el("h2", "Recent decisions")); out.append(table(cols, d.recent.map(row)));
}
function benchmarks(d, out) {
  out.append(note(d.note));
  if (!d.models.length) out.append(el("p", "No data recorded.", "empty"));
  d.models.forEach(m => {
    out.append(el("h2", m.model)); out.append(note(m.model_kind));
    const rows = [["ALL", m.total], ...Object.entries(m.by_day)];
    out.append(table(["window", "observed", "router req", "router ok", "router p50", "p95", "p99", "avg in tok", "max in tok", "harness req", "harness ok", "harness p50", "p95", "p99", "avg/max req chars", "route→codex", "route→worker", "→worker p50", "stalls", "timeouts", "retries", "cost"],
      rows.map(([k, b]) => [k, (b.window[0] || NA) + " … " + (b.window[1] || NA), b.router.requests, pct(b.router.success_rate), ms(b.router.latency_ms.p50), ms(b.router.latency_ms.p95), ms(b.router.latency_ms.p99),
        num(b.router.in_tokens.avg, 0), num(b.router.in_tokens.max, 0), b.harness.requests, pct(b.harness.success_rate), ms(b.harness.latency_ms.p50), ms(b.harness.latency_ms.p95), ms(b.harness.latency_ms.p99),
        num(b.harness.req_chars.avg, 0) + " / " + num(b.harness.req_chars.max, 0), pct(b.routing.route_to_codex_rate), pct(b.routing.route_to_worker_rate), ms(b.routing.to_worker_ms.p50), b.routing.stalls,
        "router: NOT RECORDED; harness: " + b.harness.timeouts, b.harness.retries, "N/A"])));
  });
}
const R = {overview, latency, routing, benchmarks};
function cur() { const h = location.hash.slice(1); return VIEWS.includes(h) ? h : "overview"; }
async function load() {
  const v = cur(), nav = document.getElementById("nav"); nav.replaceChildren();
  VIEWS.forEach(x => { const a = el("a", LABEL[x]); a.href = "#" + x; if (x === v) a.setAttribute("aria-current", "page"); nav.append(a); });
  const main = document.getElementById("main");
  try {
    const res = await fetch("/api/" + v), j = await res.json(), out = document.createElement("div");
    if (!res.ok || !j.data) throw new Error(j.error || "HTTP " + res.status);
    if (j.errors && j.errors.length) out.append(el("p", "Data source: " + j.errors.join("; "), "note warn"));
    R[v](j.data, out); main.replaceChildren(out); document.getElementById("stamp").textContent = "snapshot " + j.generated_at;
  } catch (e) { main.replaceChildren(el("p", "Unable to load data: " + e.message, "empty bad")); }
}
window.addEventListener("hashchange", load); load(); setInterval(load, 15000);
