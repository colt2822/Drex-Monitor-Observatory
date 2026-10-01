"""Drex Observatory: tiny read-only Drex monitor. GET only, 127.0.0.1 only, no secrets, no writes."""
from __future__ import annotations

import json
import os
import socket
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import collector  # noqa: E402

HOST = os.environ.get("DREX_MONITOR_HOST", "127.0.0.1")
STATIC = Path(__file__).resolve().parent / "static"
FILES = {"/": ("index.html", "text/html; charset=utf-8"), "/app.js": ("app.js", "application/javascript; charset=utf-8"),
         "/style.css": ("style.css", "text/css; charset=utf-8")}
VIEWS = {"/api/overview": "overview", "/api/latency": "latency", "/api/routing": "routing", "/api/benchmarks": "benchmarks"}
SECURITY = {"Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'", "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}
MONITOR = collector.Monitor()
PORT_BOX = {"port": 4010}


class Handler(BaseHTTPRequestHandler):
    server_version = "drex-observatory"

    def _send(self, code, body: bytes, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in SECURITY.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _host_ok(self) -> bool:
        host = (self.headers.get("Host") or "").lower()
        port = PORT_BOX["port"]
        return host in {f"{h}:{port}" for h in {"127.0.0.1", "localhost", HOST.lower()}}

    def do_GET(self):
        if not self._host_ok():
            return self._send(403, b'{"error":"forbidden host"}')
        path = self.path.split("?", 1)[0]  # query string is ignored entirely
        if path in FILES:
            name, ctype = FILES[path]
            return self._send(200, (STATIC / name).read_bytes(), ctype)
        if path in VIEWS:
            try:
                snap = MONITOR.snapshot()
            except Exception as exc:  # never crash on odd data
                return self._send(500, json.dumps({"error": type(exc).__name__}).encode())
            return self._send(200, json.dumps({"generated_at": snap["generated_at"], "errors": snap["errors"],
                                               "data": snap[VIEWS[path]]}).encode())
        if path == "/favicon.ico":  # no icon shipped; 204 keeps browser consoles clean
            return self._send(204, b"")
        if path == "/healthz":
            return self._send(200, b'{"ok":true}')
        self._send(404, b'{"error":"not found"}')

    def _reject(self):
        self._send(405, b'{"error":"read-only"}')

    do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = _reject

    def log_message(self, *args):
        pass


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 16


def free_port(start: int) -> int:
    for port in range(start, start + 50):
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # match HTTPServer; ignore TIME_WAIT
            try:
                s.bind((HOST, port))
                return port
            except OSError:
                continue
    raise SystemExit("no free port")


def main():
    port = free_port(int(os.environ.get("DREX_MONITOR_PORT", "4010")))
    PORT_BOX["port"] = port
    httpd = Server((HOST, port), Handler)
    print(f"drex-observatory listening on http://{HOST}:{port}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
