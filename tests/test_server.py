import json, os, sys, threading, unittest, urllib.error, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = server.Server((server.HOST, 0), server.Handler)  # unconfigured sources: empty states
        server.PORT_BOX["port"] = cls.httpd.server_address[1]
        cls.base = f"http://127.0.0.1:{server.PORT_BOX['port']}"
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown(); cls.httpd.server_close()

    def req(self, path, method="GET"):
        try:
            with urllib.request.urlopen(urllib.request.Request(self.base + path, method=method, data=b"" if method != "GET" else None)) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def test_static_assets(self):
        for path in ("/", "/app.js", "/style.css"):
            code, body = self.req(path)
            self.assertEqual(code, 200); self.assertTrue(body)
        self.assertIn(b"Drex Observatory", self.req("/")[1])

    def test_api_views_degrade_without_data(self):
        for view in ("overview", "latency", "routing", "benchmarks"):
            code, body = self.req("/api/" + view)
            self.assertEqual(code, 200)
            j = json.loads(body); self.assertEqual(len(j["errors"]), 2); self.assertIn("data", j)

    def test_read_only(self):
        for m in ("POST", "PUT", "DELETE", "PATCH"):
            self.assertEqual(self.req("/api/overview", m)[0], 405)
        self.assertEqual(self.req("/nope")[0], 404)


if __name__ == "__main__":
    unittest.main()
