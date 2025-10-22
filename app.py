#!/usr/bin/env python3
"""
app.py - Single-file web + API server using Python standard library only.

Features:
- Serves a minimal HTML single-page app at /
- JSON API endpoints: GET /api/hello, POST /api/echo
- Health check at /health
- Graceful shutdown on SIGINT/SIGTERM
- No external dependencies
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import argparse
import json
import logging
import threading
import signal
import sys
import urllib.parse
import webbrowser
from typing import Tuple

# Configuration defaults
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
LOG = logging.getLogger("app")

HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Single-file App</title>
  <style>
    body {{ font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; margin: 2rem; }}
    .card {{ border: 1px solid #ddd; padding: 1rem; border-radius: 6px; max-width: 600px; }}
    pre {{ background:#f6f8fa; padding: .5rem; overflow:auto; }}
    button {{ padding:.5rem 1rem; }}
  </style>
</head>
<body>
  <h1>Single-file App</h1>
  <div class="card">
    <p>This page demonstrates a minimal single-file Python web app and JSON API.</p>
    <div>
      <button id="helloBtn">Call /api/hello</button>
      <button id="echoBtn">Call /api/echo</button>
    </div>
    <h3>Response</h3>
    <pre id="out">No requests yet.</pre>
  </div>

  <script>
    async function fetchJson(path, opts) {{
      const resp = await fetch(path, opts);
      const text = await resp.text();
      let data;
      try {{ data = JSON.parse(text); }} catch(e) {{ data = text; }}
      return {{ status: resp.status, data }};
    }}

    document.getElementById('helloBtn').addEventListener('click', async () => {{
      const name = prompt('Name (optional):');
      const qs = name ? '?name=' + encodeURIComponent(name) : '';
      const r = await fetchJson('/api/hello' + qs);
      document.getElementById('out').textContent = JSON.stringify(r, null, 2);
    }});

    document.getElementById('echoBtn').addEventListener('click', async () => {{
      const body = {{ time: new Date().toISOString(), note: 'sample' }};
      const r = await fetchJson('/api/echo', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body)
      }});
      document.getElementById('out').textContent = JSON.stringify(r, null, 2);
    }});
  </script>
</body>
</html>
"""


def parse_query(path: str) -> Tuple[str, dict]:
    """Return path without query and parsed query dict."""
    parsed = urllib.parse.urlparse(path)
    qs = urllib.parse.parse_qs(parsed.query)
    # convert single-element lists to values
    qs_simple = {k: v[0] if len(v) == 1 else v for k, v in qs.items()}
    return parsed.path, qs_simple


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "SingleFileApp/1.0"

    def _set_headers(self, status=200, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        # Minimal CORS for convenience
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers()
        # No body for OPTIONS

    def do_GET(self):
        path, qs = parse_query(self.path)
        LOG.debug("GET %s qs=%s", path, qs)
        if path == "/" or path == "/index.html":
            content = HTML_PAGE.encode("utf-8")
            self._set_headers(200, "text/html; charset=utf-8")
            self.wfile.write(content)
            return

        if path == "/health":
            self._set_headers(200)
            self.wfile.write(json.dumps({"status": "ok"}).encode("utf-8"))
            return

        if path == "/api/hello":
            name = qs.get("name") or "world"
            resp = {"message": f"Hello, {name}!"}
            self._set_headers(200)
            self.wfile.write(json.dumps(resp).encode("utf-8"))
            return

        # Not found
        self._set_headers(404)
        self.wfile.write(json.dumps({"error": "not_found"}).encode("utf-8"))

    def _read_json(self):
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length <= 0:
            return None
        raw = self.rfile.read(content_length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def do_POST(self):
        path, _ = parse_query(self.path)
        LOG.debug("POST %s", path)
        if path == "/api/echo":
            data = self._read_json()
            if data is None:
                self._set_headers(400)
                self.wfile.write(json.dumps({"error": "invalid_json"}).encode("utf-8"))
                return
            # Echo back with server timestamp
            resp = {"received": data, "server_time": threading.current_thread().name}
            self._set_headers(200)
            self.wfile.write(json.dumps(resp).encode("utf-8"))
            return

        self._set_headers(404)
        self.wfile.write(json.dumps({"error": "not_found"}).encode("utf-8"))

    def log_message(self, format, *args):
        LOG.info("%s - - %s", self.address_string(), format % args)


def run_server(host: str, port: int, open_browser: bool):
    server_address = (host, port)
    httpd = ThreadingHTTPServer(server_address, RequestHandler)
    LOG.info("Starting server at http://%s:%d", host, port)

    def handle_signal(signum, frame):
        LOG.info("Signal %s received, shutting down...", signum)
        # shutdown called from another thread is safe
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if open_browser:
        try:
            webbrowser.open(f"http://{host}:{port}/")
        except Exception:
            LOG.debug("Failed to open browser", exc_info=True)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt received, exiting.")
    finally:
        httpd.server_close()
        LOG.info("Server stopped.")


def parse_args():
    p = argparse.ArgumentParser(description="Single-file Python app (no externals).")
    p.add_argument("--host", "-H", default=DEFAULT_HOST, help="Host to bind to (default: %(default)s)")
    p.add_argument("--port", "-p", type=int, default=DEFAULT_PORT, help="Port to listen on (default: %(default)s)")
    p.add_argument("--open", action="store_true", help="Open default web browser on start")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        run_server(args.host, args.port, args.open)
    except OSError as e:
        LOG.error("Failed to start server: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
