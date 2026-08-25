"""web/server.py — phục vụ dashboard tĩnh và proxy /api, /ws sang API.

Nhờ proxy, trình duyệt chỉ gọi đúng origin của dashboard. Không còn phải cấu
hình cổng API ở phía client (nguồn gốc của lỗi gọi nhầm sang ứng dụng khác
đang chiếm cổng 8000), cũng không cần CORS.
"""
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlsplit
import json
import os
import selectors
import socket
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

# API nội bộ trong mạng docker. Chạy ngoài docker thì đặt API_UPSTREAM.
API_UPSTREAM = (os.getenv("API_UPSTREAM") or "http://api:8000").rstrip("/")

# Để trống = dùng proxy cùng origin (khuyến nghị). Chỉ đặt API_BASE khi muốn
# trình duyệt gọi thẳng API ở host/cổng khác.
API_BASE = (os.getenv("API_BASE") or "").strip()

PROXY_PREFIXES = ("/api/", "/ws/")
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}

CONFIG_TEMPLATE = """// File này được web/server.py sinh ra lúc chạy — đừng sửa tay.
// Mặc định rỗng = gọi API qua chính origin của dashboard (server proxy sang API).
(function () {
  var API_BASE = %(api_base)s;

  var override = new URLSearchParams(window.location.search).get("api");
  if (override !== null) {
    try {
      if (override) { localStorage.setItem("API_BASE", override); }
      else { localStorage.removeItem("API_BASE"); }
    } catch (e) {}
  }

  var saved = null;
  try { saved = localStorage.getItem("API_BASE"); } catch (e) {}

  window.API_BASE = saved || API_BASE || "";
})();
"""


def render_config() -> bytes:
    return (CONFIG_TEMPLATE % {"api_base": json.dumps(API_BASE)}).encode("utf-8")


class Handler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    # ---------------- static ----------------
    def do_GET(self):
        path = self.path.split("?")[0]

        if path.startswith("/ws/"):
            return self._proxy_websocket()
        if path.startswith(PROXY_PREFIXES):
            return self._proxy_http("GET")

        if path in ("/", ""):
            self.path = "/index.html"
        if path == "/config.js":
            return self._send_bytes(render_config(), "application/javascript; charset=utf-8")
        return super().do_GET()

    def do_HEAD(self):
        if self.path.split("?")[0].startswith(PROXY_PREFIXES):
            return self._proxy_http("HEAD")
        return super().do_HEAD()

    def do_POST(self):
        return self._proxy_http("POST")

    def do_PUT(self):
        return self._proxy_http("PUT")

    def do_PATCH(self):
        return self._proxy_http("PATCH")

    def do_DELETE(self):
        return self._proxy_http("DELETE")

    def _send_bytes(self, payload: bytes, content_type: str, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    # ---------------- proxy ----------------
    def _proxy_http(self, method: str):
        if not self.path.startswith(PROXY_PREFIXES):
            self.send_error(404, "Not Found")
            return

        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None

        req = urllib.request.Request(API_UPSTREAM + self.path, data=body, method=method)
        for name, value in self.headers.items():
            if name.lower() in HOP_BY_HOP or name.lower() == "host":
                continue
            req.add_header(name, value)

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = resp.read()
                status, headers = resp.status, resp.headers
        except urllib.error.HTTPError as exc:  # 4xx/5xx vẫn phải trả nguyên văn
            payload = exc.read()
            status, headers = exc.code, exc.headers
        except Exception as exc:
            msg = json.dumps({"detail": f"Không kết nối được API ({API_UPSTREAM}): {exc}"})
            self._send_bytes(msg.encode("utf-8"), "application/json", status=502)
            return

        self.send_response(status)
        for name, value in headers.items():
            if name.lower() in HOP_BY_HOP or name.lower() == "content-length":
                continue
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if method != "HEAD":
            self.wfile.write(payload)

    def _proxy_websocket(self):
        """Chuyển tiếp nguyên xi kết nối WebSocket sang API."""
        upstream_url = urlsplit(API_UPSTREAM)
        host = upstream_url.hostname or "api"
        port = upstream_url.port or (443 if upstream_url.scheme == "https" else 80)

        try:
            upstream = socket.create_connection((host, port), timeout=10)
        except OSError as exc:
            self.send_error(502, f"Không kết nối được API: {exc}")
            return

        try:
            request = [f"GET {self.path} HTTP/1.1", f"Host: {host}:{port}"]
            for name, value in self.headers.items():
                if name.lower() == "host":
                    continue
                request.append(f"{name}: {value}")
            upstream.sendall(("\r\n".join(request) + "\r\n\r\n").encode("latin-1"))

            self.close_connection = True
            _relay(self.connection, upstream)
        finally:
            for sock in (upstream, self.connection):
                try:
                    sock.close()
                except OSError:
                    pass


def _relay(a: socket.socket, b: socket.socket):
    """Nối 2 socket cho tới khi một bên đóng."""
    sel = selectors.DefaultSelector()
    a.setblocking(False)
    b.setblocking(False)
    sel.register(a, selectors.EVENT_READ, b)
    sel.register(b, selectors.EVENT_READ, a)
    try:
        while True:
            for key, _ in sel.select(timeout=60):
                try:
                    chunk = key.fileobj.recv(65536)
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    return
                if not chunk:
                    return
                try:
                    key.data.sendall(chunk)
                except OSError:
                    return
    finally:
        sel.close()


if __name__ == "__main__":
    # Mặc định 3000 cho khớp cổng publish trong docker-compose
    port = int(os.getenv("PORT", "3000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(
        f"Serving dashboard at http://localhost:{port} (proxy /api,/ws -> {API_UPSTREAM})",
        file=sys.stderr,
        flush=True,
    )
    server.serve_forever()
