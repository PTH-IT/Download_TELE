from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import json
import os

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

# Cổng/địa chỉ API được truyền từ docker-compose, nhờ vậy chỉ cần đổi API_PORT
# ở một chỗ duy nhất thay vì sửa cả compose lẫn file js.
API_PORT = os.getenv("API_PORT", "8000")
API_BASE = (os.getenv("API_BASE") or "").strip()

CONFIG_TEMPLATE = """// File này được web/server.py sinh ra lúc chạy — đừng sửa tay.
// Nguồn cấu hình là API_PORT / API_BASE trong .env (xem docker-compose.yml).
(function () {
  var API_PORT = %(api_port)s;
  var API_BASE = %(api_base)s;

  var override = new URLSearchParams(window.location.search).get("api");
  if (override) {
    try { localStorage.setItem("API_BASE", override); } catch (e) {}
  }

  var saved = null;
  try { saved = localStorage.getItem("API_BASE"); } catch (e) {}

  var host = window.location.hostname || "localhost";
  var proto = window.location.protocol === "https:" ? "https:" : "http:";

  window.API_BASE = saved || API_BASE || proto + "//" + host + ":" + API_PORT;
})();
"""


def render_config() -> bytes:
    body = CONFIG_TEMPLATE % {
        "api_port": json.dumps(API_PORT),
        "api_base": json.dumps(API_BASE),
    }
    return body.encode("utf-8")


class Handler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        if self.path in ("/", ""):
            self.path = "/index.html"
        if self.path.split("?")[0] == "/config.js":
            payload = render_config()
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
            return
        return super().do_GET()


if __name__ == "__main__":
    # Mặc định 3000 cho khớp cổng publish trong docker-compose
    port = int(os.getenv("PORT", "3000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Serving dashboard at http://localhost:{port} (API_PORT={API_PORT})", flush=True)
    server.serve_forever()
