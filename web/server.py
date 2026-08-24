from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import os

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

class Handler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8001"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Serving dashboard at http://localhost:{port}")
    server.serve_forever()
