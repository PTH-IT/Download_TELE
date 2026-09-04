// Fallback khi mở html trực tiếp không qua web/server.py.
// Khi chạy bằng docker compose, server.py sinh file này và proxy /api,/ws
// nên API_BASE để rỗng = gọi cùng origin.
(function () {
  if (window.API_BASE !== undefined) return;

  var override = new URLSearchParams(window.location.search).get("api");
  if (override !== null) {
    try {
      if (override) { localStorage.setItem("API_BASE", override); }
      else { localStorage.removeItem("API_BASE"); }
    } catch (e) {}
  }

  var saved = null;
  try { saved = localStorage.getItem("API_BASE"); } catch (e) {}

  window.API_BASE = saved || "";
})();
