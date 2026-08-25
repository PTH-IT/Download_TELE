// Fallback khi mở file html trực tiếp (không qua web/server.py).
// Khi chạy bằng docker compose, server.py sinh file này từ API_PORT trong .env.
(function () {
  if (window.API_BASE) return;

  var override = new URLSearchParams(window.location.search).get("api");
  if (override) {
    try { localStorage.setItem("API_BASE", override); } catch (e) {}
  }

  var saved = null;
  try { saved = localStorage.getItem("API_BASE"); } catch (e) {}

  var host = window.location.hostname || "localhost";
  var proto = window.location.protocol === "https:" ? "https:" : "http:";

  window.API_BASE = saved || proto + "//" + host + ":8000";
})();
