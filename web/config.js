// Cấu hình chung cho dashboard.
// Mặc định API chạy cùng host với web, cổng 8000 (xem docker-compose: API_PORT).
// Đổi cổng API thì sửa API_PORT bên dưới, hoặc thêm ?api=http://host:port vào URL.
(function () {
  var API_PORT = 8000;

  var override = new URLSearchParams(window.location.search).get("api");
  if (override) {
    try {
      localStorage.setItem("API_BASE", override);
    } catch (e) {}
  }

  var saved = null;
  try {
    saved = localStorage.getItem("API_BASE");
  } catch (e) {}

  var host = window.location.hostname || "localhost";
  var proto = window.location.protocol === "https:" ? "https:" : "http:";

  window.API_BASE = saved || proto + "//" + host + ":" + API_PORT;
})();
