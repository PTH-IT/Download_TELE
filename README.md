# tgcopy — copy media Telegram (download → upload) có nhiều worker

Hệ thống gồm 4 service: `api` (FastAPI), `worker` (Pyrogram), `web` (dashboard tĩnh),
`redis` + `postgres`.

```
web (3000)  ──►  api (8000)  ──►  postgres
                    │
                    ▼
                  redis  ◄──►  worker × N
```

## Chạy

Trước tiên tạo Telegram app tại https://my.telegram.org/apps rồi điền
`API_ID` / `API_HASH` vào `.env`:

```bash
cp .env.example .env
```

```bash
docker compose up -d --build
```

- Dashboard: http://localhost:3000
- API: http://localhost:8000 (đổi bằng biến `API_PORT`)

Nếu máy đã có postgres/redis/dịch vụ khác chiếm cổng, đặt `PG_PORT`,
`REDIS_PORT`, `API_PORT`, `WEB_PORT` trong `.env`. Dashboard tự lấy đúng cổng
API từ `API_PORT` (server sinh `/config.js` lúc chạy), nên **không cần sửa file
js**. Kiểm tra nhanh bằng `curl http://localhost:<WEB_PORT>/config.js`.

Nếu API nằm ở host khác, đặt thẳng `API_BASE=http://ip-may-chu:8000` trong
`.env`. Muốn đổi tạm thời cho một trình duyệt: mở dashboard với
`?api=http://localhost:<cổng>`.

Lần đầu vào dashboard sẽ bị chuyển sang `/login.html` để đăng nhập Telegram bằng
số điện thoại + OTP. Worker giữ khoá `lock:auth_session` là bên gửi OTP và tạo
session; session được lưu ở `sessions/session_string.txt` và trong Redis nên các
worker khác dùng lại được.

## Chạy nhiều worker

```bash
docker compose up -d --scale worker=5
```

Mỗi worker tự lấy `hostname` (container id) làm `WORKER_ID`. **Không set
`WORKER_ID` cố định trong `docker-compose.yml`** — mọi replica sẽ trùng id, ghi đè
heartbeat của nhau và dashboard chỉ hiện đúng 1 worker.

Nếu chạy nhiều tài khoản Telegram, đặt `SESSION_STRING` riêng cho từng worker
(dùng chung một session string cho nhiều kết nối có thể bị Telegram thu hồi auth
key).

## Biến môi trường chính

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `API_ID` / `API_HASH` | — | **bắt buộc**, lấy từ https://my.telegram.org/apps |
| `API_PORT` / `WEB_PORT` | `8000` / `3000` | cổng publish của api và web |
| `PG_PORT` / `REDIS_PORT` | `5432` / `6379` | cổng publish của postgres/redis (đổi khi trùng) |
| `WORKER_REPLICAS` | `1` | số worker khi `docker compose up` |
| `MAX_DL` / `MAX_UP` | `2` / `4` | số download/upload song song mỗi worker |
| `SESSION_STRING` | — | session Telegram riêng cho worker đó |
| `MAX_ATTEMPTS` | `3` | số lần thử lại trước khi task bị đánh `failed` |
| `SCAN_CAP` | `200` | số message quét khi job không chỉ định khoảng msg_id |
| `DELETE_AFTER_UPLOAD` | `1` | xoá file sau khi upload xong |
| `LOG_LEVEL` | `INFO` | mức log của worker |
| `AUTH_LOCK_TTL` | `60` | thời gian giữ quyền auth master trước khi worker khác tiếp quản |
| `WORKER_STALE_AFTER` | `600` | heartbeat cũ hơn ngần này bị xoá khỏi dashboard |

Sao chép `.env.example` thành `.env` để đổi các giá trị trên:

```bash
cp .env.example .env
```

## Luồng xử lý

1. `POST /api/jobs` tạo job → đẩy vào Redis list `queue:new_job`.
2. Worker nhận job, resolve chat, quét message theo lô 200 id, tạo `tasks` và
   đẩy task id vào ZSET `queue:download`.
3. `download_worker` pop task, khoá `lock:task:{id}`, tải file về `downloads/`,
   rồi đẩy sang ZSET `queue:upload`.
4. `upload_worker` (worker nào rảnh cũng được, vì `downloads/` là volume chung)
   gửi file sang chat đích, ghi `transferred`, cập nhật lại `total/done/failed`
   của job.
5. Task đang chạy mà mất lock (worker chết) sẽ được `janitor` đưa lại hàng đợi
   sau tối đa 60 giây.

## Test

```bash
pip install -r requirements-dev.txt
pytest -q
```

## Lưu ý dữ liệu

`downloads/`, `sessions/` và mọi file binary đã được đưa vào `.gitignore` và
`.dockerignore`. Trước đó chúng nằm trong build context nên `docker build` phải
nén ~5GB mỗi lần.
