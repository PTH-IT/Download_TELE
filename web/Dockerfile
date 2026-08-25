FROM python:3.11-slim

WORKDIR /app

COPY server.py ./server.py
COPY config.js ./config.js
COPY index.html ./index.html
COPY login.html ./login.html
COPY jobs.html ./jobs.html
COPY job-detail.html ./job-detail.html

ENV PORT=3000
EXPOSE 3000

CMD ["python", "server.py"]
