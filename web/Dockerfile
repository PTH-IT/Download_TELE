FROM python:3.11-slim

WORKDIR /app

COPY server.py ./server.py
COPY index.html ./index.html
COPY login.html ./login.html
COPY jobs.html ./jobs.html
COPY job-detail.html ./job-detail.html

CMD ["python", "server.py"]

