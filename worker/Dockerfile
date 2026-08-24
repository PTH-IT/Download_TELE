FROM python:3.11-slim

WORKDIR /app

# deps
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


# source
COPY . /app

ENV PYTHONPATH=/app

# default entry
CMD ["python", "worker/worker.py"]

