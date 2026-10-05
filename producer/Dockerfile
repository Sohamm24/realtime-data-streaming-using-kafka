FROM python:3.11-slim

WORKDIR /app

# Ensure logs are flushed immediately to stdout
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "producer.py"]
