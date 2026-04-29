FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orderbook_collector.py .

# 데이터 저장 경로 (Railway Volume 마운트 포인트)
RUN mkdir -p /data/orderbook

CMD ["python", "orderbook_collector.py", "--symbol", "BTCUSDT", "--depth", "20"]
