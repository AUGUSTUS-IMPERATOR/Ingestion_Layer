FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir aiokafka httpx

COPY consumer.py .

CMD ["python", "consumer.py"]