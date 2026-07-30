FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

ENV KAFKA_BOOTSTRAP=kafka:29092
ENV KAFKA_TOPIC=demo-events
ENV KAFKA_GROUP_ID=angular-gateway

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
