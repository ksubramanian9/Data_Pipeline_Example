FROM python:3.11-slim

RUN pip install --no-cache-dir kafka-python

WORKDIR /app
COPY services/event-generator/kafka_event_producer.py /app/producer.py

ENTRYPOINT ["python", "producer.py"]
