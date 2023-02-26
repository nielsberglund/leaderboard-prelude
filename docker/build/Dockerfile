FROM python:3.9.16-slim-bullseye

ENV BOOTSTRAP_SERVERS = 'localhost:9092'

COPY ./app/gameplay.py .
COPY ./app/runConsumer.py .
COPY ./app/runProducer.py .

RUN pip install confluent-kafka
