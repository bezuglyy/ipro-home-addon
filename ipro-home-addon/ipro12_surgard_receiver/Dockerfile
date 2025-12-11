FROM python:3.11-slim

WORKDIR /app

RUN pip install paho-mqtt requests

COPY surgard.py .
COPY run.sh .

RUN chmod +x run.sh

CMD ["/app/run.sh"]
