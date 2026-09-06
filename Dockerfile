
FROM python:3.12-slim

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY shared/           shared/
COPY simulator/        simulator/
COPY gateway/          gateway/
COPY traffic_generator/ traffic_generator/
COPY websocket_server/ websocket_server/


ENV PYTHONPATH=/app

EXPOSE 8001

CMD ["uvicorn", "websocket_server.main:app", "--host", "0.0.0.0", "--port", "8001"]
