FROM python:3.12-slim
WORKDIR /app
COPY server/requirements.txt .
RUN pip install -r requirements.txt
COPY server/server.py .
COPY web/ ../web/
CMD ["gunicorn", "--worker-class", "eventlet", "-w", "1", "--bind", "0.0.0.0:8080", "server:app"]