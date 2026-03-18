FROM python:3.12-slim
WORKDIR /app
COPY server/requirements.txt .
RUN pip install -r requirements.txt
COPY server/server.py .
COPY web/ ../web/
CMD ["python", "-u", "server.py"]