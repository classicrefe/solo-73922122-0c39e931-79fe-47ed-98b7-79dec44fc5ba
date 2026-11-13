FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY data ./data
COPY demos ./demos

# SQLite 数据库放在可写卷；忙等待与上传并发均可用环境变量调节
ENV DB_PATH=/data/app.db \
    BUSY_TIMEOUT_MS=5000 \
    UPLOAD_WORKERS=2 \
    DEMO_ENABLED=1 \
    SELF_URL=http://127.0.0.1:8000 \
    PYTHONUNBUFFERED=1
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/', timeout=2).status==200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
