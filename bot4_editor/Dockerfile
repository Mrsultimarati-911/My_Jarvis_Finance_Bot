FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY main.py .
COPY templates/ ./templates/
CMD ["python", "main.py"]
