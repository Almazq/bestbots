FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ВАЖНО: пробрасываем переменную окружения внутрь контейнера
ENV BOT_TOKEN=${BOT_TOKEN}

CMD ["python", "main.py"]