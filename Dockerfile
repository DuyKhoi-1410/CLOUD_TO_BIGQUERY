# syntax=docker/dockerfile:1

# ---- Base image ----
# Dùng Python slim để giữ image nhỏ gọn
FROM python:3.11-slim

# Tránh các câu hỏi interactive trong quá trình cài đặt
ENV DEBIAN_FRONTEND=noninteractive
# Đảm bảo output Python được flush ngay (quan trọng cho Cloud Run logging)
ENV PYTHONUNBUFFERED=1
# Tránh tạo file .pyc không cần thiết
ENV PYTHONDONTWRITEBYTECODE=1

# Cài đặt các thư viện hệ thống cần thiết cho PyTorch & transformers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgomp1 \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cài dependencies trước (tận dụng Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY main.py pipeline.py bq_loader.py ./

# Pre-download các model HuggingFace khi build image
# Giúp container khởi động nhanh hơn, không cần tải model lúc chạy
RUN python -c "\
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification; \
pipeline('ner', model='NlpHUST/ner-vietnamese-electra-base', aggregation_strategy='simple'); \
AutoTokenizer.from_pretrained('wonrax/phobert-base-vietnamese-sentiment'); \
AutoModelForSequenceClassification.from_pretrained('wonrax/phobert-base-vietnamese-sentiment'); \
print('Models downloaded successfully.')"

# Cloud Run sẽ set PORT tự động (mặc định 8080)
ENV PORT=8080
EXPOSE 8080

# Dùng Gunicorn làm production WSGI server
# --timeout 0: pipeline NLP có thể chạy lâu, không bị timeout
# Cloud Run có request timeout riêng (tối đa 3600s)
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "1", "--timeout", "0", "main:app"]
