FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ✅ no COPY assets
# ✅ no LOCAL_OVERLAY
ENV OVERLAY_URL=""

COPY app.py ./

ENV S3_BUCKET=""
ENV S3_PREFIX="renders/"
ENV PUBLIC_BASE_URL=""
ENV REGION="us-east-1"
ENV PORT=8080
EXPOSE 8080

CMD ["python", "app.py"]
