# Use a small Python image
FROM python:3.11-slim

# System deps (curl for health debug; add ffmpeg later if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy code
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render provides $PORT; we must bind to it
ENV PORT=10000

# Health check path (Render pings it automatically if you configured)
# Not strictly required, but nice for local docker runs:
EXPOSE 10000

# Use gunicorn, not Flask dev server
CMD gunicorn app:app -b 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 120
