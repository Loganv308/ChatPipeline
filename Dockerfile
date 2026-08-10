FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies first (cached layer — only rebuilds if requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Don't buffer Python output so logs show up immediately
ENV PYTHONUNBUFFERED=1

# Same image runs both the collector and the sync worker — which one
# starts is decided by docker-compose's `command:` for that service.
# This default is just a sane fallback if the image is run directly.
CMD ["python", "src/Worker.py"]