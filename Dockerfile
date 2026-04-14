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

CMD ["python", "src/main.py"]