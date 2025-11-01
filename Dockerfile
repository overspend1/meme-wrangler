# Use Python 3.12 slim image
FROM python:3.12-slim

# Configure Python environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies required for asyncpg build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Ensure backups directory exists inside the container
RUN mkdir -p /app/backups

# Set environment variables (will be overridden by docker-compose or run command)
ENV TELEGRAM_BOT_TOKEN="" \
    OWNER_ID="" \
    CHANNEL_ID="" \
    DATABASE_URL="" \
    MEMEBOT_BACKUP_DIR="/app/backups"

# Run the bot
CMD ["python", "bot.py"]
