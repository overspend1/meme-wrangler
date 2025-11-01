# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir python-telegram-bot==20.7

# Copy the bot code
COPY bot.py .

# Create directory for backups
RUN mkdir -p /app/backups

# Set environment variables (will be overridden by docker-compose or run command)
ENV TELEGRAM_BOT_TOKEN=""
ENV OWNER_ID=""
ENV CHANNEL_ID=""
ENV DATABASE_URL=""
ENV MEMEBOT_BACKUP_DIR="/app/backups"

# Run the bot
CMD ["python", "bot.py"]
