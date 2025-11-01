#!/bin/bash

# Meme Wrangler Bot Docker Deployment Script
# This script helps you deploy the bot to a remote server via SSH

set -e  # Exit on error

echo "======================================"
echo "  Meme Wrangler Bot Deployment"
echo "======================================"
echo ""

# Check if SSH key path is provided
if [ -z "$1" ]; then
    echo "Usage: ./deploy.sh <ssh_key_path> <username@server_ip>"
    echo "Example: ./deploy.sh ~/Downloads/server_key.pem ubuntu@123.45.67.89"
    exit 1
fi

SSH_KEY="$1"
SERVER="$2"

if [ -z "$SERVER" ]; then
    echo "Error: Please provide username@server_ip"
    echo "Example: ./deploy.sh ~/Downloads/server_key.pem ubuntu@123.45.67.89"
    exit 1
fi

ENV_FILE=".ENV"
echo "Step 1: Checking for environment file (.ENV or .env)..."
if [ -f "$ENV_FILE" ]; then
    echo "✓ .ENV file found"
elif [ -f ".env" ]; then
    ENV_FILE=".env"
    echo "✓ .env file found"
else
    echo "Error: No environment file found!"
    echo "Please create one with your credentials:"
    echo "  cp .ENV.example .ENV"
    echo "  nano .ENV"
    exit 1
fi
echo ""

echo "Step 2: Uploading files to server..."
ssh -i "$SSH_KEY" "$SERVER" "mkdir -p ~/meme-wrangler"
scp -i "$SSH_KEY" \
    Dockerfile \
    docker-compose.yml \
    bot.py \
    requirements.txt \
    "$ENV_FILE" \
    "$SERVER":~/meme-wrangler/

echo "✓ Files uploaded"
echo ""

echo "Step 3: Installing Docker on server (if needed)..."
ssh -i "$SSH_KEY" "$SERVER" << 'ENDSSH'
# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker not found. Installing..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo apt install docker-compose-plugin -y
    sudo usermod -aG docker $USER
    echo "✓ Docker installed"
else
    echo "✓ Docker already installed"
fi
ENDSSH

echo ""

echo "Step 4: Building and starting the bot..."
ssh -i "$SSH_KEY" "$SERVER" << 'ENDSSH'
cd ~/meme-wrangler

# Stop existing container if running
if docker ps -a | grep -q meme-wrangler; then
    echo "Stopping existing container..."
    docker-compose down
fi

# Build and start
echo "Building Docker image..."
docker-compose build

echo "Starting bot..."
docker-compose up -d

echo ""
echo "======================================"
echo "  Deployment Complete! 🎉"
echo "======================================"
echo ""
echo "Your bot is now running on the server!"
echo ""
echo "Useful commands:"
echo "  View logs:    ssh -i $SSH_KEY $SERVER 'cd ~/meme-wrangler && docker-compose logs -f'"
echo "  Stop bot:     ssh -i $SSH_KEY $SERVER 'cd ~/meme-wrangler && docker-compose down'"
echo "  Restart bot:  ssh -i $SSH_KEY $SERVER 'cd ~/meme-wrangler && docker-compose restart'"
echo ""
ENDSSH

echo ""
echo "Showing bot logs (press Ctrl+C to exit)..."
ssh -i "$SSH_KEY" "$SERVER" "cd ~/meme-wrangler && docker-compose logs -f"
