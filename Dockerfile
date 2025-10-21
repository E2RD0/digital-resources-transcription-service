FROM python:3.10-bookworm

# Set working directory
WORKDIR /workspace

# Install dependencies
RUN apt-get update && apt-get install -y \
    git \
    ffmpeg \
    supervisor

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Create logs directory with correct permissions
RUN mkdir -p /workspace/logs && chmod 755 /workspace/logs

# Copy source code
COPY src src

# Copy the supervisord configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Set environment variables
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
