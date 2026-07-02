# Use Python 3.12 slim base image
FROM python:3.12-slim

# Prevent Python from writing pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies for audio capture, playback, and offline TTS
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    portaudio19-dev \
    libasound2-dev \
    libasound2 \
    espeak \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Set target workspace
WORKDIR /app

# Copy requirement list first to optimize Docker layer caching
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code and tests
COPY src/ ./src/
COPY tests/ ./tests/
COPY pyproject.toml .

# Run as a non-root user — least privilege for the container process
RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER appuser

# Basic liveness check: the CLI can import and report its version
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -m src.main --help > /dev/null || exit 1

# Define standard entrypoint for container execution
ENTRYPOINT ["python", "-m", "src.main"]
CMD ["benchmark", "--turns", "5", "--mock"]
