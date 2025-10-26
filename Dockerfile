# File: Dockerfile
# Docker image for Python HDFS client

FROM python:3.10-slim

WORKDIR /app

# Copy application code
COPY app.py .

# Install HDFS Python client
RUN pip install --no-cache-dir hdfs

# Run the application
CMD ["python", "app.py"]