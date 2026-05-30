FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Default entrypoint — overridden per-service in docker-compose.yml
CMD ["python", "--version"]
