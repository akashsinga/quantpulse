# This line tells Docker to start with Python 3.11 on a lightweight Linux system
FROM python:3.11-slim

# These prevent Python from creating .pyc files and ensure output shows immediately
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set the working directory inside the container to /app
WORKDIR /app

# Install system packages that our Python packages might need
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt first (this helps with Docker's caching)
COPY backend/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your backend code into the container
COPY backend/ .

# IMPORTANT: Add the current directory to Python path
ENV PYTHONPATH=/app:$PYTHONPATH

# Tell Docker this container will listen on port 8000
EXPOSE 8000

# The default command to run when container starts
CMD ["uvicorn", "app.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--reload-dir", "/app"]