# Use a lightweight Python image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies (if needed, e.g. for psycopg2)
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create uploads directory
RUN mkdir -p /app/uploads

# Expose the Flask port
EXPOSE 5000

# Set environment (if needed)
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["python", "run.py"]
