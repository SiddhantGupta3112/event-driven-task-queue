FROM python:3.11-slim-bookworm

# Set working directory inside container
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source files
COPY . .

# Default command
CMD ["python", "monitor.py"]
