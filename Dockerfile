# Use official Python slim image
FROM python:3.12-slim
LABEL maintainer="BayoAdejare"

# Set working directory
WORKDIR /app

# Install runtime dependencies (curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Create data directories
RUN mkdir -p /app/data/Load /app/data/Processed /app/data/Analytics

# Copy application code
COPY src/ ./src/
COPY app/ ./app/

# Run initialization script during build (if needed)
# RUN python3 src/flows.py

# Expose Streamlit port
EXPOSE 8501

# Healthcheck for Streamlit
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run Streamlit as the main process
CMD ["streamlit", "run", "app/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]