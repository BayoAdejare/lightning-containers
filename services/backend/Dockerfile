FROM python:3.12-slim

WORKDIR /app

# Install system dependencies with curl first
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    libgomp1 \
    curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && export PATH="$HOME/.cargo/bin:$PATH" \
    && rustup default stable \
    && rm -rf /var/lib/apt/lists/*

# Environment variables (including Rust PATH)
ENV PATH="/root/.cargo/bin:${PATH}" \
    CC=/usr/bin/gcc \
    CXX=/usr/bin/g++

# Copy requirements first for better caching
COPY services/backend/requirements.txt .

# Install Python packages with proper user context
RUN python -m venv /opt/venv \
    && . /opt/venv/bin/activate \
    && export PATH="/root/.cargo/bin:$PATH" \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Use non-root user for security
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 4200
CMD ["sh", "-c", "prefect server start & prefect agent start -q 'default' & python src/flows.py"]