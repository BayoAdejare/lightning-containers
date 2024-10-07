# Getting the official Python image from Docker Hub
FROM python:3.11-slim-buster
LABEL maintainer="BayoAdejare"

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY src/ ./src/
COPY ../app/ ./app/

RUN python3 src/flows.py
# Start dashboard frontend 
EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

RUN streamlit run app/dashboard.py --server.port=8501 --server.address=0.0.0.0
RUN prefect server start

# Run our flow script when the container starts
# CMD ["python", "src/flows.py"]