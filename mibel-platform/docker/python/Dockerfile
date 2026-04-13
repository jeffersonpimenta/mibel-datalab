FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    pandas==2.2.* \
    numpy==1.26.* \
    requests==2.31.* \
    openpyxl==3.1.* \
    clickhouse-driver==0.2.*

WORKDIR /app

ENV PYTHONUNBUFFERED=1

CMD ["tail", "-f", "/dev/null"]
