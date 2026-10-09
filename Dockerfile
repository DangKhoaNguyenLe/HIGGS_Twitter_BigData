# Dockerfile for HIGGS Twitter Big Data Analysis
# Modular Architecture 
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Working directory
WORKDIR /app

# Install lightweight system utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Fix permissions
RUN chown -R ${NB_UID}:${NB_GID} /app

USER ${NB_UID}

# Install Python requirements
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Environment variables for lean memory execution 
ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    _JAVA_OPTIONS="-Xmx1536m -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
    SPARK_LOCAL_IP="127.0.0.1"

# Default to running the master orchestrator
CMD ["python", "/app/main.py"]
