# Image Spark officielle du cours + numpy (requis par Spark MLlib en Python)
FROM apache/spark:3.5.0
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --no-cache-dir numpy==1.24.4 pandas \
    && rm -rf /var/lib/apt/lists/*
# Cache Ivy accessible pour --packages
RUN mkdir -p /opt/spark/.ivy2 && chmod -R 777 /opt/spark/.ivy2
USER spark
