# Hum ek halki python image se shuru karenge
FROM python:3.10-slim-bullseye

# Working directory set karte hain
WORKDIR /app

# Requirements file copy karte hain
COPY ml_scripts/requirements_ml.txt /app/requirements_ml.txt

# Zaroori system packages install karte hain (gcc snowflake connector ke liye chahiye hota hai)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# ML libraries install karte hain
RUN pip install --no-cache-dir -r requirements_ml.txt

# Baaki ki scripts copy karte hain (hum baad mein likhenge)
COPY ml_scripts/ /app/ml_scripts/

# Default command (jab container chalega toh kya karega - abhi ke liye dummy hai)
CMD ["python", "--version"]