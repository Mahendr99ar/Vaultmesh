FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so Docker caches this layer between code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vaultmesh ./vaultmesh

# 8080 = gateway, 9000 = storage node (inside the Docker network)
EXPOSE 8080 9000

ENTRYPOINT ["python", "-m", "vaultmesh"]
