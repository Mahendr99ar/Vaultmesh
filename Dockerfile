FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so Docker caches this layer between code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vaultmesh ./vaultmesh

# 8080 = gateway / dashboard, 9000 = storage node (docker-compose network)
EXPOSE 8080 9000

ENTRYPOINT ["python", "-m", "vaultmesh"]
# With no arguments the image runs the single-process demo (used by Render). The port comes from
# $PORT when the host sets it. docker-compose.yml overrides this to run real separate nodes.
CMD ["demo"]
