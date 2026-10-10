# Free hosting on Hugging Face Spaces (Docker SDK, free "CPU basic": 2 vCPU, 16 GB RAM).
# Runs the WHOLE cluster (6 Raft nodes + gateway) inside one container.
# Copy this file to the ROOT of your Space repo (next to vaultmesh/, scripts/, requirements.txt).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Spaces run the container as user id 1000, so the app folder must belong to that user.
RUN useradd -m -u 1000 user
WORKDIR /home/user/app

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=user vaultmesh ./vaultmesh
COPY --chown=user scripts ./scripts

USER user
EXPOSE 7860

# Gateway + dashboard on 7860 (the port Spaces exposes); nodes use 9101-9203 inside the container.
CMD ["python", "scripts/local_cluster.py", "--clean", "--gateway-port", "7860"]
