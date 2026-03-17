# ---------- Base Image (Python 3.13 slim) ----------
FROM python:3.13-slim AS base

# Install system deps + Node.js in one layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gcc libpq-dev \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g npm@latest \
    && rm -rf /var/lib/apt/lists/*

# Install uv (fast Python package manager)
RUN pip install --no-cache-dir uv

# Set working directory
WORKDIR /app

# Copy project
COPY . .

# Install Python + frontend dependencies in one layer
RUN cd /app && uv sync --frozen --no-dev \
    && cd /app/agent-ui && npm ci && npm run build

# Expose backend + frontend ports
EXPOSE 8000 5173

# ---------- Default Command ----------
CMD sh -c "cd /app && uv run uvicorn agent_backend.main:app --host 0.0.0.0 --port 8000 & \
           cd /app/agent-ui && npm run preview -- --host 0.0.0.0 --port 5173"





# docker build -t ec2-agent-app .
# docker run --name ec2-agent-container --rm --network host --env-file .env ec2-agent-app

