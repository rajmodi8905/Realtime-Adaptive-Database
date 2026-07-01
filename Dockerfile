# ==========================================
# Stage 1: Build the React Frontend
# ==========================================
FROM node:20-bullseye-slim AS frontend-builder
WORKDIR /workspace/dashboard/frontend

# Copy frontend source
COPY dashboard/frontend/package*.json ./
RUN npm ci

COPY dashboard/frontend/ ./
# Build the frontend (Vite config outputs to ../static)
RUN npm run build

# ==========================================
# Stage 2: Build the Python Backend
# ==========================================
FROM python:3.10-slim
WORKDIR /workspace

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Install k6 from official image
COPY --from=grafana/k6:0.53.0 /usr/bin/k6 /usr/local/bin/k6

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the backend codebase
COPY . .

# Copy built frontend assets from Stage 1 into dashboard/static
# Note: vite outDir is set to '../static', meaning from /workspace/dashboard/frontend it outputs to /workspace/dashboard/static
# We'll just copy it explicitly to be safe
COPY --from=frontend-builder /workspace/dashboard/static ./dashboard/static

# Expose the API server port
EXPOSE 8080

# Run the FastAPI server
CMD ["python", "-m", "dashboard.api_server"]
