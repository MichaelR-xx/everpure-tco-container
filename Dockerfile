# Everpure Azure Managed Disk Visualization Tool — container image.
# Cross-platform: builds and runs the same on Linux, macOS (Docker Desktop) and
# Windows. Chromium is included so PDF export works out of the box.

FROM python:3.12-slim

# System deps: Chromium (headless PDF export) + fonts + CA certs (Azure retail
# pricing + S3 are HTTPS). tini for correct signal handling / zombie reaping.
RUN apt-get update && apt-get install -y --no-install-recommends \
        chromium \
        fonts-liberation \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python deps first (better layer caching).
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# App source.
COPY app.py .
COPY templates ./templates
COPY static ./static
COPY notes ./notes
COPY tools ./tools

# Seed the engine config files into the default local-storage config location so a
# fresh Local-Storage deployment works with no AWS account (mirrors README step 5).
RUN mkdir -p /data/EverpureTCO/TCO-GUI/_config \
    && cp notes/pscd_config.json /data/EverpureTCO/TCO-GUI/_config/pscd_config.json \
    && cp notes/ecan_config.json /data/EverpureTCO/TCO-GUI/_config/ecan_config.json

# Defaults: Local Storage on the /data volume, bound on all interfaces, Chromium
# for PDF. Override any of these at `docker run -e ...` time.
ENV EVERPURE_STORAGE=local \
    EVERPURE_LOCAL_ROOT=/data \
    HOST=0.0.0.0 \
    PORT=5000 \
    CHROMIUM_PATH=/usr/bin/chromium \
    AZURE_PRICE_WORKERS=8 \
    FLASK_SECRET_KEY=change-me-in-production

VOLUME ["/data"]
EXPOSE 5000

# Serve with waitress (production WSGI server), not Flask's dev server.
# `waitress-serve` is the console script installed by the waitress package.
ENTRYPOINT ["tini", "--"]
CMD ["waitress-serve", "--host=0.0.0.0", "--port=5000", "--threads=8", "app:app"]
