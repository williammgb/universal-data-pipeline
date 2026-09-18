FROM ghcr.io/astral-sh/uv:0.12.5 AS uv

# The dashboard is built here, so the machine running the build needs no Node of its own.
FROM node:24.18.0-trixie-slim AS dashboard
# The console check's browsers belong to the machine running the smoke, never to the image.
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json frontend/.npmrc ./
RUN npm ci
COPY frontend ./
RUN npm run build

FROM python:3.14.4-slim-trixie

COPY --from=uv /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN uv sync --frozen --no-dev

COPY --from=dashboard /frontend/dist ./frontend/dist

ENTRYPOINT ["udp"]
