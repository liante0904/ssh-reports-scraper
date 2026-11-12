# syntax=docker/dockerfile:1.7
# Shared base contains Python and the unprivileged runtime account.
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid 1001 --shell /bin/bash --create-home appuser

# Minimal musl-based runtime for PostgreSQL keyword queries and Telegram notifications.
FROM python:3.12-alpine AS keyword-alert
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Seoul
WORKDIR /app
RUN addgroup -g 1001 appgroup \
    && adduser -D -u 1001 -G appgroup -s /bin/sh appuser
RUN apk add --no-cache tzdata
COPY --chown=appuser:appgroup requirements-keyword-alert.txt ./
RUN pip install --no-cache-dir uv \
    && uv venv .venv \
    && uv pip install --python .venv/bin/python --no-cache -r requirements-keyword-alert.txt \
    && pip uninstall -y uv \
    && chown -R appuser:appgroup /app/.venv

COPY --chown=appuser:appgroup scheduler_keyword_alert.py ./
COPY --chown=appuser:appgroup run/keyword_alert.py ./run/keyword_alert.py
COPY --chown=appuser:appgroup models/KeywordAlertDB.py ./models/KeywordAlertDB.py
COPY --chown=appuser:appgroup models/ConfigManager.py ./models/ConfigManager.py
COPY --chown=appuser:appgroup utils/logger_util.py ./utils/logger_util.py
COPY --chown=appuser:appgroup utils/telegram_message_builder.py ./utils/telegram_message_builder.py
COPY --chown=appuser:appgroup utils/telegram_util.py ./utils/telegram_util.py
RUN mkdir -p /log && chown appuser:appgroup /log /app
USER appuser
CMD [".venv/bin/python", "scheduler_keyword_alert.py"]

# Build the main runtime virtualenv separately so uv and its installer layer
# do not ship in the production image.
FROM base AS main-deps
RUN pip install --no-cache-dir uv
COPY --chown=appuser:appgroup pyproject.toml uv.lock ./

# 6-1. 선택적 private ssh-library 설치
# GitHub Actions/Compose에서 ssh_library named context를 넘기고
# INSTALL_SSH_LIBRARY=1일 때만 현재 프로젝트 venv에 설치한다.
ARG INSTALL_SSH_LIBRARY=0
RUN --mount=from=ssh_library,target=/tmp/ssh-library \
    uv sync --frozen --no-cache --no-install-project --no-default-groups --group main-runtime && \
    if [ "$INSTALL_SSH_LIBRARY" = "1" ]; then \
      uv pip install --python .venv/bin/python /tmp/ssh-library; \
    else \
      echo "Skipping ssh-library install"; \
    fi && \
    chown -R appuser:appgroup /app/.venv

# Main scraper keeps PDF/OCR utilities but uses the shared browser service.
FROM base AS main-scraper
ENV TZ=Asia/Seoul
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    poppler-utils \
    tzdata \
    tesseract-ocr \
    tesseract-ocr-kor \
    && rm -rf /var/lib/apt/lists/*
COPY --from=main-deps --chown=appuser:appgroup /app/.venv ./.venv

# Playwright connects to PLAYWRIGHT_WS_ENDPOINT; no local browser is installed.
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Copy main scraper runtime sources.
COPY --chown=appuser:appgroup run/ ./run/
COPY --chown=appuser:appgroup models/ ./models/
COPY --chown=appuser:appgroup utils/ ./utils/
COPY --chown=appuser:appgroup modules/ ./modules/
COPY --chown=appuser:appgroup scrapers/ ./scrapers/
COPY --chown=appuser:appgroup config/ ./config/
COPY --chown=appuser:appgroup scripts/backfill_ls_pdf_quality.py ./scripts/backfill_ls_pdf_quality.py
# 최적화 실패로 인한 enrich 작업 일시 주석 처리
# COPY --chown=appuser:appgroup enricher/ ./enricher/
COPY --chown=appuser:appgroup *.py ./
# COPY .env ./ # .env는 docker-compose의 env_file을 통해 주입됩니다.

# Prepare runtime directories and permissions.
RUN mkdir -p /log && chown appuser:appgroup /log /app

USER appuser

CMD [".venv/bin/python", "scheduler.py"]
