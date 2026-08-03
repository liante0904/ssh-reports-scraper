# syntax=docker/dockerfile:1.7
# 1. Base Image
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1

# 2. 필수 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    poppler-utils \
    rclone \
    tesseract-ocr \
    tesseract-ocr-kor \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# 3. uv 설치
RUN pip install uv

# 4. Create a non-root user and group
RUN groupadd --gid 1001 appgroup && useradd --uid 1001 --gid 1001 --shell /bin/bash --create-home appuser

# 5. 작업 디렉토리 설정
WORKDIR /app

# 6. 의존성 설치 (캐시 활용)
# 소유권을 appuser로 지정하여 파일 복사
COPY --chown=appuser:appgroup pyproject.toml uv.lock ./ 
RUN uv sync --frozen --no-cache

# 6-1. 선택적 private ssh-library 설치
# GitHub Actions/Compose에서 ssh_library named context를 넘기고
# INSTALL_SSH_LIBRARY=1일 때만 현재 프로젝트 venv에 설치한다.
ARG INSTALL_SSH_LIBRARY=0
RUN --mount=from=ssh_library,target=/tmp/ssh-library \
    if [ "$INSTALL_SSH_LIBRARY" = "1" ]; then \
      uv pip install --python .venv/bin/python /tmp/ssh-library; \
    else \
      echo "Skipping ssh-library install"; \
    fi && \
    chown -R appuser:appgroup /app/.venv

# 7. 필요한 소스 코드 복사
COPY --chown=appuser:appgroup run/ ./run/
COPY --chown=appuser:appgroup models/ ./models/
COPY --chown=appuser:appgroup utils/ ./utils/
COPY --chown=appuser:appgroup modules/ ./modules/
COPY --chown=appuser:appgroup scrapers/ ./scrapers/
COPY --chown=appuser:appgroup config/ ./config/
# 최적화 실패로 인한 enrich 작업 일시 주석 처리
# COPY --chown=appuser:appgroup enricher/ ./enricher/
COPY --chown=appuser:appgroup *.py ./
# COPY .env ./ # .env는 docker-compose의 env_file을 통해 주입됩니다.

# 8. 실행 권한 부여 및 디렉토리 준비
RUN mkdir -p /log && chown -R appuser:appgroup /log /app

# 9. Switch to the non-root user
USER appuser

# 10. 기본 실행 명령 (스케줄러 실행)
# 런타임에서 uv run을 쓰면 appuser 권한으로 .venv 동기화를 시도할 수 있다.
CMD [".venv/bin/python", "scheduler.py"]
