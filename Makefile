SECRETS := python3 $(HOME)/secrets/generate_env.py $(CURDIR)
COMPOSE := docker compose

.PHONY: up down build restart restart-scraper restart-alert logs logs-scraper logs-alert ps env env-scraper env-alert env-api test test-ci test-imports lint

## 전체 서비스 기동 (빌드 포함, 환경 변수 갱신)
up: env build
	$(COMPOSE) up -d

## 전체 서비스 중단
down:
	$(COMPOSE) down

## 이미지 빌드
build:
	$(COMPOSE) build

## 전체 재시작
restart: env build
	$(COMPOSE) restart

## 서비스별 시크릿 갱신 및 빌드 후 재시작
restart-scraper: env
	$(COMPOSE) up -d --build main-scraper

restart-alert: env
	$(COMPOSE) up -d --build report-keyword-alert

## 전체 로그 (follow)
logs:
	$(COMPOSE) logs -f

logs-scraper:
	$(COMPOSE) logs -f main-scraper

logs-alert:
	$(COMPOSE) logs -f report-keyword-alert

## 컨테이너 상태 확인
ps:
	$(COMPOSE) ps

## 테스트 실행 (표준 인터페이스)
test:
	uv run pytest tests/test_scrapers_health.py -v

## CI와 동일한 네트워크/운영 의존성 없는 전체 회귀 검증
test-ci:
	bash scripts/verify_ci.sh

## 네트워크 호출 없는 모듈 import/config 가드 테스트
test-imports:
	uv run pytest tests/test_config_manager.py tests/test_db_factory.py tests/test_scraper_imports.py -q

## 린트 체크 (표준 인터페이스)
lint:
	uv run ruff check .

## 환경 변수 생성 (현재 디렉토리 기준)
env:
	$(SECRETS)

## 서비스별 환경 변수 생성 (alias 호환 — 모두 $(CURDIR) 사용)
env-scraper: env

env-alert: env

env-api: env
