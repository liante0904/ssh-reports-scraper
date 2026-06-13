#!/bin/bash
# Dockerfile 디렉토리 누락 방지 검증
# pre-push 훅에서 실행

DOCKERFILE="Dockerfile"
errors=0

# Dockerfile에 COPY 되어야 할 디렉토리 목록
REQUIRED_DIRS=("run" "models" "utils" "modules" "scrapers" "scripts" "sql" "tests" "tools" "enricher")

echo "=== Dockerfile COPY 검증 ==="
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        if grep -q "COPY.*${dir}/" "$DOCKERFILE" 2>/dev/null; then
            echo "  ✅ $dir/"
        else
            echo "  ❌ $dir/ 존재하지만 Dockerfile에 COPY 누락!"
            errors=$((errors + 1))
        fi
    fi
done

# *.py 파일 COPY 확인
if ! grep -q 'COPY.*\*\.py' "$DOCKERFILE" 2>/dev/null; then
    echo "  ❌ 루트 *.py 파일 COPY 누락!"
    errors=$((errors + 1))
else
    echo "  ✅ *.py"
fi

if [ $errors -gt 0 ]; then
    echo ""
    echo "❌ $errors 개 누락 발견. Dockerfile 수정 후 다시 push하세요."
    exit 1
fi
echo "✅ 모든 디렉토리 COPY 완료"
