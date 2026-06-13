#!/bin/bash
# GA standalone 문법 오류 방지 검증
echo "=== GA standalone 문법 검증 ==="
errors=0
for f in run/standalone/*.py; do
    [ "$(basename $f)" = "_TEMPLATE.py" ] && continue
    python3 -c "import py_compile; py_compile.compile('$f', doraise=True)" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "  ✅ $(basename $f)"
    else
        echo "  ❌ $(basename $f) 문법 오류!"
        errors=$((errors + 1))
    fi
done
[ $errors -gt 0 ] && echo "❌ $errors 개 오류" && exit 1
echo "✅ 모든 standalone 정상"
