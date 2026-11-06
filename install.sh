#!/usr/bin/env bash
set -euo pipefail
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log(){ echo -e "${GREEN}[+]${NC} $*"; }
warn(){ echo -e "${YELLOW}[!]${NC} $*"; }
err(){ echo -e "${RED}[x]${NC} $*" >&2; }

# 1) إيجاد Python
PYTHON_BIN=""
for cmd in python3.12 python3.11 python3.10 python3 python; do
    command -v "$cmd" >/dev/null 2>&1 && { PYTHON_BIN="$cmd"; break; }
done
[ -z "$PYTHON_BIN" ] && { err "Python غير مثبت. ثبّت Python 3.10+"; exit 1; }

PY_VER=$("$PYTHON_BIN" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=${PY_VER%%.*}
PY_MINOR=${PY_VER##*.}
log "Python $PY_VER ($PYTHON_BIN)"

if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 10 ]; }; then
    err "Python 3.10+ مطلوب. الموجود: $PY_VER"
    exit 1
fi

# 2) إنشاء virtualenv
if [ ! -d ".venv" ]; then
    log "إنشاء virtualenv..."
    "$PYTHON_BIN" -m venv .venv
else
    warn "virtualenv موجود — إعادة استخدامه"
fi

source .venv/bin/activate
log "Python: $(which python)"

# 3) ترقية pip
log "ترقية pip..."
python -m pip install --upgrade pip setuptools wheel 2>&1 | tail -1

# 4) تثبيت المكتبات
[ ! -f "requirements.txt" ] && { err "requirements.txt غير موجود"; exit 1; }
log "تثبيت المكتبات..."
pip install -r requirements.txt

# 5) تحقق
log "التحقق من المكتبات..."
python - <<'PY'
import sys
try:
    import fastapi, uvicorn, httpx
    print(f"  fastapi : {fastapi.__version__}")
    print(f"  uvicorn : {uvicorn.__version__}")
    print(f"  httpx   : {httpx.__version__}")
    from httpx import HTTPTransport  # يتحقق من h2
    print("  http2   : OK")
    print("✅ جميع المكتبات جاهزة")
    sys.exit(0)
except Exception as e:
    print(f"❌ فشل: {e}", file=sys.stderr)
    sys.exit(1)
PY

echo
log "التثبيت اكتمل بنجاح"
echo
echo "  للتشغيل:"
echo "    source .venv/bin/activate"
echo "    python main-yo-api.py"
echo
