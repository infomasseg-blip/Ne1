#!/usr/bin/env bash
set -euo pipefail
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log(){ echo -e "${GREEN}[+]${NC} $*"; }
warn(){ echo -e "${YELLOW}[!]${NC} $*"; }
err(){ echo -e "${RED}[x]${NC} $*" >&2; }

PYTHON_BIN=""
for cmd in python3.12 python3.11 python3.10 python3 python; do
    command -v "$cmd" >/dev/null 2>&1 && { PYTHON_BIN="$cmd"; break; }
done
[ -z "$PYTHON_BIN" ] && { err "Python غير مثبت. ثبّت Python 3.10+"; exit 1; }

PY_VER=$("$PYTHON_BIN" -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
log "Python $PY_VER"

[ ! -d ".venv" ] && "$PYTHON_BIN" -m venv .venv || warn "virtualenv موجود"
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel 2>&1 | tail -1
pip install -r requirements.txt

log "التحقق..."
python -c "import fastapi, uvicorn, httpx; from httpx import HTTPTransport; print('OK')"

echo
log "التثبيت اكتمل"
echo "  شغّل: source .venv/bin/activate && python main-yo-api.py"
