#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
GO_CLIENT_DIR="$(cd -- "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd)"
REQUIREMENTS="${GO_CLIENT_DIR}/requirements.txt"
PYTHON_BIN="${PYTHON_BIN:-python3}"

ensure_python() {
    if command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
        return
    fi
    if command -v apt-get >/dev/null 2>&1 && [ "$(id -u)" = "0" ]; then
        apt-get update
        apt-get install -y --no-install-recommends python3 python3-pip
        return
    fi
    echo "${PYTHON_BIN} is not available and cannot be installed automatically" >&2
    exit 1
}

ensure_pip() {
    if "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
        return
    fi
    if command -v apt-get >/dev/null 2>&1 && [ "$(id -u)" = "0" ]; then
        apt-get update
        apt-get install -y --no-install-recommends python3-pip
        return
    fi
    echo "pip is not available for ${PYTHON_BIN}" >&2
    exit 1
}

check_deps() {
    "${PYTHON_BIN}" - <<'PY'
import importlib
import sys

missing = []
for module in ("pyarrow", "pyiceberg", "vortex", "obstore", "substrait"):
    try:
        importlib.import_module(module)
    except Exception:
        missing.append(module)

try:
    lance = importlib.import_module("lance")
    if not hasattr(lance, "write_dataset"):
        missing.append("pylance (lance.write_dataset)")
except Exception:
    missing.append("pylance")

if missing:
    print("missing python packages: " + ", ".join(missing))
    sys.exit(1)
PY
}

install_deps() {
    # The unrelated PyPI package named "lance" shadows pylance's "lance"
    # module but does not provide write_dataset.
    "${PYTHON_BIN}" -m pip uninstall -y lance >/dev/null 2>&1 || true
    if ! "${PYTHON_BIN}" -m pip install \
        --no-cache-dir --break-system-packages -r "${REQUIREMENTS}"; then
        "${PYTHON_BIN}" -m pip install --no-cache-dir -r "${REQUIREMENTS}"
    fi
}

ensure_python
ensure_pip

if check_deps; then
    echo "external table python deps already installed"
    exit 0
fi

echo "installing external table python deps from ${REQUIREMENTS}"
install_deps
check_deps
