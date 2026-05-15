#!/bin/bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

PYTHON_ENV_SOURCE="${BASH_SOURCE[0]}"
while [ -h "$PYTHON_ENV_SOURCE" ]; do
  PYTHON_ENV_DIR="$(cd -P "$(dirname "$PYTHON_ENV_SOURCE")" && pwd)"
  PYTHON_ENV_SOURCE="$(readlink "$PYTHON_ENV_SOURCE")"
  [[ $PYTHON_ENV_SOURCE != /* ]] && PYTHON_ENV_SOURCE="$PYTHON_ENV_DIR/$PYTHON_ENV_SOURCE"
done
PYTHON_ENV_ROOT="$(cd -P "$(dirname "$PYTHON_ENV_SOURCE")/../.." && pwd)"
ROOT="${ROOT:-$PYTHON_ENV_ROOT}"

function ensure_uv_available() {
  if command -v uv >/dev/null 2>&1; then
    return
  fi

  local bootstrap_python="${PYTEST_UV_BOOTSTRAP_PYTHON:-}"
  if [[ -z "${bootstrap_python}" ]]; then
    if command -v python3 >/dev/null 2>&1; then
      bootstrap_python="$(command -v python3)"
    elif command -v python >/dev/null 2>&1; then
      bootstrap_python="$(command -v python)"
    fi
  fi

  if [[ -z "${bootstrap_python}" ]]; then
    echo "uv is required to create Python virtualenv, but no bootstrap Python was found."
    exit 1
  fi

  "${bootstrap_python}" -m pip install --user --no-cache-dir --force-reinstall uv \
    --timeout "${PYTEST_UV_INSTALL_TIMEOUT:-300}" \
    --retries "${PYTEST_UV_INSTALL_RETRIES:-6}"
  export PATH="${HOME}/.local/bin:${PATH}"

  if ! command -v uv >/dev/null 2>&1; then
    echo "uv installation succeeded but uv is not available in PATH."
    exit 1
  fi
}

function activate_pytest_python_env() {
  local python_version_file="${ROOT}/tests/.python-version"
  local python_version="${PYTEST_PYTHON_VERSION:-3.12}"
  if [[ -f "${python_version_file}" ]]; then
    python_version="${PYTEST_PYTHON_VERSION:-$(tr -d '[:space:]' < "${python_version_file}")}"
  fi

  local venv_path="${PYTEST_VENV_PATH:-${ROOT}/tests/.venv}"
  local python_bin="${PYTEST_PYTHON_BIN:-}"

  if [[ ! -x "${venv_path}/bin/python" ]] || ! "${venv_path}/bin/python" -c \
    "import sys; raise SystemExit(0 if sys.version_info[:2] == tuple(map(int, '${python_version}'.split('.')[:2])) else 1)"; then
    rm -rf "${venv_path}"
    if [[ -n "${python_bin}" ]]; then
      "${python_bin}" -m venv --system-site-packages "${venv_path}"
    elif command -v uv >/dev/null 2>&1; then
      uv venv --python "${python_version}" --system-site-packages "${venv_path}"
    elif command -v "python${python_version}" >/dev/null 2>&1; then
      "python${python_version}" -m venv --system-site-packages "${venv_path}"
    else
      ensure_uv_available
      uv venv --python "${python_version}" --system-site-packages "${venv_path}"
    fi
  fi

  # Shellcheck disable=SC1091
  source "${venv_path}/bin/activate"
  python - <<PY
import sys

expected = tuple(map(int, "${python_version}".split(".")[:2]))
actual = sys.version_info[:2]
if actual != expected:
    raise SystemExit(f"Expected Python ${python_version}, got {sys.version.split()[0]}")
PY
  python -V
}
