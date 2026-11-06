#!/usr/bin/env bash

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

# Builds the CMVP-validated OpenSSL FIPS provider for MILVUS_FIPS_STRICT=ON.
#
# Conan's OpenSSL produces a fips.so of the same (non-validated) version as the
# library. OpenSSL allows other releases to load a validated provider but not
# to build their own, so this replaces that fips.so with one built per section
# 11.1 of the 3.1.2 Security Policy (CMVP certificate #4985):
#   ./Configure enable-fips && make && openssl fipsinstall -pedantic
# -pedantic is what sets the options the policy requires for the Approved mode;
# it needs the matching 3.1.2 tools, so the module config is generated here.
#
# Usage: build_fips_provider.sh <repo root>

set -euo pipefail

OPENSSL_VERSION="3.1.2"
OPENSSL_SHA256="a0ce69b8b97ea6a35b96875235aa453b966ba3cba8af2de23657d8b6767d6539"
OPENSSL_URL="https://github.com/openssl/openssl/releases/download/openssl-${OPENSSL_VERSION}/openssl-${OPENSSL_VERSION}.tar.gz"

REPO_ROOT="$(cd "${1:?usage: build_fips_provider.sh <repo root>}" && pwd)"
WORK_DIR="${REPO_ROOT}/cmake_build/fips_provider"
SRC_DIR="${WORK_DIR}/openssl-${OPENSSL_VERSION}"
TARBALL="${WORK_DIR}/openssl-${OPENSSL_VERSION}.tar.gz"
MODULE_DST_DIR="${REPO_ROOT}/internal/core/output/lib/ossl-modules"
CONFIG_DST="${REPO_ROOT}/configs/ssl/fipsmodule-strict.cnf"

if [[ "$(uname)" != "Linux" ]]; then
    echo "ERROR: MILVUS_FIPS_STRICT=ON is only supported on Linux" >&2
    exit 1
fi

# The module must be built as the policy describes; none of the build
# environment's flags or OpenSSL configuration may leak into it.
unset CFLAGS CXXFLAGS CPPFLAGS LDFLAGS OPENSSL_CONF OPENSSL_MODULES

mkdir -p "${WORK_DIR}"
if [[ ! -f "${TARBALL}" ]]; then
    curl -fsSL --retry 3 -o "${TARBALL}.tmp" "${OPENSSL_URL}"
    mv "${TARBALL}.tmp" "${TARBALL}"
fi
if ! echo "${OPENSSL_SHA256}  ${TARBALL}" | sha256sum -c --status -; then
    echo "ERROR: checksum mismatch for ${TARBALL}" >&2
    exit 1
fi

rm -rf "${SRC_DIR}"
tar -xzf "${TARBALL}" -C "${WORK_DIR}"
cd "${SRC_DIR}"
./Configure enable-fips
make -j "${jobs:-$(nproc)}"

export LD_LIBRARY_PATH="${SRC_DIR}"
version="$(./apps/openssl version -v)"
if [[ "${version}" != "OpenSSL ${OPENSSL_VERSION} "* ]]; then
    echo "ERROR: unexpected OpenSSL version: ${version}" >&2
    exit 1
fi

./apps/openssl fipsinstall -pedantic -module providers/fips.so -provider_name fips -out fipsmodule.cnf

# Fail the build rather than ship a provider outside the Approved mode.
for setting in "security-checks = 1" "conditional-errors = 1" "drbg-no-trunc-md = 1" "tls1-prf-ems-check = 1"; do
    if ! grep -qF "${setting}" fipsmodule.cnf; then
        echo "ERROR: ${setting} missing from generated fipsmodule.cnf" >&2
        exit 1
    fi
done

mkdir -p "${MODULE_DST_DIR}"
cp -f providers/fips.so "${MODULE_DST_DIR}/fips.so"
cp -f fipsmodule.cnf "${CONFIG_DST}"
echo "Installed OpenSSL ${OPENSSL_VERSION} FIPS provider to ${MODULE_DST_DIR}/fips.so"
