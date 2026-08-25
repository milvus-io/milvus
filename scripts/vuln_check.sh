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

# Report the same dependency CVEs the image scanner reports, but at commit time
# instead of weeks later on a released image.
#
# Two checks, because a Milvus image has two independent sources of Go CVEs:
#
#   1. the Go toolchain      - most of the stdlib. The version linked into the
#                              image comes from the Go installed in the builder
#                              image, NOT from the `go` directive, and
#                              osv-scanner reading go.mod does not see either.
#                              So it is checked explicitly, against every place
#                              a Go version is pinned.
#   2. the module graph      - osv-scanner over every shipped Go module.
#
# Neither builds the tree, so this runs without the cgo core libraries. That
# also means no reachability analysis: a finding says "this version is
# affected", not "this is exploitable from Milvus". Do that triage by hand
# (`go mod why -m <module>`) when deciding urgency.
#
# Accepted findings live in osv-scanner.toml, each with a reason and a review
# date. Nothing is suppressed silently.
#
# Usage:
#   scripts/vuln_check.sh          gate: Go toolchain + Go modules
#   scripts/vuln_check.sh --all    the above, plus a report-only pass over the
#                                  Rust and Python dependencies. Those are not
#                                  gated yet - they carry a backlog that has to
#                                  be worked down before it can block a merge.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_PATH="${ROOT_DIR}/bin"
OSV_SCANNER_VERSION="${OSV_SCANNER_VERSION:-v2.5.1}"
OSV_API="${OSV_API:-https://api.osv.dev/v1/query}"
# The modules whose findings land in a Milvus image: the server, pkg, and the
# go_client test image.
#
# client/ is deliberately not gated, and this is not a temporary omission.
# Its pins never reach an image - the server build resolves versions from the
# root module, which replaces client/v3 with ./client. Its own backlog cannot
# be cleared while the SDK supports Go 1.24: every fixed version of x/net,
# grpc, x/text and x/sys declares `go 1.25.0`, and the highest releases that
# still declare go1.24 carry every one of the advisories. Revisit when the
# SDK's minimum Go moves; see #53001.
GO_MODULES=("." "pkg" "tests/go_client")
# Everything that pins a Go version. A bump that misses one of these leaves the
# old stdlib in some image.
GO_PIN_FILES=(
    'build/docker/builder/*/Dockerfile'
    'build/docker/meta-migration/builder/Dockerfile'
    'tests/go_client/Dockerfile'
    'build/rpm/setup-env.sh'
)

SCAN_ALL=false
[[ "${1:-}" == "--all" ]] && SCAN_ALL=true

mkdir -p "${INSTALL_PATH}"
export PATH="${INSTALL_PATH}:${PATH}"
cd "${ROOT_DIR}"

toolchain_rc=0

# --- 1. Go toolchain -------------------------------------------------------
#
# Collect every pinned Go version - the `go` directive of each module and the
# version each builder installs - and ask OSV about each distinct one. Checking
# all of them rather than just go.mod is deliberate: the go directive is only a
# floor, and it is the builder's Go that ends up in the binary.

echo "=== Go toolchain ==="
declare -A go_versions=()
for module in "${GO_MODULES[@]}"; do
    version=$(awk '/^go /{print $2; exit}' "${module}/go.mod")
    go_versions["${version}"]+=" ${module}/go.mod"
done
while read -r file; do
    [[ -n "${file}" ]] || continue
    # Two spellings in the tree: a go1.26.7.linux-*.tar.gz download, and a
    # `FROM golang:1.26.7-*` base image.
    version=$(grep -oE '(go1|golang:)[0-9.]*[0-9]' "${file}" | head -1 | sed -E 's/^(go|golang:)//' || true)
    [[ -n "${version}" ]] && go_versions["${version}"]+=" ${file}"
done < <(git ls-files "${GO_PIN_FILES[@]}")

for version in "${!go_versions[@]}"; do
    result=$(curl -sS --max-time 60 "${OSV_API}" \
        -d "{\"package\":{\"name\":\"stdlib\",\"ecosystem\":\"Go\"},\"version\":\"${version}\"}")
    ids=$(printf '%s' "${result}" | python3 -c \
        'import json,sys; print(" ".join(v["id"] for v in json.load(sys.stdin).get("vulns",[])))')
    if [[ -n "${ids}" ]]; then
        echo "  go${version}: AFFECTED - ${ids}"
        echo "    pinned in:${go_versions[${version}]}"
        toolchain_rc=1
    else
        echo "  go${version}: clean (${go_versions[${version}]# })"
    fi
done

# A builder that pins a different Go than the module declares still ships that
# different stdlib, even when both happen to be clean today - and that drift is
# how an out-of-date builder survives a go.mod bump unnoticed.
want=$(awk '/^go /{print $2; exit}' go.mod)
for version in "${!go_versions[@]}"; do
    [[ "${version}" == "${want}" ]] && continue
    for pin in ${go_versions[${version}]}; do
        echo "  drift: ${pin} pins go${version}, go.mod declares go${want}"
        toolchain_rc=1
    done
done

# --- 2. Module graph -------------------------------------------------------

echo
echo "=== Go modules ==="
if ! command -v osv-scanner >/dev/null 2>&1 || \
   ! osv-scanner --version 2>&1 | grep -q "${OSV_SCANNER_VERSION#v}"; then
    echo "Installing osv-scanner ${OSV_SCANNER_VERSION} into ./bin/"
    GOBIN="${INSTALL_PATH}" go install \
        "github.com/google/osv-scanner/v2/cmd/osv-scanner@${OSV_SCANNER_VERSION}"
fi

lockfiles=()
for module in "${GO_MODULES[@]}"; do
    if [[ "${module}" == "." ]]; then
        lockfiles+=(--lockfile "go.mod")
    else
        lockfiles+=(--lockfile "${module}/go.mod")
    fi
done

set +e
# --no-call-analysis=go: the built-in call graph pass has to compile the tree,
# which needs the cgo core libraries. Keeping it off is what lets this run
# anywhere, at the cost of reachability - see the note at the top.
osv-scanner scan source \
    --config="${ROOT_DIR}/osv-scanner.toml" \
    --no-call-analysis=go \
    "${lockfiles[@]}"
modules_rc=$?
set -e

if [[ "${SCAN_ALL}" == "true" ]]; then
    echo
    echo "=== Rust and Python dependencies (report only, does not gate) ==="
    osv-scanner scan source \
        --config="${ROOT_DIR}/osv-scanner.toml" \
        --lockfile internal/core/thirdparty/tantivy/tantivy-binding/Cargo.lock \
        --lockfile tests/python_client/requirements.txt \
        --lockfile tests/restful_client_v2/requirements.txt \
        --no-resolve || true
fi

echo
# osv-scanner: 0 = clean, 1 = vulnerabilities found. Anything else is the
# scanner itself failing (no network, bad config); do not let that pass as
# "clean".
if [[ ${modules_rc} -gt 1 ]]; then
    echo "osv-scanner failed to run (exit ${modules_rc})."
    exit "${modules_rc}"
fi
if [[ ${toolchain_rc} -eq 0 && ${modules_rc} -eq 0 ]]; then
    echo "No unaccepted vulnerabilities."
    exit 0
fi
if [[ ${toolchain_rc} -ne 0 ]]; then
    echo "Vulnerable Go toolchain. Bump every pin listed above together - the"
    echo "builder Dockerfiles decide what the image actually ships, the go"
    echo "directives only decide what the scanners see."
fi
if [[ ${modules_rc} -ne 0 ]]; then
    echo "Vulnerable modules. Either bump the dependency, or - if upstream has"
    echo "no fixed release - add an entry to osv-scanner.toml with a reason and"
    echo "a review date."
fi
exit 1
