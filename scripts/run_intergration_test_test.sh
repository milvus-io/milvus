#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT

repo_root="$test_root/repo"
fake_bin="$test_root/bin"
run_log="$test_root/packages.log"
mkdir -p "$repo_root/scripts" "$repo_root/tests/integration" "$fake_bin"
cp "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/run_intergration_test.sh" "$repo_root/scripts/"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    "export MILVUS_WORK_DIR=\"$repo_root\"" \
    "export RPATH=\"$test_root/lib\"" > "$repo_root/scripts/setenv.sh"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'if [[ ${1:-} != list ]]; then' \
    '    exit 1' \
    'fi' \
    'pattern=${!#}' \
    'case $pattern in' \
    '    ./...)' \
    '        printf "%s\n" example.test/root example.test/cmek example.test/cmek/inspector' \
    '        ;;' \
    '    ./cmek)' \
    '        printf "%s\n" example.test/cmek' \
    '        ;;' \
    '    *)' \
    '        exit 1' \
    '        ;;' \
    'esac' > "$fake_bin/go"
chmod +x "$fake_bin/go"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'package=$1' \
    'printf "%s\n" "$package" >> "$RUN_LOG"' \
    'printf "mode: atomic\n%s/file.go:1.1,1.2 1 1\n" "$package" > profile.out' > "$fake_bin/test-command"
chmod +x "$fake_bin/test-command"

(
    cd "$repo_root"
    PATH="$fake_bin:$PATH" RUN_LOG="$run_log" \
        bash scripts/run_intergration_test.sh --exclude-package ./cmek "$fake_bin/test-command"
    PATH="$fake_bin:$PATH" RUN_LOG="$run_log" MILVUS_INTEGRATION_COVERAGE_APPEND=true \
        bash scripts/run_intergration_test.sh --package ./cmek "$fake_bin/test-command"
)

test "$(grep -c '^example.test/cmek$' "$run_log")" -eq 1
grep -qx 'example.test/root' "$run_log"
grep -qx 'example.test/cmek/inspector' "$run_log"

coverage_file="$repo_root/it_coverage.txt"
test "$(grep -c '^mode: atomic$' "$coverage_file")" -eq 1
grep -q '^example.test/cmek/file.go:' "$coverage_file"
grep -q '^example.test/root/file.go:' "$coverage_file"
grep -q '^example.test/cmek/inspector/file.go:' "$coverage_file"

(
    cd "$repo_root"
    PATH="$fake_bin:$PATH" RUN_LOG="$run_log" \
        bash scripts/run_intergration_test.sh --package ./cmek "$fake_bin/test-command"
)

test "$(grep -c '^mode: atomic$' "$coverage_file")" -eq 1
grep -q '^example.test/cmek/file.go:' "$coverage_file"
! grep -q '^example.test/root/file.go:' "$coverage_file"
! grep -q '^example.test/cmek/inspector/file.go:' "$coverage_file"
