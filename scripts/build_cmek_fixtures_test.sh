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

set -euo pipefail

test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT

repo_root="$test_root/repo"
caller_dir="$test_root/caller"
fake_bin="$test_root/bin"
mkdir -p "$repo_root/scripts" "$caller_dir" "$fake_bin"
cp "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/build_cmek_fixtures.sh" "$repo_root/scripts/"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'while (( $# > 0 )); do' \
    '    if [[ $1 == -o ]]; then' \
    '        printf x > "$2"' \
    '        exit 0' \
    '    fi' \
    '    shift' \
    'done' \
    'exit 1' > "$fake_bin/go"
chmod +x "$fake_bin/go"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'build_dir=' \
    'output_dir=' \
    'while (( $# > 0 )); do' \
    '    case $1 in' \
    '        -B)' \
    '            build_dir=$2' \
    '            shift 2' \
    '            ;;' \
    '        -DCMAKE_LIBRARY_OUTPUT_DIRECTORY=*)' \
    '            output_dir=${1#*=}' \
    '            shift' \
    '            ;;' \
    '        *)' \
    '            shift' \
    '            ;;' \
    '    esac' \
    'done' \
    'if [[ -n $build_dir ]]; then' \
    '    mkdir -p "$build_dir" "$output_dir"' \
    '    printf x > "$output_dir/libCipherPlugin.so"' \
    '    printf "#!/usr/bin/env bash\nexit 0\n" > "$build_dir/CipherPluginTest"' \
    '    chmod +x "$build_dir/CipherPluginTest"' \
    'fi' > "$fake_bin/cmake"
chmod +x "$fake_bin/cmake"

(
    cd "$caller_dir"
    GO="$fake_bin/go" PATH="$fake_bin:$PATH" bash "$repo_root/scripts/build_cmek_fixtures.sh" fixtures
)

for artifact in libGoCipherPlugin.so libGoCipherPluginRace.so libCipherPlugin.so; do
    test -f "$caller_dir/fixtures/$artifact"
done
test ! -e "$repo_root/fixtures"
