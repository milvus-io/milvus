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

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT

# Exercise the actual installer with tiny fixtures instead of compiling Milvus.
repo_root="$test_root/source"
fake_bin="$test_root/bin"
runtime_dir="$test_root/runtime"
mkdir -p "$repo_root/bin" "$repo_root/internal/core/output/lib/ossl-modules" \
    "$repo_root/internal/core/output/lib64" "$repo_root/cmake_build/runtime/pyudf/wheels" \
    "$fake_bin" "$runtime_dir"
printf 'milvus fixture\n' > "$repo_root/bin/milvus"
printf 'wheel fixture\n' > \
    "$repo_root/cmake_build/runtime/pyudf/wheels/milvus_pyudf_runtime-test.whl"
printf '#!/usr/bin/env bash\nexit 0\n' > "$fake_bin/ldd"
chmod +x "$fake_bin/ldd"

create_library() {
    local directory=$1
    local name=$2
    local soname=$3
    local version=$4
    printf 'library fixture: %s\n' "$name" > "$directory/$version"
    ln -s "$version" "$directory/$soname"
    ln -s "$soname" "$directory/$name"
}

create_library "$repo_root/internal/core/output/lib" libdemo.so libdemo.so.1 libdemo.so.1.2
create_library "$repo_root/internal/core/output/lib64" libother.so libother.so.2 libother.so.2.3
create_library "$repo_root/internal/core/output/lib" libdemo.dylib libdemo.1.dylib libdemo.1.2.dylib
printf 'module fixture\n' > "$repo_root/internal/core/output/lib/ossl-modules/fips.so"

run_install() {
    (
        cd "$repo_root"
        GOPATH="$test_root/gopath" LIBRARY_PATH="$runtime_dir" PATH="$fake_bin:$PATH" \
            bash "$script_dir/install_milvus.sh"
    )
}

assert_library() {
    local name=$1
    local soname=$2
    local version=$3
    test -L "$runtime_dir/$name"
    test -L "$runtime_dir/$soname"
    test "$(readlink "$runtime_dir/$name")" = "$soname"
    test "$(readlink "$runtime_dir/$soname")" = "$version"
    test -f "$runtime_dir/$version"
    test ! -L "$runtime_dir/$version"
    cmp "$runtime_dir/$name" "$runtime_dir/$version"
}

assert_install() {
    assert_library libdemo.so libdemo.so.1 libdemo.so.1.2
    assert_library libother.so libother.so.2 libother.so.2.3
    assert_library libdemo.dylib libdemo.1.dylib libdemo.1.2.dylib
    cmp "$repo_root/bin/milvus" "$test_root/gopath/bin/milvus"
    cmp "$repo_root/internal/core/output/lib/ossl-modules/fips.so" \
        "$runtime_dir/ossl-modules/fips.so"
    cmp "$repo_root/cmake_build/runtime/pyudf/wheels/milvus_pyudf_runtime-test.whl" \
        "$runtime_dir/pyudf/milvus_pyudf_runtime-test.whl"
}

run_install
assert_install

# Reinstall over aliases materialized by older versions of the installer.
for alias in libdemo.so libdemo.so.1 libother.so libother.so.2 libdemo.dylib libdemo.1.dylib; do
    rm "$runtime_dir/$alias"
    printf 'old materialized alias\n' > "$runtime_dir/$alias"
done
run_install
assert_install

# Optional lib64 and macOS artifacts need not exist in a Linux build.
mv "$repo_root" "$test_root/source with spaces"
repo_root="$test_root/source with spaces"
rm -rf "$repo_root/internal/core/output/lib64"
rm "$repo_root/internal/core/output/lib/"*.dylib
run_install
assert_library libdemo.so libdemo.so.1 libdemo.so.1.2

echo "Runtime library symlink installation tests passed."
