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
cat > "$fake_bin/ldd" <<'EOF'
#!/usr/bin/env bash
cat "$LDD_FIXTURE"
EOF
chmod +x "$fake_bin/ldd"
ldd_fixture="$test_root/ldd.txt"
compiler_lib_dir="$test_root/compiler-libs"
mkdir -p "$compiler_lib_dir"
for library in libstdc++.so.6 libgcc_s.so.1 libgomp.so.1 libgfortran.so.5 libquadmath.so.0 libasan.so.8 libtsan.so.2 libatomic.so.1 libc.so.6; do
    printf 'compiler runtime fixture: %s\n' "$library" > "$compiler_lib_dir/$library"
    printf '%s => %s/%s (0x1234)\n' "$library" "$compiler_lib_dir" "$library" >> "$ldd_fixture"
done

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
        GOPATH="$test_root/gopath" LIBRARY_PATH="$runtime_dir" PATH="$fake_bin:$PATH" LDD_FIXTURE="$ldd_fixture" \
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
    for library in libstdc++.so.6 libgcc_s.so.1 libgomp.so.1 libgfortran.so.5 libquadmath.so.0 libasan.so.8 libtsan.so.2 libatomic.so.1; do
        cmp "$compiler_lib_dir/$library" "$runtime_dir/$library"
    done
    test ! -e "$runtime_dir/libc.so.6"
    test ! -e "$runtime_dir/libasan.so.6"
}

printf 'stale GCC 11 ASan runtime\n' > "$runtime_dir/libasan.so.6"
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

# Reinstallation may resolve the already packaged runtime through the loader.
cp "$ldd_fixture" "$ldd_fixture.saved"
sed "s|$compiler_lib_dir/libasan.so.8|$runtime_dir/libasan.so.8|" \
    "$ldd_fixture.saved" > "$ldd_fixture"
run_install
cmp "$compiler_lib_dir/libasan.so.8" "$runtime_dir/libasan.so.8"

# Switching back to a non-ASan build must not retain an obsolete preload.
sed '/libasan/d' "$ldd_fixture.saved" > "$ldd_fixture"
run_install
test ! -e "$runtime_dir/libasan.so.8"

echo "Runtime library symlink installation tests passed."
