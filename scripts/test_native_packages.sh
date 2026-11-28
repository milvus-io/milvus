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

# Run with root privileges on an x86_64 glibc host with gcc and Debian/RPM tools.
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d)
trap 'rm -rf "$test_root"' EXIT
fixtures="$test_root/fixtures"
mkdir -p "$fixtures/lib" "$fixtures/bin"

# Model Milvus -> libssl -> libcrypto, including a SONAME symlink and a stale
# build RUNPATH. An unrelated program can have the same OpenSSL dependencies.
printf '%s\n' 'int fixture_crypto(void) { return 42; }' |
    gcc -x c - -shared -fPIC -Wl,-soname,libcrypto.so.3 -o "$fixtures/lib/libcrypto.so.3.0.0"
ln -s libcrypto.so.3.0.0 "$fixtures/lib/libcrypto.so.3"
printf '%s\n' 'int fixture_crypto(void); int fixture_ssl(void) { return fixture_crypto(); }' |
    gcc -x c - -shared -fPIC -Wl,-soname,libssl.so.3 -Wl,-rpath,/build/milvus/lib \
        -o "$fixtures/lib/libssl.so.3" -L"$fixtures/lib" -l:libcrypto.so.3
cat > "$fixtures/main.c" <<'C'
#include <string.h>
int fixture_ssl(void);
int main(int argc, char **argv) {
    if (fixture_ssl() != 42)
        return 1;
    if (argc == 2 && strcmp(argv[1], "arg with spaces") == 0)
        return 0;
    return !(argc == 3 && strcmp(argv[1], "run") == 0 && strcmp(argv[2], "standalone") == 0);
}
C
gcc "$fixtures/main.c" -o "$fixtures/bin/milvus" -L"$fixtures/lib" \
    -Wl,-rpath-link,"$fixtures/lib" -Wl,-rpath,/build/milvus/lib -l:libssl.so.3
cp /bin/true "$fixtures/bin/etcd"
cp /bin/true "$fixtures/bin/minio"

build_deb() (
    mkdir -p "$test_root/deb"
    cd "$test_root/deb"
    cp -r "$repo_root/build/deb/"{debian,scripts} .
    cp -r "$repo_root/configs" .
    mkdir milvus-bin
    cp "$fixtures/bin/milvus" milvus-bin/
    cp -a "$fixtures/lib" milvus-lib
    touch README.Debian README.source configs/embedEtcd.yaml
    cat > debian/changelog <<'CHANGELOG'
milvus (2.0.2-1) unstable; urgency=medium

  * Build native package test fixture.

 -- Milvus Test <test@example.invalid>  Thu, 01 Jan 2026 00:00:00 +0000
CHANGELOG
    fakeroot debian/rules binary
)

build_rpm() (
    local rpm_top="$test_root/rpm"
    mkdir -p "$rpm_top/BUILD"
    cd "$rpm_top/BUILD"
    cp -r "$fixtures/bin" "$fixtures/lib" "$repo_root/configs" "$repo_root/build/rpm/services" .
    # Supply the other bundled names expected by the spec.
    local pattern
    while read -r pattern; do
        touch "lib/${pattern%\*}"
    done < <(sed -n 's/^    \(lib.*\.so\*\).*$/\1/p' "$repo_root/build/rpm/milvus.spec" |
        sed '/^libcrypto\.so\*$/d; /^libssl\.so\*$/d')
    QA_RPATHS=0x0022 rpmbuild -bb --define "_topdir $rpm_top" \
        --define 'version 2.0.2' --define 'release 1' --define 'debug_package %{nil}' \
        --build-in-place "$repo_root/build/rpm/milvus.spec"
)

check_linker_cache() {
    local label=$1
    local root=$2
    mkdir -p "$root/etc"
    printf '%s\n' 'include /etc/ld.so.conf.d/*.conf' > "$root/etc/ld.so.conf"
    # Let glibc read the package's actual configuration inside its isolated root.
    ldconfig -i -X -r "$root"
    local entries
    entries=$(ldconfig -p -C "$root/etc/ld.so.cache")
    if [[ "$entries" == *'/milvus/'* ]]; then
        printf '%s: bundled OpenSSL entered the system linker cache\n' "$label" >&2
        printf '%s\n' "$entries" | awk '/\/milvus\//'
        return 1
    fi
    printf '%s: bundled OpenSSL stayed out of the system linker cache\n' "$label"
}

echo 'test_native_packages'
build_deb
build_rpm
deb_package="$test_root/milvus_2.0.2-1_amd64.deb"
rpm_package="$test_root/rpm/RPMS/x86_64/milvus-2.0.2-1.x86_64.rpm"
deb_root="$test_root/deb-root"
rpm_root="$test_root/rpm-root"
mkdir "$deb_root" "$rpm_root"
dpkg-deb -x "$deb_package" "$deb_root"
rpm2cpio "$rpm_package" | (cd "$rpm_root" && cpio -id --quiet)

# Model the usr-merged alias used by the RPM linker configuration.
mkdir -p "$rpm_root/usr"
ln -s ../lib64 "$rpm_root/usr/lib64"

failed=0
check_linker_cache DEB "$deb_root" || failed=1
check_linker_cache RPM "$rpm_root" || failed=1
(( failed == 0 ))

check_launcher() {
    local label=$1
    local root=$2
    local unit=$3
    local -a service_command
    mkdir -p "$root/bin"
    cp -L /bin/sh "$root/bin/sh"
    local library
    while read -r library; do
        cp --parents -L "$library" "$root"
    done < <(ldd /bin/sh | awk '{for (i=1; i<=NF; i++) if ($i ~ /^\//) print $i}')
    env -u LD_LIBRARY_PATH chroot "$root" /usr/bin/milvus 'arg with spaces'
    read -r -a service_command < <(sed -n 's/^ExecStart=//p' "$unit")
    env -u LD_LIBRARY_PATH chroot "$root" "${service_command[@]}"
    printf '%s: CLI and service command loaded bundled OpenSSL\n' "$label"
}

check_launcher DEB "$deb_root" "$deb_root/lib/systemd/system/milvus.service"
check_launcher RPM "$rpm_root" "$rpm_root/etc/systemd/system/milvus.service"
echo 'test_native_packages passed'
