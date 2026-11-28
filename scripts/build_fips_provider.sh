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

# Build the OpenSSL 3.1.2 provider using the procedure in section 11.1 of its
# Security Policy (CMVP #4985). The module and its matching configuration are
# staged together for the installer. Only the verified source archive is reused.
set -euo pipefail

if [[ $# -ne 1 || -z "$1" ]]; then
    echo "Usage: $0 <output directory>" >&2
    exit 2
fi
if [[ "$(uname -s)" != Linux ]]; then
    echo "The OpenSSL FIPS provider build requires Linux" >&2
    exit 1
fi

version=3.1.2
archive="openssl-${version}.tar.gz"
expected_sha256=a0ce69b8b97ea6a35b96875235aa453b966ba3cba8af2de23657d8b6767d6539
url="https://github.com/openssl/openssl/releases/download/openssl-${version}/${archive}"
build_jobs=${jobs:-$(nproc)}
if [[ ! "$build_jobs" =~ ^[1-9][0-9]*$ ]]; then
    echo "Expected a positive build job count, got '$build_jobs'" >&2
    exit 2
fi

mkdir -p "$1"
output_dir=$(cd "$1" && pwd)
cache_dir="$(dirname "$output_dir")/sources"
mkdir -p "$cache_dir"
work_dir=$(mktemp -d)
stage_dir=$(mktemp -d "$output_dir/.build.XXXXXXXX")
trap 'rm -rf -- "$work_dir" "$stage_dir"' EXIT

# Serialize builds targeting this directory; an interrupted publication leaves
# the config absent so it cannot describe a stale or partially copied module.
exec 9<"$output_dir"
flock 9

verify_archive() {
    local actual
    actual=$(sha256sum < "$1") && [[ "${actual%% *}" == "$expected_sha256" ]]
}

if [[ ! -f "$cache_dir/$archive" ]] || ! verify_archive "$cache_dir/$archive"; then
    echo "Downloading OpenSSL $version"
    curl -fsSL --retry 3 -o "$stage_dir/$archive" "$url"
    if ! verify_archive "$stage_dir/$archive"; then
        echo "OpenSSL $version archive checksum mismatch" >&2
        exit 1
    fi
    mv -f -- "$stage_dir/$archive" "$cache_dir/$archive"
fi

tar -xzf "$cache_dir/$archive" -C "$work_dir"
cd "$work_dir/openssl-$version"

# Milvus make variables (including command-line overrides carried by MAKEFLAGS),
# sanitizer flags and OpenSSL settings must not enter this independent build.
build_env=(env -i "PATH=$PATH" "HOME=$HOME" LC_ALL=C)
"${build_env[@]}" ./Configure enable-fips
"${build_env[@]}" make -j "$build_jobs"
"${build_env[@]}" make install_fips "DESTDIR=$work_dir/install"

openssl=("${build_env[@]}" "LD_LIBRARY_PATH=$PWD" OPENSSL_CONF=/dev/null ./apps/openssl)
reported_version=$("${openssl[@]}" version -v)
if [[ "$reported_version" != "OpenSSL $version "*"(Library: OpenSSL $version "*")" ]]; then
    echo "Unexpected OpenSSL version: $reported_version" >&2
    exit 1
fi

module=$(find "$work_dir/install" -type f -name fips.so)
install -m 0755 "$module" "$stage_dir/fips.so"
"${openssl[@]}" fipsinstall -pedantic -module "$stage_dir/fips.so" \
    -provider_name fips -out "$stage_dir/fipsmodule-validated.cnf"
for setting in security-checks conditional-errors drbg-no-trunc-md tls1-prf-ems-check; do
    if ! grep -Fxq "$setting = 1" "$stage_dir/fipsmodule-validated.cnf"; then
        echo "Missing required OpenSSL FIPS setting: $setting = 1" >&2
        exit 1
    fi
done
"${openssl[@]}" fipsinstall -pedantic -verify -module "$stage_dir/fips.so" \
    -provider_name fips -in "$stage_dir/fipsmodule-validated.cnf"

rm -f -- "$output_dir/fipsmodule-validated.cnf"
mv -f -- "$stage_dir/fips.so" "$output_dir/fips.so"
mv -f -- "$stage_dir/fipsmodule-validated.cnf" "$output_dir/fipsmodule-validated.cnf"
echo "Staged OpenSSL $version FIPS provider and configuration in $output_dir"
