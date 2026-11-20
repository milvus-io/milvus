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

gcc_version=15.3.0
gcc_sha512=0de9e296153b52c021b1c7e63c9c62151d7a0ac03f23ce6e9f772c1b0eb783f6acdd81cc4567bfe4128a6f64968c2cfc8eff40b36229cba7425349f7d637c654
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT
cd "$build_dir"

# Build on each target distribution to retain its glibc compatibility.
wget -q "https://gcc.gnu.org/pub/gcc/releases/gcc-${gcc_version}/gcc-${gcc_version}.tar.xz"
echo "$gcc_sha512  gcc-${gcc_version}.tar.xz" | sha512sum -c -
tar -xf "gcc-${gcc_version}.tar.xz"
cd "gcc-${gcc_version}"
sed -i 's|http://gcc.gnu.org/|https://gcc.gnu.org/|' contrib/download_prerequisites
./contrib/download_prerequisites
mkdir "$build_dir/build"
cd "$build_dir/build"
../gcc-${gcc_version}/configure --prefix=/usr/local --libdir=/usr/local/lib64 \
    --enable-languages=c,c++,fortran --disable-multilib \
    --disable-bootstrap --enable-checking=release
make -j16
make -j16 install

ln -sf gcc /usr/local/bin/cc
ln -sf g++ /usr/local/bin/c++
# Load GCC runtimes before distro multiarch paths, including aarch64-linux-gnu.
printf '/usr/local/lib64\n/usr/local/lib\n' > /etc/ld.so.conf.d/00-gcc-15.conf
ldconfig
hash -r
for compiler in gcc g++ gfortran cc c++; do
    test "$("$compiler" -dumpfullversion)" = "$gcc_version"
done
gcov --version | head -n 1 | grep -F "$gcc_version"

# Exercise the installed C++ headers and runtime without LD_LIBRARY_PATH.
printf '#include <iostream>\nint main() { std::cout << __VERSION__; }\n' | \
    g++ -std=c++20 -x c++ - -o "$build_dir/check-gcc"
test "$("$build_dir/check-gcc")" = "$gcc_version"
