#!/usr/bin/env bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export TZ=UTC
export SDKMAN_DIR=/root/.sdkman
export CARGO_HOME=/root/.cache/cargo
export RUSTUP_HOME=/root/.cache/rustup
export PATH="$CARGO_HOME/bin:$PATH"
export SBT_OPTS="-Xmx4g -Xms2g"
export CONAN_REVISIONS_ENABLED=1
RUST_TOOLCHAIN_VERSION=1.96.0

CONNECTOR_REPOSITORY="${CONNECTOR_REPOSITORY:-https://github.com/zilliztech/spark-milvus.git}"
CONNECTOR_COMMIT="${CONNECTOR_COMMIT:?CONNECTOR_COMMIT is required}"
SOURCE_DIR=/build/spark-milvus
ARTIFACT_DIR=/artifacts

# security.ubuntu.com can stall after this vcluster resumes from sleep, while
# archive.ubuntu.com serves the same jammy-security pocket reliably.
sed -i \
  's#http://security.ubuntu.com/ubuntu/#http://archive.ubuntu.com/ubuntu/#g' \
  /etc/apt/sources.list
APT_NETWORK_OPTIONS=(
  -o Acquire::http::Timeout=30
  -o Acquire::https::Timeout=30
  -o Acquire::Retries=5
)
apt-get "${APT_NETWORK_OPTIONS[@]}" update
apt-get "${APT_NETWORK_OPTIONS[@]}" install -y --no-install-recommends \
  automake \
  autoconf \
  ca-certificates \
  ccache \
  curl \
  g++ \
  gcc \
  git \
  jq \
  libtool \
  make \
  python3-pip \
  unzip \
  wget \
  zip
rm -rf /var/lib/apt/lists/*

ln -sf /usr/bin/aclocal-1.16 /usr/bin/aclocal-1.15
ln -sf /usr/bin/automake-1.16 /usr/bin/automake-1.15

curl -fsSL "https://cmake.org/files/v3.27/cmake-3.27.5-linux-$(uname -m).tar.gz" \
  | tar --strip-components=1 -xz -C /usr/local

python3 -m pip install --break-system-packages conan==1.64.1 \
  || python3 -m pip install conan==1.64.1
conan profile new default --detect || true
conan profile update settings.compiler.libcxx=libstdc++11 default
conan remote add default-conan-local \
  https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local \
  --insert || true

# The pinned milvus-storage revision builds its Rust DataFusion bridge as part
# of the JNI library. Rust 1.97 changes the layout of TryFromIntError and
# breaks the lockfile's ethnum 1.5.2, so use the last compatible stable release
# while keeping the upstream Cargo.lock unchanged. Keep the toolchain/registry
# under the existing restart-persistent root cache volume.
if [[ ! -x "$CARGO_HOME/bin/rustup" ]]; then
  curl -fsSL https://sh.rustup.rs -o /tmp/rustup-init.sh
  sh /tmp/rustup-init.sh \
    -y \
    --profile minimal \
    --default-toolchain "$RUST_TOOLCHAIN_VERSION" \
    --no-modify-path
  rm -f /tmp/rustup-init.sh
fi
rustup toolchain install "$RUST_TOOLCHAIN_VERSION" --profile minimal --no-self-update
rustup default "$RUST_TOOLCHAIN_VERSION"
rustc --version
cargo --version

# The public sourceware endpoint used by the upstream bzip2 Conan recipe
# returns HTTP 403 from this vcluster. Preload the recipe and point it at a
# byte-identical mirror before the full dependency installation begins.
conan download bzip2/1.0.8@ -r default-conan-local --recipe
sed -i \
  's#https://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz#https://fossies.org/linux/misc/bzip2-1.0.8.tar.gz#' \
  /root/.conan/data/bzip2/1.0.8/_/_/export/conandata.yml

# Boost's retired JFrog endpoint serves non-archive content and SourceForge is
# not reliable from this cluster. Use Boost's official archive host instead.
conan download boost/1.83.0@ -r default-conan-local --recipe
sed -i \
  -e 's#https://boostorg.jfrog.io/artifactory/main/release/1.83.0/source/boost_1_83_0.tar.bz2#https://archives.boost.io/release/1.83.0/source/boost_1_83_0.tar.bz2#' \
  -e 's#https://sourceforge.net/projects/boost/files/boost/1.83.0/boost_1_83_0.tar.bz2#https://archives.boost.io/release/1.83.0/source/boost_1_83_0.tar.bz2#' \
  /root/.conan/data/boost/1.83.0/_/_/export/conandata.yml

# Avro 1.12.1 has aged out of active Apache and Tencent mirrors. Keep the
# dependency's Conan reference, but build it from the compatible Avro 1.12.2
# source archive served by Tencent and pin that archive's checksum.
AVRO_RECIPE=/root/.conan/data/libavrocpp/1.12.1.1/milvus/dev/export/conandata.yml
conan download \
  'libavrocpp/1.12.1.1@milvus/dev' \
  -r default-conan-local \
  --recipe
sed -i \
  -e 's#https://dlcdn.apache.org/avro/avro-1.12.1/avro-src-1.12.1.tar.gz#https://mirrors.cloud.tencent.com/apache/avro/avro-1.12.2/avro-src-1.12.2.tar.gz#' \
  -e 's#268e47c0850df04f952ea6fdfc3b12a8d0042124354bff6c0239be0b70016d2e#449722c442ec9514d8e6933f9c7ccf2e0544cb75c951c25b804ea1d8e73d12bb#' \
  "$AVRO_RECIPE"

# archive.apache.org is not reachable from this vcluster. Prefer an exact
# Apache release tarball pre-seeded into the persistent Conan cache; otherwise
# use the source-equivalent GitHub tag archive with its own pinned checksum.
THRIFT_RECIPE=/root/.conan/data/thrift/0.17.0/_/_/export/conandata.yml
THRIFT_SOURCE_CACHE=/root/.conan/source-cache/thrift-0.17.0.tar.gz
conan download thrift/0.17.0@ -r default-conan-local --recipe
if [[ -f "$THRIFT_SOURCE_CACHE" ]]; then
  echo "b272c1788bb165d99521a2599b31b97fa69e5931d099015d91ae107a0b0cc58f  $THRIFT_SOURCE_CACHE" \
    | sha256sum -c -
  sed -i \
    "s#http://archive.apache.org/dist/thrift/0.17.0/thrift-0.17.0.tar.gz#file://$THRIFT_SOURCE_CACHE#" \
    "$THRIFT_RECIPE"
else
  sed -i \
    -e 's#http://archive.apache.org/dist/thrift/0.17.0/thrift-0.17.0.tar.gz#https://github.com/apache/thrift/archive/refs/tags/v0.17.0.tar.gz#' \
    -e 's#b272c1788bb165d99521a2599b31b97fa69e5931d099015d91ae107a0b0cc58f#f5888bcd3b8de40c2c2ab86896867ad9b18510deb412cba3e5da76fb4c604c29#' \
    "$THRIFT_RECIPE"
fi

# Arrow's Apache mirror selector redirects this vcluster to the same
# unreachable archive.apache.org endpoint. Preserve the pinned Conan recipe
# revision and seed Conan's checksum-addressed download cache with the official
# GitHub release asset, which is byte-identical to the Apache archive.
CONAN_DOWNLOAD_CACHE=/root/.conan/download-cache
ARROW_DOWNLOAD_CACHE_KEY=80718851411e770d39c1871c3f87561896a45a3646a334b9bf43ce3355f568da
ARROW_DOWNLOAD_CACHE_FILE="$CONAN_DOWNLOAD_CACHE/$ARROW_DOWNLOAD_CACHE_KEY"
mkdir -p "$CONAN_DOWNLOAD_CACHE"
printf '%s\n' "tools.files.download:download_cache=$CONAN_DOWNLOAD_CACHE" \
  > /root/.conan/global.conf
if [[ ! -f "$ARROW_DOWNLOAD_CACHE_FILE" ]]; then
  curl -fsSL \
    https://github.com/apache/arrow/releases/download/apache-arrow-17.0.0/apache-arrow-17.0.0.tar.gz \
    -o "$ARROW_DOWNLOAD_CACHE_FILE.tmp"
  echo "9d280d8042e7cf526f8c28d170d93bfab65e50f94569f6a790982a878d8d898d  $ARROW_DOWNLOAD_CACHE_FILE.tmp" \
    | sha256sum -c -
  mv "$ARROW_DOWNLOAD_CACHE_FILE.tmp" "$ARROW_DOWNLOAD_CACHE_FILE"
fi
echo "9d280d8042e7cf526f8c28d170d93bfab65e50f94569f6a790982a878d8d898d  $ARROW_DOWNLOAD_CACHE_FILE" \
  | sha256sum -c -
conan download \
  'arrow/17.0.0@milvus/dev-2.6#7af258a853e20887f9969f713110aac8' \
  -r default-conan-local \
  --recipe

if [[ ! -s "$SDKMAN_DIR/bin/sdkman-init.sh" ]]; then
  curl -fsSL https://get.sdkman.io -o /tmp/sdkman-install.sh
  bash /tmp/sdkman-install.sh
  rm -f /tmp/sdkman-install.sh
fi
set +u
source "$SDKMAN_DIR/bin/sdkman-init.sh"
sdk install scala 2.13.16
sdk install sbt 1.11.1
set -u

if [[ -d "$SOURCE_DIR/.git" ]] \
  && [[ "$(git -C "$SOURCE_DIR" rev-parse HEAD)" == "$CONNECTOR_COMMIT" ]]; then
  echo "Reusing existing Connector source and partial build cache at $SOURCE_DIR"
else
  rm -rf "$SOURCE_DIR"
  git clone "$CONNECTOR_REPOSITORY" "$SOURCE_DIR"
  git -C "$SOURCE_DIR" checkout "$CONNECTOR_COMMIT"
fi
cd "$SOURCE_DIR"
git submodule update --init --recursive

# The fixed Connector/submodule revisions already pin dependency recipes.
# Avoid refreshing them after the vcluster-specific Thrift source override and
# exact Arrow recipe preload above. Once the pinned Arrow package is present in
# Conan cache, do not force a full Arrow rebuild on every init restart.
sed -i \
  -e 's/ --build=arrow//' \
  -e 's/ --update$//' \
  milvus-storage/cpp/Makefile

make check-deps
make build-milvus-storage copy-native-libs

# The Connector build declares the milvus-storage Java bindings as an
# unmanaged JAR at this exact path. Building only the native JNI libraries is
# insufficient; package the Java/Scala wrapper before compiling Connector
# sources that import io.milvus.storage.*.
(
  cd milvus-storage/java
  sbt package
)
test -f \
  milvus-storage/java/target/scala-2.13/milvus-storage-jni_2.13-0.1.0-SNAPSHOT.jar

sbt assembly

CONNECTOR_JAR="$(find target/scala-2.13 -maxdepth 1 -type f -name '*assembly*.jar' -print | sort | tail -n 1)"
NATIVE_DIR="$SOURCE_DIR/src/main/resources/native"

test -n "$CONNECTOR_JAR"
test -f "$CONNECTOR_JAR"
test -f "$NATIVE_DIR/libmilvus-storage.so"
test -f "$NATIVE_DIR/libmilvus-storage-jni.so"

rm -rf "$ARTIFACT_DIR"/*
mkdir -p "$ARTIFACT_DIR/jars" "$ARTIFACT_DIR/native" "$ARTIFACT_DIR/python" "$ARTIFACT_DIR/evidence"

cp "$CONNECTOR_JAR" "$ARTIFACT_DIR/jars/spark-connector-assembly.jar"
cp "$NATIVE_DIR/libmilvus-storage.so" "$ARTIFACT_DIR/native/"
cp "$NATIVE_DIR/libmilvus-storage-jni.so" "$ARTIFACT_DIR/native/"

# Resolve the complete non-system shared-library closure while the Conan cache
# is still mounted. The two top-level JNI libraries have transitive AWS CRT,
# Arrow, Boost, Folly, and gflags dependencies that are absent from the clean
# Spark runtime image.
mapfile -t CONAN_LIBRARY_DIRS < <(
  find /root/.conan/data -type d -path '*/package/*/lib' -print | sort -u
)
CONAN_LIBRARY_PATH="$(IFS=:; echo "${CONAN_LIBRARY_DIRS[*]}")"
export LD_LIBRARY_PATH="$ARTIFACT_DIR/native:$CONAN_LIBRARY_PATH"
mapfile -t CONAN_RUNTIME_LIBRARIES < <(
  for library in "$ARTIFACT_DIR/native/libmilvus-storage.so" \
    "$ARTIFACT_DIR/native/libmilvus-storage-jni.so"; do
    ldd "$library"
  done \
    | awk '$2 == "=>" && $3 ~ /^\/root\/\.conan\// { print $3 }' \
    | sort -u
)
for library in "${CONAN_RUNTIME_LIBRARIES[@]}"; do
  cp -L "$library" "$ARTIFACT_DIR/native/$(basename "$library")"
done

python3 -m pip install --break-system-packages \
  --target "$ARTIFACT_DIR/python" \
  minio numpy pyarrow pymilvus \
  || python3 -m pip install \
    --target "$ARTIFACT_DIR/python" \
    minio numpy pyarrow pymilvus

git rev-parse HEAD > "$ARTIFACT_DIR/evidence/connector-revision.txt"
rustc --version > "$ARTIFACT_DIR/evidence/rust-version.txt"
sha256sum \
  "$ARTIFACT_DIR/jars/spark-connector-assembly.jar" \
  "$ARTIFACT_DIR/native/libmilvus-storage.so" \
  "$ARTIFACT_DIR/native/libmilvus-storage-jni.so" \
  > "$ARTIFACT_DIR/evidence/sha256sums.txt"

jar tf "$ARTIFACT_DIR/jars/spark-connector-assembly.jar" \
  | grep -q 'com/zilliz/spark/connector/operations/backfill/BackfillApp'
jar tf "$ARTIFACT_DIR/jars/spark-connector-assembly.jar" \
  | grep -q 'META-INF/services/org.apache.spark.sql.sources.DataSourceRegister'

for library in "$ARTIFACT_DIR/native"/*.so; do
  export LD_LIBRARY_PATH="$ARTIFACT_DIR/native"
  if ldd "$library" | grep -q 'not found'; then
    ldd "$library"
    exit 1
  fi
done

chown -R 185:185 "$ARTIFACT_DIR"
chmod -R a+rX "$ARTIFACT_DIR"

rm -rf "$SOURCE_DIR"
