#!/usr/bin/env bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export TZ=UTC
export SDKMAN_DIR=/root/.sdkman
export CARGO_HOME=/root/.cache/cargo
export RUSTUP_HOME=/root/.cache/rustup
export PATH="$CARGO_HOME/bin:$PATH"
export SBT_OPTS="-Xmx4g -Xms2g"
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
  libaio-dev \
  libtool \
  make \
  patchelf \
  python3-pip \
  unzip \
  wget \
  zip
rm -rf /var/lib/apt/lists/*

ln -sf /usr/bin/aclocal-1.16 /usr/bin/aclocal-1.15
ln -sf /usr/bin/automake-1.16 /usr/bin/automake-1.15

curl -fsSL "https://cmake.org/files/v3.27/cmake-3.27.5-linux-$(uname -m).tar.gz" \
  | tar --strip-components=1 -xz -C /usr/local

python3 -m pip install --break-system-packages conan==2.25.1 \
  || python3 -m pip install conan==2.25.1
conan profile detect --force
conan remote add default-conan-local2 \
  https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2 \
  || true
conan remote list

# The pinned milvus-storage revision builds its Rust DataFusion bridge as part
# of the JNI library. Keep the toolchain/registry under the restart-persistent
# root cache volume so an init-container retry can reuse downloads.
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

# The resolved Connector commit and its submodules pin dependency recipes.
# Prefer the pinned Arrow binary from the Conan 2 remote instead of forcing a
# full Arrow source build on every init-container run.
sed -i -e 's/ --build=arrow//' milvus-storage/cpp/Makefile

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

# Conan 2's milvus-storage recipe collects the non-system shared-library
# closure in build/Release/libs for clean runtime images.
CONAN_RUNTIME_DIR="$SOURCE_DIR/milvus-storage/cpp/build/Release/libs"
test -d "$CONAN_RUNTIME_DIR"
cp -a "$CONAN_RUNTIME_DIR"/. "$ARTIFACT_DIR/native/"

"$SOURCE_DIR/milvus-storage/java/patch_native_runpath.sh" "$ARTIFACT_DIR/native"
"$SOURCE_DIR/milvus-storage/java/verify_native_dependencies.sh" \
  "$ARTIFACT_DIR/native/libmilvus-storage-jni.so"

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
