#!/usr/bin/env bash
# Rebuild the Go server against the unchanged v3.0.0 native runtime libraries.
# A full C++/Rust rebuild is not needed for this Proxy-only prototype.
set -euo pipefail
multisearch_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$multisearch_root"
multisearch_image=${MULTISEARCH_BASE_IMAGE:-milvusdb/milvus@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862}
multisearch_deps="$multisearch_root/cmake_build/multisearch-deps"
multisearch_lib="$multisearch_root/internal/core/output/lib"
mkdir -p "$multisearch_deps/include/arrow/c" "$multisearch_deps/include/rocksdb" "$multisearch_deps/include/librdkafka" "$multisearch_lib/pkgconfig" bin
multisearch_container=$(docker create "$multisearch_image")
trap 'docker rm "$multisearch_container" >/dev/null' EXIT
docker cp "$multisearch_container:/milvus/lib/." "$multisearch_lib/"

curl --fail --location --retry 3 https://github.com/zilliztech/milvus-common/archive/refs/tags/v1.0.0-a3299ca.tar.gz -o "$multisearch_deps/common.tar.gz"
echo "14cbecc5af67711443f4c2b236740962c663800db43be56ae7d5f96bdfec1105  $multisearch_deps/common.tar.gz" | sha256sum -c -
mkdir -p "$multisearch_deps/common" "$multisearch_deps/storage"
tar -xzf "$multisearch_deps/common.tar.gz" --strip-components=1 -C "$multisearch_deps/common"
curl --fail --location --retry 3 https://github.com/milvus-io/milvus-storage/archive/f9f5bd1.tar.gz -o "$multisearch_deps/storage.tar.gz"
tar -xzf "$multisearch_deps/storage.tar.gz" --strip-components=1 -C "$multisearch_deps/storage"
for header in abi.h helpers.h; do
  curl --fail --location --retry 3 "https://raw.githubusercontent.com/apache/arrow/apache-arrow-17.0.0/cpp/src/arrow/c/$header" -o "$multisearch_deps/include/arrow/c/$header"
done
curl --fail --location --retry 3 https://raw.githubusercontent.com/facebook/rocksdb/v6.29.5/include/rocksdb/c.h -o "$multisearch_deps/include/rocksdb/c.h"
curl --fail --location --retry 3 https://raw.githubusercontent.com/confluentinc/librdkafka/v1.9.1/src/rdkafka.h -o "$multisearch_deps/include/librdkafka/rdkafka.h"
for library in milvus_core milvus-storage rocksdb rdkafka; do
  cat > "$multisearch_lib/pkgconfig/$library.pc" <<EOF
Name: $library
Description: MultiSearch prototype runtime dependency
Version: 3.0.0
Libs: -L$multisearch_lib -l$library
Cflags: -I$multisearch_root/internal/core/src -I$multisearch_deps/common/include -I$multisearch_deps/storage/cpp/include -I$multisearch_deps/include
EOF
done

# Generated Go protobuf files are checked in. To regenerate them, install
# protoc at cmake_build/bin/protoc, then run make generated-proto-without-cpp.
source scripts/setenv.sh
set -euo pipefail
go build -p "${MULTISEARCH_BUILD_JOBS:-16}" -tags dynamic -o bin/milvus ./cmd/main.go
sha256sum bin/milvus
docker build --build-arg BASE_IMAGE="$multisearch_image" -f examples/multisearch/Dockerfile -t milvus-multisearch:prototype .
