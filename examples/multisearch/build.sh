#!/usr/bin/env bash
# Proxy-only Go rebuild, with unchanged native libraries from the stock image.
set -euo pipefail
cd "$(dirname "$0")/../.."
bash examples/multisearch/prepare_dependencies.sh
ms_root=$PWD
ms_deps=$PWD/cmake_build/multisearch-deps
ms_lib=$PWD/internal/core/output/lib
ms_image=milvusdb/milvus@sha256:b9b79b4a284b41cd12d60abc418310f84621fa4c977d409b38f501519a7ccf8a
mkdir -p "$ms_deps"/{common,storage,include/arrow/c,include/rocksdb,include/librdkafka} "$ms_lib/pkgconfig" bin
docker pull "$ms_image"
ms_container=$(docker create "$ms_image")
trap 'docker rm "$ms_container" >/dev/null' EXIT
docker cp "$ms_container:/milvus/lib/." "$ms_lib/"
curl -fsSL --retry 3 https://github.com/zilliztech/milvus-common/archive/refs/tags/v1.0.0-78f2c29.tar.gz -o .cache/common.tar.gz
tar xzf .cache/common.tar.gz --strip-components=1 -C "$ms_deps/common"
curl -fsSL --retry 3 https://github.com/milvus-io/milvus-storage/archive/f38765d.tar.gz -o .cache/storage.tar.gz
tar xzf .cache/storage.tar.gz --strip-components=1 -C "$ms_deps/storage"
for header in abi.h helpers.h; do
 curl -fsSL --retry 3 "https://raw.githubusercontent.com/apache/arrow/apache-arrow-17.0.0/cpp/src/arrow/c/$header" -o "$ms_deps/include/arrow/c/$header"
done
curl -fsSL --retry 3 https://raw.githubusercontent.com/facebook/rocksdb/v6.29.5/include/rocksdb/c.h -o "$ms_deps/include/rocksdb/c.h"
for header in rdkafka.h rdkafka_mock.h; do
 curl -fsSL --retry 3 "https://raw.githubusercontent.com/confluentinc/librdkafka/v1.9.1/src/$header" -o "$ms_deps/include/librdkafka/$header"
done
# jemalloc is linked by cgosymbolizer's release build tag; no API header needed.
for library in milvus_core milvus-storage rocksdb rdkafka jemalloc; do
 cat > "$ms_lib/pkgconfig/$library.pc" <<PC
Name: $library
Description: pinned v3.0.5 native dependency
Version: 3.0.5
Libs: -L$ms_lib -l$library
Cflags: -I$ms_root/internal/core/src -I$ms_deps/common/include -I$ms_deps/storage/cpp/include -I$ms_deps/include
PC
done
source scripts/setenv.sh
# setenv.sh deliberately disables errexit; restore it before the build.
set -euo pipefail
go build -p 16 -pgo=configs/pgo/default.pgo -tags dynamic,sonic,with_jemalloc,bytedance_tango \
 -ldflags="-checklinkname=0 -r $RPATH -X github.com/milvus-io/milvus/cmd/milvus.MilvusVersion=3.0.5-multisearch -X github.com/milvus-io/milvus/cmd/milvus.GitCommit=af15c80751-multisearch" \
 -o bin/milvus ./cmd/main.go
go build -o bin/multisearch-load ./examples/multisearch/load
docker build -f examples/multisearch/Dockerfile -t milvus-multisearch:v305 .
