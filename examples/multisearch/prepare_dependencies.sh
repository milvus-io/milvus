#!/usr/bin/env bash
# These patches modify the existing MilvusService and official PyMilvus classes.
set -euo pipefail
cd "$(dirname "$0")/../.."
mkdir -p .cache
if [[ ! -d .cache/milvus-proto ]]; then
 git clone --depth 1 --branch v3.0.5 https://github.com/milvus-io/milvus-proto.git .cache/milvus-proto
 git -C .cache/milvus-proto apply "$PWD/examples/multisearch/patches/milvus-proto.patch"
fi
if [[ ! -d .cache/pymilvus ]]; then
 git clone --depth 1 --branch v3.0.0 https://github.com/milvus-io/pymilvus.git .cache/pymilvus
 git -C .cache/pymilvus apply "$PWD/examples/multisearch/patches/pymilvus.patch"
fi
# Fail clearly if a pre-existing cache points at different or unpatched sources.
[[ $(git -C .cache/milvus-proto rev-parse HEAD) == 2f00c7cf17db3ab3bb7b93ea8f2a7261bcc264da ]]
[[ $(git -C .cache/pymilvus rev-parse HEAD) == f63a1aa2816205c165f04d33e658b06e9b1cee11 ]]
git -C .cache/milvus-proto apply --reverse --check "$PWD/examples/multisearch/patches/milvus-proto.patch"
git -C .cache/pymilvus apply --reverse --check "$PWD/examples/multisearch/patches/pymilvus.patch"
cat > go.work <<'WORK'
go 1.26.9
use (
.
./pkg
./client
./.cache/milvus-proto/go-api
)
WORK
python3 -m venv .venv
.venv/bin/pip install -e .cache/pymilvus grpcio-tools==1.66.2 pytest psutil
