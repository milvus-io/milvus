#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
docker run -d --name milvus-multisearch-prototype \
  --cpus=16 --memory=32g --security-opt seccomp=unconfined --ulimit core=0 \
  -p 127.0.0.1:29550:19530 -p 127.0.0.1:19095:9091 \
  -v multisearch-prototype-data:/var/lib/milvus milvus-multisearch:prototype
for attempt in $(seq 1 90); do
  if curl --fail --silent http://127.0.0.1:19095/healthz; then
    exit 0
  fi
  sleep 2
done
echo "Milvus did not become healthy; inspect docker logs milvus-multisearch-prototype" >&2
exit 1
