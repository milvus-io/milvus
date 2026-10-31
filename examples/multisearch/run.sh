#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
kind=${1:?stock or multi}
[[ $kind == stock || $kind == multi ]] || { echo "expected stock or multi" >&2; exit 2; }
if [[ $kind == stock ]]; then image=milvusdb/milvus@sha256:b9b79b4a284b41cd12d60abc418310f84621fa4c977d409b38f501519a7ccf8a; port=29560; health=19096; else image=milvus-multisearch:v305; port=29561; health=19097; fi
docker run -d --name "multisearch-v305-$kind" --cpus=4 --cpuset-cpus=64-67 --memory=16g --security-opt seccomp=unconfined --ulimit core=0 \
 -p "127.0.0.1:$port:19530" -p "127.0.0.1:$health:9091" \
 -e ETCD_USE_EMBED=true -e ETCD_DATA_DIR=/var/lib/milvus/etcd -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
 -e COMMON_STORAGETYPE=local -e DEPLOY_MODE=STANDALONE -e GOMAXPROCS=4 -e OMP_NUM_THREADS=1 \
 --tmpfs /var/lib/milvus:rw,size=4g,mode=1777 \
 -v "$PWD/examples/multisearch/user.yaml:/milvus/configs/user.yaml:ro" \
 -v "$PWD/examples/multisearch/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml:ro" \
 "$image" milvus run standalone
