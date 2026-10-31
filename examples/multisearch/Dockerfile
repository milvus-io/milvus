ARG BASE_IMAGE=milvusdb/milvus@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862
FROM ${BASE_IMAGE}
COPY bin/milvus /milvus/bin/milvus
COPY configs/ /milvus/configs/
COPY examples/multisearch/user.yaml /milvus/configs/user.yaml
COPY examples/multisearch/embedEtcd.yaml /milvus/configs/embedEtcd.yaml
ENV ETCD_USE_EMBED=true ETCD_DATA_DIR=/var/lib/milvus/etcd ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml COMMON_STORAGETYPE=local DEPLOY_MODE=STANDALONE
CMD ["milvus", "run", "standalone"]
