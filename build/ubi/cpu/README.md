## HOW TO BUILD

### Build Milvus image

The following environment variables can be set for the build, shown here with defaults:

```
MILVUS_IMAGE_REPO="${MILVUS_IMAGE_REPO:-milvus}"
MILVUS_IMAGE_TAG="${MILVUS_IMAGE_TAG:-ubi9.5}"
IMAGE_ARCH="${IMAGE_ARCH:-amd64}"
BUILD_ARGS="${BUILD_ARGS:---build-arg TARGETARCH=${IMAGE_ARCH}}"
```

Run the following from the root of the repository.

```bash
./build/build_ubi.sh
```

The build takes a long time depending on your system, ~1hr

## HOW TO RUN

### Run standalone milvus service

Create the milvus data volume

```bash
mkdir -p /tmp/volumes/milvus
```

Create standalone config file for embedded etcd

```bash
cat > embedEtcd.yaml << EOF
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF
```

Run milvus standalone

```bash
podman run --rm -d \
    --name milvus-standalone \
    --security-opt seccomp:unconfined \
    -v /tmp/volumes/milvus:/var/lib/milvus:Z  \
    -v $(pwd)/embedEtcd.yaml:/etc/milvus/configs/embedEtcd.yaml:Z \
    -p 19530:19530 \
    -p 9091:9091 \
    -p 2379:2379 \
    -e ETCD_DATA_DIR=/var/lib/milvus/etcd \
    -e ETCD_USE_EMBED=true \
    -e COMMONSTORAGETYPE=local \
    --health-cmd="curl -f http://localhost:9091/healthz" \
    --health-interval=30s \
    --health-start-period=90s \
    --health-timeout=20s \
    --health-retries=3 \
    milvus:ubi9.5 \
    milvus run standalone
```
