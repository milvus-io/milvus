FROM  milvusdb/milvus-env:ubuntu20.04-20240321-1b31824 as builder

WORKDIR /home/milvus

# compile thirdparty
COPY Makefile Makefile
COPY internal/core/conanfile.py  internal/core/build.sh  internal/core/
COPY scripts/3rdparty_build.sh scripts/
RUN make build-3rdparty

# download go dependencies
COPY go.mod go.mod ./
COPY pkg/go.mod pkg/go.mod pkg/
RUN go mod download

# download milvus proto 
COPY scripts/download_milvus_proto.sh scripts/
RUN make download-milvus-proto

# generate proto
COPY cmd/tools/migration/legacy/ cmd/tools/migration/legacy/
COPY internal/proto internal/proto
COPY internal/core/src/pb/ internal/core/src/pb/
COPY scripts/generate_proto.sh scripts/
RUN make generated-proto

# compile milvus core
COPY internal/core internal/core
COPY scripts/core_build.sh  scripts/azure_build.sh scripts/
RUN make build-cpp 

# compile milvus
COPY pkg pkg
COPY internal internal
COPY cmd cmd
COPY scripts/setenv.sh scripts/
RUN go mod tidy
RUN make install

#################################################################################################3

FROM ubuntu:focal-20220426

ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates libaio-dev libgomp1 && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

COPY --chown=root:root --chmod=774 ./configs/ /milvus/configs/
COPY --from=builder --chown=root:root --chmod=774 /home/milvus/bin/ /milvus/bin/
COPY --from=builder --chown=root:root --chmod=774 /home/milvus/lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV MALLOC_CONF=background_thread:true

# Add Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

WORKDIR /milvus/