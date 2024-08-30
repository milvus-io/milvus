FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates libaio-dev libgomp1 libopenblas-dev && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

# Add Tini
RUN curl -L -o /tini https://github.com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH && \
    chmod +x /tini

COPY --chown=root:root --chmod=774 ./bin/ /milvus/bin/
COPY --chown=root:root --chmod=774 ./configs/ /milvus/configs/
COPY --chown=root:root --chmod=774 ./lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so
ENV MALLOC_CONF=background_thread:true

ENTRYPOINT ["/tini", "--"]

WORKDIR /milvus

