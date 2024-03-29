FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

COPY ./bin/ /milvus/bin/

COPY ./configs/ /milvus/configs/

COPY ./internal/core/output/lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so
ENV MALLOC_CONF=background_thread:true

WORKDIR /milvus

RUN  ldconfig && apt update && apt install -y libgomp1 libopenblas-dev  && apt clean