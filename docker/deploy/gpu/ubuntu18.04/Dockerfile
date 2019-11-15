FROM nvidia/cuda:10.1-devel-ubuntu18.04

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

RUN rm -rf /etc/apt/sources.list.d/nvidia-ml.list && rm -rf /etc/apt/sources.list.d/cuda.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    gfortran libsqlite3-dev libmysqlclient-dev libcurl4-openssl-dev python3 && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

COPY ./docker-entrypoint.sh /opt
COPY ./milvus /opt/milvus
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/opt/milvus/lib"

WORKDIR /opt/milvus

ENTRYPOINT [ "/opt/docker-entrypoint.sh" ]

CMD [ "start" ]

EXPOSE 19530

