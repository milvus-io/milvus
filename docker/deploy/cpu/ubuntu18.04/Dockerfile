FROM ubuntu:18.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    gfortran libsqlite3-dev libmysqlclient-dev libcurl4-openssl-dev python3 libopenblas-dev liblapack3 && \
    apt-get remove --purge -y && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/x86_64-linux-gnu/libmysqlclient.so /usr/lib/x86_64-linux-gnu/libmysqlclient_r.so

COPY ./milvus /var/lib/milvus
COPY ./docker-entrypoint.sh /var/lib/milvus
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/var/lib/milvus/lib"

WORKDIR /var/lib/milvus

ENTRYPOINT [ "/var/lib/milvus/docker-entrypoint.sh" ]

CMD [ "start" ]

EXPOSE 19530

