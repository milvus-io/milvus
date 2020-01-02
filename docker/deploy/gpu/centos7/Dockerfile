FROM nvidia/cuda:10.1-devel-centos7

ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

RUN yum install -y epel-release && \
    yum install -y libgomp libgfortran4 mysql-devel openblas-devel lapack-devel && \
    rm -rf /var/cache/yum/*

COPY ./milvus /var/lib/milvus
COPY ./docker-entrypoint.sh /var/lib/milvus
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/var/lib/milvus/lib"

WORKDIR /var/lib/milvus

ENTRYPOINT [ "/var/lib/milvus/docker-entrypoint.sh" ]

CMD [ "start" ]

EXPOSE 19530
