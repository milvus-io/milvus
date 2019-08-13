#!/bin/bash

./cmake_build/src/milvus_grpc_server -c ./conf/server_config.yaml -l ./conf/log_config.conf &

