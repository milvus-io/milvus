#!/usr/bin/env bash

set -ex

pip3 install -r requirements.txt
./yaml_processor.py merge -f /opt/milvus/conf/server_config.yaml -m ../yaml/update_server_config.yaml -i && \
rm /opt/milvus/conf/server_config.yaml.bak

if [ -d "/opt/milvus/unittest" ]; then
  rm -rf "/opt/milvus/unittest"
fi

tar -zcvf ./${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz -C /opt/ milvus