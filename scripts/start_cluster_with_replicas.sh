#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# start_cluster_with_replicas.sh — start a local cluster with N querynodes
# and N streamingnodes so that tests with replica_number >= 2 (e.g.
# test_milvus_client_alter_warmup, test_collection multi-replica) can run.
#
# Usage:
#   bash scripts/start_cluster_with_replicas.sh        # default 2
#   N_REPLICAS=3 bash scripts/start_cluster_with_replicas.sh

N=${N_REPLICAS:-2}

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  LIBJEMALLOC=$PWD/internal/core/output/lib/libjemalloc.so
  if test -f "$LIBJEMALLOC"; then
    export LD_PRELOAD="$LIBJEMALLOC"
    export MALLOC_CONF=background_thread:true
  else
    echo "WARN: Cannot find $LIBJEMALLOC"
  fi
  export LD_LIBRARY_PATH=$PWD/internal/core/output/lib/:$LD_LIBRARY_PATH
fi

echo "Starting mixcoord..."
nohup ./bin/milvus run mixcoord --run-with-subprocess >/tmp/mixcoord.log 2>&1 &

echo "Starting datanode..."
nohup ./bin/milvus run datanode --run-with-subprocess >/tmp/datanode.log 2>&1 &

# QueryNode ports: 21123, 21124, 21125, ...
QN_PORT_BASE=21123
echo "Starting ${N} querynode(s)..."
for i in $(seq 1 "$N"); do
  port=$((QN_PORT_BASE + i - 1))
  MILVUS_CONF_QUERYNODE_PORT=$port \
    nohup ./bin/milvus run querynode --run-with-subprocess \
    >/tmp/querynode_${i}.log 2>&1 &
  echo "  querynode #$i on port $port (log: /tmp/querynode_${i}.log)"
done

# StreamingNode ports: 22222, 22223, 22224, ...
SN_PORT_BASE=22222
echo "Starting ${N} streamingnode(s)..."
for i in $(seq 1 "$N"); do
  port=$((SN_PORT_BASE + i - 1))
  MILVUS_CONF_STREAMINGNODE_PORT=$port \
    nohup ./bin/milvus run streamingnode --run-with-subprocess \
    >/tmp/streamingnode_${i}.log 2>&1 &
  echo "  streamingnode #$i on port $port (log: /tmp/streamingnode_${i}.log)"
done

echo "Starting proxy..."
nohup ./bin/milvus run proxy --run-with-subprocess >/tmp/proxy.log 2>&1 &

echo
echo "Cluster started with ${N} querynode(s) + ${N} streamingnode(s)."
echo "Tail logs: /tmp/{mixcoord,datanode,querynode_*,streamingnode_*,proxy}.log"
