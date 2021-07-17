#!/bin/bash

cd ../bin

echo "start master service"
./milvus run rootcoord    > /tmp/milvus/root_coord.log  2>&1  &
sleep 1
echo "start service"
./milvus run datacoord   > /tmp/milvus/data_coord.log  2>&1  &
./milvus run indexcoord  > /tmp/milvus/index_coord.log 2>&1  &
./milvus run querycoord  > /tmp/milvus/query_coord.log 2>&1  &
sleep 1
echo "start node"
./milvus run proxy --alias=1 > /tmp/milvus/proxynode1.log 2>&1  &
# ./milvus run proxy --alias=2 > /tmp/milvus/proxynode2.log 2>&1  &
./milvus run datanode --alias=1 > /tmp/milvus/datanode1.log 2>&1  &
# ./milvus run datanode --alias=2 > /tmp/milvus/datanode2.log 2>&1  &
./milvus run indexnode --alias=1 > /tmp/milvus/indexnode1.log 2>&1  &
sleep 2
./milvus run indexnode --alias=2 > /tmp/milvus/indexnode2.log 2>&1  &
./milvus run querynode --alias=1 > /tmp/milvus/querynode1.log 2>&1  &
# ./milvus run querynode --alias=2 > /tmp/milvus/querynode2.log 2>&1  &                
