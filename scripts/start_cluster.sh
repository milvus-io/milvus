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

echo "Starting rootcoord..."
nohup ./bin/milvus run rootcoord > /tmp/rootcoord.log 2>&1 &

echo "Starting datacoord..."
nohup ./bin/milvus run datacoord > /tmp/datacoord.log 2>&1 &

echo "Starting datanode..."
nohup ./bin/milvus run datanode > /tmp/datanode.log 2>&1 &

echo "Starting proxy..."
nohup ./bin/milvus run proxy > /tmp/proxy.log 2>&1 &

echo "Starting querycoord..."
nohup ./bin/milvus run querycoord > /tmp/querycoord.log 2>&1 &

echo "Starting querynode..."
nohup ./bin/milvus run querynode > /tmp/querynode.log 2>&1 &

echo "Starting indexcoord..."
nohup ./bin/milvus run indexcoord > /tmp/indexcoord.log 2>&1 &

echo "Starting indexnode..."
nohup ./bin/milvus run indexnode > /tmp/indexnode.log 2>&1 &
