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

cd ../build/docker/deploy/

echo "starting rootcoord docker"
nohup docker-compose -p milvus up rootcoord > ~/rootcoord_docker.log 2>&1 &

echo "starting proxy docker"
nohup docker-compose -p milvus up  proxy > ~/proxy_docker.log 2>&1 &

echo "starting indexcoord docker"
nohup docker-compose -p milvus up  indexcoord > ~/indexcoord_docker.log 2>&1 &

echo "starting indexnode docker"
nohup docker-compose -p milvus up  indexnode > ~/indexnode_docker.log 2>&1 &

echo "starting querycoord docker"
nohup docker-compose -p milvus up querycoord > ~/querycoord_docker.log 2>&1 &

echo "starting datacoord docker"
nohup docker-compose -p milvus up datacoord > ~/datacoord_docker.log 2>&1 &

echo "starting querynode1 docker"
nohup docker-compose -p milvus run -e QUERY_NODE_ID=1 querynode > ~/querynode1_docker.log 2>&1 &

echo "starting querynode2 docker"
nohup docker-compose -p milvus run -e QUERY_NODE_ID=2 querynode > ~/querynode2_docker.log 2>&1 &

echo "starting datanode docker"
nohup docker-compose -p milvus run -e DATA_NODE_ID=3 datanode > ~/datanode_docker.log 2>&1 &
