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
# Use Internal docker mirror to solve  https://www.docker.com/increase-rate-limits

# Exit immediately for non zero status
set -e

# Use nexus as docker mirror registry
MIRROR_URL="${MIRROR_URL:-http://nexus-nexus-repository-manager-docker-5000.nexus:5000}"
#MIRROR_URL="http://nexus-nexus-repository-manager-docker-5000.nexus:5000"

# Add registry mirror config into docker daemon
set_daemon_json_file(){
    DOCKER_DAEMON_JSON_FILE="/etc/docker/daemon.json"
    mkdir -p "/etc/docker"
    echo "{\"registry-mirrors\": [\"${MIRROR_URL}\"],\"insecure-registries\":[\"${MIRROR_URL}\"]}" | tee ${DOCKER_DAEMON_JSON_FILE}
}
restart_docker () {
    echo "set-mirror.sh] service docker start"
    docker ps -aq | xargs -r docker rm -f || true
    service docker stop || true
    service docker start 
    while true; do
    # docker ps -q should only work if the daemon is ready
    docker info > /dev/null 2>&1 && break
    if [[ ${WAIT_N} -lt 5 ]]; then
      WAIT_N=$((WAIT_N+1))
      echo " set-mirror.sh] Waiting for Docker to be ready, sleeping for ${WAIT_N} seconds ..."
      sleep ${WAIT_N}
    else
      echo "set-mirror.sh] [SETUP] Reached maximum attempts, not waiting any longer ..."
      break
    fi
  done
  echo "Show Docker Info With mirror url"
  docker info
}

set_mirror(){
    set_daemon_json_file
    restart_docker 
    echo "Success."
    exit 0
    
}

set_mirror