#!/usr/bin/env bash
# Use Internal docker mirror to solve  https://www.docker.com/increase-rate-limits
set -e

# Use nexus as docker mirror registry
MIRROR_URL="http://nexus-nexus-repository-manager-docker-5000.nexus:5000"
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