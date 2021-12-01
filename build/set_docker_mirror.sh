#!/usr/bin/env bash
# Use Internal docker mirror to solve  https://www.docker.com/increase-rate-limits
set -e

MIRROR_URL="http://10.201.177.237:5000"
set_daemon_json_file(){
    DOCKER_DAEMON_JSON_FILE="/etc/docker/daemon.json"
    if test -f ${DOCKER_DAEMON_JSON_FILE}
    then
        cp  ${DOCKER_DAEMON_JSON_FILE} "${DOCKER_DAEMON_JSON_FILE}.bak"
        if  grep -q registry-mirrors "${DOCKER_DAEMON_JSON_FILE}.bak";then
             cat "${DOCKER_DAEMON_JSON_FILE}.bak" | sed -n "1h;1"'!'"H;\${g;s|\"registry-mirrors\":\s*\[[^]]*\]|\"registry-mirrors\": [\"${MIRROR_URL}\"]|g;p;}" |  tee ${DOCKER_DAEMON_JSON_FILE}
        else
            cat "${DOCKER_DAEMON_JSON_FILE}.bak" | sed -n "s|{|{\"registry-mirrors\": [\"${MIRROR_URL}\"],|g;p;" |  tee ${DOCKER_DAEMON_JSON_FILE}
        fi
    else
        mkdir -p "/etc/docker"
        echo "{\"registry-mirrors\": [\"${MIRROR_URL}\"]}" | tee ${DOCKER_DAEMON_JSON_FILE}
    fi
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