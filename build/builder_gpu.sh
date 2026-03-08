#!/usr/bin/env bash

set -euo pipefail

source ./build/util.sh

# Absolute path to the toplevel milvus directory.
toplevel=$(dirname "$(cd "$(dirname "${0}")"; pwd)")

if [[ "$IS_NETWORK_MODE_HOST" == "true" ]]; then
  sed -i '/gpubuilder:/,/^\s*$/s/image: \${IMAGE_REPO}\/milvus-env:gpu-\${OS_NAME}-\${GPU_DATE_VERSION}/&\n    network_mode: "host"/'   $toplevel/docker-compose.yml
fi

export OS_NAME="${OS_NAME:-ubuntu22.04}"

pushd "${toplevel}"

if [[ "${1-}" == "pull" ]]; then
    $DOCKER_COMPOSE_COMMAND pull  gpubuilder
    exit 0
fi

if [[ "${1-}" == "down" ]]; then
    $DOCKER_COMPOSE_COMMAND down
    exit 0
fi

# Attempt to run in the container with the same UID/GID as we have on the host,
# as this results in the correct permissions on files created in the shared
# volumes. This isn't always possible, however, as IDs less than 100 are
# reserved by Debian, and IDs in the low 100s are dynamically assigned to
# various system users and groups. To be safe, if we see a UID/GID less than
# 500, promote it to 501. This is notably necessary on macOS Lion and later,
# where administrator accounts are created with a GID of 20. This solution is
# not foolproof, but it works well in practice.
uid=$(id -u)
gid=$(id -g)
[ "$uid" -lt 500 ] && uid=501
[ "$gid" -lt 500 ] && gid=$uid

mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker-gpu}/amd64-${OS_NAME}-ccache"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker-gpu}/amd64-${OS_NAME}-go-mod"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker-gpu}/amd64-${OS_NAME}-vscode-extensions"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker-gpu}/amd64-${OS_NAME}-conan"
chmod -R 777 "${DOCKER_VOLUME_DIRECTORY:-.docker-gpu}"

docker compose pull  gpubuilder
if [[ "${CHECK_BUILDER:-}" == "1" ]]; then
    $DOCKER_COMPOSE_COMMAND build gpubuilder 
fi

if [[ "$(id -u)" != "0" ]]; then
    $DOCKER_COMPOSE_COMMAND run --no-deps --rm -u "$uid:$gid" gpubuilder "$@"
else
    $DOCKER_COMPOSE_COMMAND run --no-deps --rm gpubuilder "$@"
fi

popd
