#!/usr/bin/env bash

set -euo pipefail

# Absolute path to the toplevel milvus directory.
toplevel=$(dirname "$(cd "$(dirname "${0}")"; pwd)")

export OS_NAME="${OS_NAME:-ubuntu20.04}"

pushd "${toplevel}"

if [[ "${1-}" == "pull" ]]; then
    docker-compose pull --ignore-pull-failures builder
    exit 0
fi

if [[ "${1-}" == "down" ]]; then
    docker-compose down
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

mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/amd64-${OS_NAME}-ccache"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/amd64-${OS_NAME}-go-mod"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/thirdparty"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/amd64-${OS_NAME}-vscode-extensions"
chmod -R 777 "${DOCKER_VOLUME_DIRECTORY:-.docker}"

docker-compose pull --ignore-pull-failures builder
if [[ "${CHECK_BUILDER:-}" == "1" ]]; then
    docker-compose build builder
fi

if [[ "$(id -u)" != "0" ]]; then
    docker-compose run --rm -u "$uid:$gid" builder "$@"
else
    docker-compose run --rm --entrypoint "/tini -- /entrypoint.sh" builder "$@"
fi

popd
