#!/usr/bin/env bash

set -euo pipefail

# Absolute path to the toplevel milvus directory.
toplevel=$(dirname "$(cd "$(dirname "${0}")"; pwd)")

if [[ -f "$toplevel/.env" ]]; then
  export $(cat $toplevel/.env | xargs)
fi

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

PLATFORM_ARCH="${PLATFORM_ARCH:-${IMAGE_ARCH}}"

export IMAGE_ARCH=${PLATFORM_ARCH}

echo ${IMAGE_ARCH}

mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-ccache"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-go-mod"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-vscode-extensions"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-conan"
chmod -R 777 "${DOCKER_VOLUME_DIRECTORY:-.docker}"

docker-compose pull --ignore-pull-failures builder
if [[ "${CHECK_BUILDER:-}" == "1" ]]; then
    docker-compose build builder
fi

docker-compose run --no-deps --rm builder "$@"

popd
