#!/usr/bin/env bash

set -eo pipefail

# Absolute path to the toplevel milvus directory.
toplevel=$(dirname "$(cd "$(dirname "${0}")"; pwd)")

if [[ "$IS_NETWORK_MODE_HOST" == "true" ]]; then
  sed -i '/builder:/,/^\s*$/s/image: \${IMAGE_REPO}\/milvus-env:\${OS_NAME}-\${DATE_VERSION}/&\n    network_mode: "host"/' $toplevel/docker-compose.yml
fi

if [[ -f "$toplevel/.env" ]]; then
    set -a  # automatically export all variables from .env
    source $toplevel/.env
    set +a  # stop automatically exporting
fi

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
