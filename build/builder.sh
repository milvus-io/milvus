#!/usr/bin/env bash

set -eo pipefail

source ./build/util.sh

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
    $DOCKER_COMPOSE_COMMAND pull  builder
    exit 0
fi

if [[ "${1-}" == "down" ]]; then
    $DOCKER_COMPOSE_COMMAND down
    exit 0
fi

PLATFORM_ARCH="${PLATFORM_ARCH:-${IMAGE_ARCH}}"

export IMAGE_ARCH=${PLATFORM_ARCH}

mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-ccache"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-go-mod"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-vscode-extensions"
mkdir -p "${DOCKER_VOLUME_DIRECTORY:-.docker}/${IMAGE_ARCH}-${OS_NAME}-conan"
chmod -R 777 "${DOCKER_VOLUME_DIRECTORY:-.docker}"

$DOCKER_COMPOSE_COMMAND pull builder
if [[ "${CHECK_BUILDER:-}" == "1" ]]; then
    $DOCKER_COMPOSE_COMMAND build builder
fi

$DOCKER_COMPOSE_COMMAND run --no-deps --rm builder "$@"

popd
