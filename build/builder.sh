#!/usr/bin/env bash

set -eo pipefail

source ./build/util.sh

eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

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

# Define the base volume directory
DOCKER_BASE_DIR="${DOCKER_VOLUME_DIRECTORY:-.docker}"
VOLUME_PREFIX="${DOCKER_BASE_DIR}/${IMAGE_ARCH}-${OS_NAME}"

# Array of volume names
VOLUMES=(
    "ccache"
    "go-mod"
    "vscode-extensions"
    "conan"
)

# Loop through the volume names
for VOL_NAME in "${VOLUMES[@]}"; do
    TARGET_DIR="${VOLUME_PREFIX}-${VOL_NAME}"
    
    # Check if the directory exists before trying to create it
    if [ ! -d "$TARGET_DIR" ]; then
        # Create the directory. The -p flag handles parent directories.
        mkdir -p "$TARGET_DIR"
        
        # Apply permissions (chmod 777) only to the newly created directory
        echo "Created and set permissions for: $TARGET_DIR"
        chmod 777 "$TARGET_DIR"
    fi
done

if [[ "${CHECK_BUILDER:-}" == "1" ]]; then
    $DOCKER_COMPOSE_COMMAND build builder
else
    $DOCKER_COMPOSE_COMMAND pull builder
fi

$DOCKER_COMPOSE_COMMAND run --no-deps --rm builder "$@"

popd
