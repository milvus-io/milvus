#!/bin/sh

TARGETARCH=${1:-}
if [ -z "$TARGETARCH" ]; then
    MACHINE=$(uname -m)
    case "$MACHINE" in
        x86_64)
            TARGETARCH="amd64"
            SYSTEM_ARCH="x86_64"
            ;;
        aarch64 | arm64)
            TARGETARCH="arm64"
            SYSTEM_ARCH="aarch64"
            ;;
        *)
            echo "Error: Unsupported architecture: $MACHINE"
            exit 1
            ;;
    esac
else
    case "$TARGETARCH" in
        x86_64 | amd64)
            TARGETARCH="amd64"
            SYSTEM_ARCH="x86_64"
            ;;
        aarch64 | arm64)
            TARGETARCH="arm64"
            SYSTEM_ARCH="aarch64"
            ;;
        *)
            echo "Error: Unsupported TARGETARCH value: $TARGETARCH"
            exit 1
            ;;
    esac
fi

cat <<EOF
TARGETARCH=$TARGETARCH
SYSTEM_ARCH=$SYSTEM_ARCH
EOF
