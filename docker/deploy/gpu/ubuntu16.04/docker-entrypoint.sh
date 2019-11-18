#!/bin/bash

set -e

if [ "$1" == 'start' ]; then
    cd /opt/milvus/scripts && ./start_server.sh
fi

exec "$@"
