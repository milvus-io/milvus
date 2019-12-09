#!/bin/bash

set -e

if [ "$1" == 'start' ]; then
    cd /var/lib/milvus/scripts && ./start_server.sh
fi

exec "$@"
