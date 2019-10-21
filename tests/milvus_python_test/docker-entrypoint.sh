#!/bin/bash

set -e

if [ "$1" = 'start' ]; then
   tail -f /dev/null
fi

exec "$@"