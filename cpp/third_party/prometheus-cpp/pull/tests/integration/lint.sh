#!/usr/bin/env bash

set -o errexit
set -o pipefail

curl=$(which curl)
if [ ! -x "$curl" ] ; then
    echo "curl must be in path for this test to run"
    exit 1
fi

promtool=$(which promtool)
if [ ! -x "$promtool" ] ; then
    echo "promtool must be in path for this test to run"
    exit 1
fi

pull/tests/integration/sample-server&
sample_server_pid=$!

function stop_server {
  echo "Stopping sample-server"
  kill -9 $sample_server_pid
}
trap stop_server EXIT

sleep 1

"$curl" -s http://localhost:8080/metrics | "$promtool" check metrics
