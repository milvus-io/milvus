#!/usr/bin/env bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# The dedicated job owns deployment, dependencies, timeout, and artifact retention.
# Never invoke this entry from the shared per-PR or Nightly runner.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}/tests/python_client"

# Pytest imports -p plugins before adding test directories to sys.path.
export PYTHONPATH="${ROOT}/tests/python_client${PYTHONPATH:+:${PYTHONPATH}}"

# Match the repository log directory by default; preserve an Argo/caller path.
export DATA_INTEGRITY_DIAGNOSTICS_PATH="${DATA_INTEGRITY_DIAGNOSTICS_PATH:-${CI_LOG_PATH:-/tmp/ci_logs}/case-events.jsonl}"

# Preserve caller connection options and filters; keep workload selection serial.
exec pytest "$@" \
  -p common.data_integrity_diagnostics \
  --log-cli-level=INFO \
  -n 0 \
  --tags L3 \
  --run-compaction-integrity-serial \
  -m compaction_data_integrity_serial \
  milvus_client/test_milvus_client_data_integrity.py::TestMilvusClientCompactionDataIntegrity \
  milvus_client/test_milvus_client_continuous_data_integrity.py::TestMilvusClientContinuousStreamingDataIntegrity
