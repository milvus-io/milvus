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

# The owning CI job supplies an empty standalone test-hook deployment and profile.
# Ordinary PR jobs select Paimon L0/L1 through ci_e2e_4am.sh or the TCUS file shards.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}/tests/python_client"
export PYTHONPATH="${ROOT}/tests/python_client${PYTHONPATH:+:${PYTHONPATH}}"

# Retain the standard host/port/MinIO/URI/JUnit options from the CI caller.
exec python -m pytest "$@" \
  -n 0 \
  --tags L3 \
  -m paimon_refresh_serial \
  --timeout "${PAIMON_L3_CASE_TIMEOUT:-1200}" \
  -o junit_family=xunit1 \
  milvus_client/test_milvus_client_external_table_paimon_l3.py
