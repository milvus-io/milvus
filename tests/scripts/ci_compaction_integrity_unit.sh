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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}/tests/python_client"

# Separate, server-independent job step using the same compatible SDK as E2E.
# The explicit unit-test path bypasses default pytest discovery intentionally.
exec pytest "$@" \
  -n 0 \
  --tags CompactionIntegrityUnit \
  -- \
  milvus_client/compaction_integrity_helper_tests.py \
  milvus_client/test_milvus_client_data_integrity.py
