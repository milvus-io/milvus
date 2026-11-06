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

"""Narrow failure signatures for issue-linked Struct schema regressions."""

SUBFIELD_DROP_ISSUE = "https://github.com/milvus-io/milvus/issues/55234"
SUBFIELD_DROP_MESSAGE = 'sub-field "added" does not exist in struct field "clips": invalid parameter'


class ConcurrentSubfieldDropRejection(AssertionError):
    """Insert or full upsert rejected a cached child during schema removal."""


def is_subfield_drop_rejection(code, message):
    return code == 1100 and message == SUBFIELD_DROP_MESSAGE


def is_verified_subfield_drop_failure(error, events, errors):
    """Accept only verified write rejections from the removed-child schema race."""
    if not isinstance(error, AssertionError) or not errors or str(error) != str(errors):
        return False
    if not all(e["epoch"] == 3 and is_subfield_drop_rejection(e.get("code"), e.get("message")) for e in errors):
        return False
    failed = [e for e in events if e["event"] == "end" and e["outcome"] not in ("ack", "rejected_schema")]
    if len(failed) != len(errors) or not all(
        e["epoch"] == 3
        and e["op"] in ("insert", "full_upsert")
        and e.get("write")
        and is_subfield_drop_rejection(e.get("error_code"), e.get("error_message"))
        for e in failed
    ):
        return False
    reconciled = [e for e in events if e["event"] == "uncertain_reconciliation"]
    if len(reconciled) != len(failed) or {e["entry"]["pk"] for e in reconciled} != {e["pk"] for e in failed}:
        return False
    if not all(e["matches"] == ["before"] for e in reconciled):
        return False
    if any(e["event"] in ("read_mismatch", "final_verification_failure") for e in events):
        return False
    summaries = [e for e in events if e["event"] == "summary"]
    if (
        len(summaries) != 1
        or not summaries[0]["read_write_overlap"]
        or not all(summaries[0]["ddl_write_overlap"].values())
    ):
        return False
    return any(e["event"] == "verified" and e["phase"] == "reloaded" for e in events)
