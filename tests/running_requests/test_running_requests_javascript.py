"""Verify REST strings survive a real JavaScript JSON parser (macOS JXA)."""

import json
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    assert_canceled,
    list_complete,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def test_javascript_rest_int64_and_request_id(deployment, workload):
    js = shutil.which("osascript")
    if not js:
        pytest.skip("Real JavaScript runtime unavailable: this runner uses macOS JXA")
    uris, token, user, connect = deployment
    a, b = uris
    app, _, collection, vectors = workload
    operator = connect(b)
    reason = "js-roundtrip-" + uuid4().hex
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, app, collection, vectors)
        row = wait_for_records(
            lambda: list_complete(operator, set(uris), collection_name=collection, user=user),
            {"Running"},
            lambda r: r.state,
            [future],
            exact=True,
        )["Running"]
        try:
            body = post_rest(
                uris[b],
                token,
                set(uris),
                "list",
                {"collectionName": collection, "user": user},
                headers={"Accept-Type-Allow-Int64": "false"},
            )
            selected = [r for r in body["requests"] if r["requestId"] == row.request_id]
            assert len(selected) == 1 and row.task_ids and any(i > 2**53 for i in row.task_ids)
            code = 'function run(argv) { var r=JSON.parse(argv[0]); if(typeof r.requestId!=="string" || !r.taskIds.every(function(x){return typeof x==="string";})) throw Error("unsafe types"); return JSON.stringify({requestIds:[r.requestId],taskIds:r.taskIds}); }'
            done = subprocess.run(
                [js, "-l", "JavaScript", "-e", code, json.dumps(selected[0])],
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert done.returncode == 0, done.stderr
            decoded = json.loads(done.stdout)
            assert decoded["requestIds"] == [row.request_id]
            assert decoded["taskIds"] == [str(i) for i in row.task_ids]
            # These exact JS-produced IDs go back to the actual REST API.
            canceled = post_rest(
                uris[b], token, set(uris), "cancel", {"requestIds": decoded["requestIds"], "reason": reason}
            )
            assert [r["requestId"] for r in canceled["canceled"]] == [row.request_id]
            assert canceled["notFound"] == canceled["undetermined"] == []
        finally:
            if not future.done():
                operator.cancel_requests([row.request_id], reason=reason, timeout=10)
        assert_canceled(future, reason)
