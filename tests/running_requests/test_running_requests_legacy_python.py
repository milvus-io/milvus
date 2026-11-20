"""An independently installed pre-feature Python SDK receives REST cancellation."""

import json
import os
import subprocess
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    assert_unregistered,
    list_complete,
)

pytestmark = pytest.mark.tags("L1")


def test_legacy_python_search_rest_cancel(deployment, workload, tmp_path):
    executable = os.getenv("MILVUS_OLD_PYTHON", "")
    if not Path(executable).is_file():
        pytest.skip("Isolated pre-feature Python SDK environment is unavailable")
    uris, token, user, connect = deployment
    a, b = uris
    _, _, collection, vectors = workload
    operator = connect(b)
    import numpy as np

    data = tmp_path / "vectors.npy"
    np.save(data, vectors)
    expr = f"id >= -{int(uuid4().hex[:7], 16)}"
    reason = "legacy-" + uuid4().hex
    program = """
import os,json,numpy as np,pymilvus
from pymilvus import MilvusClient,MilvusException
assert not hasattr(MilvusClient,'list_running_requests'), 'Not a pre-feature SDK'
c=MilvusClient(uri=os.environ['RR_URI'],token=os.environ['RR_TOKEN'])
try:
 try:
  r=c.search(os.environ['RR_COLLECTION'],np.load(os.environ['RR_DATA']),anns_field='vector',filter=os.environ['RR_EXPR'],limit=10,search_params={'metric_type':'L2'},timeout=90)
  print(json.dumps({'version':pymilvus.__version__,'normal':True,'rows':len(r)}))
 except MilvusException as e:
  print(json.dumps({'version':pymilvus.__version__,'code':e.code,'message':e.message}))
finally: c.close()
"""
    env = {
        **os.environ,
        "RR_URI": uris[a],
        "RR_TOKEN": token,
        "RR_COLLECTION": collection,
        "RR_DATA": str(data),
        "RR_EXPR": expr,
    }
    process = subprocess.Popen(
        [executable, "-E", "-u", "-c", program], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    try:
        deadline = monotonic() + 60
        while monotonic() < deadline:
            matches = [r for r in records() if r.expr == expr]
            if matches:
                assert len(matches) == 1
                row = matches[0]
                break
            if process.poll() is not None:
                out, err = process.communicate()
                pytest.fail(f"Legacy search exited before capture: {out} {err[-500:]}")
            sleep(0.02)
        else:
            pytest.fail("OBSERVATION_WINDOW: legacy search not captured")
        result = post_rest(uris[b], token, set(uris), "cancel", {"requestIds": [row.request_id], "reason": reason})
        assert [r["requestId"] for r in result["canceled"]] == [row.request_id]
        assert result["notFound"] == result["undetermined"] == []
        out, err = process.communicate(timeout=95)
        assert process.returncode == 0, err[-500:]
        answer = json.loads(out.strip().splitlines()[-1])
        assert answer["code"] == 3002 and reason in answer["message"], answer
        assert_unregistered(records, {row.request_id})
        print("legacy_version=" + answer["version"] + " terminal_code=3002")
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=10)
