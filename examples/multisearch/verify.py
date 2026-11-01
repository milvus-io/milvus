"""Exercise the native MilvusClient.multi_search against a running test server."""

import argparse, base64, copy, json, time
from pathlib import Path
from unittest.mock import patch
import grpc
import numpy as np
from pymilvus import MilvusClient
from pymilvus.exceptions import MilvusException
from pymilvus.grpc_gen import milvus_pb2 as pb, milvus_pb2_grpc as pb_grpc

p = argparse.ArgumentParser()
p.add_argument("--port", type=int, default=29561)
p.add_argument("--output", default="artifacts/multisearch-v2/functional.json")
p.add_argument("--fixture", default="artifacts/multisearch-v2/fixture.json")
a = p.parse_args()
specs = json.loads(Path(a.fixture).read_text())["specs"]
uri = f"http://localhost:{a.port}"
c = MilvusClient(uri=uri, token="root:Milvus")
clients = {
    db: MilvusClient(uri=uri, token="root:Milvus", db_name=db) for db in ["ms_v2_a", "ms_v2_b"]
}
report = {"server_version": c.get_server_version(), "tests": []}


def record(name, detail=None):
    report["tests"].append({"name": name, "passed": True, "detail": detail})
    Path(a.output).write_text(json.dumps(report, indent=2))
    print("PASS", name, flush=True)


def compare(x, y):
    assert len(x) == len(y)
    for xx, yy in zip(x, y):
        assert [h["id"] for h in xx] == [h["id"] for h in yy]
        assert [h["entity"] for h in xx] == [h["entity"] for h in yy]
        np.testing.assert_allclose(
            [h["distance"] for h in xx], [h["distance"] for h in yy], rtol=1e-5, atol=1e-5
        )


items = [dict(copy.deepcopy(specs[i % 8]), id=str(i), limit=1 + i % 9) for i in range(50)]
initial_db = c._config.db_name
before = copy.deepcopy(items)
stub = c._get_connection()._stub
with patch.object(stub, "MultiSearch", wraps=stub.MultiSearch) as multi, patch.object(
    stub, "Search", wraps=stub.Search
) as ordinary:
    out = c.multi_search(items, max_concurrent_searches=8, timeout=20)
    assert multi.call_count == 1 and ordinary.call_count == 0
    assert multi.call_args.args[0].DESCRIPTOR.full_name == "milvus.proto.milvus.MultiSearchRequest"
record("One native MilvusService.MultiSearch RPC; no client Search fan-out")
with grpc.insecure_channel(f"localhost:{a.port}") as raw_channel:
    raw_stub = pb_grpc.MilvusServiceStub(raw_channel)
    request = pb.MultiSearchRequest(
        items=[
            pb.MultiSearchItem(
                id="auth-test",
                search=pb.SearchRequest(collection_name="ms_v2_0", db_name="ms_v2_a"),
            )
        ]
    )
    for md in [
        (),
        (("authorization", base64.b64encode(b"root:incorrect-prototype-password").decode()),),
    ]:
        try:
            raw_stub.MultiSearch(request, timeout=5, metadata=md)
            raise AssertionError("unauthenticated MultiSearch accepted")
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.UNAUTHENTICATED
record("Envelope rejects missing and invalid credentials before dispatch")
assert items == before
for item, result in zip(items, out):
    assert result["id"] == item["id"] and result["error"] is None
    args = dict(item)
    db = args.pop("db_name")
    args.pop("id")
    compare(result["result"], clients[db].search(**args))
record("50 searches match ordinary Search across two databases and four shared collection names")
assert c._config.db_name == initial_db
record("Input dictionaries and connection database remain unchanged")
fallback = dict(specs[4])
fallback.pop("db_name")
r = clients["ms_v2_b"].multi_search([fallback, specs[0]], max_concurrent_searches=1)
compare(r[0]["result"], out[4]["result"])
record("Omitted item database inherits client default while explicit override works")
mixed = [
    specs[0],
    dict(specs[0], db_name="ms_v2_missing"),
    dict(specs[0], data=[[1.0, 2.0]]),
    specs[4],
]
r = c.multi_search(mixed)
assert [x["error"] is None for x in r] == [True, False, False, True]
record(
    "Missing database and invalid dimension isolated from successful siblings",
    [x["error"] for x in r],
)
for name, batch in [
    ("duplicate ids", [dict(specs[0], id="x"), dict(specs[4], id="x")]),
    ("129 items", [dict(specs[0], id=str(i)) for i in range(129)]),
    ("oversized request", [dict(specs[0], id=str(i), filter="x" * (1 << 20)) for i in range(5)]),
]:
    try:
        c.multi_search(batch, timeout=5)
        raise AssertionError(name + " accepted")
    except MilvusException as e:
        record("Reject " + name, {"code": e.code})
user, password, role = "ms_v2_reader", "MultiSearch_test_123", "ms_v2_role"
if user not in c.list_users():
    c.create_user(user, password)
if role not in c.list_roles():
    c.create_role(role)
c.grant_role(user, role)
c.grant_privilege_v2(role, "Search", "ms_v2_0", db_name="ms_v2_a")
restricted = MilvusClient(uri=uri, token=f"{user}:{password}")
for attempt in range(30):
    r = restricted.multi_search([specs[0], specs[4]])
    if r[0]["error"] is None:
        break
    time.sleep(0.2)
assert r[0]["error"] is None and r[1]["error"]["grpc_code"] == 7
record("Same collection name in different databases has independent RBAC", r[1]["error"])
restricted.close()
try:
    c.multi_search(items, timeout=0.000001)
    raise AssertionError("deadline succeeded")
except grpc.RpcError as e:
    assert e.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    record("Envelope deadline", e.code().name)
assert c.multi_search([specs[0]])[0]["error"] is None
record("Successful request after cancellation")
report["all_passed"] = True
Path(a.output).write_text(json.dumps(report, indent=2))
c.close()
for client in clients.values():
    client.close()
