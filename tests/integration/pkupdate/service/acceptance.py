"""Real public RPC checks. Each invocation creates and retains a new collection pair."""

import argparse
import base64
import json
import re
import subprocess
import time
import urllib.request
from pathlib import Path

from cluster_site import ROLES
from pymilvus import DataType, MilvusClient
from pymilvus.client.prepare import Prepare
from pymilvus.client.utils import check_status
from pymilvus.grpc_gen import common_pb2, milvus_pb2


def literal(field, value):
    if value is None:
        data = {"nullValue": None}
    else:
        kind = "stringData" if isinstance(value, str) else "longData"
        data = {"scalarValue": {kind: value}}
    return {"fieldName": field, "literal": data}


def create_indexes(stub, name, indexes, timeout=90):
    """Use the public wire API so the timestamp allocation shares the build deadline."""
    deadline = time.monotonic() + timeout

    def remaining():
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError(f"Index build deadline expired for {name}")
        return seconds

    for idx in indexes:
        create = Prepare.create_index_request(name, idx.field_name, idx.get_index_configs(), index_name=idx.index_name)
        check_status(stub.CreateIndex(create, timeout=remaining()))
        snap = stub.AllocTimestamp(milvus_pb2.AllocTimestampRequest(), timeout=remaining())
        check_status(snap.status)
        describe = Prepare.describe_index_request(name, idx.index_name, timestamp=snap.timestamp)
        while True:
            response = stub.DescribeIndex(describe, timeout=remaining())
            check_status(response.status)
            state = next(x for x in response.index_descriptions if x.field_name == idx.field_name)
            if state.state == common_pb2.Finished:
                break
            if state.state == common_pb2.Failed:
                raise RuntimeError(f"Index build failed on {name}.{idx.field_name}: {state.index_state_fail_reason}")
            time.sleep(min(0.5, remaining()))


class Fixture:
    def __init__(self, site):
        self.site = site
        self.config = json.loads((site / "site.json").read_text())
        if set(self.config["metrics"]) != set(ROLES):
            raise ValueError("invalid retained site role inventory")
        self.endpoint = "127.0.0.1:" + str(self.config["conf"]["proxy.port"])
        self.client = MilvusClient(uri="http://" + self.endpoint, timeout=30)
        tag = str(time.time_ns())
        self.candidate, self.mirror = ("pk_" + k + tag for k in ("candidate", "mirror"))
        self.path = site / ("accept-" + tag + ".jsonl")

    def record(self, phase, result):
        entry = json.dumps(dict(phase=phase, result=result), default=str)
        with self.path.open("a") as stream:
            stream.write(entry + "\n")
        print(entry, flush=True)

    def put_switch(self, enabled):
        self.put_config("proxy.pkUpdate.enabled", enabled)

    def put_config(self, key, enabled):
        key = self.config["namespace"] + "/config/" + key
        payload = {"key": key, "value": str(enabled).lower()}
        encoded = {k: base64.b64encode(v.encode()).decode() for k, v in payload.items()}
        req = urllib.request.Request("http://127.0.0.1:2379/v3/kv/put", json.dumps(encoded).encode())
        with urllib.request.urlopen(req, timeout=5) as resp:
            self.record(key + " " + str(enabled), json.load(resp))

    def update(self, ids, assignments, *, partition=None):
        request = {
            "collectionName": self.candidate,
            "primaryKeys": {"intId": {"data": ids}},
            "assignments": assignments,
        }
        if partition:
            request["partitionName"] = partition
        command = [str(Path(self.config["binary"]).with_name("update-rpc")), self.endpoint]
        result = subprocess.run(
            command, input=json.dumps(request), capture_output=True, text=True, check=True, timeout=40
        )
        data = json.loads(result.stdout)
        self.record("update", [request, data])
        return data

    def create(self, name, shards=1):
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("score", DataType.INT64)
        schema.add_field("tag", DataType.VARCHAR, max_length=128, nullable=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=2)
        indexes = self.client.prepare_index_params()
        indexes.add_index("vec", index_type="FLAT", metric_type="L2")
        indexes.add_index("score", index_type="STL_SORT")
        self.client.create_collection(name, schema=schema, num_shards=shards, consistency_level="Strong", timeout=90)

        self.index_and_load(name, indexes)

    def index_and_load(self, name, indexes):
        create_indexes(self.client._get_connection()._stub, name, indexes)
        self.client.load_collection(name, timeout=90)

    def query(self, name, expr):
        rows = self.client.query(
            name, filter=expr, output_fields=["id", "score", "tag"], consistency_level="Strong", timeout=30
        )
        return sorted((dict(x) for x in rows), key=lambda x: x["id"])

    def retrieve_calls(self, role):
        addr = self.config["metrics"][role]
        with urllib.request.urlopen(f"http://127.0.0.1:{addr}/metrics", timeout=5) as resp:
            data = resp.read().decode()
        pattern = r'^milvus_cgo_cgo_duration_seconds_count\{name="retrieve",node_id="(\d+)"\} (\d+)$'
        return {int(node): int(count) for node, count in re.findall(pattern, data, re.M)}

    def run(self):
        c = self.client
        self.record("collections", [self.candidate, self.mirror])
        for name in (self.candidate, self.mirror):
            self.create(name)
        rows = [dict(id=i, score=i, tag="base", vec=[float(i), 0.0]) for i in range(1, 9)]
        c.insert(self.candidate, rows, timeout=30)
        base = [{k: v for k, v in row.items() if k != "vec"} for row in rows]
        self.record("growing base", self.query(self.candidate, "id >= 0"))
        assert self.query(self.candidate, "id >= 0") == base
        update = [literal("score", 77), literal("tag", "away")]
        rejected = self.update([1, 404], update)
        assert "admission is disabled" in rejected["status"]["reason"]
        assert self.query(self.candidate, "id >= 0") == base
        self.put_switch(True)
        for _ in range(45):
            resp = self.update([1, 404], update)
            if "admission is disabled" not in resp.get("status", {}).get("reason", ""):
                break
            time.sleep(1)
        self.accepted(resp, 2)
        self.put_switch(False)
        # Use an invalid field as the refresh probe; a successful retry cannot write data.
        for _ in range(45):
            resp = self.update([1], [literal("id", 9)])
            if "admission is disabled" in resp.get("status", {}).get("reason", ""):
                break
            time.sleep(1)
        assert "admission is disabled" in resp["status"]["reason"]
        assert "admission is disabled" in self.update([1], [literal("score", 33)])["status"]["reason"]
        expected = [dict(x) for x in base]
        expected[0].update(score=77, tag="away")
        before = sum(self.retrieve_calls("streamingnode").values())
        result = self.query(self.candidate, "id >= 0")
        after = sum(self.retrieve_calls("streamingnode").values())
        self.record("growing native reads", [before, after])
        assert after > before
        self.record("growing accepted after disable", result)
        assert result == expected
        c.flush(self.candidate, timeout=120)
        self.put_switch(True)
        for _ in range(45):
            resp = self.update([1], [literal("tag", None)])
            if "admission is disabled" not in resp.get("status", {}).get("reason", ""):
                break
            time.sleep(1)
        self.accepted(resp, 1)
        expected[0]["tag"] = None
        assert self.query(self.candidate, "id == 1") == expected[:1]
        self.accepted(self.update([1], [literal("score", 1), literal("tag", "base")]), 1)
        expected[0].update(score=1, tag="base")
        self.accepted(self.update([2, 3], [literal("score", 10), literal("tag", "new")]), 2)
        for row in expected[1:3]:
            row.update(score=10, tag="new")
        for fields in ([literal("score", 1), literal("score", 2)], [literal("id", 3)], [literal("vec", 1)]):
            assert self.update([1], fields)["status"]["reason"]
        c.insert(self.mirror, [{**row, "vec": [float(row["id"]), 0.0]} for row in expected], timeout=30)
        c.flush(self.mirror, timeout=120)
        for expr, oracle in (
            ("id >= 0", expected),
            ('score > 5 && tag == "new"', expected[1:3]),
            ("score + 1 == 11", expected[1:3]),
            ("id == 404", []),
        ):
            for name in (self.candidate, self.mirror):
                result = self.query(name, expr)
                self.record("query " + expr, result)
                assert result == oracle
        for name in (self.candidate, self.mirror):
            hits = c.search(
                name,
                data=[[2.0, 0.0]],
                anns_field="vec",
                filter='tag == "new"',
                output_fields=["score", "tag"],
                limit=2,
                consistency_level="Strong",
                timeout=30,
            )
            assert [x["id"] for x in hits[0]] == [2, 3]
            self.record("filtered search", hits)

    @staticmethod
    def accepted(response, count):
        assert not response.get("status", {}).get("reason"), response
        assert response.get("acknowledged") and int(response["updateCnt"]) == count
        assert int(response["timestamp"]) > 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("site", type=Path)
    Fixture(parser.parse_args().site.resolve()).run()
