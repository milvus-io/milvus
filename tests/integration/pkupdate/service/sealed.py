"""Bind a public write/read to a retained UP QueryView and a QN physical segment."""

import sys
import time
from pathlib import Path

from acceptance import Fixture, literal
from google.protobuf.json_format import MessageToDict
from metadata import capture


def check(f, candidate, mirror):
    f.candidate, f.mirror = candidate, mirror
    for name in (candidate, mirror):
        deadline = time.monotonic() + 120
        while True:
            segments = f.client._get_connection().get_query_segment_info(name, timeout=30)
            if len(segments) == 1 and segments[0].is_sorted and MessageToDict(segments[0]).get("state") == "Sealed":
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Waiting for one sorted sealed segment in {name}: {segments}")
            time.sleep(1)
        segment = segments[0]
        f.record("loaded segment " + name, MessageToDict(segment))
        assert segment.num_rows == 8 and segment.is_sorted and segment.storage_version == 3
        assert str(segment).find("state: Sealed") >= 0 and len(segment.nodeIds) == 1
        if name == candidate:
            target = segment
    prefix = "read-owner-" + str(time.time_ns())
    snapshot = capture(f.site, prefix)
    up = [
        x["data"]
        for x in snapshot
        if x.get("data", {}).get("meta", {}).get("state") == "QueryViewStateUp"
        and x["data"]["meta"].get("collectionId") == str(target.collectionID)
    ]
    assert len(up) == 1, up
    dv = next(
        x["data"]
        for x in snapshot
        if x.get("key", "").startswith("coord/dv/")
        and x["data"]["collectionId"] == str(target.collectionID)
        and x["data"]["dataVersion"] == up[0]["meta"]["version"]["dataVersion"]
    )
    assert [s for shard in dv["shards"] for p in shard.get("partitions", []) for s in p["segmentIds"]] == [
        str(target.segmentID)
    ]
    owner = next(qn for qn in up[0]["queryNode"] if qn["nodeId"] == str(target.nodeIds[0]))
    assert any(str(target.segmentID) in part.get("segmentIds", []) for part in owner["partitions"]), owner

    def retrieve_calls():
        return f.retrieve_calls("querynode").get(target.nodeIds[0], 0)

    f.accepted(f.update([4], [literal("score", 88)]), 1)
    before = retrieve_calls()
    rows = f.query(candidate, "id == 4")
    after = retrieve_calls()
    f.record(
        "sealed projected write/read",
        {
            "rows": rows,
            "segment": target.segmentID,
            "owner": target.nodeIds[0],
            "before": before,
            "after": after,
            "view": up[0]["meta"]["version"],
        },
    )
    assert rows == [{"id": 4, "score": 88, "tag": "base"}] and after > before
    f.accepted(f.update([4], [literal("score", 4)]), 1)
    assert f.query(candidate, "id >= 0") == f.query(mirror, "id >= 0")


if __name__ == "__main__":
    check(Fixture(Path(sys.argv[1]).resolve()), sys.argv[2], sys.argv[3])
