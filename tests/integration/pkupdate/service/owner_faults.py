"""Exercise native ApplyUpdates cancellation on an owned worker and generation."""

import argparse
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
from acceptance import Fixture, literal
from history import segments
from materialization_faults import receipt
from metadata import capture
from pymilvus.exceptions import MilvusException
from recovery import replace


def read_owner(f, binary, role, term, meta, tick):
    endpoint = f.config["conf"][role + ".port"]
    proc = subprocess.run(
        [
            str(binary.with_name("ownerread")),
            "-endpoint",
            f"127.0.0.1:{endpoint}",
            "-term",
            str(term),
            "-tick",
            str(tick),
        ],
        input=json.dumps(meta),
        capture_output=True,
        text=True,
        timeout=13,
    )
    result = dict(
        tick=tick,
        returncode=proc.returncode,
        meta=meta,
        role=role,
        stdout=proc.stdout[-5000:],
        stderr=proc.stderr[-1500:],
    )
    f.record("specific worker View read", result)
    return result


def run(f, binary, role):
    root = f.site / (f.candidate + "-native")
    root.mkdir()
    cfg = root / "arm.json"
    point = f'github.com/milvus-io/milvus/internal/util/segcore/pkNativeApply=return("{cfg}")'
    sn = role == "streamingnode"
    channel_prefix = "internal/streamingnode/server/wal/vchannel/" if sn else "internal/querynodev2/transformlogbuffer/"
    frontier_point = channel_prefix + ("growingruntime/pkGrowingFailureFrontier" if sn else "pkSealedFailureFrontier")
    stale_point = channel_prefix + ("queryresource/pkOldGrowingCallback" if sn else "pkOldSealedCallback")
    for key, path in (("frontier", frontier_point), ("callback", stale_point)):
        point += f';github.com/milvus-io/milvus/{path}=return("{root}/{key}.json")'
    owner = replace(f, role, binary, point)
    f.create(f.candidate)
    f.create(f.mirror)
    for name in (f.candidate, f.mirror):
        f.client.insert(name, [dict(id=1, score=1, tag="base", vec=[1.0, 0.0])], timeout=20)

    def rows(name):
        return f.query(name, "id == 1")

    base = [dict(id=1, score=1, tag="base")]
    target = [dict(id=1, score=91, tag="base")]
    assert rows(f.candidate) == base and rows(f.mirror) == base
    cid = str(f.client.describe_collection(f.candidate, timeout=10)["collection_id"])
    f.put_switch(True)
    for _ in range(60):
        if "admission is disabled" not in f.update([1], [literal("id", 0)])["status"].get("reason", ""):
            break
        time.sleep(0.5)
    if role == "querynode":
        f.client.flush(f.candidate, timeout=60)
    deadline = time.monotonic() + 90
    while True:
        snap = capture(f.site, f.candidate + f"-native-base-{time.time_ns()}", timeout=25)
        segs = segments(snap, cid)
        ids = [
            sid
            for sid, s in segs.items()
            if (
                s.get("state") == "Growing"
                if role == "streamingnode"
                else s.get("state") == "Flushed" and s.get("isSorted")
            )
        ]
        if role == "querynode" and len(ids) == 1:
            inventory = f.client._get_connection().get_query_segment_info(f.candidate, timeout=10)
            serving = any(
                ids[0] in partition.get("segmentIds", [])
                for item in snap
                if item.get("data", {}).get("meta", {}).get("state") == "QueryViewStateUp"
                for worker in item["data"].get("queryNode", [])
                for partition in worker.get("partitions", [])
            )
            if not serving or not any(str(item.segmentID) == ids[0] and item.is_sorted for item in inventory):
                ids = []
            else:
                before = sum(f.retrieve_calls("querynode").values())
                assert rows(f.candidate) == base
                if sum(f.retrieve_calls("querynode").values()) <= before:
                    ids = []
        if len(ids) == 1:
            sid = int(ids[0])
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(str(segs))
        time.sleep(1)
    channel = segs[str(sid)]["insertChannel"]
    pchannel = channel.rsplit("_", 1)[0]
    (pinfo,) = (x["data"] for x in snap if x.get("key") == "streamingcoord-meta/pchannel/" + pchannel)
    metas = [
        x["data"]["meta"]
        for x in snap
        if "/qv/" in x.get("key", "")
        and x["data"]["meta"]["vchannel"] == channel
        and x["data"]["meta"]["state"] == "QueryViewStateUp"
        and (
            sn
            or any(
                str(sid) in partition.get("segmentIds", [])
                for worker in x["data"].get("queryNode", [])
                for partition in worker.get("partitions", [])
            )
        )
    ]

    def version(meta):
        v = meta["version"]
        return tuple(int(v.get("dataVersion", {}).get(k, 0)) for k in ("streamingVersion", "compactVersion")) + (
            int(v.get("queryVersion", 0)),
        )

    initial_meta = meta = max(metas, key=version)

    def worker_read(tick):
        return read_owner(f, binary, role, pinfo["channel"]["term"], meta, tick)

    first = f.update([1], [literal("score", 1)])
    f.accepted(first, 1)
    baseline = worker_read(int(first["timestamp"]))
    assert (
        baseline["returncode"] == 0 and '"legacyResults"' in baseline["stdout"] and '"code":' not in baseline["stdout"]
    )
    gate = root / "apply"
    cfg.write_text(json.dumps(dict(PID=owner, Channel="", Tick=1, Segment=sid, Directory=str(gate))))
    gate.mkdir()
    f.record(
        "physical owner before write", dict(pid=owner, role=role, segment=sid, channel=segs[str(sid)]["insertChannel"])
    )
    with ThreadPoolExecutor(3) as pool:
        submitted = pool.submit(f.update, [1], [literal("score", 91)])
        try:
            physical = receipt(gate)
            ack = submitted.result(timeout=15)
            f.accepted(ack, 1)
            assert physical["tick"] == int(ack["timestamp"]) and physical["segment"] == sid
            f.record("paused native apply", physical)
            for key in ("frontier", "callback"):
                gate_dir = root / key
                gate_dir.mkdir()
                if key == "frontier":
                    (gate_dir / "release").touch()
                (root / (key + ".json")).write_text(
                    json.dumps(
                        dict(
                            PID=owner,
                            Channel=channel,
                            Tick=physical["tick"],
                            Segment=0 if sn else sid,
                            Directory=str(gate_dir),
                        )
                    )
                )
            direct = pool.submit(worker_read, physical["tick"])
            waiting = pool.submit(rows, f.candidate)
            assert rows(f.mirror) == base
            time.sleep(0.35)
            if waiting.done():
                try:
                    during = waiting.result()
                except (MilvusException, grpc.RpcError) as err:
                    f.record("public read while native apply was blocked", str(err))
                else:
                    f.record("public read while native apply was blocked", during)
                    assert during == target, (physical, during)
            else:
                f.record("public read waiting for update", physical["tick"])
        finally:
            (gate / "release").touch()
        failed = direct.result(timeout=14)
        assert (
            "DEADLINE_EXCEEDED" not in failed["stderr"].upper() and "deadline exceeded" not in failed["stderr"].lower()
        )
        assert any(
            token in (failed["stderr"] + failed["stdout"]).lower()
            for token in (
                "invalidated",
                "view not ready",
                "view not found",
                "cancelled for segment",
                "view is not ready",
            )
        ), failed
        try:
            visible = waiting.result(timeout=40)
        except (MilvusException, grpc.RpcError) as e:
            f.record("public read after native apply failure", str(e))
        else:
            f.record("public read after native apply failure", visible)
            assert visible == target
    frontier = receipt(root / "frontier")
    f.record("failed owner applied frontiers", frontier)
    assert frontier["tick"] == physical["tick"] and max(frontier["details"].values()) < physical["tick"]
    old_cb = receipt(root / "callback")
    f.record("delayed old failure callback", old_cb)
    assert old_cb["tick"] == physical["tick"]
    try:
        assert rows(f.mirror) == base
        f.client.create_partition(f.candidate, "recovery", timeout=20)
        # A flushed handoff also supplies a complete physical source to the Sealed replacement.
        if not sn:
            f.client.flush(f.candidate, timeout=40)
        end = time.monotonic() + 48
        while True:
            try:
                current = f.client.query(
                    f.candidate,
                    filter="id == 1",
                    output_fields=["id", "score", "tag"],
                    consistency_level="Strong",
                    timeout=4,
                )
            except (grpc.RpcError, MilvusException):
                if time.monotonic() >= end:
                    raise
                time.sleep(0.2)
            else:
                assert current == target
                break
        f.record("same-process replacement before old callback", current)
        latest = f.update([404], [literal("score", 92)])
        f.accepted(latest, 1)
        until = time.monotonic() + 30
        while True:
            now = capture(f.site, f.candidate + f"-replacement-{time.time_ns()}", timeout=25)
            candidates = [
                x["data"]["meta"]
                for x in now
                if x.get("data", {}).get("meta", {}).get("vchannel") == channel
                and x["data"]["meta"]["state"] == "QueryViewStateUp"
                and version(x["data"]["meta"]) > version(initial_meta)
            ]
            if candidates:
                meta = max(candidates, key=version)
                ok = worker_read(int(latest["timestamp"]))
                if ok["returncode"] == 0:
                    fields = json.loads(ok["stdout"])["legacyResults"]["fieldsData"]
                    values = {x["fieldId"]: x["scalars"]["longData"]["data"] for x in fields}
                    assert values == {"100": ["1"], "101": ["91"]}, values
                    break
            if time.monotonic() >= until:
                raise TimeoutError("addressable replacement owner")
            time.sleep(0.4)
    finally:
        (root / "callback" / "release").touch()
    end = time.monotonic() + 8
    while not (root / "callback" / "completed").exists():
        if time.monotonic() >= end:
            raise TimeoutError("old callback delivery")
        time.sleep(0.05)
    assert rows(f.candidate) == target and rows(f.mirror) == base
    assert worker_read(int(latest["timestamp"]))["returncode"] == 0
    f.record("healthy replacement and sibling after delayed callback", dict(owner=owner, target=target, sibling=base))
    replace(f, role, binary, point)
    deadline = time.monotonic() + 90
    while True:
        try:
            got = rows(f.candidate)
        except (MilvusException, grpc.RpcError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)
        else:
            assert got == target
            f.record("new worker recovered committed update", got)
            break
    assert rows(f.mirror) == base


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("site", type=Path)
    p.add_argument("binary", type=Path)
    p.add_argument("role", choices=("querynode", "streamingnode"))
    a = p.parse_args()
    run(Fixture(a.site.resolve()), a.binary.resolve(strict=True), a.role)
