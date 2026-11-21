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
        term=term,
        endpoint=endpoint,
        stdout=proc.stdout[-5000:],
        stderr=proc.stderr[-1500:],
    )
    f.record("specific worker View read", result)
    return result


def check_row(response, score, owner=None):
    assert response["returncode"] == 0, response
    result = json.loads(response["stdout"])["legacyResults"]
    assert not result.get("status", {}).get("code"), result
    assert result["ids"]["intId"]["data"] == ["1"], result
    values = {x["fieldId"]: x["scalars"]["longData"]["data"] for x in result["fieldsData"]}
    assert values == {"100": ["1"], "101": [str(score)]}, values
    node = int(result["base"]["sourceID"])
    assert owner is None or node == owner, (node, owner)
    return node


def new_score(f, name, score):
    ack = f.update([1], [literal("score", score)], collection=name)
    f.accepted(ack, 1)
    return int(ack["timestamp"])


def read_sealed(f, binary, binding, tick, score):
    node = binding["node"]
    before = f.retrieve_calls("querynode").get(node, 0)
    result = read_owner(f, binary, "querynode", binding["term"], binding["meta"], tick)
    check_row(result, score, node)
    after = f.retrieve_calls("querynode").get(node, 0)
    assert after > before, (node, before, after)
    f.record(
        "sealed sibling execution",
        dict(
            pid=binding["pid"],
            node=node,
            segment=binding["segment"],
            channel=binding["meta"]["vchannel"],
            term=binding["term"],
            version=binding["meta"]["version"],
            tick=tick,
            score=score,
            retrieves_before=before,
            retrieves_after=after,
        ),
    )


def version(meta):
    v = meta["version"]
    return tuple(int(v.get("dataVersion", {}).get(key, 0)) for key in ("streamingVersion", "compactVersion")) + (
        int(v.get("queryVersion", 0)),
    )


def sealed_binding(f, name, pid, channel=None):
    cid = str(f.client.describe_collection(name, timeout=10)["collection_id"])
    deadline = time.monotonic() + 65
    while True:
        snap = capture(f.site, name + f"-sealed-sibling-{time.time_ns()}", timeout=25)
        sealed = {
            sid for sid, seg in segments(snap, cid).items() if seg.get("state") == "Flushed" and seg.get("isSorted")
        }
        inventory = {
            str(segment.segmentID): set(segment.nodeIds)
            for segment in f.client._get_connection().get_query_segment_info(name, timeout=5)
            if segment.is_sorted
        }
        views = [
            r["data"]
            for r in snap
            if r.get("data", {}).get("meta", {}).get("collectionId") == cid
            and r["data"]["meta"]["state"] == "QueryViewStateUp"
            and (channel is None or r["data"]["meta"]["vchannel"] == channel)
        ]
        for view in sorted(views, key=lambda v: version(v["meta"]), reverse=True):
            meta = view["meta"]
            for worker in view.get("queryNode", []):
                node = int(worker["nodeId"])
                assigned = {sid for part in worker.get("partitions", []) for sid in part.get("segmentIds", [])}
                for sid in sealed.intersection(assigned):
                    if node in inventory.get(sid, set()):
                        pch = meta["vchannel"].rsplit("_", 1)[0]
                        term = next(
                            x["data"]["channel"]["term"]
                            for x in snap
                            if x.get("key") == "streamingcoord-meta/pchannel/" + pch
                        )
                        return dict(node=node, pid=pid, segment=int(sid), meta=meta, term=term)
        if time.monotonic() >= deadline:
            raise TimeoutError("healthy sealed sibling assignment: " + name)
        time.sleep(0.35)


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
    if sn:
        deadline = time.monotonic() + 90
        while True:
            snap = capture(f.site, f.candidate + f"-native-base-{time.time_ns()}", timeout=25)
            growing = [seg for seg in segments(snap, cid).values() if seg.get("state") == "Growing"]
            if len(growing) == 1:
                sid = int(growing[0]["ID"])
                channel = growing[0]["insertChannel"]
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("growing candidate segment")
            time.sleep(0.5)
        pchannel = channel.rsplit("_", 1)[0]
        term = next(
            x["data"]["channel"]["term"] for x in snap if x.get("key") == "streamingcoord-meta/pchannel/" + pchannel
        )
        meta = max(
            (
                x["data"]["meta"]
                for x in snap
                if x.get("data", {}).get("meta", {}).get("vchannel") == channel
                and x["data"]["meta"]["state"] == "QueryViewStateUp"
            ),
            key=version,
        )
    else:
        for name in (f.candidate, f.mirror):
            f.client.flush(name, timeout=60)
        candidate = sealed_binding(f, f.candidate, owner)
        sid, meta, term = candidate["segment"], candidate["meta"], candidate["term"]
        channel = meta["vchannel"]
    initial_meta = meta

    def worker_read(tick):
        return read_owner(f, binary, role, term, meta, tick)

    first = f.update([1], [literal("score", 1)])
    f.accepted(first, 1)
    baseline = worker_read(int(first["timestamp"]))
    node = check_row(baseline, 1, None if sn else candidate["node"])
    sibling_score = 1
    if not sn:
        sibling = sealed_binding(f, f.mirror, owner)
        assert sibling["node"] == node and sibling["segment"] != sid
        sibling_tick = new_score(f, f.mirror, sibling_score)
        read_sealed(f, binary, sibling, sibling_tick, sibling_score)
    gate = root / "apply"
    cfg.write_text(json.dumps(dict(PID=owner, Channel="", Tick=1, Segment=sid, Directory=str(gate))))
    gate.mkdir()
    f.record(
        "physical owner before write",
        dict(pid=owner, role=role, node=node, segment=sid, channel=channel, term=term, version=meta["version"]),
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
            if not sn:
                read_sealed(f, binary, sibling, sibling_tick, sibling_score)
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
        if not sn:
            sibling_score = 72
            sibling_tick = new_score(f, f.mirror, sibling_score)
            read_sealed(f, binary, sibling, sibling_tick, sibling_score)
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
                    check_row(ok, 91, node)
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
    if not sn:
        sibling_score = 73
        sibling_tick = new_score(f, f.mirror, sibling_score)
        read_sealed(f, binary, sibling, sibling_tick, sibling_score)
    sibling_rows = [dict(id=1, score=sibling_score, tag="base")]
    assert rows(f.candidate) == target and rows(f.mirror) == sibling_rows
    check_row(worker_read(int(latest["timestamp"])), 91, node)
    f.record(
        "healthy replacement and sibling after delayed callback", dict(owner=owner, target=target, sibling=sibling_rows)
    )
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
    assert rows(f.mirror) == sibling_rows


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("site", type=Path)
    p.add_argument("binary", type=Path)
    p.add_argument("role", choices=("querynode", "streamingnode"))
    a = p.parse_args()
    run(Fixture(a.site.resolve()), a.binary.resolve(strict=True), a.role)
