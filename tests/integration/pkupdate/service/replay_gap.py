"""Retire a worker subscription with the permanent replay-gap condition."""

import argparse
import json
import os
import re
import time
from pathlib import Path
from unittest.mock import patch

from acceptance import Fixture, literal
from materialization_faults import arm, receipt
from metadata import capture
from owner_faults import new_score, read_sealed, sealed_binding
from pymilvus.exceptions import MilvusException
from pymilvus.grpc_gen import milvus_pb2
from recovery import replace


def channel_pair(f, name):
    req = milvus_pb2.DescribeCollectionRequest(collection_name=name)
    result = f.client._get_connection()._stub.DescribeCollection(req, timeout=8)
    assert result.status.code == 0, result.status
    (pchannel,) = result.physical_channel_names
    (vchannel,) = result.virtual_channel_names
    assert vchannel.rsplit("_", 1)[0] == pchannel
    return pchannel, vchannel


def same_stream_evidence(log, pchannel, sibling, before_tick, after_tick, terminal):
    end = time.monotonic() + 8
    while True:
        with log.open(errors="replace") as stream:
            lines = [line.rstrip() for line in stream if f"[pchannel={pchannel}]" in line or terminal in line]
        entries = [
            (i, line)
            for i, line in enumerate(lines)
            if "querynode transform log buffer received entry" in line and sibling in line
        ]
        first = [(i, line) for i, line in entries if f"timeTick={before_tick}]" in line]
        last = [(i, line) for i, line in entries if f"timeTick={after_tick}]" in line]
        if first and last:
            break
        if time.monotonic() >= end:
            raise AssertionError((str(log), "missing before/after same-stream entries", before_tick, after_tick))
        time.sleep(0.25)
    first_index, first_line = first[0]
    last_index, last_line = last[-1]
    terminal_index = next(i for i, line in enumerate(lines) if terminal in line)
    assert first_index < terminal_index < last_index

    def sub(line):
        return re.search(r"\[subscriptionID=(\d+)\]", line).group(1)

    assert sub(first_line) == sub(last_line)
    assert not any(
        "acquired underlying stream" in line or "underlying stream unavailable" in line
        for line in lines[first_index : last_index + 1]
    )
    return dict(subscription_id=sub(last_line), before=first_line, terminal=terminal, after=last_line)


def run(f, binary):
    root = f.site / (f.candidate + "-gap")
    root.mkdir()
    cfg = root / "arm.json"
    point = (
        f'github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary/pkTruncatedReplay=return("{cfg}")'
    )
    owner = replace(f, "streamingnode", binary, point)
    with patch.dict(os.environ, {"MILVUS_CONF_LOG_LEVEL": "debug"}):
        pid = replace(f, "querynode", binary)
    f.create(f.candidate)
    primary_pch, primary_vch = channel_pair(f, f.candidate)
    original = f.mirror
    for i in range(6):
        f.mirror = original if i == 0 else f"{original}_{i}"
        f.create(f.mirror)
        sibling_pch, sibling_vch = channel_pair(f, f.mirror)
        f.record("sibling placement", dict(name=f.mirror, channel=sibling_vch, pchannel=sibling_pch))
        if sibling_pch == primary_pch:
            break
    else:
        raise AssertionError("no co-located sibling VChannel")
    assert sibling_vch != primary_vch
    for name in (f.candidate, f.mirror):
        f.client.insert(name, [dict(id=1, score=1, tag="base", vec=[1.0, 0.0])], timeout=20)
        f.client.flush(name, timeout=60)
    assert f.query(f.candidate, "id == 1")[0]["score"] == 1
    ack = f.update([1], [literal("score", 81)])
    f.accepted(ack, 1)
    tick = int(ack["timestamp"])
    assert f.query(f.candidate, "id == 1")[0]["score"] == 81
    cid = str(f.client.describe_collection(f.candidate, timeout=10)["collection_id"])
    before = capture(f.site, f.candidate + "-gap-before", timeout=40)
    (ch,) = (x["data"]["vchannel"] for x in before if f"_{cid}v" in x.get("data", {}).get("vchannel", ""))
    assert ch == primary_vch
    sibling = sealed_binding(f, f.mirror, pid, sibling_vch)
    before_tick = new_score(f, f.mirror, 17)
    read_sealed(f, binary, sibling, before_tick, 17)
    # Rebuild only this fresh candidate subscription. Its healthy sibling keeps
    # the same QN, physical stream, Sealed segment and admitted QueryView.
    f.client.release_collection(f.candidate, timeout=35)
    gate = root / "replay"
    arm(cfg, gate, owner, ch, tick)
    try:
        f.client.load_collection(f.candidate, timeout=40, _async=True)
        seen = receipt(gate)
        assert seen["details"]["after"] < tick <= seen["details"]["through"]
        f.record("incomplete replay requested by reloaded QN view", seen)
    finally:
        (gate / "release").touch()
    ledger = [json.loads(line) for line in (f.site / "processes.jsonl").read_text().splitlines()]
    log = Path(next(x["log"] for x in ledger if x["pid"] == pid))
    end = time.monotonic() + 35
    while True:
        logs = log.read_text(errors="replace").splitlines()
        errors = [
            line
            for line in logs
            if "STREAMING_CODE_UNRECOVERABLE" in line and "start point is truncated" in line and cid in line
        ]
        retired = [line for line in logs if "QNSegmentUnrecoverable" in line and ch in line]
        if errors and retired:
            matches = [errors[0], retired[0]]
            break
        if time.monotonic() >= end:
            raise TimeoutError(f"worker did not report truncated channel in {log}")
        time.sleep(0.25)
    f.record("permanent gap worker result", matches[:3])
    after_tick = new_score(f, f.mirror, 53)
    assert after_tick > before_tick and after_tick > tick
    read_sealed(f, binary, sibling, after_tick, 53)
    f.record(
        "same logical subscription advanced past sibling terminal",
        same_stream_evidence(log, primary_pch, sibling_vch, before_tick, after_tick, retired[0]),
    )
    assert f.query(f.mirror, "id == 1") == [dict(id=1, score=53, tag="base")]
    f.client.flush(f.candidate, timeout=60)
    comp = f.client.compact(f.candidate, is_l0=True, timeout=30)
    end = time.monotonic() + 80
    while str(f.client.get_compaction_state(comp, timeout=5)) != "Completed":
        if time.monotonic() >= end:
            raise TimeoutError("dispatch")
        time.sleep(0.5)
    after = capture(f.site, f.candidate + "-gap-dispatched", timeout=40)
    tasks = [x["data"] for x in after if x.get("data", {}).get("triggerID") == str(comp)]
    assert len(tasks) == 1 and int(tasks[0]["materializedThrough"]) >= tick
    replace(f, "querynode", binary)
    for name, score in ((f.candidate, 81), (f.mirror, 53)):
        end = time.monotonic() + 75
        while True:
            try:
                value = f.query(name, "id == 1")
            except MilvusException as err:
                if (
                    not any(
                        t in str(err) for t in ("node not match", "view not ready", "no available", "not available")
                    )
                    or time.monotonic() >= end
                ):
                    raise
                time.sleep(0.5)
            else:
                assert value == [dict(id=1, score=score, tag="base")]
                break
    f.record("complete-base recovery", dict(tick=tick, compaction=comp))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("site", type=Path)
    p.add_argument("binary", type=Path)
    a = p.parse_args()
    run(Fixture(a.site.resolve()), a.binary.resolve(strict=True))
