"""Retire a worker subscription with the permanent replay-gap condition."""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from acceptance import Fixture, literal
from materialization_faults import arm, receipt
from metadata import capture
from pymilvus.exceptions import MilvusException
from recovery import replace


def run(f, binary):
    root = f.site / (f.candidate + "-gap")
    root.mkdir()
    cfg = root / "arm.json"
    point = (
        f'github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary/pkTruncatedReplay=return("{cfg}")'
    )
    owner = replace(f, "streamingnode", binary, point)
    for name in (f.candidate, f.mirror):
        f.create(name)
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
    gate = root / "replay"
    arm(cfg, gate, owner, ch, tick)
    with ThreadPoolExecutor(1) as pool:
        qn = pool.submit(replace, f, "querynode", binary)
        try:
            seen = receipt(gate)
            assert seen["details"]["after"] < tick <= seen["details"]["through"]
            f.record("incomplete replay requested by new QN", seen)
        finally:
            (gate / "release").touch()
        pid = qn.result(timeout=120)
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
    assert f.query(f.mirror, "id == 1")[0]["score"] == 1
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
    for name, score in ((f.candidate, 81), (f.mirror, 1)):
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
