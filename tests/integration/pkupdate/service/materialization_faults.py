"""Registration retry and durable VChannel cursor through a retained service."""

import argparse
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from acceptance import Fixture, literal
from history import events
from metadata import capture
from recovery import replace

REG = "internal/flushcommon/syncmgr/pkL0Registration"
PERSIST = "internal/streamingnode/server/wal/recovery/pkVChannelPersistence"


def arm(path, directory, owner, channel, tick):
    directory.mkdir()
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(dict(PID=owner, Channel=channel, Tick=tick, Directory=str(directory))))
    temp.replace(path)


def receipt(directory):
    target = directory / "receipt.json"
    deadline = time.monotonic() + 60
    while not target.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(str(target))
        time.sleep(0.1)
    return json.loads(target.read_text())


def run(f, binary):
    root = f.site / (f.candidate + "-faults")
    root.mkdir()
    reg, persist = root / "registration.json", root / "persistence.json"
    points = ";".join(
        f'github.com/milvus-io/milvus/{point}=return("{cfg}")' for point, cfg in ((REG, reg), (PERSIST, persist))
    )
    owner = replace(f, "streamingnode", binary, points)
    f.create(f.candidate)
    c = f.client
    c.insert(f.candidate, [dict(id=1, score=1, tag="base", vec=[1.0, 0.0])], timeout=30)
    c.flush(f.candidate, timeout=75)
    cid = str(c.describe_collection(f.candidate, timeout=5)["collection_id"])

    def snap(label):
        return capture(f.site, f.candidate + "-" + label, timeout=30)

    base = snap("base")
    (channel,) = (x["data"]["vchannel"] for x in base if cid in x.get("data", {}).get("vchannel", ""))

    def durable(rows):
        (value,) = (
            int(x["data"].get("transformMaterializedTimeTick", 0))
            for x in rows
            if x.get("data", {}).get("vchannel") == channel and "/vchannel/" in x.get("key", "")
        )
        return value

    def files(rows, ack):
        return [
            (x["file"], x["referrer"], list(events(x)))
            for x in rows
            if x.get("layout", {}).get("VChannel") == channel
            and "/deltalog/" in x["referrer"]
            and any(row["TimeTick"] == ack for row in x["rows"])
        ]

    def read(score):
        assert f.query(f.candidate, f"score == {score}") == [dict(id=1, score=score, tag="base")]

    f.put_switch(True)
    for _ in range(60):
        probe = f.update([1], [literal("id", 0)])
        if "admission is disabled" not in probe["status"].get("reason", ""):
            break
        time.sleep(0.5)
    for i, cfg in enumerate((reg, persist), 1):
        gate = root / f"case-{i}"
        resp = f.update([1, 404], [literal("score", i + 10)])
        f.accepted(resp, 2)
        tick = int(resp["timestamp"])
        arm(cfg, gate, owner, channel, tick)
        with ThreadPoolExecutor(1) as pool:
            flush = pool.submit(c.flush, f.candidate, timeout=120)
            try:
                r = receipt(gate)
                assert r["channel"] == channel and r["tick"] >= tick and r["pid"] == owner
                f.record("blocked physical operation", r)
                blocked = snap(f"blocked-{i}")
                assert durable(blocked) < tick
                saved = files(blocked, tick)
                if cfg == reg:
                    assert r["tick"] == tick and r["details"] and not saved
                    pending_path = r["details"][0]["binlogs"][0]["log_path"]
                    (pending,) = capture(
                        f.site,
                        f.candidate + "-unregistered-object",
                        timeout=15,
                        inspector=binary.with_name("inspect"),
                        object_path=pending_path,
                    )
                    assert pending["layout"]["VChannel"] == channel and pending["layout"]["PartitionID"] == -1
                    assert pending["rows"][0]["TimeTick"] == tick and pending["rows"][0]["SourceOrdinal"] == 0
                    f.record("decoded object before registration", pending)
                else:
                    assert len(saved) == 1
                    f.put_switch(False)
                    for _ in range(45):
                        probe = f.update([1], [literal("id", 0)])
                        if "admission is disabled" in probe["status"].get("reason", ""):
                            break
                        time.sleep(0.5)
                    assert "admission is disabled" in probe["status"]["reason"]
                    comp = c.compact(f.candidate, is_l0=True, timeout=20)
                    during = snap("compaction-while-blocked")
                    tasks = [x["data"] for x in during if x.get("data", {}).get("triggerID") == str(comp)]
                    assert len(tasks) == 1 and tasks[0]["channel"] == channel
                    assert int(tasks[0]["materializedThrough"]) == durable(blocked)
                    assert all(row[1].split("/")[4] not in tasks[0]["inputSegments"] for row in saved)
                    pchannel = r["details"]
                    (term,) = (
                        x["data"]["channel"]["term"]
                        for x in during
                        if x.get("key", "") == "streamingcoord-meta/pchannel/" + pchannel
                    )
                    proof = subprocess.run(
                        [
                            str(binary.with_name("transform")),
                            "-endpoint",
                            f'127.0.0.1:{f.config["conf"]["streamingnode.port"]}',
                            "-pchannel",
                            pchannel,
                            "-term",
                            str(term),
                            "-vchannel",
                            channel,
                            "-after",
                            str(durable(blocked)),
                            "-through",
                            str(tick),
                        ],
                        timeout=15,
                        text=True,
                        capture_output=True,
                        check=True,
                    )
                    responses = [json.loads(line) for line in proof.stdout.splitlines() if line.startswith("{")]
                    f.record("bounded Summary replay during persistence lag", responses)
                    entries = [entry for res in responses for entry in res.get("messageBatch", {}).get("entries", [])]
                    assert len(entries) == 1 and int(entries[0]["timeTick"]) == tick
                    assert entries[0]["mutations"]["blocks"][0]["update"]
                    assert int(responses[-1]["syncUp"]["timeTick"]) == tick
                    f.record("dispatch while blocked", dict(trigger=comp, tasks=tasks))
                read(i + 10)
            finally:
                (gate / "release").touch()
            flush.result(timeout=120)
        end = time.monotonic() + 40
        while True:
            after = snap(f"published-{i}-{time.time_ns()}")
            if durable(after) >= tick:
                break
            if time.monotonic() >= end:
                raise TimeoutError("materialized tick not published")
            time.sleep(0.5)
        rows = files(after, tick)
        assert len(rows) == 1
        fields = ((101, (("LongData", i + 10),)),)
        assert rows[0][2] == [(pk, (tick, 0, 2, fields)) for pk in (1, 404)]
        f.record("published typed row", rows)
        read(i + 10)
    replace(f, "streamingnode")
    read(12)
    assert not f.query(f.candidate, "id == 404")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("site", type=Path)
    p.add_argument("binary", type=Path)
    args = p.parse_args()
    run(Fixture(args.site.resolve()), args.binary.resolve(strict=True))
