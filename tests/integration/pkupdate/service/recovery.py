"""Exercise partition scope, an acknowledged WAL crash and persisted worker replacement."""

import argparse
import json
import os
import signal
import subprocess
import time
import urllib.request
from pathlib import Path

from acceptance import Fixture, literal

# Use a distinct module name from Python's standard library site.
from cluster_site import launch
from metadata import capture
from pymilvus.exceptions import MilvusException


def replace(f, role):
    rows = [json.loads(x) for x in (f.site / "processes.jsonl").read_text().splitlines()]
    proc = next(x for x in reversed(rows) if x["role"] == role)
    cmd = subprocess.check_output(["ps", "-p", str(proc["pid"]), "-o", "command="], text=True).strip()
    alias = f.config["namespace"] + "-" + role
    assert cmd == f'{f.config["binary"]} run {role} -alias {alias}', cmd
    os.kill(proc["pid"], signal.SIGKILL)
    f.record("worker killed", [role, proc["pid"], alias])
    for _ in range(100):
        if subprocess.run(["ps", "-p", str(proc["pid"])], stdout=subprocess.DEVNULL).returncode:
            break
        time.sleep(0.1)
    launch(f.site, role)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{f.config["metrics"][role]}/healthz', timeout=2) as resp:
                if resp.read() == b"OK":
                    return
        except OSError:
            pass
        time.sleep(0.5)
    raise TimeoutError(role + " failed to recover")


def run(f, pair):
    c = f.client
    f.create(f.candidate, shards=2)
    for i, partition in enumerate(("left", "right")):
        c.create_partition(f.candidate, partition)
        c.load_partitions(f.candidate, [partition], timeout=90)
        c.insert(
            f.candidate,
            [{"id": i * 100 + n, "score": n, "tag": partition, "vec": [float(n), 0.0]} for n in range(1, 17)],
            partition_name=partition,
        )
    c.flush(f.candidate, timeout=120)
    c.delete(f.candidate, ids=[16, 116])
    c.flush(f.candidate, timeout=120)
    cid = str(c.describe_collection(f.candidate)["collection_id"])
    tag = f.candidate

    def snap(label):
        return capture(f.site, tag + "-" + label)

    before = snap("legacy")

    def selected(rows):
        return [
            x
            for x in rows
            if x.get("layout", {}).get("CollectionID") == int(cid) or (x.get("file") and "/" + cid + "/" in x["file"])
        ]

    assert any(int(row["Operation"]) == 1 for x in selected(before) for row in x["rows"])
    resp = f.update([1, 2, 101, 102], [literal("score", 40)], partition="left")
    f.accepted(resp, 4)

    def read_scope(left, right):
        for offset, part, scores in ((0, "left", left), (100, "right", right)):
            expected = [
                (offset + i, scores[i - 1] if i <= 2 else i, "shared" if i == 2 and scores[1] == 99 else part)
                for i in range(1, 16)
            ]
            got = c.query(
                f.candidate,
                filter="id >= 0",
                output_fields=["id", "score", "tag"],
                partition_names=[part],
                consistency_level="Strong",
                timeout=45,
            )
            vals = sorted((x["id"], x["score"], x["tag"]) for x in got)
            assert vals == expected, (part, vals, expected)

    def verify(left, right):
        deadline = time.monotonic() + 150
        while True:
            try:
                read_scope(left, right)
                return
            except MilvusException as error:
                if time.monotonic() >= deadline:
                    raise
                f.record("read while worker ownership converges", str(error))
                time.sleep(2)

    verify([40, 40], [1, 2])
    resp = f.update([2, 102, 999], [literal("score", 99), literal("tag", "shared")])
    f.accepted(resp, 3)
    ack = int(resp["timestamp"])
    replace(f, "streamingnode")
    verify([40, 99], [1, 99])
    assert f.query(pair, "id == 3")[0]["score"] == 10
    restarted = snap("sn-restarted")

    def owners(rows):
        return {
            x["data"]["channel"]["name"]: (x["data"]["node"]["serverId"], int(x["data"]["channel"]["term"]))
            for x in rows
            if x.get("key", "").startswith("streamingcoord-meta/pchannel/")
        }

    old, new = owners(before), owners(restarted)
    assert len(old) == 2 and old.keys() == new.keys()
    assert all(new[k][0] != v[0] and new[k][1] > v[1] for k, v in old.items())
    m_before_flush = {
        x["data"]["vchannel"]: int(x["data"].get("transformMaterializedTimeTick", 0))
        for x in restarted
        if cid in x.get("data", {}).get("vchannel", "")
    }
    assert len(m_before_flush) == 2 and max(m_before_flush.values()) < ack
    assert all(r["Operation"] == 1 for x in selected(restarted) for r in x["rows"])
    c.flush(f.candidate, timeout=120)
    persist = snap("persist")
    files = selected(persist)
    ticks = {int(row["TimeTick"]) for x in files for row in x["rows"] if row["Operation"] == 2}
    assert ack in ticks, (ack, ticks)
    replace(f, "querynode")
    verify([40, 99], [1, 99])
    assert f.query(pair, "id == 3")[0]["score"] == 10
    compaction_id = c.compact(f.candidate, is_l0=True, timeout=60)
    f.record("L0 compaction", compaction_id)
    for _ in range(30):
        state = str(c.get_compaction_state(compaction_id, timeout=10))
        if "Completed" in state:
            break
        time.sleep(2)
    f.record("L0 state", state)
    assert state == "Completed", state
    dispatched = snap("dispatch")
    tasks = [x["data"] for x in dispatched if x.get("data", {}).get("triggerID") == str(compaction_id)]
    assert len(tasks) == 2 and all(t["l0Targets"]["published"] for t in tasks)
    target_cursors = {s: int(t["materializedThrough"]) for t in tasks for s in t["l0Targets"]["segmentIds"]}
    targets = [x["data"] for x in dispatched if x.get("data", {}).get("ID") in target_cursors]
    assert len(targets) == len(target_cursors) == 4
    assert all(int(s["transformStartAfterTimetick"]) == target_cursors[s["ID"]] for s in targets)
    merged = [r for x in selected(dispatched) if x["referrer"].startswith("datacoord-meta/s/") for r in x["rows"]]
    updated = {
        pk: sorted(a["field_id"] for a in r["Assignments"])
        for r in merged
        if r["Operation"] == 2
        for pk in r["PrimaryKeys"]["IdField"]["IntId"]["data"]
    }
    assert updated == {1: [101], 2: [101, 102], 102: [101, 102]}, updated
    assert all(r["SourceOrdinal"] == 0 for r in merged)
    compact = c.compact(f.candidate, timeout=60)
    f.record("base compaction", compact)
    for _ in range(60):
        state = c.get_compaction_state(compact, timeout=15)
        if state == "Completed":
            break
        time.sleep(1)
    assert state == "Completed", state
    verify([40, 99], [1, 99])
    final = snap("compacted")
    successors = [
        x["data"]
        for x in final
        if x.get("data", {}).get("state") == "Flushed"
        and x["data"].get("collectionID") == cid
        and x["data"].get("level") == "L1"
    ]
    assert len(successors) == 4
    assert {s for seg in successors for s in seg["compactionFrom"]} == target_cursors.keys()
    assert all(
        s["manifestPath"]
        and all(int(s["transformStartAfterTimetick"]) >= target_cursors[p] for p in s["compactionFrom"])
        for s in successors
    )
    replace(f, "querynode")
    verify([40, 99], [1, 99])
    survivors = f.query(f.candidate, 'score == 99 && tag == "shared"')
    assert [r["id"] for r in survivors] == [2, 102], survivors
    f.record("reloaded scoped predicate", survivors)
    f.record("scope/recovery complete", ack)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("site", type=Path)
    parser.add_argument("pair")
    args = parser.parse_args()
    run(Fixture(args.site.resolve()), args.pair)
