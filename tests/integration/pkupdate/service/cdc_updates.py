"""Replicate one fragmented PK update, interrupt a receiving transaction and inspect both histories."""

import argparse
import json
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from acceptance import Fixture, literal
from materialization_faults import receipt
from metadata import capture
from pymilvus.exceptions import MilvusException


def effective_config(f, key):
    url = f'http://127.0.0.1:{f.config["metrics"]["mixcoord"]}/management/config/get?keys={key}'
    with urllib.request.urlopen(url, timeout=3) as response:
        (entry,) = json.load(response)["configs"]
    return entry["value"]


def run(a, b, root):
    settings = {
        key: effective_config(a, key)
        for key in ("woodpecker.maxMessageSize", "proxy.pkUpdate.maxPkCount", "proxy.pkUpdate.enabled")
    }
    try:
        replicate_updates(a, b, root)
    finally:
        for key, value in settings.items():
            a.put_config(key, value)


def replicate_updates(a, b, root):
    a.create(a.candidate)
    b.candidate = a.candidate
    rows = [dict(id=i, score=i, tag="base", vec=[float(i), 0.0]) for i in (1, 2)]
    a.client.insert(a.candidate, rows, timeout=15)

    def public(f):
        return f.query(a.candidate, "id in [1,2,404]")

    def await_rows(f, expected):
        end = time.monotonic() + 70
        while True:
            try:
                got = public(f)
                if got == expected:
                    f.record("replicated public rows", got)
                    return
            except MilvusException as err:
                got = str(err)
            if time.monotonic() >= end:
                raise AssertionError((expected, got))
            time.sleep(0.5)

    base = [dict(id=i, score=i, tag="base") for i in (1, 2)]
    for f in (a, b):
        await_rows(f, base)
        f.client.flush(f.candidate, timeout=60)
    assert "admission is disabled" in b.update([1], [literal("score", 99)])["status"]["reason"]
    a.put_switch(True)
    a.put_config("woodpecker.maxMessageSize", 262144)
    a.put_config("proxy.pkUpdate.maxPkCount", 50000)
    config_url = (
        f'http://127.0.0.1:{a.config["metrics"]["mixcoord"]}/management/config/get?keys=woodpecker.maxMessageSize'
    )
    deadline = time.monotonic() + 25
    while True:
        with urllib.request.urlopen(config_url, timeout=3) as response:
            effective = json.load(response)["configs"][0]
        if effective.get("value") == "262144":
            a.record("effective cluster WAL message limit", effective)
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("Woodpecker limit refresh")
        time.sleep(0.4)
    # Role refresh timers run independently of the coordinator's etcd poll.
    time.sleep(6)
    cid = str(b.client.describe_collection(b.candidate, timeout=10)["collection_id"])
    before = capture(b.site, b.candidate + "-cdc-base")
    (channel,) = (x["data"]["vchannel"] for x in before if f"_{cid}v" in x.get("data", {}).get("vchannel", ""))
    ch = channel.rsplit("_", 1)[0]
    source = a.config["namespace"]

    def checkpoint():
        value = b.client.get_replicate_info(source, ch, timeout=5)
        b.record("secondary checkpoint", value)
        return int(value["checkpoint"]["time_tick"])

    pre = checkpoint()
    gate = root / (a.candidate + "-txn")
    gate.mkdir()
    pid = next(
        json.loads(line)["pid"]
        for line in reversed((b.site / "processes.jsonl").read_text().splitlines())
        if json.loads(line)["role"] == "proxy"
    )
    temp = root / "body.tmp"
    temp.write_text(json.dumps(dict(PID=pid, Channel=channel, Tick=pre + 1, Directory=str(gate))))
    temp.replace(root / "body.json")
    ids = [1, 2, 404, *range(1 << 61, (1 << 61) + 40000)]
    with ThreadPoolExecutor(1) as pool:
        job = pool.submit(a.update, ids, [literal("score", 41), literal("tag", None)])
        try:
            ack = job.result(timeout=35)
            a.accepted(ack, len(ids))
            seen = receipt(gate)
            tick = int(ack["timestamp"])
            b.record("in-flight receiving update body", seen)
            assert seen["tick"] == tick and seen["details"] and seen["pid"] == pid
            assert pre <= checkpoint() < tick
        finally:
            (gate / "release").touch()
    end = time.monotonic() + 60
    while checkpoint() < tick:
        if time.monotonic() >= end:
            raise TimeoutError("replication checkpoint")
        time.sleep(0.6)
    expected = [dict(id=i, score=41, tag=None) for i in (1, 2)]
    source_blocks = None
    for f in (a, b):
        await_rows(f, expected)
        f.client.flush(f.candidate, timeout=75)
        snapshot = capture(f.site, f.candidate + "-cdc-history", timeout=60)
        mutations = [
            r
            for file in snapshot
            if file.get("layout", {}).get("CollectionID") == int(cid) and "/deltalog/" in file["referrer"]
            for r in file["rows"]
        ]
        assert len(mutations) > 1
        (local_tick,) = {r["TimeTick"] for r in mutations}
        if f is a:
            assert local_tick == tick
        ordered = sorted(mutations, key=lambda r: r["SourceOrdinal"])
        assert [r["SourceOrdinal"] for r in ordered] == list(range(len(ordered)))
        keys = [pk for row in ordered for pk in row["PrimaryKeys"]["IdField"]["IntId"]["data"]]
        assert Counter(keys) == Counter(ids)
        if source_blocks is None:
            source_blocks = [{k: v for k, v in row.items() if k != "TimeTick"} for row in ordered]
        else:
            assert [{k: v for k, v in row.items() if k != "TimeTick"} for row in ordered] == source_blocks
        f.record(
            "materialized transaction",
            dict(
                source_tick=tick,
                local_tick=local_tick,
                ordinals=[r["SourceOrdinal"] for r in ordered],
                keys=len(keys),
            ),
        )
    later = a.update([2, 404], [literal("score", 42), literal("tag", "later")])
    a.accepted(later, 2)
    assert int(later["timestamp"]) > tick
    expected[1].update(score=42, tag="later")
    for f in (a, b):
        await_rows(f, expected)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("primary", type=Path)
    p.add_argument("root", type=Path)
    a = p.parse_args()
    run(Fixture(a.primary.resolve()), Fixture(a.root.resolve() / "secondary"), a.root.resolve())
