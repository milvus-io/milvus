"""Hold an admitted physical read while the collection publishes a newer DataView."""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import grpc
from acceptance import Fixture, literal
from materialization_faults import receipt
from metadata import capture
from owner_faults import read_owner
from pymilvus.client.prepare import Prepare
from pymilvus.exceptions import MilvusException
from recovery import replace


def arm(config, gate, pid, channel, segment, tick, release=False, instance=""):
    gate.mkdir()
    if release:
        (gate / "release").touch()
    tmp = config.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(dict(PID=pid, Channel=channel, Segment=segment, Tick=tick, Instance=instance, Directory=str(gate)))
    )
    tmp.replace(config)


class IndependentBase(Exception):
    """The replacement is valid, but it cannot certify GC of the old physical store."""


@contextmanager
def release_on_error(root):
    try:
        yield
    except BaseException:
        for claim in root.glob("*/claim"):
            (claim.parent / "release").touch()
        raise


def run(f, binary):
    root = f.site / (f.candidate + "-gc")
    root.mkdir()
    ps = {
        "read": ("internal/querynodev2/segments", "pkSealedRead"),
        "output": ("internal/querynodev2/segments", "pkSealedOutput"),
        "reserve": ("internal/querynodev2/qnview", "pkReadReservation"),
        "pending": ("internal/querynodev2/qnview", "pkPendingReadBinding"),
        "gc": ("internal/util/segcore", "pkNativeGC"),
    }
    points = ";".join(
        f'github.com/milvus-io/milvus/{pkg}/{p}=return("{root}/{key}.json")' for key, (pkg, p) in ps.items()
    )
    c = f.client
    for n in (f.candidate, f.mirror):
        f.create(n, index_score=False)
        c.insert(n, [dict(id=1, score=1, tag="base", vec=[1.0, 0.0])], timeout=20)
        c.flush(n, timeout=60)
    # A separate sorted segment makes the bounded filter use two-phase projection.
    c.insert(f.candidate, [dict(id=2, score=2, tag="other", vec=[2.0, 0.0])], timeout=15)
    c.flush(f.candidate, timeout=60)
    cid = str(c.describe_collection(f.candidate, timeout=10)["collection_id"])
    deadline = time.monotonic() + 80
    while True:
        segs = c._get_connection().get_query_segment_info(f.candidate, timeout=5)
        if len(segs) == 2 and all(s.is_sorted for s in segs):
            break
        if time.monotonic() > deadline:
            raise TimeoutError("sorted base")
        time.sleep(0.5)
    sid = min(s.segmentID for s in segs)
    start = capture(f.site, f.candidate + "-gc-before", timeout=35)
    (ch,) = (r["data"]["vchannel"] for r in start if f"_{cid}v" in r.get("data", {}).get("vchannel", ""))

    pid = replace(f, "querynode", binary, points)
    # Load the settled pair of sorted segment manifests into a single new owner.
    end = time.monotonic() + 75
    while True:
        try:
            baseline = f.query(f.candidate, "id in [1,2]")
        except (grpc.RpcError, MilvusException):
            if time.monotonic() >= end:
                raise
            time.sleep(0.5)
        else:
            assert [r["id"] for r in baseline] == [1, 2]
            break

    def change(score):
        result = f.update([1], [literal("score", score)])
        f.accepted(result, 1)
        return int(result["timestamp"])

    change(10)
    old = change(11)

    def fixed():
        return c.query(
            f.candidate,
            filter="score == 11",
            output_fields=["id", "score", "tag"],
            limit=1,
            iterator="True",
            guarantee_timestamp=old,
            consistency_level="Strong",
            timeout=90,
        )

    assert fixed() == [dict(id=1, score=11, tag="base")]
    for key in ("read", "output"):
        arm(root / (key + ".json"), root / key, pid, ch, sid, old)
    arm(root / "reserve.json", root / "old-reservation", pid, ch, sid, old, True)
    with ThreadPoolExecutor(1) as pool, release_on_error(root):
        query = pool.submit(fixed)
        try:
            parked = receipt(root / "read", timeout=15)
            f.record("admitted historical physical read", parked)
            old_reservation = receipt(root / "old-reservation")
            f.record("old binding and reservation", old_reservation)
            snap = capture(f.site, f.candidate + "-admitted", timeout=20)

            def matches_old(meta):
                if meta.get("vchannel") != ch:
                    return False
                v = meta["version"]
                dv = v["dataVersion"]
                key = f'{dv.get("streamingVersion",0)}/{dv.get("compactVersion",0)}/{v.get("queryVersion",0)}'
                return old_reservation["details"]["view"].endswith("-" + key)

            (old_meta,) = (r["data"]["meta"] for r in snap if matches_old(r.get("data", {}).get("meta", {})))
            (term,) = (
                r["data"]["channel"]["term"]
                for r in snap
                if r.get("key") == "streamingcoord-meta/pchannel/" + ch.rsplit("_", 1)[0]
            )
            assert parked["tick"] == old
            # A second admitted RPC is cancelled only after deferred native output is reached.
            req = Prepare.query_request(
                f.candidate,
                "score == 11",
                ["id", "score", "tag"],
                [],
                use_default_consistency=False,
                limit=1,
                iterator="True",
                guarantee_timestamp=old,
                consistency_level=0,
            )
            cancel_read = c._get_connection()._stub.Query.future(req, timeout=85)
            witness = receipt(root / "output", timeout=12)
            assert witness["details"]["read"] == parked["details"]["read"] and witness["tick"] == old
            f.record("cancellable deferred historical output", witness)
            arm(root / "output.json", root / "success-output", pid, ch, sid, old)
            for score in (12, 13, 14):
                latest = change(score)
            version = old_reservation["details"]["view"].rsplit("-", 1)[-1]
            next_stream = int(version.split("/")[0]) + 1
            # Leave the immediate handoff published while the sorted successor is prepared.
            arm(root / "pending.json", root / "pending", pid, ch, 0, latest, instance=f"{next_stream}/3")
            c.insert(f.candidate, [dict(id=3, score=3, tag="new", vec=[3.0, 0.0])], timeout=20)
            c.flush(f.candidate, timeout=45)

            # The third segment advances the certified physical admission floor.
            def new_view(rows):
                for r in rows:
                    m = r.get("data", {}).get("meta", {})
                    if (
                        "/qv/" in r.get("key", "")
                        and m.get("vchannel") == ch
                        and m.get("state") == "QueryViewStateUp"
                        and int(m.get("readAdmissionFloor", {}).get("transformingTimetick", 0)) >= latest
                    ):
                        return m

            end = time.monotonic() + 40
            while True:
                now = capture(f.site, f.candidate + f"-held-view-{time.time_ns()}", timeout=25)
                view = new_view(now)
                if view:
                    break
                if time.monotonic() >= end:
                    raise TimeoutError("new Up view admission floor")
                time.sleep(0.5)
            f.record("new View during protected read", view)
            arm(root / "read.json", root / "current-read", pid, ch, sid, latest, True)
            arm(root / "reserve.json", root / "reservation", pid, ch, sid, latest, True)
            assert f.query(f.candidate, "id == 1") == [dict(id=1, score=14, tag="base")]
            observed = receipt(root / "reservation")
            current_native = receipt(root / "current-read", timeout=5)
            f.record("current worker physical read", current_native)
            f.record("protected reservation", observed)
            assert not query.done(), (query.result(), old, latest)
            if old_reservation["details"]["base"] != observed["details"]["base"]:
                assert current_native["details"]["base"] != parked["details"]["base"]
                cancel_read.cancel()
                raise IndependentBase(f"old={old_reservation}, replacement={observed}")
            assert observed["details"]["reserved"] <= old
            arm(root / "gc.json", root / "after-gc", pid, "", sid, latest, True, parked["details"]["base"])
            assert f.query(f.mirror, "id == 1")[0]["score"] == 1
        finally:
            (root / "read" / "release").touch()
        try:
            seen = receipt(root / "success-output", timeout=12)
            f.record("deferred historical projection", seen)
            assert seen["tick"] == old and seen["details"]["read"] == parked["details"]["read"]
            assert not (root / "after-gc" / "receipt.json").exists()
        finally:
            (root / "success-output" / "release").touch()
        assert query.result(timeout=20) == [dict(id=1, score=11, tag="base")]
    assert not cancel_read.done()
    assert f.query(f.candidate, "id == 1")[0]["score"] == 14
    assert not (root / "after-gc" / "receipt.json").exists()
    assert cancel_read.cancel()
    try:
        cancel_read.result(timeout=3)
    except grpc.FutureCancelledError:
        f.record("caller cancelled pinned historical projection", dict(tick=old, base=parked["details"]["base"]))
    else:
        raise AssertionError("cancelled RPC completed")
    end = time.monotonic() + 35
    while not (root / "after-gc" / "receipt.json").exists():
        assert f.query(f.candidate, "id == 1")[0]["score"] == 14
        if time.monotonic() > end:
            raise TimeoutError("native GC after old read")
        time.sleep(0.25)
    gc = receipt(root / "after-gc")
    f.record("native retained versions after release", gc)
    assert gc["details"]["segment_ptr"] == parked["details"]["base"]
    assert gc["details"]["before"] == 5 and gc["details"]["after"] == 1
    assert f.query(f.candidate, "score == 14") == [dict(id=1, score=14, tag="base")]
    stale = read_owner(f, binary, "querynode", term, old_meta, old)
    assert stale["returncode"] != 0 and any(
        x in stale["stderr"].lower()
        for x in ("not found", "not ready", "invalidated", "view_not_found", "view_invalidated", "view_not_ready")
    )
    assert f.query(f.mirror, "id == 1")[0]["score"] == 1
    pending = receipt(root / "pending")
    f.record("delayed sorted successor binding", pending)
    (root / "pending" / "release").touch()
    replace(f, "querynode", binary)
    end = time.monotonic() + 75
    while True:
        try:
            final = f.query(f.candidate, "score == 14")
        except (grpc.RpcError, MilvusException):
            if time.monotonic() >= end:
                raise
            time.sleep(0.5)
        else:
            assert final == [dict(id=1, score=14, tag="base")]
            f.record("reloaded current projection", final)
            break


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("site", type=Path)
    p.add_argument("binary", type=Path)
    a = p.parse_args()
    for attempt in range(4):
        fixture = Fixture(a.site.resolve())
        try:
            with release_on_error(fixture.site / (fixture.candidate + "-gc")):
                run(fixture, a.binary.resolve(strict=True))
            break
        except IndependentBase as reason:
            fixture.record("independent replacement base", str(reason))
            if attempt == 3:
                raise
