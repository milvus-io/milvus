"""Independent mutation and serving-generation checks for the two-shard fixture."""

import json
from collections import Counter, defaultdict


def segments(snapshot, cid):
    return {
        x["data"]["ID"]: x["data"]
        for x in snapshot
        if x.get("data", {}).get("collectionID") == cid and x.get("key", "").startswith("datacoord-meta/s/")
    }


def events(file):
    for row in file["rows"]:
        fields = tuple(
            sorted(
                (a["field_id"], tuple(a["literal"]["Value"]["ScalarValue"]["Data"].items()))
                for a in row["Assignments"] or []
            )
        )
        for pk in row["PrimaryKeys"]["IdField"]["IntId"]["data"]:
            yield pk, (row["TimeTick"], row["SourceOrdinal"], row["Operation"], fields)


def check_history(persist, dispatched, cid, left, right, delete_tick, private_ack, shared_ack):
    src = segments(persist, cid)
    targets = segments(dispatched, cid)
    score = ((101, (("LongData", 40),)),)
    shared = ((101, (("LongData", 99),)), (102, (("StringData", "shared"),)))
    deletes = (delete_tick, 0, 1, ())
    private = (private_ack, 0, 2, score)
    data = [x for x in persist if "file" in x and "/" + cid + "/" in x["referrer"] and "/deltalog/" in x["referrer"]]
    actual = Counter()
    shared_ticks = {}
    for f in data:
        s = src[f["referrer"].split("/")[4]]
        ch = s["insertChannel"]
        scope = s["partitionID"]
        l = f["layout"]
        for pk, event in events(f):
            tick, _, op, _ = event
            if op == 1:
                assert l["CollectionID"] == 0 and l["Fields"] is None and not l["VChannel"]
            else:
                assert str(l["CollectionID"]) == cid and l["VChannel"] == ch and str(l["PartitionID"]) == scope
                if scope == "-1":
                    assert shared_ticks.setdefault(ch, tick) == tick
            actual[(ch, scope, pk, event)] += 1
    assert len(shared_ticks) == 2 and max(shared_ticks.values()) == shared_ack
    channels = {ch.rsplit("v", 1)[1]: ch for ch in shared_ticks}
    # These fixed INT64 keys map to shard index 0 under Milvus PK routing; 999 maps to 1.
    ch0, ch1 = channels["0"], channels["1"]
    expected = Counter((ch0, "-1", k, deletes) for k in (16, 116))
    expected.update((ch0, left, k, private) for k in (1, 2, 101, 102))
    for ch, keys in ((ch0, (2, 102)), (ch1, (999,))):
        expected.update((ch, "-1", k, (shared_ticks[ch], 0, 2, shared)) for k in keys)
    assert actual == expected, (actual - expected, expected - actual)

    observed = defaultdict(list)
    for f in dispatched:
        if "file" not in f or "/" + cid + "/" not in f["referrer"] or "/s/" not in f["referrer"]:
            continue
        sid = f["referrer"].split("/")[-1]
        s = targets[sid]
        assert str(f["layout"]["CollectionID"]) == cid and str(f["layout"]["PartitionID"]) == s["partitionID"]
        assert f["layout"]["VChannel"] == s["insertChannel"]
        for pk, event in events(f):
            observed[(sid, pk)].append(event)
    destination = {
        (s["insertChannel"], s["partitionID"]): sid
        for sid, s in targets.items()
        if s.get("state") == "Flushed" and s.get("level") == "L1"
    }
    assert len(destination) == 4
    sequence = defaultdict(list)
    for (ch, scope, pk, event), n in expected.items():
        part = left if pk < 100 else right
        if pk != 999 and scope in ("-1", part):
            sequence[(destination[(ch, part)], pk)].extend([event] * n)
    assert observed == sequence, (observed, sequence)
    return shared_ticks


def serving_generation(snapshot, cid, owner, target):
    """All Up views addressable on the live QN must serve the same certified physical bases."""
    qvs = [
        x["data"]
        for x in snapshot
        if "/qv/" in x.get("key", "")
        and x["data"]["meta"].get("collectionId") == cid
        and x["data"]["meta"]["state"] == "QueryViewStateUp"
    ]
    dvs = [x["data"] for x in snapshot if x.get("key", "").startswith("coord/dv/") and x["data"]["collectionId"] == cid]
    coverage = set()
    floors = {}
    for view in qvs:
        meta = view["meta"]
        ch = meta["vchannel"]
        for worker in view["queryNode"]:
            if worker["nodeId"] != str(owner):
                continue
            shard = next(
                s
                for dv in dvs
                if dv["dataVersion"] == meta["version"]["dataVersion"]
                for s in dv["shards"]
                if s["vchannel"] == ch
            )
            bound = {}
            for p in shard["partitions"]:
                bound.update(
                    {
                        sid: (int(ver), int(cursor))
                        for sid, ver, cursor in zip(
                            p["segmentIds"],
                            p["segmentManifestVersions"],
                            p["segmentTransformStartAfterTimeticks"],
                            strict=True,
                        )
                    }
                )
            loaded = {
                sid: int(ver)
                for p in worker["partitions"]
                for sid, ver in zip(p["segmentIds"], p["segmentManifestVersions"], strict=True)
            }
            expected = {
                sid: (json.loads(s["manifestPath"])["ver"], int(s["transformStartAfterTimetick"]))
                for sid, s in target.items()
                if s["insertChannel"] == ch
            }
            if expected != bound or loaded != {sid: t[0] for sid, t in expected.items()}:
                return None
            coverage.update(bound)
            threshold = max(int(v) for v in meta["readAdmissionFloor"].values())
            floors[ch] = min(floors.get(ch, threshold), threshold)
    return floors if coverage == set(target) and len(floors) == 2 else None
