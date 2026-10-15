import threading
import time
import uuid

import pytest
from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, MilvusClient, connections, utility


@pytest.fixture
def management_collection(request):
    name = "qv_management_" + uuid.uuid4().hex
    connections.connect(
        alias=name,
        host=request.config.getoption("--host"),
        port=request.config.getoption("--port"),
    )
    collection = Collection(
        name,
        CollectionSchema(
            [
                FieldSchema("id", DataType.INT64, is_primary=True),
                FieldSchema("vector", DataType.FLOAT_VECTOR, dim=4),
            ]
        ),
        using=name,
        shards_num=2,
        consistency_level="Strong",
    )
    collection.create_index("vector", {"index_type": "FLAT", "metric_type": "L2"})
    client = MilvusClient(uri=f"http://{request.config.getoption('--host')}:{request.config.getoption('--port')}")
    try:
        yield collection, name, client
    finally:
        client.close()
        collection.drop()
        connections.disconnect(name)


@pytest.mark.tags("L1")
def test_queryview_manual_balance_keeps_queries_available(management_collection):
    collection, alias, client = management_collection
    collection.insert([list(range(100)), [[float(i), 0.0, 0.0, 0.0] for i in range(100)]])
    collection.flush()
    # Sort Compaction replaces flushed SegmentIDs. Establish stable inputs before
    # testing manual migration, without disabling production compaction behavior.
    deadline = time.monotonic() + 60
    while True:
        persistent = client.list_persistent_segments(collection.name, timeout=10)
        if (
            persistent
            and sum(item.num_rows for item in persistent) == 100
            and all(item.is_sorted for item in persistent)
        ):
            break
        assert time.monotonic() < deadline, persistent
        time.sleep(0.2)
    expected_segments = {item.segment_id for item in persistent}
    collection.load()
    while True:
        info = utility.get_query_segment_info(collection.name, using=alias)
        if {item.segmentID for item in info} == expected_segments:
            break
        assert time.monotonic() < deadline, info
        time.sleep(0.2)
    replica = collection.get_replicas().groups[0]
    leaders = {shard.shard_leader for shard in replica.shards}
    qns = set(replica.group_nodes) - leaders
    if len(qns) < 2:
        pytest.skip("manual balance requires two QueryNodes in one replica")
    info = utility.get_query_segment_info(collection.name, using=alias)
    segment = next(item for item in info if qns.intersection(item.nodeIds))
    source = min(qns.intersection(segment.nodeIds))
    destination = min(qns - {source})
    stop = threading.Event()
    started = threading.Event()
    failures = []
    reads = []

    def query_until_done():
        try:
            while not stop.is_set():
                rows = collection.query("id >= 0", output_fields=["id"], timeout=30)
                assert {row["id"] for row in rows} == set(range(100))
                reads.append(len(rows))
                started.set()
        except BaseException as error:
            failures.append(error)
            started.set()

    reader = threading.Thread(target=query_until_done)
    reader.start()
    try:
        assert started.wait(30)
        utility.load_balance(collection.name, source, [destination], [segment.segmentID], using=alias, timeout=60)
        # The RPC itself is the barrier; do not add an eventual-placement wait.
        after = utility.get_query_segment_info(collection.name, using=alias)
        assert any(item.segmentID == segment.segmentID and destination in item.nodeIds for item in after)
        assert not any(item.segmentID == segment.segmentID and source in item.nodeIds for item in after)
        assert len(collection.query("id >= 0", output_fields=["id"], timeout=30)) == 100
    finally:
        stop.set()
        reader.join(35)
    assert not reader.is_alive()
    assert not failures, failures
    assert reads


@pytest.mark.tags("L1")
def test_queryview_replicas_empty_collection_release_reload(management_collection):
    collection, _, _ = management_collection
    collection.load(replica_number=2)
    before = collection.get_replicas().groups
    assert len(before) == 2
    assert all(len(replica.shards) == 2 for replica in before)
    qns = [set(replica.group_nodes) - {shard.shard_leader for shard in replica.shards} for replica in before]
    assert all(qns)
    assert qns[0].isdisjoint(qns[1])
    collection.release()
    assert not collection.get_replicas().groups
    collection.load(replica_number=1)
    after = collection.get_replicas().groups
    assert len(after) == 1
    assert len(after[0].shards) == 2
    assert after[0].id not in {replica.id for replica in before}
