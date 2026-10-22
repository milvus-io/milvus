import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel


class TestMilvusClientPartitionReleaseHandoff(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("sealed", [False, True], ids=["growing", "sealed"])
    def test_release_partition_waits_for_serving_handoff(self, sealed):
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, name, dimension=4, shards_num=2, consistency_level="Strong")
        try:
            client.create_partition(name, "p1")
            client.create_partition(name, "p2")
            client.release_collection(name)
            client.load_partitions(name, ["p1", "p2"], replica_number=2)
            for partition, start in [("p1", 0), ("p2", 10)]:
                client.insert(
                    name,
                    [{"id": i, "vector": [1.0, 0.0, 0.0, 0.0]} for i in range(start, start + 5)],
                    partition_name=partition,
                )
            if sealed:
                client.flush(name)
                # Wait for the flushed DataView to be served by QueryViews,
                # so this variant cannot pass using retained Growing segments.
                client.refresh_load(name, timeout=60)

            def ids():
                return {
                    row["id"]
                    for row in client.query(
                        name, filter="id >= 0", output_fields=["id"], consistency_level="Strong", timeout=60
                    )
                }

            assert ids() == set(range(5)) | set(range(10, 15))
            client.release_partitions(name, ["p1"])
            assert ids() == set(range(10, 15))
            client.release_partitions(name, ["p1"])
            assert ids() == set(range(10, 15))
            client.load_partitions(name, ["p1"], replica_number=2)
            assert ids() == set(range(5)) | set(range(10, 15))
            client.release_partitions(name, ["p1", "p2"])
            client.release_partitions(name, ["p1", "p2"])
            client.load_partitions(name, ["p1"], replica_number=2)
            assert ids() == set(range(5))
        finally:
            client.drop_collection(name)
