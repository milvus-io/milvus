import logging

import pytest
import time
import copy
from utils import *
from constants import *

uid = "stats_collection"


class TestStatsBase:
    """
    ******************************************************************
      The following cases are used to test `collection_stats` function
    ******************************************************************
    """

    def test_get_collection_stats(self, client, collection):
        """
        target: get collections stats
        method: call get_collection_stats with created collection
        expected: status ok
        """
        client.insert(collection, default_entities)
        client.flush([collection])
        stats = client.stat_collection(collection)
        assert stats['row_count'] == default_nb
        assert len(stats["partitions"]) == 1
        assert stats["partitions"][0]["tag"] == default_partition_name
        assert stats["partitions"][0]["row_count"] == default_nb

    def test_get_collection_stats_collection_not_existed(self, client, collection):
        """
        target: get collection stats when collection not existed
        method: call collection_stats with a random collection_name, which is not in db
        expected: status not ok
        """
        collection_name = gen_unique_str(uid)
        assert not client.stat_collection(collection_name)

    @pytest.fixture(
        scope="function",
        params=[
            1,
            "12-s",
            " ",
            "12 s",
            " siede ",
            "(mn)",
            "中文",
            "a".join("a" for i in range(256))
        ]
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    def test_get_collection_stats_name_invalid(self, client, collection, get_invalid_collection_name):
        """
        target: get collection stats where collection name is invalid
        method: call collection_stats with invalid collection_name
        expected: status not ok
        """
        assert not client.stat_collection(get_invalid_collection_name)

    def test_get_collection_stats_empty_collection(self, client, collection):
        """
        target: get collection stats where no entity in collection
        method: call collection_stats with empty collection
        expected: segment = []
        """
        stats = client.stat_collection(collection)
        assert stats["row_count"] == 0
        assert len(stats["partitions"]) == 1
        assert stats["partitions"][0]["tag"] == default_partition_name
        assert stats["partitions"][0]["row_count"] == 0
