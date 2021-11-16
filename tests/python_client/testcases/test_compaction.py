import pytest
from pymilvus.client.types import State

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

prefix = "compact"
tmp_nb = 100


@pytest.mark.skip(reason="Waiting for development")
class TestCompactionParams(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_without_connection(self):
        """
        target: test compact without connection
        method: compact after remove connection
        expected: raise exception
        """
        # init collection with tmp_nb default data
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]

        # remove connection and delete
        self.connection_wrap.remove_connection(ct.default_alias)
        res_list, _ = self.connection_wrap.list_connections()
        assert ct.default_alias not in res_list
        error = {ct.err_code: 0, ct.err_msg: "should create connect first"}
        collection_w.compact(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_twice(self):
        """
        target: test compact twice
        method: compact twice
        expected: No exception
        """
        # init collection with tmp_nb default data
        collection_w, _, _, ids = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0:4]
        collection_w.delete(f'{ct.default_int64_field_name} in {ids}')

        collection_w.compact()
        c_plans1 = collection_w.get_compaction_plans()[0]

        collection_w.compact()
        c_plans2 = collection_w.get_compaction_plans()[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_partition(self):
        """
        target: test compact partition
        method: compact partition
        expected: Verify partition segments merged
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_growing_segment(self):
        """
        target: test compact growing data
        method: 1.insert into multi segments without flush
                2.compact
        expected: No compaction (compact just for sealed data)
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_empty_collection(self):
        """
        target: test compact an empty collection
        method: compact an empty collection
        expected: No exception
        """
        # init collection and empty
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # compact
        collection_w.compact()
        c_plans, _ = collection_w.get_compaction_plans()
        assert len(c_plans.plans) == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete_single(self):
        """
        target: test delete one entity and compact
        method: 1.create with shard_num=1
                2.delete one sealed entity
                2.compact
        expected: Verify compact result todo
        """
        collection_w = self.init_collection_general(prefix, nb=tmp_nb, insert_data=True)[0]
        single_expr = f'{ct.default_int64_field_name} in {[0]}'
        collection_w.delete(single_expr)

        collection_w.compact()
        while True:
            c_state = collection_w.get_compaction_state()
            if c_state.state == State.Completed:
                log.debug(f'state: {c_state.state}')
                break
        c_plans = collection_w.get_compaction_plans()[0]
        for plan in c_plans.plans:
            assert len(plan.sources) == 1

        self.utility_wrap.get_query_segment_info(collection_w.name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete_half(self):
        """
        target: test delete half entity and compact
        method: 1.create with shard_num=1
                2.insert and flush
                3.delete half of nb
                4.compact
        expected: collection num_entities decrease
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        res, _ = collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb
        # delete half
        half_expr = f'{ct.default_int64_field_name} in {res.primary_keys[:tmp_nb // 2]}'
        collection_w.delete(half_expr)

        collection_w.compact()
        while True:
            c_state = collection_w.get_compaction_state()
            if c_state.state == State.Completed:
                log.debug(f'state: {c_state.state}')
                break
        c_plans = collection_w.get_compaction_plans()[0]
        for plan in c_plans.plans:
            assert len(plan.sources) == 1

        collection_w.load()
        log.debug(collection_w.num_entities)

        self.utility_wrap.get_query_segment_info(collection_w.name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete_all(self):
        """
        target: test delete all and compact
        method: 1.create with shard_num=1
                2.delete all sealed data
                3.compact
        expected: collection num_entities is close to 0
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data()
        res, _ = collection_w.insert(df)

        assert collection_w.num_entities == ct.default_nb

        expr = f'{ct.default_int64_field_name} in {res.primary_keys}'
        collection_w.delete(expr)

        # assert collection_w.num_entities == ct.default_nb
        collection_w.compact()
        log.debug(collection_w.num_entities)


@pytest.mark.skip(reason="Waiting for development")
class TestCompactionOperation(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_index(self):
        """
        target: test compact after create index
        method: 1.insert data into two segments
                2.create index
                3.compact
                4.search
        expected: Verify segment info and index info
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_binary_index(self):
        """
        target: test compact after create index
        method: 1.insert binary data into two segments
                2.create binary index
                3.compact
                4.search
        expected: Verify segment info and index info
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_and_index(self):
        """
        target: test compact and create index
        method: 1.insert data into two segments
                2.compact
                3.create index
                4.load and search
        expected: Verify search result and index info
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_compact_and_search(self):
        """
        target: test compact and search
        method: 1.insert data into two segments
                2.compact
                3.load and search
        expected: Verify search result
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_search_after_delete_channel(self):
        """
        target: test search after compact, and queryNode get delete request from channel,
                rather than compacted delta log
        method: 1.insert, flush and load
                2.delete half
                3.compact
                4.search
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_delete_inside_time_travel(self):
        """
        target: test compact inside time_travel range
        method: 1.insert data and get ts
                2.delete ids
                3.search with ts
                4.compact
                5.search with ts
        expected: Both search are successful
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_delete_outside_time_travel(self):
        """
        target: test compact outside time_travel range
        method: todo
        expected: Verify compact result
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_compact_merge_two_segments(self):
        """
        target: test compact merge two segments
        method: 1.create with shard_num=1
                2.insert half nb and flush
                3.insert half nb and flush
                4.compact
                5.search
        expected: Verify segments are merged
        """
        half = ct.default_nb // 2
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data()
        res, _ = collection_w.insert(df[:half])
        log.debug(collection_w.num_entities)

        collection_w.insert(df[half:])
        log.debug(collection_w.num_entities)

        collection_w.compact()

        state, _ = collection_w.get_compaction_state()
        plan, _ = collection_w.get_compaction_plans()

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_no_merge(self):
        """
        target: test compact when no segments merge
        method: 1.create with shard_num=1
                2.insert and flush
                3.compact and search
        expected: No exception
        """
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        collection_w.compact()
        while True:
            c_state = collection_w.get_compaction_state()
            log.debug(c_state)
            if c_state.state == State.Completed and c_state.in_timeout == 0:
                break
        c_plans, _ = collection_w.get_compaction_plans()
        assert len(c_plans.plans) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_merge_multi_segments(self):
        """
        target: test compact and merge multi small segments
        method: 1.create with shard_num=1
                2.insert one and flush (multi times)
                3.compact
                4.load and search
        expected: Verify segments info
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_merge_inside_time_travel(self):
        """
        target: test compact and merge segments inside time_travel range
        method: todo
        expected: Verify segments inside time_travel merged
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_threshold_auto_merge(self):
        """
        target: test num (segment_size < 1/2Max) reaches auto-merge threshold
        method: todo
        expected: Auto-merge segments
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_less_threshold_no_merge(self):
        """
        target: test compact the num of segments that size less than 1/2Max, does not reach the threshold
        method: todo
        expected: No auto-merge
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_multi_collections(self):
        """
        target: test compact multi collections with merge
        method: create 50 collections, add entities into them and compact in turn
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_and_insert(self):
        """
        target: test insert after compact
        method: 1.create and insert with flush
                2.delete and compact
                3.insert new data
                4.load and search
        expected: Verify search result and segment info
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_and_delete(self):
        """
        target: test delete after compact
        method: 1.delete half and compact
                2.load and query
                3.delete and query
        expected: Verify deleted ids
        """
        pass
