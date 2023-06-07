import threading
from time import time, sleep

import pytest
from pymilvus.grpc_gen.common_pb2 import SegmentState
from pymilvus.exceptions import MilvusException

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

prefix = "compact"
tmp_nb = 100


class TestCompactionParams(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_twice(self):
        """
        target: test compact twice
        method: 1.create with shard_num=1
                2.insert and flush twice (two segments)
                3.compact
                4.insert new data
                5.compact
        expected: Merge into one segment
        """
        # init collection with one shard, insert into two segments
        pytest.skip("DataCoord: for A, B -> C, will not compact segment C before A, B GCed, no method to check whether a segment is GCed")
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=tmp_nb)

        # first compact two segments
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans1 = collection_w.get_compaction_plans()[0]
        target_1 = c_plans1.plans[0].target

        # insert new data
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        log.debug(collection_w.num_entities)

        # second compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_state()
        c_plans2 = collection_w.get_compaction_plans()[0]

        assert target_1 in c_plans2.plans[0].sources
        log.debug(c_plans2.plans[0].target)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/20747")
    def test_compact_partition(self):
        """
        target: test compact partition
        method: compact partition
        expected: Verify partition segments merged
        """
        # create collection with shard_num=1, and create partition
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # insert flush twice
        for i in range(2):
            df = cf.gen_default_dataframe_data(tmp_nb)
            partition_w.insert(df)
            assert partition_w.num_entities == tmp_nb * (i + 1)

        # create index
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]

        assert len(c_plans.plans) == 1
        assert len(c_plans.plans[0].sources) == 2
        target = c_plans.plans[0].target

        # verify queryNode load the compacted segments
        cost = 180
        start = time()
        while time() - start < cost:
            collection_w.load()
            segment_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(segment_info) == 1:
                break
            sleep(1.0)
        assert target == segment_info[0].segmentID

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_only_growing_segment(self):
        """
        target: test compact growing data
        method: 1.insert into multi segments without flush
                2.compact
        expected: No compaction (compact just for sealed data)
        """
        # create and insert without flush
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)

        # compact when only growing segment
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]
        assert len(c_plans.plans) == 0

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

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("delete_pos", [1, tmp_nb // 2])
    def test_compact_after_delete(self, delete_pos):
        """
        target: test delete one entity and compact
        method: 1.create with shard_num=1
                2.delete one sealed entity, half entities
                2.compact
        expected: Verify compact result
        """
        # create, insert without flush
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        insert_res, _ = collection_w.insert(df)

        # delete single entity, flush
        single_expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:delete_pos]}'
        collection_w.delete(single_expr)
        collection_w.flush()
        
        # compact, get plan
        sleep(ct.compact_retention_duration + 1)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)

        collection_w.load()
        collection_w.query(single_expr, check_items=CheckTasks.check_query_empty)

        res = df.iloc[-1:, :1].to_dict('records')
        collection_w.query(f'{ct.default_int64_field_name} in {insert_res.primary_keys[-1:]}',
                           check_items={'exp_res': res})

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_after_delete_index(self):
        """
        target: test compact after delete and create index
        method: 1.create with 1 shard and insert nb entities (ensure can be index)
                2.delete some entities and flush (ensure generate delta log)
                3.create index
                4.compact outside retentionDuration
                5.load and search with travel time
        expected: Empty search result
        """
        # create, insert without flush
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)

        # delete and flush
        expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:ct.default_nb // 2]}'
        collection_w.delete(expr)
        assert collection_w.num_entities == ct.default_nb

        # build index
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # compact, get plan
        sleep(ct.compact_retention_duration + 1)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)

        collection_w.load()
        res, _ = collection_w.search(df[ct.default_float_vec_field_name][:1].to_list(),
                                     ct.default_float_vec_field_name,
                                     ct.default_search_params, ct.default_limit)
        # Travel time currently does not support travel back to retention ago, so just verify search is available.
        assert len(res[0]) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_delete_ratio(self):
        """
        target: test delete entities reaches ratio and auto-compact
        method: 1.create with shard_num=1
                2.insert (compact load delta log, not from dmlChannel)
                3.delete 20% of nb, flush
        expected: Verify auto compaction, merge insert log and delta log
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        insert_res, _ = collection_w.insert(df)

        # delete 20% entities
        ratio_expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:tmp_nb // ct.compact_delta_ratio_reciprocal]}'
        collection_w.delete(ratio_expr)
        collection_w.flush()

        # Flush a new segment and meet condition 20% deleted entities, triggre compaction but no way to get plan
        collection_w.insert(cf.gen_default_dataframe_data(1, start=tmp_nb))

        exp_num_entities_after_compact = tmp_nb - (tmp_nb // ct.compact_delta_ratio_reciprocal) + 1
        start = time()
        while True:
            if collection_w.num_entities == exp_num_entities_after_compact:
                break
            if time() - start > 180:
                raise MilvusException(1, "Auto delete ratio compaction cost more than 180s")
            sleep(1)

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(ratio_expr, check_items=CheckTasks.check_query_empty)

        res = df.iloc[-1:, :1].to_dict('records')
        collection_w.query(f'{ct.default_int64_field_name} in {insert_res.primary_keys[-1:]}',
                           check_items={'exp_res': res})

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_delete_less_ratio(self):
        """
        target: test delete entities less ratio and no compact
        method: 1.create collection shard_num=1
                2.insert without flush
                3.delete 10% entities and flush
        expected: Verify no compact (can't), delete successfully
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        insert_res, _ = collection_w.insert(df)

        # delete 10% entities, ratio = 0.1
        less_ratio_reciprocal = 10
        ratio_expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:tmp_nb // less_ratio_reciprocal]}'
        collection_w.delete(ratio_expr)
        assert collection_w.num_entities == tmp_nb

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(ratio_expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_after_delete_all(self):
        """
        target: test delete all and compact
        method: 1.create with shard_num=1
                2.delete all sealed data
                3.compact
        expected: Get compaction plan
        """
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data()
        res, _ = collection_w.insert(df)

        expr = f'{ct.default_int64_field_name} in {res.primary_keys}'
        collection_w.delete(expr)
        assert collection_w.num_entities == ct.default_nb

        # currently no way to verify whether it is compact after delete,
        # because the merge compact plan is generate first
        sleep(ct.compact_retention_duration + 1)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load()
        collection_w.query(expr, check_items=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete(self):
        """
        target: test delete and then compact
        method: 1. create a collection and insert data
                2. delete all data and compact
                3. load query and release
        expected: can't find the deleted data
        """
        nb = 50000
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(nb=nb)
        res, _ = collection_w.insert(df)
        assert collection_w.num_entities == nb

        expr = f'{ct.default_int64_field_name} in {res.primary_keys}'
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.delete(expr)
        assert collection_w.num_entities == nb
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        # collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)

        collection_w.load()
        res = collection_w.query(expr)[0]
        assert len(res) == 0
        collection_w.release()

        sleep(30)
        collection_w.load()
        res = collection_w.query(expr)[0]
        assert len(res) == 0
        collection_w.release()

        sleep(40)
        collection_w.load()
        res = collection_w.query(expr)[0]
        assert len(res) == 0
        collection_w.release()

        sleep(60)
        collection_w.load()
        res = collection_w.query(expr)[0]
        assert len(res) == 0
        collection_w.release()

    @pytest.mark.skip(reason="TODO")
    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_delete_max_delete_size(self):
        """
        target: test compact delta log reaches max delete size 10MiB
        method: todo
        expected: auto merge single segment
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_max_time_interval(self):
        """
        target: test auto compact with max interval 60s
        method: 1.create with shard_num=1
                2.insert flush twice (two segments)
                3.wait max_compaction_interval (60s)
        expected: Verify compaction results
        """
        # create collection shard_num=1, insert 2 segments, each with tmp_nb entities
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.compact()

        # Notice:The merge segments compaction triggered by max_compaction_interval also needs to meet
        # the compaction_segment_ num_threshold
        for i in range(ct.compact_segment_num_threshold):
            df = cf.gen_default_dataframe_data(tmp_nb)
            collection_w.insert(df)
            assert collection_w.num_entities == tmp_nb * (i + 1)

        sleep(ct.max_compaction_interval + 1)

        # verify queryNode load the compacted segments
        collection_w.load()
        replicas = collection_w.get_replicas()[0]
        replica_num = len(replicas.groups)
        cost = 180
        start = time()
        while time() - start < cost:
            sleep(1.0)
            collection_w.load()
            segment_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(segment_info) == 1*replica_num:
                break
            if time() - start > cost:
                raise MilvusException(1, f"Waiting more than {cost}s for the compacted segment indexed")
            collection_w.release()

    @pytest.mark.skip(reason="TODO")
    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_delta_max_time_interval(self):
        """
        target: test merge insert and delta log triggered by max_compaction_interval
        method: todo
        expected: auto compact binlogs
        """
        pass


class TestCompactionOperation(TestcaseBase):

    # @pytest.mark.xfail(reason="Issue https://github.com/milvus-io/milvus/issues/15665")
    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_both_delete_merge(self):
        """
        target: test compact both delete and merge
        method: 1.create collection with shard_num=1
                2.insert data into two segments
                3.delete and flush (new insert)
                4.compact
                5.load and search
        expected: Triggre two types compaction
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        ids = []
        for i in range(2):
            df = cf.gen_default_dataframe_data(tmp_nb, start=i * tmp_nb)
            insert_res, _ = collection_w.insert(df)
            assert collection_w.num_entities == (i + 1) * tmp_nb
            ids.extend(insert_res.primary_keys)

        # delete_ids = ids[:tmp_nb]
        delete_ids = [0, tmp_nb // 2]
        expr = f'{ct.default_int64_field_name} in {delete_ids}'
        collection_w.delete(expr)

        collection_w.insert(cf.gen_default_dataframe_data(1, start=2 * tmp_nb))
        assert collection_w.num_entities == 2 * tmp_nb + 1

        sleep(ct.compact_retention_duration + 1)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]

        # assert len(c_plans.plans) == 2
        # todo assert two types compaction plan

        # search
        ids.pop(0)
        ids.pop(-1)
        collection_w.load()
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            check_items={"nq": ct.default_nq,
                                                         "ids": ids,
                                                         "limit": ct.default_limit})
        collection_w.query(f"{ct.default_int64_field_name} in {delete_ids}",
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_delete_multi_segments(self):
        """
        target: test compact multi delete segments
        method: 1.create collection with shard_num=2
                2.insert data into two segments
                3.delete entities from two segments
                4.compact
                5.load and search
        expected: Verify two delta compaction plans
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))
        df = cf.gen_default_dataframe_data(2*tmp_nb)
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == 2 * tmp_nb

        collection_w.load()
        log.debug(self.utility_wrap.get_query_segment_info(collection_w.name))

        delete_ids = [i for i in range(10)]
        expr = f'{ct.default_int64_field_name} in {delete_ids}'
        collection_w.delete(expr)

        sleep(ct.compact_retention_duration + 1)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]
        assert len(c_plans.plans) == 2
        for plan in c_plans.plans:
            assert len(plan.sources) == 1

        collection_w.query(f"{ct.default_int64_field_name} in {delete_ids}",
                           check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_merge_multi_shards(self):
        """
        target: test compact merge multi shards
        method: 1.Create a collection with 2 shards
                2.Insert twice and generate 4 segments
                3.Compact and wait it completed
        expected: Verify there are 2 merge type complation plans
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=2)
        for i in range(2):
            df = cf.gen_default_dataframe_data(2 * tmp_nb)
            insert_res, _ = collection_w.insert(df)
            log.debug(collection_w.num_entities)

        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()
        log.debug(self.utility_wrap.get_query_segment_info(collection_w.name))

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]
        assert len(c_plans.plans) == 2
        targets = []
        for plan in c_plans.plans:
            assert len(plan.sources) == 2
            targets.append(plan.target)

        collection_w.release()
        collection_w.load()
        seg_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        for seg in seg_info:
            seg.segmentID in targets

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_after_index(self):
        """
        target: test compact after create index
        method: 1.insert data into two segments
                2.create index
                3.compact
                4.search
        expected: Verify segment info and index info
        """
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=ct.default_nb,
                                                                       is_dup=False)

        # create index
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)

        # search
        collection_w.load()
        self.utility_wrap.get_query_segment_info(collection_w.name)
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert len(search_res) == ct.default_nq
        for hits in search_res:
            assert len(hits) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_after_binary_index(self):
        """
        target: test compact after create index
        method: 1.insert binary data into two segments
                2.create binary index
                3.compact
                4.search
        expected: Verify segment info and index info
        """
        # create collection with 1 shard and insert 2 segments
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1,
                                                 schema=cf.gen_default_binary_collection_schema())
        for i in range(2):
            df, _ = cf.gen_default_binary_dataframe_data()
            collection_w.insert(data=df)
            assert collection_w.num_entities == (i + 1) * ct.default_nb

        # create index
        collection_w.create_index(ct.default_binary_vec_field_name, ct.default_binary_index)
        log.debug(collection_w.index())

        # load and search
        collection_w.load()
        search_params = {"metric_type": "JACCARD", "params": {"nprobe": 32}}
        search_res_one, _ = collection_w.search(df[ct.default_binary_vec_field_name][:ct.default_nq].to_list(),
                                                ct.default_binary_vec_field_name, search_params, ct.default_limit)

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)[0]

        # waiting for handoff completed and search
        cost = 180
        start = time()
        while True:
            sleep(1)
            segment_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(segment_info) != 0 and segment_info[0].segmentID == c_plans.plans[0].target:
                log.debug(segment_info)
                break
            if time() - start > cost:
                raise MilvusException(1, f"Handoff after compact and index cost more than {cost}s")

        # verify search result
        search_res_two, _ = collection_w.search(df[ct.default_binary_vec_field_name][:ct.default_nq].to_list(),
                                                ct.default_binary_vec_field_name, search_params, ct.default_limit)
        assert len(search_res_one) == ct.default_nq
        for hits in search_res_one:
            assert len(hits) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_and_index(self):
        """
        target: test compact and create index
        method: 1.insert data into two segments
                2.compact
                3.create index
                4.load and search
        expected: Verify search result and index info
        """
        pytest.skip("Compaction requires segment indexed")
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=ct.default_nb,
                                                                       is_dup=False)

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)

        # create index
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # search
        collection_w.load()
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert len(search_res) == ct.default_nq
        for hits in search_res:
            assert len(hits) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_delete_and_search(self):
        """
        target: test delete and compact segment, and search
        method: 1.create collection and insert
                2.delete part entities
                3.compact
                4.load and search
        expected: Verify search result
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)

        expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:ct.default_nb // 2]}'
        collection_w.delete(expr)
        assert collection_w.num_entities == ct.default_nb

        sleep(ct.compact_retention_duration + 1)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)

        # search
        collection_w.load()
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": ct.default_nq,
                                                         "ids": insert_res.primary_keys[ct.default_nb // 2:],
                                                         "limit": ct.default_limit}
                                            )
        collection_w.query("int64 in [0]", check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L0)
    def test_compact_merge_and_search(self):
        """
        target: test compact and search
        method: 1.insert data into two segments
                2.compact
                3.load and search
        expected: Verify search result
        """
        pytest.skip("Compaction requires segment indexed")
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=ct.default_nb,
                                                                       is_dup=False)

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)

        # search
        collection_w.load()
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert len(search_res) == ct.default_nq
        for hits in search_res:
            assert len(hits) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_search_after_delete_channel(self):
        """
        target: test search after compact, and queryNode get delete request from channel,
                rather than compacted delta log
        method: 1.insert, flush and load
                2.delete half
                3.compact
                4.search
        expected: No compact, compact get delta log from storage
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)

        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        assert collection_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()

        expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys[:ct.default_nb // 2]}'
        collection_w.delete(expr)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]
        assert len(c_plans.plans) == 1

        # search
        collection_w.load()
        search_res, _ = collection_w.search(cf.gen_vectors(ct.default_nq, ct.default_dim),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            check_task=CheckTasks.check_search_results,
                                            check_items={"nq": ct.default_nq,
                                                         "ids": insert_res.primary_keys[ct.default_nb // 2:],
                                                         "limit": ct.default_limit}
                                            )

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_repeatedly_delete_same_id(self):
        """
        target: test compact after repeatedly delete same entity
        method: 1.Create and insert entities
                2.repeatedly delete the same id
                3.compact
        expected: No exception or delta log just delete one
        """
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)

        df = cf.gen_default_dataframe_data()
        insert_res, _ = collection_w.insert(df)
        expr = f'{ct.default_int64_field_name} in [0]'

        for _ in range(100):
            collection_w.delete(expr=expr)
        assert collection_w.num_entities == ct.default_nb

        sleep(ct.compact_retention_duration + 1)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)

        collection_w.load()
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_delete_inside_time_travel(self):
        """
        target: test compact inside time_travel range
        method: 1.insert data and get ts
                2.delete all ids
                4.compact
                5.search with ts
        expected: Verify search result
        """
        from pymilvus import utility
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)

        # insert and get tt
        df = cf.gen_default_dataframe_data(tmp_nb)
        insert_res, _ = collection_w.insert(df)
        tt = utility.mkts_from_hybridts(insert_res.timestamp, milliseconds=0.)

        # delete all
        expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys}'
        delete_res, _ = collection_w.delete(expr)
        log.debug(collection_w.num_entities)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans()

        collection_w.load()
        search_one, _ = collection_w.search(df[ct.default_float_vec_field_name][:1].to_list(),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            travel_timestamp=tt)
        assert 0 in search_one[0].ids

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_delete_outside_time_travel(self):
        """
        target: test compact outside time_travel range
        method: 1.create and insert
                2.get time stamp
                3.delete
                4.compact after compact_retention_duration
                5.load and search with travel time tt
        expected: Empty search result
                  But no way to verify, because travel time does not support travel back to retentionDuration ago so far
        """
        from pymilvus import utility
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix), shards_num=1)

        # insert
        df = cf.gen_default_dataframe_data(tmp_nb)
        insert_res, _ = collection_w.insert(df)
        tt = utility.mkts_from_hybridts(insert_res.timestamp, milliseconds=0.)

        expr = f'{ct.default_int64_field_name} in {insert_res.primary_keys}'
        delete_res, _ = collection_w.delete(expr)
        log.debug(collection_w.num_entities)

        # ensure compact remove delta data that delete outside retention range
        sleep(ct.compact_retention_duration + 1)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact)
        collection_w.load()

        # search with travel_time tt
        collection_w.search(df[ct.default_float_vec_field_name][:1].to_list(),
                            ct.default_float_vec_field_name,
                            ct.default_search_params, ct.default_limit,
                            travel_timestamp=tt,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: "only support to travel back to"})

    @pytest.mark.tags(CaseLabel.L0)
    def test_compact_merge_two_segments(self):
        """
        target: test compact merge two segments
        method: 1.create with shard_num=1
                2.insert and flush
                3.insert and flush again
                4.compact
                5.load
        expected: Verify segments are merged
        """
        num_of_segment = 2
        # create collection shard_num=1, insert 2 segments, each with tmp_nb entities
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, num_of_segment, tmp_nb)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)

        # verify the two segments are merged into one
        c_plans = collection_w.get_compaction_plans()[0]

        # verify queryNode load the compacted segments
        collection_w.load()

        start = time()
        cost = 180
        while True:
            sleep(1)
            segments_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]

            # verify segments reaches threshold, auto-merge ten segments into one
            if len(segments_info) == 1:
                break
            end = time()
            if end - start > cost:
                raise MilvusException(1, "Compact merge two segments more than 180s")
        assert c_plans.plans[0].target == segments_info[0].segmentID

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_no_merge(self):
        """
        target: test compact when no segments merge
        method: 1.create with shard_num=1
                2.insert and flush
                3.compact and search
        expected: No exception and compact plans
        """
        # create collection
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb

        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()

        seg_before, _ = self.utility_wrap.get_query_segment_info(collection_w.name)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans, _ = collection_w.get_compaction_plans()
        assert len(c_plans.plans) == 1
        assert [seg_before[0].segmentID] == c_plans.plans[0].sources

        collection_w.release()
        collection_w.load()
        seg_after, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
        assert seg_after[0].segmentID == c_plans.plans[0].target

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_manual_and_auto(self):
        """
        target: test compact manual and auto
        method: 1.create with shard_num=1
                2.insert one and flush (11 times)
                3.compact
                4.load and search
        expected: Verify segments info
        """
        # greater than auto-merge threshold 10
        pytest.skip("DataCoord: for A, B -> C, will not compact segment C before A, B GCed, no method to check whether a segment is GCed")
        num_of_segment = ct.compact_segment_num_threshold + 1

        # create collection shard_num=1, insert 11 segments, each with one entity
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, num_of_segment=num_of_segment)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # waiting for auto compaction finished
        sleep(60)

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans, _ = collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact, check_items={"segment_num": 2})

        collection_w.load()
        start = time()
        cost = 180
        while True:
            sleep(1)
            segments_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]

            # verify segments reaches threshold, auto-merge ten segments into one
            if len(segments_info) == 1:
                break
            end = time()
            if end - start > cost:
                raise MilvusException(1, "Compact auto and manual more than 180s")
        assert segments_info[0].segmentID == c_plans.plans[0].target

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_merge_multi_segments(self):
        """
        target: test compact and merge multi small segments
        method: 1.create with shard_num=1
                2.insert one and flush (less than threshold)
                3.compact
                4.load and search
        expected: Verify segments info
        """
        # less than auto-merge threshold 10
        num_of_segment = ct.compact_segment_num_threshold - 1

        # create collection shard_num=1, insert 11 segments, each with one entity
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, num_of_segment=num_of_segment)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        collection_w.compact()
        collection_w.wait_for_compaction_completed()

        c_plans = collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact,
                                                    check_items={"segment_num": num_of_segment})[0]
        target = c_plans.plans[0].target

        collection_w.load()
        cost = 180
        start = time()
        while True:
            sleep(1)
            segments_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]

            # verify segments reaches threshold, auto-merge ten segments into one
            if len(segments_info) == 1:
                break
            end = time()
            if end - start > cost:
                raise MilvusException(1, "Compact merge multiple segments more than 180s")
        replicas = collection_w.get_replicas()[0]
        replica_num = len(replicas.groups)
        assert len(segments_info) == 1*replica_num
        assert segments_info[0].segmentID == target

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_merge_inside_time_travel(self):
        """
        target: test compact and merge segments inside time_travel range
        method: search with time travel after merge compact
        expected: Verify segments inside time_travel merged
        """
        from pymilvus import utility
        # create collection shard_num=1, insert 2 segments, each with tmp_nb entities
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)

        # insert twice
        df1 = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df1)
        assert collection_w.num_entities == tmp_nb

        df2 = cf.gen_default_dataframe_data(tmp_nb, start=tmp_nb)
        insert_two = collection_w.insert(df2)[0]
        assert collection_w.num_entities == tmp_nb * 2

        tt = utility.mkts_from_hybridts(insert_two.timestamp, milliseconds=0.1)

        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)

        collection_w.load()
        search_res, _ = collection_w.search(df2[ct.default_float_vec_field_name][:1].to_list(),
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit,
                                            travel_timestamp=0)
        assert tmp_nb in search_res[0].ids
        assert len(search_res[0]) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_threshold_auto_merge(self):
        """
        target: test num (segment_size < 1/2Max) reaches auto-merge threshold 10
        method: 1.create with shard_num=1
                2.insert flush 10 times (merge threshold 10)
                3.wait for compaction, load
        expected: Get query segments info to verify segments auto-merged into one
        """
        threshold = ct.compact_segment_num_threshold

        # create collection shard_num=1, insert 10 segments, each with one entity
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, num_of_segment=threshold)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # Estimated auto-merging takes 30s
        cost = 180
        collection_w.load()
        replicas = collection_w.get_replicas()[0]
        replica_num = len(replicas.groups)
        start = time()
        while True:
            sleep(1)
            segments_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]

            # verify segments reaches threshold, auto-merge ten segments into one
            if len(segments_info) == 1*replica_num:
                break
            end = time()
            if end - start > cost:
                raise MilvusException(1, "Compact auto-merge more than 180s")

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_less_threshold_no_merge(self):
        """
        target: test compact the num of segments that size less than 1/2Max, does not reach the threshold
        method: 1.create collection with shard_num = 1
                2.insert flush 9 times (segments threshold 10)
                3.after a while, load
        expected: Verify segments are not merged
        """
        less_threshold = ct.compact_segment_num_threshold - 1

        # create collection shard_num=1, insert 9 segments, each with one entity
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, num_of_segment=less_threshold)

        # create index
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)

        # load and verify no auto-merge
        collection_w.load()
        replicas = collection_w.get_replicas()[0]
        replica_num = len(replicas.groups)
        segments_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
        assert len(segments_info) == less_threshold*replica_num

    @pytest.mark.skip(reason="Todo")
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
        # create collection shard_num=1, insert 2 segments, each with tmp_nb entities
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=tmp_nb)

        # compact two segments
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans()

        # insert new data, verify insert flush successfully
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb * 3

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_and_delete(self):
        """
        target: test delete after compact
        method: 1.delete half and compact
                2.load and query
                3.delete and query
        expected: Verify deleted ids
        """
        # init collection with one shard, insert into two segments
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, is_dup=False)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())
        # compact and complete
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact)

        # delete and query
        expr = f'{ct.default_int64_field_name} in {[0]}'
        collection_w.delete(expr)
        collection_w.load()
        collection_w.query(expr, check_task=CheckTasks.check_query_empty)

        expr_1 = f'{ct.default_int64_field_name} in {[1]}'
        collection_w.query(expr_1, check_task=CheckTasks.check_query_results, check_items={
                           'exp_res': [{'int64': 1}]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_cross_shards(self):
        """
        target: test compact cross shards
        method: 1.create with shard_num=2
                2.insert once and flush (two segments, belonging to two shards)
                3.compact and completed
        expected: Verify compact plan sources only one segment
        """
        # insert into two segments with two shard
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=2)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]

        # Actually no merged
        assert len(c_plans.plans) == 2
        for plan in c_plans.plans:
            assert len(plan.sources) == 1

    @pytest.mark.tags(CaseLabel.L3)
    def test_compact_delete_cross_shards(self):
        """
        target: test delete compact cross different shards
        method: 1.create with 2 shards
                2.insert entities into 2 segments
                3.delete one entity from each segment
                4.call compact and get compact plan
        expected: Generate compaction plan for each segment
        """
        shards_num = 2
        # insert into two segments with two shard
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=shards_num)
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        expr = f"{ct.default_int64_field_name} in [0, 99]"
        collection_w.delete(expr)
        assert collection_w.num_entities == tmp_nb
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # compact
        sleep(ct.compact_retention_duration + 1)
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_delete_compact,
                                          check_items={"plans_num": shards_num})

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_cross_partition(self):
        """
        target: test compact cross partitions
        method: 1.create with shard_num=1
                2.create partition and insert, flush
                3.insert _default partition and flush
                4.compact
        expected: Verify two independent compaction plans
        """
        # create collection and partition
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), shards_num=1)
        partition_w = self.init_partition_wrap(collection_wrap=collection_w)

        # insert
        df = cf.gen_default_dataframe_data(tmp_nb)
        collection_w.insert(df)
        assert collection_w.num_entities == tmp_nb
        partition_w.insert(df)
        assert collection_w.num_entities == tmp_nb * 2

        # compaction only applied to indexed segments.
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())

        # compact
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        c_plans = collection_w.get_compaction_plans()[0]

        # since manual compaction, segment should be compacted any way
        assert len(c_plans.plans) == 2
        for plan in c_plans.plans:
            assert len(plan.sources) == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_during_insert(self):
        """
        target: test compact during insert and flush
        method: 1.insert entities into multi segments
                2.start a thread to load and search
                3.compact collection
        expected: Search and compact both successfully
        """
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=ct.default_nb,
                                                                       is_dup=False)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        log.debug(collection_w.index())
        df = cf.gen_default_dataframe_data()

        def do_flush():
            collection_w.insert(df)
            log.debug(collection_w.num_entities)

        # compact during insert
        t = threading.Thread(target=do_flush, args=())
        t.start()
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans()
        t.join()

        # waitting for new segment index and compact
        index_cost = 240
        start = time()
        while True:
            sleep(10)
            collection_w.load()
            # new segment compacted
            seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(seg_info) == 2:
                break
            end = time()
            collection_w.release()
            if end - start > index_cost:
                raise MilvusException(1, f"Waiting more than {index_cost}s for the new segment indexed")

        # compact new segment
        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans()

        # waitting for new segment index and compact
        compact_cost = 180
        start = time()
        while True:
            sleep(1)
            collection_w.load()
            # new segment compacted
            seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
            if len(seg_info) == 1:
                break
            end = time()
            collection_w.release()
            if end - start > compact_cost:
                raise MilvusException(1, f"Waiting more than {compact_cost}s for the new target segment to load")

        # search
        search_res, _ = collection_w.search([df[ct.default_float_vec_field_name][0]],
                                            ct.default_float_vec_field_name,
                                            ct.default_search_params, ct.default_limit)
        assert len(search_res[0]) == ct.default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_during_index(self):
        """
        target: test compact during index
        method: while compact collection start a thread to create index
        expected: No exception
        """
        collection_w = self.collection_insert_multi_segments_one_shard(prefix, nb_of_segment=ct.default_nb,
                                                                       is_dup=False)

        def do_index():
            collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
            assert collection_w.index()[0].params == ct.default_index

        # compact during index
        t = threading.Thread(target=do_index, args=())
        t.start()
        collection_w.compact()
        collection_w.wait_for_compaction_completed(timeout=180)
        collection_w.get_compaction_plans()

        t.join()
        collection_w.load()
        replicas = collection_w.get_replicas()[0]
        replica_num = len(replicas.groups)
        seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
        if not (len(seg_info) == 1*replica_num or len(seg_info) == 2*replica_num):
            assert False

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_during_search(self):
        """
        target: test compact during search
        method: while compact collection start a thread to search
        expected: No exception
        """
        # less than auto-merge threshold 10
        num_of_segment = ct.compact_segment_num_threshold - 1

        # create collection shard_num=1, insert 11 segments, each with one entity
        collection_w = self.collection_insert_multi_segments_one_shard(prefix,
                                                                       num_of_segment=num_of_segment,
                                                                       nb_of_segment=100)

        def do_search():
            for _ in range(5):
                search_res, _ = collection_w.search(cf.gen_vectors(1, ct.default_dim),
                                                    ct.default_float_vec_field_name,
                                                    ct.default_search_params, ct.default_limit)
                assert len(search_res[0]) == ct.default_limit

        # compact during search
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()
        t = threading.Thread(target=do_search, args=())
        t.start()

        collection_w.compact()
        collection_w.wait_for_compaction_completed()
        collection_w.get_compaction_plans(check_task=CheckTasks.check_merge_compact,
                                          check_items={"segment_num": num_of_segment})
        t.join()
