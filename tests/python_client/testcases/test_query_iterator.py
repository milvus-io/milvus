import os
import random

from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_base import TestcaseBase
import pytest


prefix = "query_iter_"


class TestQueryIterator(TestcaseBase):
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("primary_field", [ct.default_string_field_name, ct.default_int64_field_name])
    @pytest.mark.parametrize("with_growing", [False, True])
    def test_query_iterator_normal(self, primary_field, with_growing):
        """
        target: test query iterator normal
        method: 1. query iterator
                2. check the result, expect pk
                verify: no pk lost in interator results
                3. query iterator with checkpoint file
                4. iterator.next() for 10 times
                5. delete some entities before calling a new query iterator
                6. call a new query iterator with the same checkpoint file, with diff batch_size and output_fields
                7. iterator.next() until the end
                verify:
                  1. no pk lost in interator results for the 2 iterators
                  2. no dup pk in the 2 iterators
        expected: query iterators successfully
        """
        # 1. initialize with data
        nb = 4000
        batch_size = 200
        collection_w, _, _, insert_ids, _ = \
            self.init_collection_general(prefix, True, is_index=False, nb=nb, is_flush=True,
                                         auto_id=False, primary_field=primary_field)
        collection_w.create_index(ct.default_float_vec_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. query iterator
        expr = "float >= 0"
        collection_w.query_iterator(batch_size, expr=expr,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": nb,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": batch_size})
        # 3. query iterator with checkpoint file
        iterator_cp_file = f"/tmp/it_{collection_w.name}_cp"
        iterator = collection_w.query_iterator(batch_size, expr=expr, iterator_cp_file=iterator_cp_file)[0]
        iter_times = 0
        first_iter_times = nb // batch_size // 2      # only iterate half of the data for the 1st time
        pk_list1 = []
        while iter_times < first_iter_times:
            iter_times += 1
            res = iterator.next()
            if len(res) == 0:
                iterator.close()
                assert False, f"The iterator ends before {first_iter_times} times iterators: iter_times: {iter_times}"
                break
            for i in range(len(res)):
                pk_list1.append(res[i][primary_field])
        file_exist = os.path.isfile(iterator_cp_file)
        assert file_exist is True, "The checkpoint file exists without iterator close"

        # 4. try to delete and insert some entities before calling a new query iterator
        delete_ids = random.sample(insert_ids[:nb//2], 101) + random.sample(insert_ids[nb//2:], 101)
        del_res, _ = collection_w.delete(expr=f"{primary_field} in {delete_ids}")
        assert del_res.delete_count == len(delete_ids)

        data = cf.gen_default_list_data(nb=333, start=nb)
        collection_w.insert(data)
        if not with_growing:
            collection_w.flush()

        # 5. call a new query iterator with the same checkpoint file to continue the first iterator
        iterator2 = collection_w.query_iterator(batch_size*2, expr=expr,
                                                output_fields=[primary_field, ct.default_float_field_name],
                                                iterator_cp_file=iterator_cp_file)[0]
        while True:
            res = iterator2.next()
            if len(res) == 0:
                iterator2.close()
                break
            for i in range(len(res)):
                pk_list1.append(res[i][primary_field])
        # 6. verify
        assert len(pk_list1) == len(set(pk_list1)) == nb
        file_exist = os.path.isfile(iterator_cp_file)
        assert file_exist is False, "The checkpoint was deleted after the iterator close"

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_iterator_using_default_batch_size(self):
        """
        target: test query iterator normal
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. query iterator
        collection_w.query_iterator(check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": ct.default_batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [500, 1000, 1777])
    def test_query_iterator_with_offset(self, offset):
        """
        target: test query iterator normal
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        batch_size = 300
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        expr = "int64 >= 0"
        collection_w.query_iterator(batch_size, expr=expr, offset=offset,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb - offset,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_query_iterator_output_different_vector_type(self, vector_data_type):
        """
        target: test query iterator with output fields
        method: 1. query iterator output different vector type
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        batch_size = 400
        collection_w = self.init_collection_general(prefix, True,
                                                    vector_data_type=vector_data_type)[0]
        # 2. query iterator
        expr = "int64 >= 0"
        collection_w.query_iterator(batch_size, expr=expr,
                                    output_fields=[ct.default_float_vec_field_name],
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("batch_size", [10, 777, 2000])
    def test_query_iterator_with_different_batch_size(self, batch_size):
        """
        target: test query iterator normal
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        offset = 500
        collection_w = self.init_collection_general(prefix, True, is_index=False)[0]
        collection_w.create_index(ct.default_float_vec_field_name, {"metric_type": "L2"})
        collection_w.load()
        # 2. search iterator
        expr = "int64 >= 0"
        collection_w.query_iterator(batch_size=batch_size, expr=expr, offset=offset,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": ct.default_nb - offset,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [0, 10, 1000])
    @pytest.mark.parametrize("limit", [0, 100, 10000])
    def test_query_iterator_with_different_limit(self, limit, offset):
        """
        target: test query iterator normal
        method: 1. query iterator
                2. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        # 2. query iterator
        Count = limit if limit + offset <= ct.default_nb else ct.default_nb - offset
        collection_w.query_iterator(limit=limit, expr="", offset=offset,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": max(Count, 0),
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": ct.default_batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_iterator_invalid_batch_size(self):
        """
        target: test query iterator invalid limit and offset
        method: query iterator using invalid limit and offset
        expected: raise exception
        """
        # 1. initialize with data
        nb = 17000  # set nb > 16384
        collection_w = self.init_collection_general(prefix, True, nb=nb)[0]
        # 2. search iterator
        expr = "int64 >= 0"
        error = {"err_code": 1, "err_msg": "batch size cannot be less than zero"}
        collection_w.query_iterator(batch_size=-1, expr=expr, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("batch_size", [500])
    @pytest.mark.parametrize("auto_id", [False])
    def test_query_iterator_empty_expr_with_cp_file_for_times(self, auto_id, batch_size):
        """
        target: verify 2 query iterators with/out checkpoint file works independently
        method: 1. create a collection
                2. query the 1st iterator with empty expr and checkpoint file
                3. iterator.next() for some times
                4. call a new query iterator with the same checkpoint file
        expected: verify the 2nd iterator can get the whole results
        """
        # 0. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, auto_id=auto_id)[0:4]

        # 1. call a new query iterator and iterator for some times
        iterator_cp_file = f"/tmp/it_{collection_w.name}_cp"
        iterator = collection_w.query_iterator(batch_size=batch_size//2, iterator_cp_file=iterator_cp_file)[0]
        iter_times = 0
        first_iter_times = ct.default_nb // batch_size // 2 // 2  # only iterate half of the data for the 1st time
        while iter_times < first_iter_times:
            iter_times += 1
            res = iterator.next()
            if len(res) == 0:
                iterator.close()
                assert False, f"The iterator ends before {first_iter_times} times iterators: iter_times: {iter_times}"
                break

        # 2. call a new query iterator to get all the results of the collection
        collection_w.query_iterator(batch_size=batch_size,
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"batch_size": batch_size,
                                                 "count": ct.default_nb,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "exp_ids": insert_ids})
        file_exist = os.path.isfile(iterator_cp_file)
        assert file_exist is True, "The checkpoint exists if not iterator.close()"
        iterator.close()
        file_exist = os.path.isfile(iterator_cp_file)
        assert file_exist is False, "The checkpoint was deleted after the iterator close"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("offset", [1000])
    @pytest.mark.parametrize("batch_size", [500, 1000])
    def test_query_iterator_expr_empty_with_random_pk_pagination(self, batch_size, offset):
        """
        target: test query iterator with empty expression
        method: create a collection using random pk, query empty expression with a limit
        expected: return topK results by order
        """
        # 1. initialize with data
        collection_w, _, _, insert_ids = self.init_collection_general(prefix, True, random_primary_key=True)[0:4]

        # 2. query with empty expr and check the result
        exp_ids = sorted(insert_ids)
        collection_w.query_iterator(batch_size, output_fields=[ct.default_string_field_name],
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"batch_size": batch_size,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "count": ct.default_nb, "exp_ids": exp_ids})

        # 3. query with pagination
        exp_ids = sorted(insert_ids)[offset:]
        collection_w.query_iterator(batch_size, offset=offset, output_fields=[ct.default_string_field_name],
                                    check_task=CheckTasks.check_query_iterator,
                                    check_items={"batch_size": batch_size,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "count": ct.default_nb - offset, "exp_ids": exp_ids})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("primary_field", [ct.default_string_field_name, ct.default_int64_field_name])
    def test_query_iterator_with_dup_pk(self, primary_field):
        """
        target: test query iterator with duplicate pk
        method: 1. insert entities with duplicate pk
                2. query iterator
                3. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        nb = 3000
        collection_w = self.init_collection_general(prefix, insert_data=False, is_index=False,
                                                    auto_id=False, primary_field=primary_field)[0]
        # insert entities with duplicate pk
        data = cf.gen_default_list_data(nb=nb)
        for _ in range(3):
            collection_w.insert(data)
        collection_w.flush()
        # create index
        index_type = "HNSW"
        index_params = {"index_type": index_type, "metric_type": ct.default_L0_metric,
                        "params": cf.get_index_params_params(index_type)}
        collection_w.create_index(ct.default_float_vec_field_name, index_params)
        collection_w.load()
        # 2. query iterator
        collection_w.query_iterator(check_task=CheckTasks.check_query_iterator,
                                    check_items={"count": nb,
                                                 "primary_field": collection_w.primary_field.name,
                                                 "batch_size": ct.default_batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip("issue #37109, need debug due to the resolution of the issue")
    def test_query_iterator_on_two_collections(self):
        """
        target: test query iterator on two collections
        method: 1. create two collections
                2. query iterator on the first collection
                3. check the result, expect pk
        expected: query successfully
        """
        # 1. initialize with data
        collection_w = self.init_collection_general(prefix, True)[0]
        collection_w2 = self.init_collection_general(prefix, False, primary_field=ct.default_string_field_name)[0]

        data = cf.gen_default_list_data(nb=ct.default_nb, primary_field=ct.default_string_field_name)
        string_values = [cf.gen_str_by_length(20) for _ in range(ct.default_nb)]
        data[2] = string_values
        collection_w2.insert(data)

        # 2. call a new query iterator and iterator for some times
        batch_size = 150
        iterator_cp_file = f"/tmp/it_{collection_w.name}_cp"
        iterator2 = collection_w2.query_iterator(batch_size=batch_size // 2, iterator_cp_file=iterator_cp_file)[0]
        iter_times = 0
        first_iter_times = ct.default_nb // batch_size // 2 // 2  # only iterate half of the data for the 1st time
        while iter_times < first_iter_times:
            iter_times += 1
            res = iterator2.next()
            if len(res) == 0:
                iterator2.close()
                assert False, f"The iterator ends before {first_iter_times} times iterators: iter_times: {iter_times}"
                break

        # 3. query iterator on the second collection with the same checkpoint file

        iterator = collection_w.query_iterator(batch_size=batch_size, iterator_cp_file=iterator_cp_file)[0]
        print(iterator.next())
