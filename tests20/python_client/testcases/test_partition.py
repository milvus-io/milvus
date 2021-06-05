import threading
import pytest

from pymilvus_orm import Partition
from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckParams

prefix = "partition_"


class TestPartitionParams(TestcaseBase):
    """ Test case of partition interface in parameters"""

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("partition_name, descriptions", [(cf.gen_unique_str(prefix), cf.gen_unique_str("desc_"))])
    def test_partition_default(self, partition_name, descriptions):
        """
        target: verify create a partition
        method: 1. create a partition
        expected: 1. create successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name, description=descriptions,
                                           check_res=CheckParams.partition_property_check)

        # check that the partition has been created
        self.collection_wrap.has_partition(partition_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [""])
    def test_partition_empty_name(self, partition_name):
        """
        target: verify create a partition with empyt name
        method: 1. create a partition empty none name
        expected: 1. raise exception
        """

        # create collection
        self._collection()

        # init partition
        res, cr = self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name, check_res=CheckParams.err_res)
        # waiting to be extracted as a public method
        assert res.code == 1
        assert "Partition tag should not be empty" in res.message

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name, descriptions", [(cf.gen_unique_str(prefix), "")])
    def test_partition_empty_description(self, partition_name, descriptions):
        """
        target: verify create a partition with empty description
        method: 1. create a partition with empty description
        expected: 1. create successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name, description=descriptions,
                                           check_res=CheckParams.partition_property_check)

        # check that the partition has been created
        self.collection_wrap.has_partition(partition_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("collection_name, partition_name, descriptions",
                             [(cf.gen_unique_str(), cf.gen_unique_str(prefix), cf.gen_unique_str())])
    def test_partition_dup_name(self, collection_name, partition_name, descriptions):
        """
        target: verify create partitions with duplicate name
        method: 1. create partitions with duplicate name
        expected: 1. create successfully
                  2. the same partition returned with diff object id
        """

        # create collection
        self._collection(name=collection_name)

        # init two objects of partition
        self._partition_object_multiple(mul_number=2)
        self.partition_mul[0].partition_init(self.collection_wrap.collection, partition_name, descriptions)
        self.partition_mul[1].partition_init(self.collection_wrap.collection, partition_name, descriptions)

        # public check func to be extracted
        assert (id(self.partition_mul[0]) != id(self.partition_mul[1]))
        assert self.partition_mul[0].name == self.partition_mul[1].name
        assert self.partition_mul[0].description == self.partition_mul[1].description

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("descriptions", ct.get_invalid_strs)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_special_chars_description(self, partition_name, descriptions):
        """
        target: verify create a partition with special characters in description
        method: 1. create a partition with special characters in description
        expected: 1. create successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name, description=descriptions,
                                           check_res=CheckParams.partition_property_check)

        self.collection_wrap.has_partition(partition_name)
        assert self.partition_wrap.description == descriptions

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5373")
    def test_partition_default_name(self):
        """
        target: verify create a partition with default name
        method: 1. get the _default partition
                2. create a partition with _default name
        expected: 1. the same partition returned
        """

        # create collection
        self._collection()

        # check that the default partition exists
        self.collection_wrap.has_partition(ct.default_partition_name)

        res_mp, cr = self.collection_wrap.partition(ct.default_partition_name)
        self.partition_wrap.partition_init(self.collection_wrap.collection, ct.default_partition_name)

        assert id(self.partition_wrap.partition) == id(res_mp)

        # m_collection = self._collection()
        # assert m_collection.has_partition(ct.default_partition_name)
        # m_partition = m_collection.partition(ct.default_partition_name)
        # m_partition2, _ = self.partition_wrap.partition_init(m_collection, ct.default_partition_name)
        # assert (id(m_partition2) == id(m_partition))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ct.get_invalid_strs)
    def test_partition_invalid_name(self, partition_name):
        """
        target: verify create a partition with invalid name
        method: 1. create a partition with invalid names
        expected: 1. raise exception
        """

        # create collection
        self._collection()

        # init partition
        res, cr = self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name,
                                                     check_res=CheckParams.err_res)

        # TODO: need an error code issue #5144 and assert independently
        assert "is illegal" in res.message or res.code == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_none_collection(self, partition_name):
        """
        target: verify create a partition with none collection
        method: 1. create a partition with none collection
        expected: 1. raise exception
        """

        # init partition with collection is None
        res, cr = self.partition_wrap.partition_init(collection=None, name=partition_name, check_res=CheckParams.err_res)
        assert "'NoneType' object has no attribute" in res.message

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_drop(self, partition_name):
        """
        target: verify drop a partition in one collection
        method: 1. create a partition in one collection
                2. drop the partition
        expected: 1. drop successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)

        # check that the partition exists
        self.collection_wrap.has_partition(partition_name)

        # drop partition
        self.partition_wrap.drop()

        # check that the partition not exists
        res, cr = self.collection_wrap.has_partition(partition_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5384")
    @pytest.mark.parametrize("search_vectors", [cf.gen_vectors(1, ct.default_dim)])
    def test_partition_release(self, search_vectors):
        """
        target: verify release partition
        method: 1. create a collection and several partitions
                2. insert data into each partition
                3. flush and load the partitions
                4. release partition1
                5. release partition1 twice
        expected: 1. the released partition is released
                  2. the other partition is not released
        """

        # create collection
        self._collection()

        # init two objects of partition
        self._partition_object_multiple(mul_number=2)

        # init two partitions
        self._partition(p_object=self.partition_mul[0])
        self._partition(p_object=self.partition_mul[1])

        # insert data to two partition
        self.partition_mul[0].insert(cf.gen_default_list_data())
        self.partition_mul[1].insert(cf.gen_default_list_data())

        # load two partitions
        self.partition_mul[0].load()
        self.partition_mul[1].load()

        # search two partitions
        res0, cr0 = self.partition_mul[0].search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                                 params={"nprobe": 32}, limit=1)
        res1, cr1 = self.partition_mul[1].search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                                 params={"nprobe": 32}, limit=1)
        assert len(res0) == 1 and len(res1) == 1

        # release the first one of partition
        for _ in range(2):
            self.partition_mul[0].release()

            # check result
            res0, cr0 = self.partition_mul[0].search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                                     params={"nprobe": 32}, limit=1)
            res1, cr1 = self.partition_mul[1].search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                                     params={"nprobe": 32}, limit=1)
            assert len(res0) == 0 and len(res1) == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    @pytest.mark.parametrize("data, nums", [(cf.gen_default_dataframe_data(10), 10),
                                            (cf.gen_default_list_data(1), 1),
                                            (cf.gen_default_tuple_data(10), 10)])
    def test_partition_insert(self, data, nums):
        """
        target: verify insert multi entities by dataFrame
        method: 1. create a collection and a partition
                2. partition.insert(data)
                3. insert data again
        expected: 1. insert data successfully
        """

        # create collection
        self._collection()

        # init partition
        self._partition()
        assert self.partition_wrap.is_empty
        assert self.partition_wrap.num_entities == 0

        # insert data
        self.partition_wrap.insert(data)  # TODO: add ndarray type data
        assert self.partition_wrap.is_empty is False
        assert self.partition_wrap.num_entities == nums

        # insert data
        self.partition_wrap.insert(data)
        assert self.partition_wrap.is_empty is False
        assert self.partition_wrap.num_entities == (nums + nums)


class TestPartitionOperations(TestcaseBase):
    """ Test case of partition interface in operations """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_dropped_collection(self, partition_name):
        """
        target: verify create partition against a dropped collection
        method: 1. create collection1
                2. drop collection1
                3. create partition in collection1
        expected: 1. raise exception
        """

        # create collection
        self._collection()

        # drop collection
        self.collection_wrap.drop()

        # init partition failed
        res, cr = self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name,
                                                     check_res=CheckParams.err_res)

        assert res.code == 1 and "can't find collection" in res.message

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_same_name_in_diff_collections(self, partition_name):
        """
        target: verify create partitions with sanme name in diff collections
        method: 1. create a partition in collection1
                2. create a partition in collection2
        expected: 1. create successfully
        """

        # init two objects of collection
        self._collection_object_multiple(mul_number=2)

        # create two collections
        for c in self.collection_mul:
            self._collection(c_object=c)

        # init partition
        for c in self.collection_mul:
            self.partition_wrap.partition_init(c.collection, partition_name)

        # check result
        for c in self.collection_mul:
            c.has_partition(partition_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_multi_partitions_in_collection(self):
        """
        target: verify create multiple partitions in one collection
        method: 1. create multiple partitions in one collection
        expected: 1. create successfully
        """
        # create collection
        self._collection()

        for _ in range(10):
            partition_name = cf.gen_unique_str(prefix)

            # init partition with different name and check partition is exists
            self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
            self.collection_wrap.has_partition(partition_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_maximum_partitions(self):
        """
        target: verify create maximum partitions
        method: 1. create maximum partitions
                2. create one more partition
        expected: 1. raise exception
        """

        threads_num = 8
        threads = []

        def create_partition(collection, threads_n):
            for _ in range(ct.max_partition_num // threads_n):
                name = cf.gen_unique_str(prefix)
                Partition(collection, name)

        m_collection = self._collection()
        for _ in range(threads_num):
            t = threading.Thread(target=create_partition, args=(m_collection, threads_num))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        p_name = cf.gen_unique_str()
        ex, _ = self.partition_wrap.partition_init(m_collection, p_name,
                                                   check_res=CheckParams.err_res)
        assert ex.code == 1
        assert "maximum partition's number should be limit to 4096" in ex.message

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    @pytest.mark.parametrize("partition_name", [ct.default_partition_name])
    def test_partition_drop_default_partition(self, partition_name):
        """
        target: verify drop the _default partition
        method: 1. drop the _default partition
        expected: 1. raise exception
        """

        # create collection
        self._collection()

        # init partition
        res_mp, cr = self.collection_wrap.partition(ct.default_partition_name)

        # insert data
        res_mp.insert(cf.gen_default_list_data())

        # TODO need a flush?
        assert res_mp.is_empty is False

        # drop partition
        with pytest.raises(Exception) as e:
            res_mp.drop()
            log.error(e)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_drop_partition_twice(self, partition_name):
        """
        target: verify drop the same partition twice
        method: 1.create a partition with default schema
                2. drop the partition
                3. drop the same partition again
        expected: raise exception when 2nd time
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
        self.collection_wrap.has_partition(partition_name)

        # drop partition
        self.partition_wrap.drop()
        assert not self.collection_wrap.has_partition(partition_name)[0]

        # drop partition again
        self.partition_wrap.drop(check_res=CheckParams.err_res)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_create_and_drop_multi_times(self, partition_name):
        """
        target: verify create and drop for times
        method: 1.create a partition with default schema
                2. drop the partition
                3. loop #1 and #2 for times
        expected: create and drop successfully
        """

        # create collection
        self._collection()

        # range for 5 times
        for i in range(5):
            # init partition and check that the partition exists
            self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
            assert self.collection_wrap.has_partition(partition_name)[0] is True

            # drop partition and check that the partition not exists
            self.partition_wrap.drop()
            assert self.collection_wrap.has_partition(partition_name)[0] is False

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("flush", [True, False])
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_drop_non_empty_partition(self, flush, partition_name):
        """
        target: verify drop a partition which has data inserted
        method: 1.create a partition with default schema
                2. insert some data
                3. flush / not flush
                3. drop the partition
        expected: drop successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
        assert self.collection_wrap.has_partition(partition_name)[0] is True

        # insert data to partition
        self.partition_wrap.insert(cf.gen_default_dataframe_data())

        # flush
        if flush:
            # TODO: self.partition_wrap.flush()
            pass

        # drop partition
        self.partition_wrap.drop()
        assert self.collection_wrap.has_partition(partition_name)[0] is False

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("flush", [True, False])
    @pytest.mark.parametrize("partition_name, data", [(cf.gen_unique_str(prefix), cf.gen_default_list_data(nb=10))])
    @pytest.mark.parametrize("index_param", cf.gen_simple_index())
    def test_partition_drop_indexed_partition(self, flush, partition_name, data, index_param):
        """
        target: verify drop an indexed partition
        method: 1.create a partition
                2. insert same data
                3. create an index
                4. flush or not flush
                5. drop the partition
        expected: drop successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
        assert self.collection_wrap.has_partition(partition_name)[0] is True

        # insert data to partition
        self.partition_wrap.insert(data)

        # create index of collection
        self.collection_wrap.create_index(ct.default_float_vec_field_name, index_param)

        # flush
        if flush:
            # TODO: self.partition_wrap.flush()
            pass

        # drop partition
        self.partition_wrap.drop()
        assert self.collection_wrap.has_partition(partition_name)[0] is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_empty_partition(self):
        """
        target: verify release an empty partition
        method: 1.create a partition
                2. release the partition
        expected: release successfully
        """

        # create collection
        self._collection()

        # init partition
        self._partition()
        assert self.partition_wrap.is_empty

        # release partition
        self.partition_wrap.release()
        # TODO: assert no more memory consumed

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_dropped_partition(self):
        """
        target: verify release an dropped partition
        method: 1.create a partition
                2. drop the partition
                2. release the partition
        expected: raise exception
        """

        # create collection
        self._collection()

        # init partition
        self._partition()

        # drop partition
        self.partition_wrap.drop()

        # release partition and check err res
        res, cr = self.partition_wrap.release(check_res=CheckParams.err_res)
        # TODO assert the error code
        log.error(res)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_release_dropped_collection(self, partition_name):
        """
        target: verify release an dropped collection
        method: 1.create a collection and partition
                2. drop the collection
                2. release the partition
        expected: raise exception
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
        assert self.collection_wrap.has_partition(partition_name)[0] is True

        # drop collection
        self.collection_wrap.drop()

        # release partition and check err res
        res, cr = self.partition_wrap.release(check_res=CheckParams.err_res)
        # TODO assert the error code
        log.error(res)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5384")
    @pytest.mark.parametrize("partition_name, search_vectors",
                             [(cf.gen_unique_str(prefix), cf.gen_vectors(1, ct.default_dim))])
    def test_partition_release_after_collection_released(self, partition_name, search_vectors):
        """
        target: verify release a partition after the collection released
        method: 1.create a collection and partition
                2. insert some data
                2. release the collection
                2. release the partition
        expected: partition released successfully
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
        assert self.collection_wrap.has_partition(partition_name)[0] is True

        # insert data to partition
        self.partition_wrap.insert(cf.gen_default_list_data())

        # load partition
        self.partition_wrap.load()

        # search of partition
        res_1, cr = self.partition_wrap.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                               params={"nprobe": 32}, limit=1)
        assert len(res_1) == 1

        # release collection
        self.collection_wrap.release()

        # search of partition
        res_2, cr = self.partition_wrap.search(data=search_vectors, anns_field=ct.default_float_vec_field_name,
                                               params={"nprobe": 32}, limit=1)
        assert len(res_2) == 0

        # release partition
        self.partition_wrap.release()
        # TODO assert release successfully

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    @pytest.mark.parametrize("partition_name, data", [(ct.default_partition_name, cf.gen_default_dataframe_data())])
    def test_partition_insert_default_partition(self, partition_name, data):
        """
        target: verify insert data into _default partition
        method: 1.create a collection
                2. insert some data into _default partition
        expected: insert successfully
        """

        # create collection
        self._collection()

        # init partition
        res_mp, cr = self.collection_wrap.partition(partition_name)

        # insert data to partition
        res_mp.insert(data)
        assert res_mp.num_entities == len(data)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_partition(self):
        """
        target: verify insert data into dropped partition
        method: 1.create a collection
                2. insert some data into dropped partition
        expected: raise exception
        """

        # create collection
        self._collection()

        # init partition
        self._partition()

        # drop partition
        self.partition_wrap.drop()

        # insert data to partition
        res, cr = self.partition_wrap.insert(cf.gen_default_dataframe_data(), check_res=CheckParams.err_res)
        # TODO: assert the error code
        log.error(res)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_insert_dropped_collection(self, partition_name):
        """
        target: verify insert data into dropped collection
        method: 1.create a collection
                2. insert some data into dropped collection
        expected: raise exception
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)
        assert self.collection_wrap.has_partition(partition_name)[0] is True

        # drop collection
        self.collection_wrap.drop()

        # insert data to partition
        res, cr = self.partition_wrap.insert(cf.gen_default_dataframe_data(), check_res=CheckParams.err_res)
        # TODO: assert the error code
        log.error(res)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_partition_insert_maximum_size_data(self, data):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1.create a partition
                2. insert maximum size data
        expected: insert successfully
        """

        # create collection
        self._collection()

        # init partition
        self._partition()

        # insert data to partition
        max_size = 100000  # TODO: clarify the max size of data
        self.partition_wrap.insert(cf.gen_default_dataframe_data(max_size))
        # TODO: need a flush for #5302
        assert self.partition_wrap.num_entities == max_size

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim, expected_err",
                             [(ct.default_dim - 1, "error"), (ct.default_dim + 1, "error")])
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_insert_mismatched_dimensions(self, dim, expected_err, partition_name):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1.create a collection with default dim
                2. insert dismatch dim data
        expected: raise exception
        """

        # create collection
        self._collection()

        # init partition
        self.partition_wrap.partition_init(self.collection_wrap.collection, partition_name)

        data = cf.gen_default_list_data(nb=10, dim=dim)
        # insert data to partition
        res, cr = self.partition_wrap.insert(data, check_res=CheckParams.err_res)
        # TODO: assert expected_err in error code
        log.error(res)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("sync", [True, False])
    def test_partition_insert_sync(self, sync):
        """
        target: verify insert sync
        method: 1.create a partition
                2. insert data in sync
        expected: insert successfully
        """
        pass

