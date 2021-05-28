import threading
import pytest

from pymilvus_orm import Partition
from base.client_request import ApiReq
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckParams


prefix = "partition_"


class TestPartitionParams(ApiReq):
    """ Test case of partition interface in parameters"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_default(self):
        """
        target: verify create a partition
        method: 1. create a partition
        expected: 1. create successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        descriptions = cf.gen_unique_str("desc_")
        _, _ = self.partition.partition_init(
            m_collection, p_name, description=descriptions,
            check_res=CheckParams.partition_property_check
        )
        assert (m_collection.has_partition(p_name))

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_empty_name(self):
        """
        target: verify create a partition with empyt name
        method: 1. create a partition empty none name
        expected: 1. raise exception
        """
        m_collection = self._collection()
        p_name = ""
        ex, _ = self.partition.partition_init(m_collection, p_name)
        assert ex.code == 1
        assert "Partition tag should not be empty" in ex.message

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_empty_description(self):
        """
        target: verify create a partition with empty description
        method: 1. create a partition with empty description
        expected: 1. create successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        descriptions = ""
        _, _ = self.partition.partition_init(
            m_collection, p_name, description=descriptions,
            check_res=CheckParams.partition_property_check)
        assert (m_collection.has_partition(p_name))

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_dup_name(self):
        """
        target: verify create partitions with duplicate name
        method: 1. create partitions with duplicate name
        expected: 1. create successfully
                  2. the same partition returned with diff object id
        """
        m_collection = self._collection(name=cf.gen_unique_str())
        p_name = cf.gen_unique_str(prefix)
        descriptions = cf.gen_unique_str()
        m_partition, _ = self.partition.partition_init(m_collection, p_name, descriptions)
        m_partition2, _ = self.partition.partition_init(m_collection, p_name, descriptions)
        assert (id(m_partition2) != id(m_partition))
        assert m_partition.name == m_partition2.name
        assert m_partition.description == m_partition2.description

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_specialchars_description(self, get_invalid_string):
        """
        target: verify create a partition with special characters in description
        method: 1. create a partition with special characters in description
        expected: 1. create successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        descriptions = get_invalid_string
        m_partition, _ = self.partition.partition_init(
            m_collection, p_name, description=descriptions,
            check_res=CheckParams.partition_property_check)
        assert (m_collection.has_partition(p_name))
        assert (m_partition.description == descriptions)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5373")
    def test_partition_default_name(self):
        """
        target: verify create a partition with default name
        method: 1. get the _default partition
                2. create a partition with _default name
        expected: 1. the same partition returned
        """
        m_collection = self._collection()
        assert m_collection.has_partition(ct.default_partition_name)
        m_partition = m_collection.partition(ct.default_partition_name)
        m_partition2, _ = self.partition.partition_init(m_collection, ct.default_partition_name)
        assert (id(m_partition2) == id(m_partition))

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_invalid_name(self, get_invalid_string):
        """
        target: verify create a partition with invalid name
        method: 1. create a partition with invalid names
        expected: 1. raise exception
        """
        m_collection = self._collection()
        p_name = get_invalid_string
        ex, _ = self.partition.partition_init(m_collection, p_name)
        # assert ex.code == 1
        # TODO: need an error code issue #5144 and assert independently
        assert "is illegal" in ex.message or ex.code == 1

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_none_collection(self):
        """
        target: verify create a partition with none collection
        method: 1. create a partition with none collection
        expected: 1. raise exception
        """
        m_collection = None
        p_name = cf.gen_unique_str(prefix)
        ex, _ = self.partition.partition_init(m_collection, p_name)
        assert "'NoneType' object has no attribute" in ex.message

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_drop(self):
        """
        target: verify drop a partition in one collection
        method: 1. create a partition in one collection
                2. drop the partition
        expected: 1. drop successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        m_partition.drop()
        assert m_collection.has_partition(p_name) is False

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5384")
    def test_partition_release(self):
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
        m_partition = self._partition()
        m_partition2 = self._partition()
        m_partition.insert(cf.gen_default_list_data())
        m_partition2.insert(cf.gen_default_list_data())
        m_partition.load()
        m_partition2.load()
        search_vec = cf.gen_vectors(1, ct.default_dim)
        result = m_partition.search(data=search_vec,
                                    anns_field=ct.default_float_vec_field_name,
                                    params={"nprobe": 32},
                                    limit=1
                                    )
        result2 = m_partition2.search(data=search_vec,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32},
                                      limit=1
                                      )
        assert len(result) == 1
        assert len(result2) == 1

        for _ in range(2):
            m_partition.release()
            result = m_partition.search(data=search_vec,
                                        anns_field=ct.default_float_vec_field_name,
                                        params={"nprobe": 32},
                                        limit=1
                                        )
            result2 = m_partition2.search(data=search_vec,
                                          anns_field=ct.default_float_vec_field_name,
                                          params={"nprobe": 32},
                                          limit=1
                                          )
            assert len(result) == 0
            assert len(result2) == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    @pytest.mark.parametrize("data, nums", [(cf.gen_default_dataframe_data(10), 10),
                                            (cf.gen_default_list_data(1), 1),
                                            (cf.gen_default_tuple_data(10), 10)
                                            ])
    def test_partition_insert(self, data, nums):
        """
        target: verify insert multi entities by dataFrame
        method: 1. create a collection and a partition
                2. partition.insert(data)
                3. insert data again
        expected: 1. insert data successfully
        """
        m_partition = self._partition()
        assert m_partition.is_empty
        assert m_partition.num_entities == 0
        m_partition.insert(data)  # TODO: add ndarray type data
        assert m_partition.is_empty is False
        assert m_partition.num_entities == nums
        m_partition.insert(data)
        assert m_partition.is_empty is False
        assert m_partition.num_entities == (nums + nums)


class TestPartitionOperations(ApiReq):
    """ Test case of partition interface in operations """

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_dropped_collection(self):
        """
        target: verify create partition against a dropped collection
        method: 1. create collection1
                2. drop collection1
                3. create partition in collection1
        expected: 1. raise exception
        """
        m_collection = self._collection()
        m_collection.drop()
        p_name = cf.gen_unique_str(prefix)
        ex, _ = self.partition.partition_init(m_collection, p_name)
        assert ex.code == 1
        assert "can't find collection" in ex.message

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_same_name_in_diff_collections(self):
        """
        target: verify create partitions with sanme name in diff collections
        method: 1. create a partition in collection1
                2. create a partition in collection2
        expected: 1. create successfully
        """
        m_collection = self._collection(cf.gen_unique_str())
        m_collection2 = self._collection(cf.gen_unique_str())
        p_name = cf.gen_unique_str(prefix)
        _, _ = self.partition.partition_init(m_collection, p_name)
        _, _ = self.partition.partition_init(m_collection2, p_name)
        assert m_collection.has_partition(p_name)
        assert m_collection2.has_partition(p_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_multi_partitions_in_collection(self):
        """
        target: verify create multiple partitions in one collection
        method: 1. create multiple partitions in one collection
        expected: 1. create successfully
        """
        m_collection = self._collection()
        for _ in range(10):
            p_name = cf.gen_unique_str(prefix)
            _, _ = self.partition.partition_init(m_collection, p_name)
            assert m_collection.has_partition(p_name)

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
        # if args["handler"] == "HTTP":
        #    pytest.skip("skip in http mode")

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
        ex, _ = self.partition.partition_init(m_collection, p_name)
        assert ex.code == 1
        assert "maximum partition's number should be limit to 4096" in ex.message

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_partition_drop_default_partition(self):
        """
        target: verify drop the _default partition
        method: 1. drop the _default partition
        expected: 1. raise exception
        """
        m_collection = self._collection()
        default_partition = m_collection.partition(ct.default_partition_name)
        default_partition.insert(cf.gen_default_list_data())
        # TODO need a flush?
        assert default_partition.is_empty is False
        with pytest.raises(Exception) as e:
            default_partition.drop()
        log.info(e)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop_partition_twice(self):
        """
        target: verify drop the same partition twice
        method: 1.create a partition with default schema
                2. drop the partition
                3. drop the same partition again
        expected: raise exception when 2nd time
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        m_partition.drop()
        assert m_collection.has_partition(p_name) is False
        with pytest.raises(Exception) as e:
            m_partition.drop()
            log.info(e)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_create_and_drop_multi_times(self):
        """
        target: verify create and drop for times
        method: 1.create a partition with default schema
                2. drop the partition
                3. loop #1 and #2 for times
        expected: create and drop successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        for _ in range(5):
            m_partition, _ = self.partition.partition_init(m_collection, p_name)
            assert m_collection.has_partition(p_name)
            m_partition.drop()
            assert m_collection.has_partition(p_name) is False

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("flush", [True, False])
    def test_partition_drop_non_empty_partition(self, flush):
        """
        target: verify drop a partition which has data inserted
        method: 1.create a partition with default schema
                2. insert some data
                3. flush / not flush
                3. drop the partition
        expected: drop successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        m_partition.insert(cf.gen_default_dataframe_data())
        if flush:
            # conn = self._connect()
            # conn.flush([m_collection.name])
            # TODO: m_partition.flush()
            pass
        m_partition.drop()
        assert m_collection.has_partition(p_name) is False

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("flush", [True, False])
    def test_partition_drop_indexed_partition(self, flush, get_index_param):
        """
        target: verify drop an indexed partition
        method: 1.create a partition
                2. insert same data
                3. create an index
                4. flush or not flush
                5. drop the partition
        expected: drop successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        data = cf.gen_default_list_data(nb=10)
        m_partition.insert(data)
        index_param = get_index_param
        log.info(m_collection.schema)
        m_collection.create_index(
            ct.default_float_vec_field_name,
            index_param)
        if flush:
            # TODO: m_partition.flush()
            pass
        m_partition.drop()
        assert m_collection.has_partition(p_name) is False
        log.info("collection name: %s", m_collection.name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_empty_partition(self):
        """
        target: verify release an empty partition
        method: 1.create a partition
                2. release the partition
        expected: release successfully
        """
        m_partition = self._partition()
        assert m_partition.is_empty
        m_partition.release()
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
        m_partition = self._partition()
        m_partition.drop()
        with pytest.raises(Exception) as e:
            m_partition.release()
            log.info(e)
            # TODO assert the error code

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_dropped_collection(self):
        """
        target: verify release an dropped collection
        method: 1.create a collection and partition
                2. drop the collection
                2. release the partition
        expected: raise exception
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        m_collection.drop()
        with pytest.raises(Exception) as e:
            m_partition.release()
            log.info(e)
            # TODO assert the error code

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5384")
    def test_partition_release_after_collection_released(self):
        """
        target: verify release a partition after the collection released
        method: 1.create a collection and partition
                2. insert some data
                2. release the collection
                2. release the partition
        expected: partition released successfully
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        m_partition.insert(cf.gen_default_list_data())
        m_partition.load()
        search_vec = cf.gen_vectors(1, ct.default_dim)
        result = m_partition.search(data=search_vec,
                                    anns_field=ct.default_float_vec_field_name,
                                    params={"nprobe": 32},
                                    limit=1
                                    )
        assert len(result) == 1
        m_collection.release()
        result = m_partition.search(data=search_vec,
                                    anns_field=ct.default_float_vec_field_name,
                                    params={"nprobe": 32},
                                    limit=1
                                    )
        assert len(result) == 0
        m_partition.release()
        # TODO assert release successfully

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_partition_insert_default_partition(self):
        """
        target: verify insert data into _default partition
        method: 1.create a collection
                2. insert some data into _default partition
        expected: insert successfully
        """
        m_collection = self._collection()
        default_partition = m_collection.partition(ct.default_partition_name)
        data = cf.gen_default_dataframe_data()
        default_partition.insert(data)
        assert default_partition.num_entities == len(data)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_partition(self):
        """
        target: verify insert data into dropped partition
        method: 1.create a collection
                2. insert some data into dropped partition
        expected: raise exception
        """
        m_partition = self._partition()
        m_partition.drop()
        with pytest.raises(Exception) as e:
            m_partition.insert(cf.gen_default_dataframe_data())
            log.info(e)
            # TODO: assert the error code

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_collection(self):
        """
        target: verify insert data into dropped collection
        method: 1.create a collection
                2. insert some data into dropped collection
        expected: raise exception
        """
        m_collection = self._collection()
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        assert m_collection.has_partition(p_name)
        m_collection.drop()
        with pytest.raises(Exception) as e:
            m_partition.insert(cf.gen_default_dataframe_data())
            log.info(e)
            # TODO: assert the error code

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_partition_insert_maximum_size_data(self):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1.create a partition
                2. insert maximum size data
        expected: insert successfully
        """
        m_partition = self._partition()
        max_size = 100000  # TODO: clarify the max size of data
        data = cf.gen_default_dataframe_data(max_size)
        m_partition.insert(data)
        assert m_partition.num_entities == max_size

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim, expected_err",
                             [(ct.default_dim - 1, "error"), (ct.default_dim + 1, "error")])
    def test_partition_insert_dismatched_dimensions(self, dim, expected_err):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1.create a collection with default dim
                2. insert dismatch dim data
        expected: raise exception
        """
        m_collection = self._collection(schema=cf.gen_default_collection_schema())
        p_name = cf.gen_unique_str(prefix)
        m_partition, _ = self.partition.partition_init(m_collection, p_name)
        data = cf.gen_default_list_data(nb=10, dim=dim)
        with pytest.raises(Exception) as e:
            m_partition.insert(data)
            log.info(e)
            # TODO: assert expected_err in error code

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

