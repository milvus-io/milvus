import threading
import pytest

from base.partition_wrapper import ApiPartitionWrapper
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.code_mapping import PartitionErrorMessage

prefix = "partition_"


class TestPartitionParams(TestcaseBase):
    """ Test case of partition interface in parameters"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_default(self):
        """
        target: verify create a partition
        method: 1. create a partition
        expected: 1. create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str("desc_")
        self.init_partition_wrap(collection_w, partition_name,
                                 description=description,
                                 check_task=CheckTasks.check_partition_property,
                                 check_items={"name": partition_name, "description": description,
                                              "is_empty": True, "num_entities": 0}
                                 )

        # check that the partition has been created
        assert collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [""])
    def test_partition_empty_name(self, partition_name):
        """
        target: verify create a partition with empty name
        method: 1. create a partition with empty name
        expected: 1. raise exception
        """
        # create a collection
        collection_w = self.init_collection_wrap()

        # create partition
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1, ct.err_msg: "Partition name should not be empty"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_empty_description(self):
        """
        target: verify create a partition with empty description
        method: 1. create a partition with empty description
        expected: 1. create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # init partition
        partition_name = cf.gen_unique_str(prefix)
        description = ""
        self.init_partition_wrap(collection_w, partition_name,
                                 description=description,
                                 check_task=CheckTasks.check_partition_property,
                                 check_items={"name": partition_name, "description": description,
                                              "is_empty": True, "num_entities": 0}
                                 )

        # check that the partition has been created
        assert collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_max_description_length(self):
        """
        target: verify create a partition with 255 length name and 1024 length description
        method: 1. create a partition with 255 length name and 1024 length description
        expected: 1. create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # init partition
        partition_name = cf.gen_str_by_length(255)
        description = cf.gen_str_by_length(2048)
        self.init_partition_wrap(collection_w, partition_name,
                                 description=description,
                                 check_task=CheckTasks.check_partition_property,
                                 check_items={"name": partition_name, "description": description,
                                              "is_empty": True}
                                 )

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_dup_name(self):
        """
        target: verify create partitions with duplicate names
        method: 1. create partitions with duplicate names
        expected: 1. create successfully
                  2. the same partition returned with diff object ids
        """
        # create a collection
        collection_w = self.init_collection_wrap()

        # create two partitions
        partition_name = cf.gen_unique_str(prefix)
        description = cf.gen_unique_str()
        partition_w1 = self.init_partition_wrap(collection_w, partition_name, description)
        partition_w2 = self.init_partition_wrap(collection_w, partition_name, description)

        # public check func to be extracted
        assert id(partition_w1.partition) != id(partition_w2.partition)
        assert partition_w1.name == partition_w2.name
        assert partition_w1.description == partition_w2.description

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("description", ct.get_invalid_strs)
    def test_partition_special_chars_description(self, description):
        """
        target: verify create a partition with special characters in description
        method: 1. create a partition with special characters in description
        expected: 1. create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        self.init_partition_wrap(collection_w, partition_name,
                                 description=description,
                                 check_task=CheckTasks.check_partition_property,
                                 check_items={"name": partition_name, "description": description,
                                              "is_empty": True, "num_entities": 0}
                                 )
        assert collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_default_name(self):
        """
        target: verify create a partition with default name
        method: 1. get the _default partition
                2. create a partition with _default name
        expected: 1. the same partition returned
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # check that the default partition exists
        assert collection_w.has_partition(ct.default_partition_name)[0]

        # check that can get the _default partition
        collection, _ = collection_w.partition(ct.default_partition_name)

        # check that init the _default partition object
        partition_w = self.init_partition_wrap(collection_w, ct.default_partition_name)
        assert collection.name == partition_w.name

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_max_length_name(self):
        """
        target: verify create a partition with max length(256) name
        method: 1. create a partition with max length name
        expected: 1. raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_str_by_length(256)
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1, 'err_msg': "is illegal"}
                                           )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", ct.get_invalid_strs)
    def test_partition_invalid_name(self, partition_name):
        """
        target: verify create a partition with invalid name
        method: 1. create a partition with invalid names
        expected: 1. raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1, 'err_msg': "is illegal"}
                                           )
        # TODO: need an error code issue #5144 and assert independently

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_none_collection(self):
        """
        target: verify create a partition with none collection
        method: 1. create a partition with none collection
        expected: 1. raise exception
        """
        # create partition with collection is None
        partition_name = cf.gen_unique_str(prefix)
        self.partition_wrap.init_partition(collection=None, name=partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1,
                                                        ct.err_msg: "must be pymilvus.Collection"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop(self):
        """
        target: verify drop a partition in one collection
        method: 1. create a partition in one collection
                2. drop the partition
        expected: 1. drop successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)

        # check that the partition exists
        assert collection_w.has_partition(partition_name)[0]

        # drop partition
        partition_w.drop()

        # check that the partition not exists
        assert not collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release(self):
        """
        target: verify release partition
        method: 1. create a collection and two partitions
                2. insert data into each partition
                3. flush and load the both partitions
                4. release partition1
                5. release partition1 twice
        expected: 1. the 1st partition is released
                  2. the 2nd partition is not released
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create two partitions
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)

        # insert data to two partition
        partition_w1.insert(cf.gen_default_list_data())
        partition_w2.insert(cf.gen_default_list_data())

        # load two partitions
        partition_w1.load()
        partition_w2.load()

        # search two partitions
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        res1, _ = partition_w1.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1)
        res2, _ = partition_w2.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1)
        assert len(res1) == 1 and len(res2) == 1

        # release the first partition
        partition_w1.release()

        # check result
        res1, _ = partition_w1.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1,
                                      check_task=ct.CheckTasks.err_res,
                                      check_items={ct.err_code: 1, ct.err_msg: "partitions have been released"})
        res2, _ = partition_w2.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1)
        assert len(res2) == 1

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", [cf.gen_default_dataframe_data(10),
                                      cf.gen_default_list_data(10),
                                      cf.gen_default_tuple_data(10)])
    def test_partition_insert(self, data):
        """
        target: verify insert multi entities
        method: 1. create a collection and a partition
                2. partition.insert(data)
                3. insert data again
        expected: 1. insert data successfully
        """
        nums = 10
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name,
                                               check_task=CheckTasks.check_partition_property,
                                               check_items={"name": partition_name,
                                                            "is_empty": True, "num_entities": 0}
                                               )

        # insert data
        partition_w.insert(data)
        # self._connect().flush([collection_w.name])     # don't need flush for issue #5737
        assert not partition_w.is_empty
        assert partition_w.num_entities == nums

        # insert data
        partition_w.insert(data)
        # self._connect().flush([collection_w.name])
        assert not partition_w.is_empty
        assert partition_w.num_entities == (nums + nums)


class TestPartitionOperations(TestcaseBase):
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
        # create collection
        collection_w = self.init_collection_wrap()

        # drop collection
        collection_w.drop()

        # create partition failed
        self.partition_wrap.init_partition(collection_w.collection, cf.gen_unique_str(prefix),
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_same_name_in_diff_collections(self):
        """
        target: verify create partitions with same name in diff collections
        method: 1. create a partition in collection1
                2. create a partition in collection2
        expected: 1. create successfully
        """
        # create two collections
        collection_w1 = self.init_collection_wrap()
        collection_w2 = self.init_collection_wrap()

        # create 2 partitions in 2 diff collections
        partition_name = cf.gen_unique_str(prefix)
        self.init_partition_wrap(collection_wrap=collection_w1, name=partition_name)
        self.init_partition_wrap(collection_wrap=collection_w2, name=partition_name)

        # check result
        assert collection_w1.has_partition(partition_name)[0]
        assert collection_w2.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_multi_partitions_in_collection(self):
        """
        target: verify create multiple partitions in one collection
        method: 1. create multiple partitions in one collection
        expected: 1. create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        for _ in range(10):
            partition_name = cf.gen_unique_str(prefix)
            # create partition with different names and check the partition exists
            self.init_partition_wrap(collection_w, partition_name)
            assert collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="skip temporarily for debug")
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
                par_wrap = ApiPartitionWrapper()
                par_wrap.init_partition(collection, name, check_task=CheckTasks.check_nothing)

        collection_w = self.init_collection_wrap()
        for _ in range(threads_num):
            t = threading.Thread(target=create_partition, args=(collection_w.collection, threads_num))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        p_name = cf.gen_unique_str()
        self.partition_wrap.init_partition(
            collection_w.collection, p_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1,
                         ct.err_msg: "maximum partition's number should be limit to 4096"})

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_drop_default_partition(self):
        """
        target: verify drop the _default partition
        method: drop the _default partition
        expected: raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # get the default partition
        default_partition, _ = collection_w.partition(ct.default_partition_name)
        partition_w = self.init_partition_wrap(collection_w, ct.default_partition_name)
        assert default_partition.name == partition_w.name

        # verify that drop partition with error
        partition_w.drop(check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1, ct.err_msg: "default partition cannot be deleted"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop_partition_twice(self):
        """
        target: verify drop the same partition twice
        method: 1.create a partition with default schema
                2. drop the partition
                3. drop the same partition again
        expected: raise exception when 2nd time
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        collection_w.has_partition(partition_name)

        # drop partition
        partition_w.drop()
        assert not collection_w.has_partition(partition_name)[0]

        # verify that drop the partition again with exception
        partition_w.drop(check_task=CheckTasks.err_res,
                         check_items={ct.err_code: 1, ct.err_msg: PartitionErrorMessage.PartitionNotExist})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_create_and_drop_multi_times(self):
        """
        target: verify create and drop for times
        method: 1.create a partition with default schema
                2. drop the partition
                3. loop #1 and #2 for times
        expected: create and drop successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # range for 5 times
        partition_name = cf.gen_unique_str(prefix)
        for i in range(5):
            # create partition and check that the partition exists
            partition_w = self.init_partition_wrap(collection_w, partition_name)
            assert collection_w.has_partition(partition_name)[0]

            # drop partition and check that the partition not exists
            partition_w.drop()
            assert not collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_drop_non_empty_partition(self):
        """
        target: verify drop a partition which has data inserted
        method: 1.create a partition with default schema
                2. insert some data
                3. drop the partition
        expected: drop successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]

        # insert data to partition
        partition_w.insert(cf.gen_default_dataframe_data())

        # drop partition
        partition_w.drop()
        assert not collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.parametrize("flush", [True, False])
    @pytest.mark.parametrize("data", [cf.gen_default_list_data(nb=3000)])
    @pytest.mark.parametrize("index_param", cf.gen_simple_index())
    def test_partition_drop_indexed_partition(self, data, index_param):
        """
        target: verify drop an indexed partition
        method: 1.create a partition
                2. insert same data
                3. create an index
                4. flush or not flush (remove flush step for issue # 5837)
                5. drop the partition
        expected: drop successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]

        # insert data to partition
        ins_res, _ = partition_w.insert(data)
        assert len(ins_res.primary_keys) == len(data[0])

        # create index of collection
        collection_w.create_index(ct.default_float_vec_field_name, index_param)

        # # flush
        # if flush:
        #     self._connect().flush([collection_w.name])

        # drop partition
        partition_w.drop()
        assert not collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_empty_partition(self):
        """
        target: verify release an empty partition
        method: 1.create a partition
                2. release the partition
        expected: release successfully
        """
        # create partition
        partition_w = self.init_partition_wrap()
        assert partition_w.is_empty

        # release partition
        partition_w.release()
        # TODO: assert no more memory consumed

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_dropped_partition(self):
        """
        target: verify release a dropped partition
        method: 1.create a partition
                2. drop the partition
                3. release the partition
        expected: raise exception
        """
        # create partition
        partition_w = self.init_partition_wrap()

        # drop partition
        partition_w.drop()

        # release the dropped partition and check err response
        partition_w.release(check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: PartitionErrorMessage.PartitionNotExist})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_dropped_collection(self):
        """
        target: verify release a dropped collection
        method: 1.create a collection and partition
                2. drop the collection
                3. release the partition
        expected: raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]

        # drop collection
        collection_w.drop()

        # release the partition and check err response
        partition_w.release(check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_after_collection_released(self):
        """
        target: verify release a partition after the collection released
        method: 1.create a collection and partition
                2. insert some data
                3. release the collection
                4. release the partition
        expected: partition released successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]

        # insert data to partition
        data = cf.gen_default_list_data()
        partition_w.insert(data)
        assert partition_w.num_entities == len(data[0])
        assert collection_w.num_entities == len(data[0])

        # load partition
        partition_w.load()

        # search of partition
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        res_1, _ = partition_w.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1)
        assert len(res_1) == 1


        # release collection
        collection_w.release()

        # search of partition
        res_2, _ = partition_w.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1,
                                      check_task=ct.CheckTasks.err_res,
                                      check_items={ct.err_code: 0,
                                                   ct.err_msg: "not loaded into memory"})
        # release partition
        partition_w.release()

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_default_partition(self):
        """
        target: verify insert data into _default partition
        method: 1.create a collection
                2. insert some data into _default partition
        expected: insert successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # get the default partition
        partition_name = ct.default_partition_name
        assert collection_w.has_partition(partition_name)[0]
        partition_w = self.init_partition_wrap(collection_w, partition_name)

        # insert data to partition
        data = cf.gen_default_dataframe_data()
        partition_w.insert(data)
        # self._connect().flush([collection_w.name])
        assert partition_w.num_entities == len(data)

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_partition(self):
        """
        target: verify insert data into a dropped partition
        method: 1.create a collection
                2. insert some data into a dropped partition
        expected: raise exception
        """
        # create partition
        partition_w = self.init_partition_wrap()

        # drop partition
        partition_w.drop()

        # insert data to partition
        partition_w.insert(cf.gen_default_dataframe_data(),
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "Partition not exist"})
        # TODO: update the assert error

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_dropped_collection(self):
        """
        target: verify insert data into a dropped collection
        method: 1.create a collection
                2. insert some data into a dropped collection
        expected: raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]

        # drop collection
        collection_w.drop()

        # insert data to partition
        partition_w.insert(cf.gen_default_dataframe_data(),
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "None Type"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_insert_maximum_size_data(self):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1.create a partition
                2. insert maximum size data
        expected: insert successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_w = self.init_partition_wrap(collection_w)

        # insert data to partition
        max_size = 100000  # TODO: clarify the max size of data
        ins_res, _ = partition_w.insert(cf.gen_default_dataframe_data(max_size), timeout=40)
        assert len(ins_res.primary_keys) == max_size
        # self._connect().flush([collection_w.name])
        assert partition_w.num_entities == max_size

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [ct.default_dim - 1, ct.default_dim + 1])
    def test_partition_insert_mismatched_dimensions(self, dim):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1.create a collection with default dim
                2. insert dismatch dim data
        expected: raise exception
        """
        # create partition
        partition_w = self.init_partition_wrap()

        data = cf.gen_default_list_data(nb=10, dim=dim)
        # insert data to partition
        partition_w.insert(data, check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "but entities field dim"})

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

