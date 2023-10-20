import threading
import pytest
import time

from base.partition_wrapper import ApiPartitionWrapper
from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common.code_mapping import PartitionErrorMessage

prefix = "partition_"


class TestPartitionParams(TestcaseBase):
    """ Test case of partition interface in parameters"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_partition_default(self):
        """
        target: verify create a partition
        method: create a partition
        expected: create successfully
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", [""])
    def test_partition_empty_name(self, partition_name):
        """
        target: verify create a partition with empty name
        method: create a partition with empty name
        expected: raise exception
        """
        # create a collection
        collection_w = self.init_collection_wrap()

        # create partition
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1,
                                                        ct.err_msg: "Partition name should not be empty"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_empty_description(self):
        """
        target: verify create a partition with empty description
        method: create a partition with empty description
        expected: create successfully
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_max_description_length(self):
        """
        target: verify create a partition with 255 length name and 1024 length description
        method: create a partition with 255 length name and 1024 length description
        expected: create successfully
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
        method: create partitions with duplicate names
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("description", ct.get_invalid_strs)
    def test_partition_special_chars_description(self, description):
        """
        target: verify create a partition with special characters in description
        method: create a partition with special characters in description
        expected: create successfully
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
        expected: the same partition returned
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_max_length_name(self):
        """
        target: verify create a partition with max length(256) name
        method: create a partition with max length name
        expected: raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_str_by_length(256)
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 65535,
                                                        ct.err_msg: f"Invalid partition name: {partition_name}. "
                                                                    f"The length of a partition name must be less "
                                                                    f"than 255 characters."})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ["_Partiti0n", "pArt1_ti0n"])
    def test_partition_naming_rules(self, partition_name):
        """
        target: test partition naming rules
        method: 1. connect milvus
                2. Create a collection
                3. Create a partition with a name which uses all the supported elements in the naming rules
        expected: Partition create successfully
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.check_partition_property,
                                           check_items={"name": partition_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("partition_name", ct.get_invalid_strs)
    def test_partition_invalid_name(self, partition_name):
        """
        target: verify create a partition with invalid name
        method: create a partition with invalid names
        expected: raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        error1 = {ct.err_code: 1, ct.err_msg: f"`partition_name` value {partition_name} is illegal"}
        error2 = {ct.err_code: 65535, ct.err_msg: f"Invalid partition name: {partition_name}. Partition name can"
                                                  f" only contain numbers, letters and underscores."}
        error = error1 if partition_name in [None, [], 1, [1, "2", 3], (1,), {1: 1}] else error2
        self.partition_wrap.init_partition(collection_w.collection, partition_name,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)
        # TODO: need an error code issue #5144 and assert independently

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_none_collection(self):
        """
        target: verify create a partition with none collection
        method: create a partition with none collection
        expected: raise exception
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
        expected: drop successfully
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partiton_respectively(self):
        """
        target: test release the partition after load partition
        method: load partition1 and load another partition
        expected: raise no exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)
        partition_w1.insert(cf.gen_default_list_data())
        partition_w2.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w1.load()
        partition_w2.load()

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partitions_after_release(self):
        """
        target: test release the partition after load partition
        method: load partitions and release partitions
        expected: no exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w1 = self.init_partition_wrap(collection_w, name="partition_w1")
        partition_w2 = self.init_partition_wrap(collection_w, name="partition_w2")
        partition_w1.insert(cf.gen_default_list_data())
        partition_w2.insert(cf.gen_default_list_data())
        partition_names = ["partition_w1", "partition_w2"]
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        collection_w.load(partition_names)
        collection_w.release(partition_names)

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_partition_after_load_partition(self):
        """
        target: test release the partition after load partition
        method: load partition1 and release the partition1
                load partition2
        expected: no exception
        """
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)
        partition_w1.insert(cf.gen_default_list_data())
        partition_w2.insert(cf.gen_default_list_data())
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w1.load()
        partition_w1.release()
        partition_w2.load()

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_non_number_replicas(self, request):
        if request.param == 1:
            pytest.skip("1 is valid replica number")
        if request.param is None:
            pytest.skip("None is valid replica number")
        yield request.param

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="issue #21618")
    def test_load_partition_replica_non_number(self, get_non_number_replicas):
        """
        target: test load partition with non-number replicas
        method: load with non-number replicas
        expected: raise exceptions
        """
        # create, insert
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w = self.init_partition_wrap(collection_w)
        partition_w.insert(cf.gen_default_list_data(nb=100))

        # load with non-number replicas
        error = {ct.err_code: 0, ct.err_msg: f"but expected one of: int, long"}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load(replica_number=get_non_number_replicas, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("replicas", [0, -1])
    def test_load_replica_invalid_number(self, replicas):
        """
        target: test load partition with invalid replica number
        method: load with invalid replica number
        expected: raise exception
        """
        # create, insert
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w = self.init_partition_wrap(collection_w)
        partition_w.insert(cf.gen_default_list_data())
        assert partition_w.num_entities == ct.default_nb

        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load(replica_number=replicas)
        p_replicas = partition_w.get_replicas()[0]
        assert len(p_replicas.groups) == 1
        query_res, _ = partition_w.query(expr=f"{ct.default_int64_field_name} in [0]")
        assert len(query_res) == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_load_replica_greater_than_querynodes(self):
        """
        target: test load with replicas that greater than querynodes
        method: load with 3 replicas (2 querynode)
        expected: Verify load successfully and 1 available replica
        """
        # create, insert
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w = self.init_partition_wrap(collection_w)
        partition_w.insert(cf.gen_default_list_data())
        assert partition_w.num_entities == ct.default_nb

        # load with 2 replicas
        error = {ct.err_code: 65535,
                 ct.err_msg: "failed to load partitions: failed to spawn replica for collection: nodes not enough"}
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)
        partition_w.load(replica_number=3, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_load_replica_change(self):
        """
        target: test load replica change
        method: 1.load with replica 1
                2.load with a new replica number
                3.release partition
                4.load with a new replica
                5.create index is a must because get_query_segment_info could
                  only return indexed and loaded segment
        expected: The second time successfully loaded with a new replica number
        """
        # create, insert
        self._connect()
        collection_w = self.init_collection_wrap()
        partition_w = self.init_partition_wrap(collection_w)
        partition_w.insert(cf.gen_default_list_data())
        assert partition_w.num_entities == ct.default_nb
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)

        partition_w.load(replica_number=1)
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]", check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}]})
        error = {ct.err_code: 1100, ct.err_msg: "failed to load partitions: can't change the replica number for "
                                                "loaded partitions: expected=1, actual=2: invalid parameter"}
        partition_w.load(replica_number=2, check_task=CheckTasks.err_res, check_items=error)

        partition_w.release()
        partition_w.load(replica_number=2)
        collection_w.query(expr=f"{ct.default_int64_field_name} in [0]", check_task=CheckTasks.check_query_results,
                           check_items={'exp_res': [{'int64': 0}]})

        two_replicas, _ = collection_w.get_replicas()
        assert len(two_replicas.groups) == 2

        # verify loaded segments included 2 replicas and twice num entities
        seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
        num_entities = 0
        for seg in seg_info:
            assert len(seg.nodeIds) == 2
            num_entities += seg.num_rows
        assert num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.ClusterOnly)
    def test_partition_replicas_change_cross_partitions(self):
        """
        target: test load with different replicas between partitions
        method: 1.Create two partitions and insert data
                2.Create index is a must because get_query_segment_info could
                  only return indexed and loaded segment
                3.Load two partitions with different replicas
        expected: Raise an exception
        """
        # Create two partitions and insert data
        collection_w = self.init_collection_wrap()
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)
        partition_w1.insert(cf.gen_default_dataframe_data())
        partition_w2.insert(cf.gen_default_dataframe_data(start=ct.default_nb))
        assert collection_w.num_entities == ct.default_nb * 2
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)

        # load with different replicas
        partition_w1.load(replica_number=1)
        partition_w1.release()
        partition_w2.load(replica_number=2)

        # verify different have same replicas
        replicas_1, _ = partition_w1.get_replicas()
        replicas_2, _ = partition_w2.get_replicas()
        group1_ids = list(map(lambda g: g.id, replicas_1.groups))
        group2_ids = list(map(lambda g: g.id, replicas_1.groups))
        assert group1_ids.sort() == group2_ids.sort()

        # verify loaded segments included 2 replicas and 1 partition
        seg_info = self.utility_wrap.get_query_segment_info(collection_w.name)[0]
        num_entities = 0
        for seg in seg_info:
            assert len(seg.nodeIds) == 2
            num_entities += seg.num_rows
        assert num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release(self):
        """
        target: verify release partition
        method: 1. create a collection and two partitions
                2. insert data into each partition
                3. flush and load the partition1
                4. release partition1
                5. release partition2
        expected: 1. the 1st partition is released
                  2. the 2nd partition is released
        """
        # create collection

        collection_w = self.init_collection_wrap()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

        # create two partitions
        partition_w1 = self.init_partition_wrap(collection_w)
        partition_w2 = self.init_partition_wrap(collection_w)

        # insert data to two partition
        partition_w1.insert(cf.gen_default_list_data())
        partition_w2.insert(cf.gen_default_list_data())

        # load two partitions
        partition_w1.load()

        # search  partition1
        search_vectors = cf.gen_vectors(1, ct.default_dim)
        res1, _ = partition_w1.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1)
        assert len(res1) == 1

        # release the first partition
        partition_w1.release()
        partition_w2.release()

        # check result
        res1, _ = partition_w1.search(data=search_vectors,
                                      anns_field=ct.default_float_vec_field_name,
                                      params={"nprobe": 32}, limit=1,
                                      check_task=ct.CheckTasks.err_res,
                                      check_items={ct.err_code: 65535,
                                                   ct.err_msg: "collection not loaded"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", [cf.gen_default_dataframe_data(10),
                                      cf.gen_default_list_data(10)])
    def test_partition_insert(self, data):
        """
        target: verify insert entities multiple times
        method: 1. create a collection and a partition
                2. partition.insert(data)
                3. insert data again
        expected: insert data successfully
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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="not stable")
    def test_partition_upsert(self):
        """
        target: verify upsert entities multiple times
        method: 1. create a collection and a partition
                2. partition.upsert(data)
                3. upsert data again
        expected: upsert data successfully
        """
        # create collection and a partition
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)

        # insert data and load
        cf.insert_data(collection_w)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w.load()

        # upsert data
        upsert_nb = 1000
        data, values = cf.gen_default_data_for_upsert(nb=upsert_nb, start=2000)
        partition_w.upsert(data)
        res = partition_w.query("int64 >= 2000 && int64 < 3000", [ct.default_float_field_name])[0]
        time.sleep(5)
        assert partition_w.num_entities == ct.default_nb // 2
        assert [res[i][ct.default_float_field_name] for i in range(upsert_nb)] == values.to_list()

        # upsert data
        data, values = cf.gen_default_data_for_upsert(nb=upsert_nb, start=ct.default_nb)
        partition_w.upsert(data)
        res = partition_w.query("int64 >= 3000 && int64 < 4000", [ct.default_float_field_name])[0]
        time.sleep(5)
        assert partition_w.num_entities == upsert_nb + ct.default_nb // 2
        assert [res[i][ct.default_float_field_name] for i in range(upsert_nb)] == values.to_list()


class TestPartitionOperations(TestcaseBase):
    """ Test case of partition interface in operations """

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_dropped_collection(self):
        """
        target: verify create partition against a dropped collection
        method: 1. create a collection
                2. drop collection
                3. create partition in collection
        expected: raise exception
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # drop collection
        collection_w.drop()

        # create partition failed
        self.partition_wrap.init_partition(collection_w.collection, cf.gen_unique_str(prefix),
                                           check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 4, ct.err_msg: "collection not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_same_name_in_diff_collections(self):
        """
        target: verify create partitions with same name in diff collections
        method: 1. create a partition in collection1
                2. create a partition in collection2
        expected: create successfully
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
        method: create multiple partitions in one collection
        expected: create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        for _ in range(10):
            partition_name = cf.gen_unique_str(prefix)
            # create partition with different names and check the partition exists
            self.init_partition_wrap(collection_w, partition_name)
            assert collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    # @pytest.mark.skip(reason="skip temporarily for debug")
    def test_partition_maximum_partitions(self):
        """
        target: verify create maximum partitions
        method: 1. create maximum partitions
                2. create one more partition
        expected: raise exception
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
        log.info(f"partitions: {len(collection_w.partitions)}")
        self.partition_wrap.init_partition(
            collection_w.collection, p_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 65535,
                         ct.err_msg: "partition number (4096) exceeds max configuration (4096), "
                                     "collection: {}".format(collection_w.name)})

        # TODO: Try to verify load collection with a large number of partitions. #11651

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
        expected: raise exception for 2nd time
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
        method: 1. create a partition with default schema
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

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_drop_non_empty_partition(self):
        """
        target: verify drop a partition which has data inserted
        method: 1. create a partition with default schema
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
    @pytest.mark.parametrize("data", [cf.gen_default_list_data(nb=3000)])
    @pytest.mark.parametrize("index_param", cf.gen_simple_index())
    def test_partition_drop_indexed_partition(self, data, index_param):
        """
        target: verify drop an indexed partition
        method: 1. create a partition
                2. insert same data
                3. create an index
                4. drop the partition
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

        # drop partition
        partition_w.drop()
        assert not collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_release_empty_partition(self):
        """
        target: verify release an empty partition
        method: 1. create a partition
                2. release the partition
        expected: release successfully
        """
        # create partition
        partition_w = self.init_partition_wrap()
        assert partition_w.is_empty

        # release partition
        partition_w.release()
        # TODO: assert no more memory consumed

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_release_dropped_partition(self):
        """
        target: verify release a dropped partition
        method: 1. create a partition
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

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_release_dropped_collection(self):
        """
        target: verify release a dropped collection
        method: 1. create a collection and partition
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
                            check_items={ct.err_code: 4, ct.err_msg: "collection not found"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_release_after_collection_released(self):
        """
        target: verify release a partition after the collection released
        method: 1. create a collection and partition
                2. insert some data
                3. release the collection
                4. release the partition
        expected: partition released successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()
        collection_w.create_index(ct.default_float_vec_field_name, index_params=ct.default_flat_index)

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
                                                   ct.err_msg: "not loaded"})
        # release partition
        partition_w.release()

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_insert_default_partition(self):
        """
        target: verify insert data into _default partition
        method: 1. create a collection
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
        method: 1. create a collection
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
        method: 1. create a collection
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
                           check_items={ct.err_code: 4, ct.err_msg: "collection not found"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_insert_maximum_size_data(self):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1. create a partition
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

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim", [ct.default_dim - 1, ct.default_dim + 1])
    def test_partition_insert_mismatched_dimensions(self, dim):
        """
        target: verify insert maximum size data(256M?) a time
        method: 1. create a collection with default dim
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
        method: 1. create a partition
                2. insert data in sync
        expected: insert successfully
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", [cf.gen_default_list_data(nb=3000)])
    @pytest.mark.parametrize("index_param", cf.gen_simple_index())
    def test_partition_delete_indexed_data(self, data, index_param):
        """
        target: verify delete entities with an expression condition from an indexed partition
        method: 1. create collection
                2. create an index
                3. create a partition
                4. insert same data
                5. delete entities with an expression condition
        expected: delete successfully
        issue #15456
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create index of collection
        collection_w.create_index(ct.default_float_vec_field_name, index_param)

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_name)[0]

        # insert data to partition
        ins_res, _ = partition_w.insert(data)
        assert len(ins_res.primary_keys) == len(data[0])

        # delete entities with an expression condition
        expr = "int64 in [0,1]"
        res = partition_w.delete(expr)
        assert len(res) == 2

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_upsert_empty_partition(self):
        """
        target: verify upsert data in empty partition
        method: 1. create a collection
                2. upsert some data in empty partition
        expected: upsert successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # get the default partition
        partition_name = ct.default_partition_name
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert partition_w.num_entities == 0

        # upsert data to the empty partition
        data = cf.gen_default_data_for_upsert()[0]
        partition_w.upsert(data)
        assert partition_w.num_entities == ct.default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_partition_upsert_dropped_partition(self):
        """
        target: verify upsert data in a dropped partition
        method: 1. create a partition and drop
                2. upsert some data into the dropped partition
        expected: raise exception
        """
        # create partition
        partition_w = self.init_partition_wrap()

        # drop partition
        partition_w.drop()

        # insert data to partition
        partition_w.upsert(cf.gen_default_dataframe_data(),
                           check_task=CheckTasks.err_res,
                           check_items={ct.err_code: 1, ct.err_msg: "Partition not exist"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_upsert_mismatched_data(self):
        """
        target: test upsert mismatched data in partition
        method: 1. create a partition
                2. insert some data
                3. upsert with mismatched data
        expected: raise exception
        """
        # create a partition
        partition_w = self.init_partition_wrap()

        # insert data
        data = cf.gen_default_dataframe_data()
        partition_w.insert(data)

        # upsert mismatched data
        upsert_data = cf.gen_default_data_for_upsert(dim=ct.default_dim-1)[0]
        error = {ct.err_code: 1, ct.err_msg: "Collection field dim is 128, but entities field dim is 127"}
        partition_w.upsert(upsert_data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_upsert_with_auto_id(self):
        """
        target: test upsert data in partition when auto_id=True
        method: 1. create a partition
                2. insert some data
                3. upsert data
        expected: raise exception
        """
        # create a partition
        schema = cf.gen_default_collection_schema(auto_id=True)
        collection_w = self.init_collection_wrap(schema=schema)
        partition_w = self.init_partition_wrap(collection_w)

        # insert data
        data = cf.gen_default_dataframe_data()
        data.drop(ct.default_int64_field_name, axis=1, inplace=True)
        partition_w.insert(data)

        # upsert data
        upsert_data = cf.gen_default_data_for_upsert()[0]
        upsert_data.drop(ct.default_int64_field_name, axis=1, inplace=True)
        error = {ct.err_code: 1, ct.err_msg: "Upsert don't support autoid == true"}
        partition_w.upsert(upsert_data, check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_upsert_same_pk_in_different_partitions(self):
        """
        target: test upsert same pk in different partitions
        method: 1. create 2 partitions
                2. insert some data
                3. upsert data
        expected: raise exception
        """
        # create 2 partitions
        collection_w = self.init_collection_wrap()
        partition_1 = self.init_partition_wrap(collection_w)
        partition_2 = self.init_partition_wrap(collection_w)

        # insert data
        nb = 1000
        data = cf.gen_default_dataframe_data(nb)
        partition_1.insert(data)
        data = cf.gen_default_dataframe_data(nb, start=nb)
        partition_2.insert(data)

        # upsert data in 2 partitions
        upsert_data, values = cf.gen_default_data_for_upsert(1)
        partition_1.upsert(upsert_data)
        partition_2.upsert(upsert_data)

        # load
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        collection_w.load()

        # query and check the results
        expr = "int64 == 0"
        res1 = partition_1.query(expr, [ct.default_float_field_name], consistency_level="Strong")[0]
        res2 = partition_2.query(expr, [ct.default_float_field_name], consistency_level="Strong")[0]
        assert res1 == res2

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_partition_repeat(self):
        """
        target: test create partition, check status returned
        method: call function: create_partition
        expected: status is ok
        """

        # create partition
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)

        partition_w = self.init_partition_wrap(collection_w, partition_name)
        partition_e = self.init_partition_wrap(collection_w, partition_w.name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_create_partition_name_none(self):
        """
        target: test create partition,partition name set None, check status returned
        method: call function: create_partition
        expected: status ok
        """

        collection_w = self.init_collection_wrap()
        partition_name = None
        partition_w = self.init_partition_wrap(collection_w, partition_name)


class TestShowBase(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test list partition
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_partitions(self):
        """
        target: test show partitions, check status and partitions returned
        method: create partition first, then call : collection.partitions
        expected: status ok, partition correct
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)

        assert collection_w.partitions[1].name == partition_w.name

    @pytest.mark.tags(CaseLabel.L0)
    def test_show_multi_partitions(self):
        """
        target: test show partitions, check status and partitions returned
        method: create partitions first, then call : collection.partitions
        expected: status ok, partitions correct
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)

        partition_w = self.init_partition_wrap(collection_w, partition_name)
        partition_e = self.init_partition_wrap(collection_w, partition_name)

        assert collection_w.partitions[1].name == partition_w.name
        assert collection_w.partitions[1].name == partition_e.name


class TestHasBase(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `has_partition` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_a(self):
        """
        target: test has_partition, check status and result
        method: create partition first, then call function: has_partition
        expected: status ok, result true
        """
        collection_w = self.init_collection_wrap()

        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        assert collection_w.has_partition(partition_w.name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_has_partition_multi_partitions(self):
        """
        target: test has_partition, check status and result
        method: create partition first, then call function: has_partition
        expected: status ok, result true
        """
        collection_w = self.init_collection_wrap()

        partition_name = cf.gen_unique_str(prefix)

        partition_w1 = self.init_partition_wrap(collection_w, partition_name)
        partition_w2 = self.init_partition_wrap(collection_w, partition_name)

        assert collection_w.has_partition(partition_w1.name)
        assert collection_w.has_partition(partition_w2.name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_name_not_existed(self):
        """
        target: test has_partition, check status and result
        method: then call function: has_partition, with partition name not existed
        expected: status ok, result empty
        """
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str(prefix)
        assert not collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_collection_not_existed(self):
        """
        target: test has_partition, check status and result
        method: then call function: has_partition, with collection not existed
        expected: status not ok
        """

        collection_w = self.init_collection_wrap()
        collection_e = self.init_collection_wrap()

        partition_name = cf.gen_unique_str(prefix)

        partition_w1 = self.init_partition_wrap(collection_w, partition_name)

        assert not collection_e.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_partition_with_invalid_partition_name(self):
        """
        target: test has partition, with invalid partition name, check status returned
        method: call function: has_partition
        expected: status ok
        """
        collection_w = self.init_collection_wrap()
        partition_name = ct.get_invalid_strs
        collection_w.has_partition(partition_name, check_task=CheckTasks.err_res,
                                   check_items={ct.err_code: 1, 'err_msg': "is illegal"})


class TestDropBase(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test `drop_partition` function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_partition_repeatedly(self):
        """
        target: test drop partition twice, check status and partition if existed
        method: create partitions first, then call function: drop_partition
        expected: status not ok, no partitions in db
        """
        collection_w = self.init_collection_wrap()

        partition_name = cf.gen_unique_str(prefix)
        partition_w = self.init_partition_wrap(collection_w, partition_name)

        # drop partition
        collection_w.drop_partition(partition_w.name)

        # check that the partition not exists
        assert not collection_w.has_partition(partition_name)[0]

        collection_w.drop_partition(partition_w.name, check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1, 'err_msg': "Partition not exist"})

    @pytest.mark.tags(CaseLabel.L0)
    def test_drop_partition_create(self):
        """
        target: test drop partition, and create again, check status
        method: create partitions first, then call function: drop_partition, create_partition
        expected: status is ok, partition in db
        """
        collection_w = self.init_collection_wrap()

        partition_name = cf.gen_unique_str(prefix)

        partition_w = self.init_partition_wrap(collection_w, partition_name)

        collection_w.drop_partition(partition_w.name)

        partition_w = self.init_partition_wrap(collection_w, partition_name)

        assert collection_w.has_partition(partition_w.name)


class TestNameInvalid(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test invalid partition name
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_partition_with_invalid_name(self):
        """
        target: test drop partition, with invalid partition name, check status returned
        method: call function: drop_partition
        expected: status not ok
        """
        collection_w = self.init_collection_wrap()
        partition_name = ct.get_invalid_strs
        collection_w.drop_partition(partition_name, check_task=CheckTasks.err_res,
                                    check_items={ct.err_code: 1, 'err_msg': "is illegal"})
