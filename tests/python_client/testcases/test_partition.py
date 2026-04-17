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
    """Test case of partition interface in parameters"""

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
        self.init_partition_wrap(
            collection_w,
            partition_name,
            description=description,
            check_task=CheckTasks.check_partition_property,
            check_items={"name": partition_name, "description": description, "is_empty": True, "num_entities": 0},
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
        self.init_partition_wrap(
            collection_w,
            partition_name,
            description=description,
            check_task=CheckTasks.check_partition_property,
            check_items={"name": partition_name, "description": description, "is_empty": True},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_special_chars_description(self):
        """
        target: verify create a partition with special characters in description
        method: create a partition with special characters in description
        expected: create successfully
        """
        # create collection
        collection_w = self.init_collection_wrap()

        # create partition
        partition_name = cf.gen_unique_str(prefix)
        description = "！@#￥%……&*（"
        self.init_partition_wrap(
            collection_w,
            partition_name,
            description=description,
            check_task=CheckTasks.check_partition_property,
            check_items={"name": partition_name, "description": description, "is_empty": True, "num_entities": 0},
        )
        assert collection_w.has_partition(partition_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
    def test_partition_none_collection(self):
        """
        target: verify create a partition with none collection
        method: create a partition with none collection
        expected: raise exception
        """
        # create partition with collection is None
        partition_name = cf.gen_unique_str(prefix)
        self.partition_wrap.init_partition(
            collection=None,
            name=partition_name,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1, ct.err_msg: "Collection must be of type pymilvus.Collection or String"},
        )

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

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("data", [cf.gen_default_dataframe_data(10)])
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
        partition_w = self.init_partition_wrap(
            collection_w,
            partition_name,
            check_task=CheckTasks.check_partition_property,
            check_items={"name": partition_name, "is_empty": True, "num_entities": 0},
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
    """Test case of partition interface in operations"""

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
