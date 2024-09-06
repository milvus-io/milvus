import pytest
import random

from base.client_base import TestcaseBase
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "alias"
exp_name = "name"
exp_schema = "schema"
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "int64 >= 0"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params


class TestAliasParams(TestcaseBase):
    """ Test cases of alias interface parameters"""
    pass


class TestAliasParamsInvalid(TestcaseBase):
    """ Negative test cases of alias interface parameters"""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("alias_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "a".join("a" for i in range(256))])
    def test_alias_create_alias_with_invalid_name(self, alias_name):
        """
        target: test alias inserting data
        method: create a collection with invalid alias name
        expected: create alias failed
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        error = {ct.err_code: 1100, ct.err_msg: "Invalid collection alias"}
        self.utility_wrap.create_alias(collection_w.name, alias_name,
                                       check_task=CheckTasks.err_res,
                                       check_items=error)


class TestAliasOperation(TestcaseBase):
    """ Test cases of alias interface operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_create_operation_default(self):
        """
        target: test collection creating alias
        method: 
                1.create a collection and create 10 partitions for it
                2.collection create an alias, then init a collection with this alias but not create partitions
        expected: collection is equal to alias
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        for _ in range(10):
            partition_name = cf.gen_unique_str("partition")
            # create partition with different names and check the partition exists
            self.init_partition_wrap(collection_w, partition_name)
            assert collection_w.has_partition(partition_name)[0]

        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        collection_alias = self.init_collection_wrap(name=alias_name,
                                                     check_task=CheckTasks.check_collection_property,
                                                     check_items={exp_name: alias_name, exp_schema: default_schema})
        # assert collection is equal to alias according to partitions
        assert [p.name for p in collection_w.partitions] == [
            p.name for p in collection_alias.partitions]

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_alter_operation_default(self):
        """
        target: test collection altering alias
        method: 
                1. create collection_1 with 10 partitions and its alias alias_a
                2. create collection_2 with 5 partitions and its alias alias_b
                3. collection_1 alter alias to alias_b
        expected: 
                in step 1, collection_1 is equal to alias_a
                in step 2, collection_2 is equal to alias_b
                in step 3, collection_1 is equal to alias_a and alias_b, and collection_2 is not equal to alias_b
        """
        self._connect()

        # create collection_1 with 10 partitions and its alias alias_a
        c_1_name = cf.gen_unique_str("collection")
        collection_1 = self.init_collection_wrap(name=c_1_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_1_name, exp_schema: default_schema})
        for _ in range(10):
            partition_name = cf.gen_unique_str("partition")
            # create partition with different names and check the partition exists
            self.init_partition_wrap(collection_1, partition_name)
            assert collection_1.has_partition(partition_name)[0]

        alias_a_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_1.name, alias_a_name)
        collection_alias_a = self.init_collection_wrap(name=alias_a_name,
                                                       check_task=CheckTasks.check_collection_property,
                                                       check_items={exp_name: alias_a_name, exp_schema: default_schema})
        # assert collection is equal to alias according to partitions
        assert [p.name for p in collection_1.partitions] == [
            p.name for p in collection_alias_a.partitions]

        # create collection_2 with 5 partitions and its alias alias_b
        c_2_name = cf.gen_unique_str("collection")
        collection_2 = self.init_collection_wrap(name=c_2_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_2_name, exp_schema: default_schema})

        for _ in range(5):
            partition_name = cf.gen_unique_str("partition")
            # create partition with different names and check the partition exists
            self.init_partition_wrap(collection_2, partition_name)
            assert collection_2.has_partition(partition_name)[0]

        alias_b_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_2.name, alias_b_name)
        collection_alias_b = self.init_collection_wrap(name=alias_b_name,
                                                       check_task=CheckTasks.check_collection_property,
                                                       check_items={exp_name: alias_b_name, exp_schema: default_schema})
        # assert collection is equal to alias according to partitions
        assert [p.name for p in collection_2.partitions] == [
            p.name for p in collection_alias_b.partitions]

        # collection_1 alter alias to alias_b
        self.utility_wrap.alter_alias(collection_1.name, alias_b_name)

        # collection_1 has two alias name, alias_a and alias_b, but collection_2 has no alias any more
        assert [p.name for p in collection_1.partitions] == [
            p.name for p in collection_alias_b.partitions]
        assert [p.name for p in collection_1.partitions] == [
            p.name for p in collection_alias_a.partitions]
        assert [p.name for p in collection_2.partitions] != [
            p.name for p in collection_alias_b.partitions]

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_drop_operation_default(self):
        """
        target: test collection dropping alias
        method: 
                1.create a collection with 10 partitions
                2.collection create an alias
                3.collection drop the alias
        expected: 
                after step 2, collection is equal to alias
                after step 3, collection with alias name is not exist
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        for _ in range(10):
            partition_name = cf.gen_unique_str("partition")
            # create partition with different names and check the partition exists
            self.init_partition_wrap(collection_w, partition_name)
            assert collection_w.has_partition(partition_name)[0]

        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias = self.init_collection_wrap(name=alias_name,
                                                     check_task=CheckTasks.check_collection_property,
                                                     check_items={exp_name: alias_name, exp_schema: default_schema})
        # assert collection is equal to alias according to partitions
        assert [p.name for p in collection_w.partitions] == [
            p.name for p in collection_alias.partitions]
        self.utility_wrap.drop_alias(alias_name)
        error = {ct.err_code: 0,
                 ct.err_msg: f"Collection '{alias_name}' not exist, or you can pass in schema to create one"}
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.err_res,
                                                                   check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_exec_operations_as_collection(self):
        """
        target: test alias 
                1.creating partition, 
                2.inserting data, 
                3.creating index, 
                4.loading collection,  
                5.searching, 
                6.releasing collection,
        method: follow the steps in target
        expected: all steps operated by alias can work
        """
        create_partition_flag = True
        insert_data_flag = True
        create_index_flag = True
        load_collection_flag = True
        search_flag = True
        release_collection_flag = True

        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name,
                                                                                exp_schema: default_schema})

        # create partition by alias
        partition_name = cf.gen_unique_str("partition")
        try:
            collection_alias.create_partition(partition_name)
        except Exception as e:
            log.info(f"alias create partition failed with exception {e}")
            create_partition_flag = False
            collection_w.create_partition(partition_name)

        # assert partition
        pytest.assume(create_partition_flag is True and
                      [p.name for p in collection_alias.partitions] == [p.name for p in collection_w.partitions])

        # insert data by alias
        df = cf.gen_default_dataframe_data(ct.default_nb)
        try:
            collection_alias.insert(data=df)
        except Exception as e:
            log.info(f"alias insert data failed with exception {e}")
            insert_data_flag = False
            collection_w.insert(data=df)

        # assert insert data
        pytest.assume(insert_data_flag is True and
                      collection_w.num_entities == ct.default_nb and
                      collection_alias.num_entities == ct.default_nb)

        # create index by alias
        default_index = {"index_type": "IVF_FLAT",
                         "params": {"nlist": 128}, "metric_type": "L2"}
        try:
            collection_alias.create_index(
                field_name="float_vector", index_params=default_index)
        except Exception as e:
            log.info(f"alias create index failed with exception {e}")
            create_index_flag = False
            collection_w.create_index(
                field_name="float_vector", index_params=default_index)

        # assert create index
        pytest.assume(create_index_flag is True and
                      collection_alias.has_index() is True and
                      collection_w.has_index()[0] is True)

        # load by alias
        try:
            collection_alias.load()
        except Exception as e:
            log.info(f"alias load collection failed with exception {e}")
            load_collection_flag = False
            collection_w.load()
        # assert load
        pytest.assume(load_collection_flag is True)

        # search by alias
        topK = 5
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}

        query = [[random.random() for _ in range(ct.default_dim)]
                 for _ in range(1)]
        alias_res = None
        try:
            alias_res = collection_alias.search(
                query, "float_vector", search_params, topK,
                "int64 >= 0", output_fields=["int64"]
            )
        except Exception as e:
            log.info(f"alias search failed with exception {e}")
            search_flag = False

        collection_res, _ = collection_w.search(
            query, "float_vector", search_params, topK,
            "int64 >= 0", output_fields=["int64"]
        )
        # assert search
        pytest.assume(
            search_flag is True and alias_res[0].ids == collection_res[0].ids)

        # release by alias
        try:
            collection_alias.release()
        except Exception as e:
            log.info(f"alias release failed with exception {e}")
            release_collection_flag = False
            collection_w.release()
        # assert release
        pytest.assume(release_collection_flag is True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_called_by_utility_has_collection(self):
        """
        target: test utility has collection by alias
        method: 
               1.create collection with alias
               2.call has_collection function with alias as param
        expected: result is True
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})

        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name,
                                                                                exp_schema: default_schema})
        res, _ = self.utility_wrap.has_collection(alias_name)

        assert res is True

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_called_by_utility_drop_collection(self):
        """
        target: test utility drop collection by alias
        method: 
               1.create collection with alias
               2.call drop_collection function with alias as param
        expected: Got error: collection cannot be dropped via alias.
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})

        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name,
                                                                                exp_schema: default_schema})
        assert self.utility_wrap.has_collection(c_name)[0]
        error = {ct.err_code: 1,
                 ct.err_msg: f"cannot drop the collection via alias = {alias_name}"}
        self.utility_wrap.drop_collection(alias_name,
                                          check_task=CheckTasks.err_res,
                                          check_items=error)
        self.utility_wrap.drop_collection(c_name)
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_called_by_utility_has_partition(self):
        """
        target: test utility has partition by alias
        method: 
               1.create collection with partition and alias
               2.call has_partition function with alias as param
        expected: result is True
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        partition_name = cf.gen_unique_str("partition")
        self.init_partition_wrap(collection_w, partition_name)

        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name,
                                                                                exp_schema: default_schema})
        res, _ = self.utility_wrap.has_partition(alias_name, partition_name)

        assert res is True

    @pytest.mark.tags(CaseLabel.L1)
    def test_enable_mmap_by_alias(self):
        """
        target: enable or disable mmap by alias
        method: enable or disable mmap by alias
        expected: successfully enable mmap
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(c_name, schema=default_schema)
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name,
                                                                                exp_schema: default_schema})
        collection_alias.set_properties({'mmap.enabled': True})
        pro = collection_w.describe()[0].get("properties")
        assert pro["mmap.enabled"] == 'True'
        collection_w.set_properties({'mmap.enabled': False})
        pro = collection_alias.describe().get("properties")
        assert pro["mmap.enabled"] == 'False'


class TestAliasOperationInvalid(TestcaseBase):
    """ Negative test cases of alias interface operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_create_duplication_alias(self):
        """
        target: test two collections creating alias with same name
        method: 
                1.create a collection_1 with alias name alias_a
                2.create a collection_2 also with alias name alias_a
        expected: 
                in step 2, creating alias with a duplication name is not allowed
        """
        self._connect()
        c_1_name = cf.gen_unique_str("collection")
        collection_1 = self.init_collection_wrap(name=c_1_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_1_name, exp_schema: default_schema})
        alias_a_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_1.name, alias_a_name)
        # collection_1.create_alias(alias_a_name)

        c_2_name = cf.gen_unique_str("collection")
        collection_2 = self.init_collection_wrap(name=c_2_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_2_name, exp_schema: default_schema})
        error = {ct.err_code: 1602,
                 ct.err_msg: f"alias exists and already aliased to another collection, alias: {alias_a_name}, "
                             f"collection: {c_1_name}, other collection: {c_2_name}"}
        self.utility_wrap.create_alias(collection_2.name, alias_a_name,
                                       check_task=CheckTasks.err_res,
                                       check_items=error)
        # collection_2.create_alias(alias_a_name,
        #                           check_task=CheckTasks.err_res,
        #                           check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_alter_not_exist_alias(self):
        """
        target: test collection altering to alias which is not exist
        method: 
                1.create a collection with alias
                2.collection alters to an alias name which is not exist
        expected: 
                in step 2, alter alias with a not exist name is not allowed
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)

        alias_not_exist_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1600,
                 ct.err_msg: "Alter alias failed: alias does not exist"}
        self.utility_wrap.alter_alias(collection_w.name, alias_not_exist_name,
                                      check_task=CheckTasks.err_res,
                                      check_items=error)
        # collection_w.alter_alias(alias_not_exist_name,
        #                          check_task=CheckTasks.err_res,
        #                          check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_drop_not_exist_alias(self):
        """
        target: test collection dropping alias which is not exist
        method: 
                1.create a collection with alias
                2.collection drop alias which is not exist
        expected: drop alias failed
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)

        alias_not_exist_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1,
                 ct.err_msg: "Drop alias failed: alias does not exist"}
        # self.utility_wrap.drop_alias(alias_not_exist_name,
        #                              check_task=CheckTasks.err_res,
        #                              check_items=error)
        # @longjiquan: dropping alias should be idempotent.
        self.utility_wrap.drop_alias(alias_not_exist_name)

        #
        # collection_w.drop_alias(alias_not_exist_name,
        #                         check_task=CheckTasks.err_res,
        #                         check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_drop_same_alias_twice(self):
        """
        target: test collection dropping same alias twice
        method: 
                1.create a collection with alias
                2.collection drop alias
                3.collection drop alias again
        expected: drop alias failed
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        self.utility_wrap.drop_alias(alias_name)
        # collection_w.create_alias(alias_name)
        # collection_w.drop_alias(alias_name)

        # @longjiquan: dropping alias should be idempotent.
        self.utility_wrap.drop_alias(alias_name)

        # error = {ct.err_code: 1, ct.err_msg: "Drop alias failed: alias does not exist"}
        # self.utility_wrap.drop_alias(alias_name,
        #                              check_task=CheckTasks.err_res,
        #                              check_items=error)

        # collection_w.drop_alias(alias_name,
        #                         check_task=CheckTasks.err_res,
        #                         check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_create_dup_name_collection(self):
        """
        target: test creating a collection with a same name as alias, but a different schema
        method:
                1.create a collection with alias
                2.create a collection with same name as alias, but a different schema
        expected: in step 2, create collection failed
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)

        error = {ct.err_code: 0,
                 ct.err_msg: "The collection already exist, but the schema is not the same as the schema passed in"}
        self.init_collection_wrap(alias_name, schema=default_binary_schema,
                                  check_task=CheckTasks.err_res,
                                  check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_drop_collection_by_alias(self):
        """
        target: test dropping a collection by alias
        method:
                1.create a collection with alias
                2.drop a collection by alias
        expected: in step 2, drop collection by alias failed by design
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name,
                                                                                exp_schema: default_schema})

        with pytest.raises(Exception):
            collection_alias.drop()
