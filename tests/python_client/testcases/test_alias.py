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


class TestAliasParamsInvalid(TestcaseBase):
    """ Negative test cases of alias interface parameters"""

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L0)
    def test_alias_alter_operation_default(self):
        """
        target: test collection altering alias
        method:
                1. create collection_1, bind alias to collection_1 and insert 2000 entities
                2. create collection_2 with 1500 entitiesa
                3. search on alias
                verify num_entities=2000
                4. alter alias to collection_2 and search on alias
                verify num_entities=1500
        """
        c_name1 = cf.gen_unique_str("collection1")
        collection_w1 = self.init_collection_wrap(name=c_name1, schema=default_schema,
                                                  check_task=CheckTasks.check_collection_property,
                                                  check_items={exp_name: c_name1, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        # create a collection alias and bind to collection1
        self.utility_wrap.create_alias(collection_w1.name, alias_name)
        collection_alias = self.init_collection_wrap(name=alias_name)

        nb1 = 2000
        data1 = cf.gen_default_dataframe_data(nb=nb1)
        import pandas as pd
        string_values = pd.Series(data=[str(i) for i in range(nb1)], dtype="string")
        data1[ct.default_string_field_name] = string_values
        collection_alias.insert(data1)
        collection_alias.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_alias.load()

        assert collection_alias.num_entities == nb1 == collection_w1.num_entities
        res1 = collection_alias.query(expr="", output_fields=["count(*)"])[0]
        assert res1[0].get("count(*)") == nb1

        # create collection2
        c_name2 = cf.gen_unique_str("collection2")
        collection_w2 = self.init_collection_wrap(name=c_name2, schema=default_schema,
                                                  check_task=CheckTasks.check_collection_property,
                                                  check_items={exp_name: c_name2, exp_schema: default_schema})
        nb2 = 1500
        data2 = cf.gen_default_dataframe_data(nb=nb2)
        string_values = pd.Series(data=[str(i) for i in range(nb2)], dtype="string")
        data2[ct.default_string_field_name] = string_values
        collection_w2.insert(data2)
        collection_w2.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w2.load()

        # alter the collection alias to collection2
        self.utility_wrap.alter_alias(collection_w2.name, alias_name)
        assert collection_alias.num_entities == nb2 == collection_w2.num_entities
        res1 = collection_alias.query(expr="", output_fields=["count(*)"])[0]
        assert res1[0].get("count(*)") == nb2

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

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        self.utility_wrap.drop_alias(alias_name)
        self.utility_wrap.drop_collection(c_name)
        assert not self.utility_wrap.has_collection(c_name)[0]

    @pytest.mark.tags(CaseLabel.L2)
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

        alias_not_exist_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1600,
                 ct.err_msg: "Alter alias failed: alias does not exist"}
        self.utility_wrap.alter_alias(collection_w.name, alias_not_exist_name,
                                      check_task=CheckTasks.err_res,
                                      check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_alias_drop_not_exist_alias(self):
        """
        target: test collection dropping alias which is not exist
        method:
                1.create a collection with alias
                2.collection drop alias which is not exist
        expected: drop alias succ
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        alias_not_exist_name = cf.gen_unique_str(prefix)
        self.utility_wrap.drop_alias(alias_not_exist_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_alias_drop_same_alias_twice(self):
        """
        target: test collection dropping same alias twice
        method:
                1.create a collection with alias
                2.collection drop alias
                3.collection drop alias again
        expected: drop alias succ
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        self.utility_wrap.drop_alias(alias_name)
        # @longjiquan: dropping alias should be idempotent.
        self.utility_wrap.drop_alias(alias_name)

    @pytest.mark.tags(CaseLabel.L2)
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

    @pytest.mark.tags(CaseLabel.L2)
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
        schema = cf.gen_default_collection_schema(description="this is for alias decsription")
        collection_w = self.init_collection_wrap(name=c_name, schema=schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        # collection_w.create_alias(alias_name)
        collection_alias = self.init_collection_wrap(name=alias_name, schema=schema,
                                                     check_task=CheckTasks.check_collection_property,
                                                     check_items={exp_name: alias_name,
                                                                  exp_schema: schema})

        error = {ct.err_code: 999,
                 ct.err_msg: f"cannot drop the collection via alias = {alias_name}"}
        collection_alias.drop(check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #36963")
    def test_alias_reuse_alias_name_from_dropped_collection(self):
        """
        target: test dropping a collection which has a alias
        method:
                1.create a collection
                2.create an alias for the collection
                3.drop the collection
                4.create a new collection
                5.create an alias with the same alias name for the new collection
            expected: in step 5, create alias with the same name for the new collection succ
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        res = self.utility_wrap.list_aliases(c_name)[0]
        assert len(res) == 1

        # dropping collection that has an alias shall drop the alias as well
        self.utility_wrap.drop_alias(alias_name)
        collection_w.drop()
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        res2 = self.utility_wrap.list_aliases(c_name)[0]
        assert len(res2) == 0
        # the same alias name can be reused for another collection
        self.utility_wrap.create_alias(c_name, alias_name)
        res2 = self.utility_wrap.list_aliases(c_name)[0]
        assert len(res2) == 1

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #36963")
    def test_alias_rename_collection_to_alias_name(self):
        """
        target: test renaming a collection to a alias name
        method:
                1.create a collection
                2.create an alias for the collection
                3.rename the collection to the alias name
        expected: in step 3, rename collection to alias name failed
        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        self.utility_wrap.create_alias(collection_w.name, alias_name)
        error = {ct.err_code: 999,
                 ct.err_msg: f"duplicated new collection name default:{alias_name} with other collection name or alias"}
        self.utility_wrap.rename_collection(collection_w.name, alias_name,
                                            check_task=CheckTasks.err_res, check_items=error)
