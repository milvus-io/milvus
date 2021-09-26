from typing import Collection
import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.utils import *
from common.constants import *

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
        error = {ct.err_code: 1, ct.err_msg: f"Invalid collection alias: {alias_name}. The first character of a collection alias must be an underscore or letter"}
        collection_w.create_alias(alias_name,
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
                2.collection create a alias, then init a collection with this alias name but not create partitions 
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
        collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name, exp_schema: default_schema})
        # assert collection is equal to alias
        assert [p.name for p in collection_w.partitions] == [p.name for p in collection_alias.partitions]

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
                in step 3, collection_1 is equal to alias_a and alias_b, and collection_2 is not equal to alias_b any more
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
        collection_1.create_alias(alias_a_name)
        collection_alias_a, _ = self.collection_wrap.init_collection(name=alias_a_name,
                                                                     check_task=CheckTasks.check_collection_property,
                                                                     check_items={exp_name: alias_a_name, exp_schema: default_schema})

        assert [p.name for p in collection_1.partitions] == [p.name for p in collection_alias_a.partitions]        
        
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
        collection_2.create_alias(alias_b_name)
        collection_alias_b, _ = self.collection_wrap.init_collection(name=alias_b_name,
                                                                     check_task=CheckTasks.check_collection_property,
                                                                     check_items={exp_name: alias_b_name, exp_schema: default_schema})

        assert [p.name for p in collection_2.partitions] == [p.name for p in collection_alias_b.partitions]
        
        # collection_1 alter alias to alias_b
        # so collection_1 has two alias name, alias_a and alias_b, but collection_2 has no alias
        collection_1.alter_alias(alias_b_name)

        # assert collection_1 is equal to alias_b
        # assert collection_1 is equal to alias_a
        # assert collections_2 is not equal to alias_b
        assert [p.name for p in collection_1.partitions] == [p.name for p in collection_alias_b.partitions]
        assert [p.name for p in collection_1.partitions] == [p.name for p in collection_alias_a.partitions]
        assert [p.name for p in collection_2.partitions] != [p.name for p in collection_alias_b.partitions]

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_drop_operation_default(self):
        """
        target: test collection dropping alias
        method: 
                1.create a collection with 10 partitons
                2.collection create a alias
                3.collection drop the alias
        expected: 
                in step 2, collection is equal to alias
                in step 3, collection with alias name is exist
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
        collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name, exp_schema: default_schema})

        assert [p.name for p in collection_w.partitions] == [p.name for p in collection_alias.partitions]

        collection_w.drop_alias(alias_name)
        error = {ct.err_code: 0, ct.err_msg: f"Collection '{alias_name}' not exist, or you can pass in schema to create one"}
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.err_res,
                                                                   check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_insert_data_default(self):
        """
        target: test alias inserting data
        method: 
                1.create a collection with alias
                2.collection insert data by alias
        expected: inserting data by alias can work and the result is same as inserting data directly

        """
        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        collection_w.create_alias(alias_name)
        collection_alias, _ = self.collection_wrap.init_collection(name=alias_name,
                                                                   check_task=CheckTasks.check_collection_property,
                                                                   check_items={exp_name: alias_name, exp_schema: default_schema})
        df = cf.gen_default_dataframe_data(ct.default_nb)
        collection_alias.insert(data=df)

        assert collection_w.num_entities == ct.default_nb
        assert collection_alias.num_entities == ct.default_nb


class TestAliasOperationInvalid(TestcaseBase):
    """ Negative test cases of alias interface operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_create_dupcation_alias(self):
        """
        target: test two collections creating alias with same name
        method: 
                1.create a collection_1 with alias name alias_a
                2.create a collection_2 also with alias name alias_a
        expected: 
                in step 2, creating alias with a dupcation name is not allowed
        """        

        self._connect()
        c_1_name = cf.gen_unique_str("collection")
        collection_1 = self.init_collection_wrap(name=c_1_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_1_name, exp_schema: default_schema})
        alias_a_name = cf.gen_unique_str(prefix)
        collection_1.create_alias(alias_a_name)

        c_2_name = cf.gen_unique_str("collection")
        collection_2 = self.init_collection_wrap(name=c_2_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_2_name, exp_schema: default_schema})
        error = {ct.err_code: 1, ct.err_msg: "Create alias failed: duplicate collection alias"}                                         
        collection_2.create_alias(alias_a_name,
                                  check_task=CheckTasks.err_res,
                                  check_items=error) 

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_alter_not_exist_alias(self):
        """
        target: test collection altering to alias which is not exist
        method: 
                1.create a collection with alias
                2.collection alters to a alias name which is not exist
        expected: 
                in step 2, alter alias with a not exist name is not allowed
        """

        self._connect()
        c_name = cf.gen_unique_str("collection")
        collection_w = self.init_collection_wrap(name=c_name, schema=default_schema,
                                                 check_task=CheckTasks.check_collection_property,
                                                 check_items={exp_name: c_name, exp_schema: default_schema})
        alias_name = cf.gen_unique_str(prefix)
        collection_w.create_alias(alias_name)

        alias_not_exist_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "Alter alias failed: alias does not exist"}                                         
        collection_w.alter_alias(alias_not_exist_name,
                                 check_task=CheckTasks.err_res,
                                 check_items=error)

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
        collection_w.create_alias(alias_name)

        alias_not_exist_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 1, ct.err_msg: "Drop alias failed: alias does not exist"}                                         
        collection_w.drop_alias(alias_not_exist_name,
                                check_task=CheckTasks.err_res,
                                check_items=error)

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
        collection_w.create_alias(alias_name)
        collection_w.drop_alias(alias_name)

        error = {ct.err_code: 1, ct.err_msg: "Drop alias failed: alias does not exist"}                                         
        collection_w.drop_alias(alias_name,
                                check_task=CheckTasks.err_res,
                                check_items=error)

    def test_alias_cerate_dup_name_collection(self):
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
        collection_w.create_alias(alias_name)

        error = {ct.err_code: 0, ct.err_msg: "The collection already exist, but the schema is not the same as the schema passed in"} 
        self.init_collection_wrap(alias_name, schema=default_binary_schema,
                                  check_task=CheckTasks.err_res,
                                  check_items=error)
