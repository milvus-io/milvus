import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from utils.utils import *
from common.constants import *

prefix = "alias"
exp_name = "name"
exp_schema = "schema"
default_schema = cf.gen_default_collection_schema()


class TestAliasParams(TestcaseBase):
    """ Test cases of alias interface parameters"""
    pass


class TestAliasParamsInvalid(TestcaseBase):
    """ Negative test cases of alias interface parameters"""
    pass


class TestAliasOperation(TestcaseBase):
    """ Test cases of alias interface operations"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_alias_create_operation_default(self):
        """
        target: test collection create alias
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
        collection_alias = self.init_collection_wrap(name=alias_name,
                                                     check_task=CheckTasks.check_collection_property,
                                                     check_items={exp_name: alias_name, exp_schema: default_schema})
        # assert collection is equal to alias
        assert [p.name for p in collection_w.partitions] == [p.name for p in collection_alias.partitions]

    def test_alias_alter_operation_default(self):
        """
        target: test collection alter alias
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
        collection_alias_a = self.init_collection_wrap(name=alias_a_name,
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
        collection_alias_b = self.init_collection_wrap(name=alias_b_name,
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
        target: test collection drop alias
        method: 
                1.create a collection with 10 partitons
                2.collection create a alias
                3.collection drop the alias
        expected: 
                in step 2, collection is equal to alias
                in step 3, collection is not equal to alias any more
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
        collection_alias = self.init_collection_wrap(name=alias_name,
                                                     check_task=CheckTasks.check_collection_property,
                                                     check_items={exp_name: alias_name, exp_schema: default_schema})

        assert [p.name for p in collection_w.partitions] == [p.name for p in collection_alias.partitions]

        collection_w.drop_alias(alias_name)

        collection_alias = self.init_collection_wrap(name=alias_name,
                                                     check_task=CheckTasks.check_collection_property,
                                                     check_items={exp_name: alias_name, exp_schema: default_schema})

        assert [p.name for p in collection_w.partitions] != [p.name for p in collection_alias.partitions]


class TestAliasOperationInvalid(TestcaseBase):
    """ Negative test cases of alias interface operations"""
    pass
