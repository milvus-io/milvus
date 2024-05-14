import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

config_nodes = 8


class TestResourceGroupParams(TestcaseBase):

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_rg_default(self):
        """
        method:
        1. create a rg abc
        2. describe the default rg and the rg abc
        3. transfer 1 query node from default rg to the rg abc
        4. describe the rg abc and verify both the capacity and available nodes increased 1
        5. describe the default rg and verify both the capacity and available nodes decreased 1
        6. try to drop the rg abc, expected fail with err mgs
        7. transfer 1 query node from the rg abc to the default rg
        8. describe both rgs and verify the values changed accordingly
        9. drop the rg abc, expected success
        verify: transfer node successfully and rg info updated accordingly
        """
        self._connect()
        rgs, _ = self.utility_wrap.list_resource_groups()
        rgs_count = len(rgs)
        default_rg_init_available_node = config_nodes
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": default_rg_init_available_node,
                           "num_loaded_replica": {},
                           "num_outgoing_node": {},
                           "num_incoming_node": {}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)

        # create my rg
        m_rg_name = cf.gen_unique_str("rg")
        self.init_resource_group(name=m_rg_name)
        rgs, _ = self.utility_wrap.list_resource_groups()
        new_rgs_count = len(rgs)
        assert rgs_count + 1 == new_rgs_count
        new_rg_init_cap = 0
        new_rg_init_num_available_node = 0
        new_rg_init_info = {"name": m_rg_name,
                            "capacity": new_rg_init_cap,
                            "num_available_node": new_rg_init_num_available_node,
                            "num_loaded_replica": {},
                            "num_outgoing_node": {},
                            "num_incoming_node": {},
                            }
        self.utility_wrap.describe_resource_group(name=m_rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=new_rg_init_info)

        # transfer 1 query node from default to my rg
        num_node = 1
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name, target=m_rg_name, num_node=num_node)
        rgs, _ = self.utility_wrap.list_resource_groups()
        assert new_rgs_count == len(rgs)
        target_rg_info = {"name": m_rg_name,
                          "capacity": new_rg_init_cap + num_node,
                          "num_available_node": new_rg_init_num_available_node + num_node,
                          }
        self.utility_wrap.describe_resource_group(name=m_rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=target_rg_info)
        source_rg_info = {"name": ct.default_resource_group_name,
                          "capacity": ct.default_resource_group_capacity,
                          "num_available_node": default_rg_init_available_node - num_node,
                          }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=source_rg_info)

        # try to drop my rg
        error = {ct.err_code: 999, ct.err_msg: 'failed to drop resource group, err=delete non-empty rg is not permitted'}
        self.utility_wrap.drop_resource_group(name=m_rg_name, check_task=ct.CheckTasks.err_res,
                                              check_items=error)

        # transfer back the query node to default rg
        self.utility_wrap.transfer_node(source=m_rg_name, target=ct.default_resource_group_name, num_node=num_node)
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)
        self.utility_wrap.describe_resource_group(name=m_rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=new_rg_init_info)

        # try to drop my rg again
        self.utility_wrap.drop_resource_group(name=m_rg_name)
        rgs, _ = self.utility_wrap.list_resource_groups()
        assert len(rgs) == rgs_count
        error = {ct.err_code: 999, ct.err_msg: "failed to describe resource group, err=resource group doesn't exist"}
        self.utility_wrap.describe_resource_group(name=m_rg_name,
                                                  check_task=ct.CheckTasks.err_res,
                                                  check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("rg_name", ct.invalid_resource_names)
    def test_create_n_drop_rg_invalid_name(self, rg_name):
        """
        method: create a rg with an invalid name(what are invalid names? types, length, chinese,symbols)
        verify: fail with error msg
        """
        self._connect()
        error = {ct.err_code: 999, ct.err_msg: "Invalid resource group name"}
        if rg_name is None or rg_name == "":
            error = {ct.err_code: 999, ct.err_msg: "is illegal"}
            self.init_resource_group(rg_name, check_task=ct.CheckTasks.err_res, check_items=error)
        else:
            self.init_resource_group(rg_name, check_task=ct.CheckTasks.err_res, check_items=error)
            # verify drop succ with invalid names
            self.utility_wrap.drop_resource_group(rg_name)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_create_rg_max_length_name(self):
        """
        method: create a rg with a max length name
        verify: create a rg successfully
        """
        name_max_length = 255
        rg_name = cf.gen_str_by_length(name_max_length, letters_only=True)
        self.init_resource_group(name=rg_name)
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items={"name": rg_name})
        rg_name = cf.gen_str_by_length(name_max_length + 1)
        error = {ct.err_code: 999,
                 ct.err_msg: "Invalid resource group name"}
        self.init_resource_group(name=rg_name,
                                 check_task=ct.CheckTasks.err_res,
                                 check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_create_rg_dup_name(self):
        """
        method:
        1. create a rg with name starts with "_"
        2. create a rg with name abc again
        verify: fail with error msg when creating with a dup name
        """
        self._connect()
        rg_name = cf.gen_unique_str('_rg')
        self.init_resource_group(name=rg_name)
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items={"name": rg_name})
        error = {ct.err_code: 999,
                 ct.err_msg: "resource group already exist"}
        self.init_resource_group(name=rg_name,
                                 check_task=ct.CheckTasks.err_res,
                                 check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_create_rg_dropped_name(self):
        """
        method:
        1. create a rg with name abc
        2. list rgs and describe the rg abc
        3. drop the rg abc
        4. list rgs and describe the rg abc
        5. create rg with the same name
        6. list rgs and describe the rg abc
        verify: create rg successfully for both times
        """
        self._connect()
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(name=rg_name)
        rgs_count = len(self.utility_wrap.list_resource_groups()[0])
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items={"name": rg_name})
        # drop the rg
        self.utility_wrap.drop_resource_group(name=rg_name)
        assert len(self.utility_wrap.list_resource_groups()[0]) == rgs_count - 1
        error = {ct.err_code: 999,
                 ct.err_msg: "failed to describe resource group, err=resource group doesn't exist"}
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.err_res,
                                                  check_items=error)
        # create the rg with the same name again
        self.init_resource_group(name=rg_name)
        assert rgs_count == len(self.utility_wrap.list_resource_groups()[0])
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items={"name": rg_name})

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_create_max_number_rgs(self):
        """
        method:
        1. create rgs at a max number (max=1024)
        2. list rgs
        3. create one more rg
        4. list rgs
        verify: create successfully at a max number and fail when create one more
        """
        from pymilvus.exceptions import MilvusException
        max_rg_num = 1024  # including default rg
        try:
            for i in range(max_rg_num):
                rg_name = cf.gen_unique_str(f'rg_{i}')
                self.init_resource_group(rg_name, check_task=CheckTasks.check_nothing)
        except MilvusException:
            pass

        rgs = self.utility_wrap.list_resource_groups()[0]
        assert len(rgs) == max_rg_num
        error = {ct.err_code: 999, ct.err_msg: 'resource group num reach limit'}
        self.init_resource_group(name=cf.gen_unique_str('rg'),
                                 check_task=CheckTasks.err_res,
                                 check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_drop_rg_non_existing(self):
        """
        method: drop a rg with a non-existing name
        verify: drop successfully
        """
        self._connect()
        rgs_count = len(self.utility_wrap.list_resource_groups()[0])
        rg_name = 'non_existing'
        self.utility_wrap.drop_resource_group(name=rg_name)
        assert rgs_count == len(self.utility_wrap.list_resource_groups()[0])

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_drop_rg_twice(self):
        """
        method:
        1. create a rg abc
        2. list rgs
        3. drop the rg abc
        4. list rgs and describe the rg
        verify: drop fail with error msg at second time
        """
        self._connect()
        rgs_count = len(self.utility_wrap.list_resource_groups()[0])
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(name=rg_name)
        assert rgs_count + 1 == len(self.utility_wrap.list_resource_groups()[0])
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items={"name": rg_name})
        self.utility_wrap.drop_resource_group(name=rg_name)
        assert rgs_count == len(self.utility_wrap.list_resource_groups()[0])
        self.utility_wrap.drop_resource_group(name=rg_name)
        assert rgs_count == len(self.utility_wrap.list_resource_groups()[0])

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_drop_default_rg(self):
        """
        method:
        1. drop default rg when there are available nodes
        2. drop default rg when there are no available nodes
        3. drop the rg abc
        4. list rgs and describe the rg
        verify: drop fail with error msg at second time
        """
        self._connect()
        rgs_count = len(self.utility_wrap.list_resource_groups()[0])
        default_rg_init_available_node = config_nodes
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": default_rg_init_available_node,
                           "num_loaded_replica": {},
                           "num_outgoing_node": {},
                           "num_incoming_node": {}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)
        error = {ct.err_code: 5, ct.err_msg: 'delete default rg is not permitted'}
        self.utility_wrap.drop_resource_group(name=ct.default_resource_group_name,
                                              check_task=CheckTasks.err_res,
                                              check_items=error)
        assert len(self.utility_wrap.list_resource_groups()[0]) == rgs_count
        default_rg, _ = self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                                  check_task=CheckTasks.check_rg_property,
                                                                  check_items=default_rg_info)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("rg_name", ct.invalid_resource_names)
    def test_describe_rg_invalid_name(self, rg_name):
        """
        method: describe a rg with an invalid name(what are invalid names? types, length, chinese,symbols)
        verify: fail with error msg
        """
        self._connect()
        error = {ct.err_code: 999, ct.err_msg: f"resource group not found[rg={rg_name}]"}
        if rg_name is None or rg_name == "":
            error = {ct.err_code: 999, ct.err_msg: f"`resource_group_name` value {rg_name} is illegal"}
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=ct.CheckTasks.err_res,
                                                  check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_describe_default_rg(self):
        """
        method: describe the default rg
        verify: verify the capacity, available nodes, num_loaded_replica,
        num_outgoing_node and  num_incoming_node
        """
        self._connect()
        default_rg_init_available_node = config_nodes
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": default_rg_init_available_node,
                           "num_loaded_replica": {},
                           "num_outgoing_node": {},
                           "num_incoming_node": {}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=ct.CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)


class TestTransferNode(TestcaseBase):
    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_transfer_node_default(self):
        """
        Method:
        1. create 2 rgs: RgA and RgB
        2. transfer 1 node from RgA to RgB
        verify failure with error
        3. transfer 1 node from default to RgA
        verify transfer successfully
        4. transfer 1 node from RgA to RgB
        verify transfer successfully
        5. drop rg RgB
        verify fail with error
        6. drop rg RgA
        verify success rg RgA
        """
        self._connect()
        rg1_name = cf.gen_unique_str('rg1')
        rg2_name = cf.gen_unique_str('rg2')
        self.init_resource_group(rg1_name)
        self.init_resource_group(rg2_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name, target=rg1_name, num_node=1)
        rg_init_cap = 0
        rg_init_available_node = 0
        rg1_info = {"name": rg1_name,
                    "capacity": rg_init_cap + 1,
                    "num_available_node": rg_init_available_node + 1,
                    "num_loaded_replica": {},
                    "num_outgoing_node": {},
                    "num_incoming_node": {}
                    }
        self.utility_wrap.describe_resource_group(name=rg1_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg1_info)

        # transfer a query node to rgB
        self.utility_wrap.transfer_node(source=rg1_name, target=rg2_name, num_node=1)
        rg2_info = {"name": rg2_name,
                    "capacity": rg_init_cap + 1,
                    "num_available_node": rg_init_available_node + 1,
                    "num_loaded_replica": {},
                    "num_outgoing_node": {},
                    "num_incoming_node": {}
                    }
        self.utility_wrap.describe_resource_group(name=rg2_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg2_info)
        rg1_info = {"name": rg1_name,
                    "capacity": rg_init_cap,
                    "num_available_node": rg_init_available_node,
                    "num_loaded_replica": {},
                    "num_outgoing_node": {},
                    "num_incoming_node": {}
                    }
        self.utility_wrap.describe_resource_group(name=rg1_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg1_info)

        # drop rgB
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to drop resource group, err=delete non-empty rg is not permitted'}
        self.utility_wrap.drop_resource_group(name=rg2_name,
                                              check_task=CheckTasks.err_res,
                                              check_items=error
                                              )
        self.utility_wrap.drop_resource_group(name=rg1_name)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_transfer_node_from_default_rg(self, with_growing):
        """
        Method:
        1. prepare a collection to search
        2. create a rgA
        3. transfer the node from default rg to rgA
        4. insert to make some growing
        verify search keeps succ
        5. transfer the node back to default rg
        6. insert to make some growing
        verify search keeps succ
        """
        # create a collectionA and insert data
        dim = 128
        collection_w, _, _, insert_ids, _ = self.init_collection_general(insert_data=True, dim=dim)
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=100))
            insert_ids.extend(res.primary_keys)
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": config_nodes,
                           "num_loaded_replica": {collection_w.name: 1},
                           "num_outgoing_node": {},
                           "num_incoming_node": {}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)

        # create a rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)
        # transfer query node from default to rgA
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=1)
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=100))
            insert_ids.extend(res.primary_keys)

        # verify rg state
        rg_info = {"name": rg_name,
                   "capacity": 1,
                   "num_available_node": 1,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {},
                   "num_incoming_node": {collection_w.name: 1}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": config_nodes - 1,
                           "num_loaded_replica": {collection_w.name: 1},
                           "num_outgoing_node": {collection_w.name: 1},
                           "num_incoming_node": {}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)

        # verify search keeps succ after transfer
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # transfer query node back to default
        self.utility_wrap.transfer_node(source=rg_name,
                                        target=ct.default_resource_group_name,
                                        num_node=1)
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=100))
            insert_ids.extend(res.primary_keys)

        # verify search keeps succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # verify rg state
        rg_info = {"name": rg_name,
                   "capacity": 0,
                   "num_available_node": 0,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": config_nodes,
                           "num_loaded_replica": {collection_w.name: 1},
                           "num_outgoing_node": {},
                           "num_incoming_node": {}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("num_node", [0, 99, -1, 0.5, "str"])
    def test_transfer_node_with_wrong_num_node(self, num_node):
        """
        Method:
        1. create rgA
        2. transfer invalid number of nodes from default to rgA
        verify fail with error
        """
        # create rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)

        # transfer replicas
        error = {ct.err_code: 999, ct.err_msg: f"failed to transfer node between resource group, err=nodes not enough"}
        if num_node in [0, -1]:
            error = {ct.err_code: 999, ct.err_msg: f"transfer node num can't be [{num_node}]"}
        if type(num_node) is not int:
            error = {ct.err_code: 999, ct.err_msg: 'expected one of: int'}
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=num_node,
                                        check_task=CheckTasks.err_res,
                                        check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("num_replica", [0, 99, -1, 0.5, True, "str"])
    def test_transfer_replica_with_wrong_num_replica(self, num_replica):
        """
        Method:
        1. prepare a collection with 1 replica loaded
        2. prepare rgA
        2. transfer invalid number of replicas from default to rgA
        verify fail with error
        """
        # prepare a collection with 1 replica loaded
        collection_w, _, _, _, _ = self.init_collection_general(insert_data=True, nb=200)
        # create rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)

        # transfer replicas
        error = {ct.err_code: 999, ct.err_msg: f"failed to transfer replica between resource group, err=nodes not enough"}
        if not type(num_replica) in [int, bool]:
            error = {ct.err_code: 999, ct.err_msg: 'expected one of: int'}
        if num_replica in [0, -1]:
            error = {ct.err_code: 999, ct.err_msg: f"transfer replica num can't be [{num_replica}]"}
        if num_replica == 99:
            error = {ct.err_code: 999, ct.err_msg: f"only found [1] replicas in source resource group[__default_resource_group]"}

        self.utility_wrap.transfer_replica(source=ct.default_resource_group_name,
                                           target=rg_name,
                                           collection_name=collection_w.name,
                                           num_replica=num_replica,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_load_collection_with_no_available_node(self):
        """
        Method:
        1. create a collectionA and insert data
        2. release the collection if loaded
        3. create a rgA
        4. transfer query node from default to rgA
        5. load the collection with default rg
        verify load fail with no available query node in default rg
        6. load the collection with rgA
        verify load succ and search succ
        verify rgA state
        7. transfer query node from rgA to default rg
        verify search keeps succ
        verify rgA and default rg state
        8. load the collection without rg specified
        verify load succ
        """
        self._connect()
        dim = ct.default_dim
        # 1. create a collectionA and insert data
        collection_w, _, _, insert_ids, _ = self.init_collection_general(insert_data=True, dim=dim)
        # 2. release the collection if loaded
        collection_w.release()
        # 3. create a rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)
        # 4. transfer query node from default to rgA
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=config_nodes)
        # 5. load the collection with default rg
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to load collection, err=failed to spawn replica for collection[nodes not enough]'}
        collection_w.load(check_task=CheckTasks.err_res, check_items=error)

        # 6. load the collection with rgA
        rg_info = {"name": rg_name,
                   "capacity": config_nodes,
                   "num_available_node": config_nodes,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        collection_w.load(_resource_groups=[rg_name])
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # check rgA info
        rg_info = {"name": rg_name,
                   "capacity": config_nodes,
                   "num_available_node": config_nodes,
                   "num_loaded_replica": {collection_w.name: 1},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)

        # 7. transfer query node from rgA to default rg
        self.utility_wrap.transfer_node(source=rg_name,
                                        target=ct.default_resource_group_name,
                                        num_node=config_nodes)
        # verify search keeps succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # check rgA info after transfer
        rg_info = {"name": rg_name,
                   "capacity": 0,
                   "num_available_node": 0,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {collection_w.name: config_nodes},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": config_nodes,
                           "num_loaded_replica": {},
                           "num_outgoing_node": {},
                           "num_incoming_node": {collection_w.name: config_nodes}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)

        # 8. load the collection with default rg
        collection_w.load()

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("replicas", [1, 3])
    def test_load_collection_with_multi_replicas_multi_rgs(self, replicas):
        """
        Method:
        1. create a collection and insert data
        2. release if loaded
        3. create rgA and rgB
        4. load collection with different replicas into rgA and rgB
        5. load collection with different replicas into default rg and rgA
        """
        self._connect()
        # 1. create a collectionA and insert data
        collection_w, _, _, _, _ = self.init_collection_general(insert_data=True)
        # 2. release the collection if loaded
        collection_w.release()
        # 3. create a rgA and rgB
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(rgA_name)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(rgB_name)

        # load with different replicas
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to load collection, err=failed to spawn replica for collection[resource group num can only be 0, 1 or same as replica number]'}
        collection_w.load(replica_number=replicas,
                          _resource_groups=[rgA_name, rgB_name],
                          check_task=CheckTasks.err_res, check_items=error)

        error = {ct.err_code: 999,
                 ct.err_msg: "failed to load collection, err=load operation can't use default resource group and other resource group together"}
        collection_w.load(replica_number=replicas,
                          _resource_groups=[ct.default_resource_group_name, rgB_name],
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("rg_names", [[""], ["non_existing"], "不合法"])
    def test_load_collection_with_empty_rg_name(self, rg_names):
        """
        Method:
        1. create a collection
        2. load with empty rg name
        """
        collection_w = self.init_collection_wrap()
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        error = {ct.err_code: 999,
                 ct.err_msg: "failed to load collection, err=failed to spawn replica for collection"}
        collection_w.load(_resource_groups=rg_names,
                          check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_load_partition_with_no_available_node(self):
        """
        Method:
        1. create a partition and insert data
        3. create a rgA
        4. transfer query node from default to rgA
        5. load the collection with default rg
        verify load fail with no available query node in default rg
        6. load the partition with rgA
        verify load succ and search succ
        verify rgA state
        7. transfer query node from rgA to default rg
        verify search keeps succ
        verify rgA and default rg state
        8. load the partition without rg specified
        verify load succ
        """
        self._connect()
        dim = ct.default_dim
        # 1. create a partition and insert data
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str('par')
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        data = cf.gen_default_list_data(nb=2000)
        ins_res, _ = partition_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        # 3. create a rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)
        # 4. transfer all query nodes from default to rgA
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=config_nodes)
        # 5. load the collection with default rg
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to load partitions, err=failed to spawn replica for collection[nodes not enough]'}
        partition_w.load(check_task=CheckTasks.err_res, check_items=error)

        # 6. load the collection with rgA
        partition_w.load(_resource_groups=[rg_name])
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # verify search succ
        partition_w.search(vectors[:nq],
                           ct.default_float_vec_field_name,
                           ct.default_search_params,
                           ct.default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": nq,
                                        "ids": ins_res.primary_keys,
                                        "limit": ct.default_limit}
                           )
        # check rgA info
        rg_info = {"name": rg_name,
                   "capacity": config_nodes,
                   "num_available_node": config_nodes,
                   "num_loaded_replica": {collection_w.name: 1},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)

        # verify replica
        replicas = partition_w.get_replicas()
        num_nodes_for_replicas = 0
        assert len(replicas[0].groups) == 1
        for rep in replicas[0].groups:
            assert rep.resource_group == rg_name
            assert rep.num_outbound_node == {}
            num_nodes_for_replicas += len(rep.group_nodes)
        assert num_nodes_for_replicas == config_nodes

        # 7. transfer query node from rgA to default rg
        self.utility_wrap.transfer_node(source=rg_name,
                                        target=ct.default_resource_group_name,
                                        num_node=config_nodes)
        # verify search keeps succ
        partition_w.search(vectors[:nq],
                           ct.default_float_vec_field_name,
                           ct.default_search_params,
                           ct.default_limit,
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": nq,
                                        "ids": ins_res.primary_keys,
                                        "limit": ct.default_limit}
                           )
        # check rgA info after transfer
        rg_info = {"name": rg_name,
                   "capacity": 0,
                   "num_available_node": 0,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {collection_w.name: config_nodes},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        default_rg_info = {"name": ct.default_resource_group_name,
                           "capacity": ct.default_resource_group_capacity,
                           "num_available_node": config_nodes,
                           "num_loaded_replica": {},
                           "num_outgoing_node": {},
                           "num_incoming_node": {collection_w.name: config_nodes}
                           }
        self.utility_wrap.describe_resource_group(name=ct.default_resource_group_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=default_rg_info)

        # 8. load the collection without rg specified
        partition_w.load()

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("replicas", [1, 3])
    def test_load_partition_with_multi_replicas_multi_rgs(self, replicas):
        """
        Method:
        1. create a partition and insert data
        3. create rgA and rgB
        4. load partition with different replicas into rgA and rgB
        5. load partition with different replicas into default rg and rgA
        """
        self._connect()
        dim = ct.default_dim
        # 1. create a partition and insert data
        collection_w = self.init_collection_wrap()
        partition_name = cf.gen_unique_str('par')
        partition_w = self.init_partition_wrap(collection_w, partition_name)
        data = cf.gen_default_list_data(nb=3000)
        ins_res, _ = partition_w.insert(data)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)
        # 3. create a rgA and rgB
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(rgA_name)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(rgB_name)

        # load with different replicas
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to load partitions, err=failed to spawn replica for collection[resource group num can only be 0, 1 or same as replica number]'}
        partition_w.load(replica_number=replicas,
                         _resource_groups=[rgA_name, rgB_name],
                         check_task=CheckTasks.err_res, check_items=error)

        error = {ct.err_code: 999,
                 ct.err_msg: "failed to load partitions, err=load operation should use collection's resource group"}
        partition_w.load(replica_number=replicas,
                         _resource_groups=[ct.default_resource_group_name, rgB_name],
                         check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("rg_names", [[""], ["non_existing"], "不合法", [ct.default_resource_group_name, None]])
    def test_load_partition_with_empty_rg_name(self, rg_names):
        """
        Method:
        1. create a partition
        2. load with empty rg name
        """
        self._connect()
        # 1. create a partition
        collection_w = self.init_collection_wrap()
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)
        partition_name = cf.gen_unique_str('par')
        partition_w = self.init_partition_wrap(collection_w, partition_name)

        error = {ct.err_code: 1,
                 ct.err_msg: 'failed to load partitions, err=failed to spawn replica for collection[resource num can only be 0, 1 or same as replica number]'}
        partition_w.load(_resource_groups=rg_names,
                         check_task=CheckTasks.err_res, check_items=error)


# run the multi node tests with 8 query nodes
class TestResourceGroupMultiNodes(TestcaseBase):
    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_load_with_multi_collections_one_rg(self):
        """
        Method：
        1. prepare collectionA and collectionB
        2. prepare one rg with 4 nodes
        3. load collectionA with 1 replica into the rg
        4. load collectionB with 3 replica into the rg
        verify the rg state
        verify the replicas state for both collections
        """
        self._connect()
        dim = ct.default_dim
        # create a collectionA and collectionB
        collection_w_a = self.init_collection_wrap(shards_num=2)
        collection_w_b = self.init_collection_wrap(shards_num=2)
        nb = 400
        for i in range(5):
            collection_w_a.insert(cf.gen_default_list_data(nb=nb, dim=dim))
            collection_w_b.insert(cf.gen_default_list_data(nb=nb, dim=dim))
            collection_w_a.flush()
            collection_w_b.flush()
        collection_w_a.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w_b.create_index(ct.default_float_vec_field_name, ct.default_index)

        # create a rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)
        # transfer 4 nodes to rgA
        num_nodes_to_rg = 4
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=num_nodes_to_rg)

        # load 1 replica of collectionA to rgA
        collection_w_a.load(replica_number=1,
                            _resource_groups=[rg_name])
        # load 3 replica of collectionB to rgA
        collection_w_b.load(replica_number=3,
                            _resource_groups=[rg_name])

        # verify rg state
        rg_info = {"name": rg_name,
                   "capacity": num_nodes_to_rg,
                   "num_available_node": num_nodes_to_rg,
                   "num_loaded_replica": {collection_w_a.name: 1, collection_w_b.name: 3},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)

        # verify replica state
        replicas = collection_w_a.get_replicas()
        num_nodes_for_replicas = 0
        assert len(replicas[0].groups) == 1
        for rep in replicas[0].groups:
            assert rep.resource_group == rg_name
            assert rep.num_outbound_node == {}
            num_nodes_for_replicas += len(rep.group_nodes)
        assert num_nodes_for_replicas == num_nodes_to_rg

        replicas = collection_w_b.get_replicas()
        num_nodes_for_replicas = 0
        assert len(replicas[0].groups) == 3
        for rep in replicas[0].groups:
            assert rep.resource_group == rg_name
            assert rep.num_outbound_node == {}
            num_nodes_for_replicas += len(rep.group_nodes)
        assert num_nodes_for_replicas == num_nodes_to_rg

        # verify search succ on both collections
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w_a.search(vectors[:nq],
                              ct.default_float_vec_field_name,
                              ct.default_search_params,
                              ct.default_limit)
        collection_w_b.search(vectors[:nq],
                              ct.default_float_vec_field_name,
                              ct.default_search_params,
                              ct.default_limit)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_load_with_replicas_and_nodes_num(self, with_growing):
        """
        Method：
        1. prepare a collection with segments
        2. create rgA
        3. transfer 4 nodes to rgA
        4. load 5 replicas in rgA
        verify load fail with msg
        5. load 3 replica in rgA
        verify load succ
        6. release and reload 4 replica in rgA
        verify load succ and each replica occupies 1 query node in rgA
        7. verify load successfully again with no parameters
        """
        # prepare a collection with segments
        dim = 128
        collection_w = self.init_collection_wrap(shards_num=2)
        insert_ids = []
        nb = 3000
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i*nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        # create rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(name=rg_name)
        # transfer 4 nodes to rgA
        num_nodes_to_rg = 4
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=num_nodes_to_rg)
        rg_info = {"name": rg_name,
                   "capacity": num_nodes_to_rg,
                   "num_available_node": num_nodes_to_rg,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)

        # load 5 replicas in rgA and verify error msg
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to load collection, err=failed to spawn replica for collection[nodes not enough]'}
        collection_w.load(replica_number=num_nodes_to_rg+1,
                          _resource_groups=[rg_name],
                          check_task=ct.CheckTasks.err_res,
                          check_items=error)

        # load 3 replicas in rgA
        replica_number = 3
        collection_w.load(replica_number=replica_number,
                          _resource_groups=[rg_name])
        # verify rg state after loaded
        rg_info = {"name": rg_name,
                   "capacity": num_nodes_to_rg,
                   "num_available_node": num_nodes_to_rg,
                   "num_loaded_replica": {collection_w.name: replica_number},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        # verify replica state
        replicas = collection_w.get_replicas()
        num_nodes_for_replicas = 0
        assert len(replicas[0].groups) == replica_number
        for rep in replicas[0].groups:
            assert rep.resource_group == rg_name
            assert rep.num_outbound_node == {}
            num_nodes_for_replicas += len(rep.group_nodes)
        assert num_nodes_for_replicas == num_nodes_to_rg

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=7 * nb))
            insert_ids.extend(res.primary_keys)

        # verify search succ
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # release
        collection_w.release()
        # verify rg state after release
        rg_info = {"name": rg_name,
                   "capacity": num_nodes_to_rg,
                   "num_available_node": num_nodes_to_rg,
                   "num_loaded_replica": {},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)
        # reload 4 replica in rgA
        replica_number = 4
        collection_w.load(replica_number=replica_number,
                          _resource_groups=[rg_name])
        # verify rg state after reload
        rg_info = {"name": rg_name,
                   "capacity": num_nodes_to_rg,
                   "num_available_node": num_nodes_to_rg,
                   "num_loaded_replica": {collection_w.name: replica_number},
                   "num_outgoing_node": {},
                   "num_incoming_node": {}
                   }
        self.utility_wrap.describe_resource_group(name=rg_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rg_info)

        # verify replica state
        replicas = collection_w.get_replicas()
        assert len(replicas[0].groups) == replica_number
        for rep in replicas[0].groups:
            assert rep.resource_group == rg_name
            assert rep.num_outbound_node == {}
            assert len(rep.group_nodes) == 1   # one replica for each node

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=8 * nb))
            insert_ids.extend(res.primary_keys)

        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # verify load successfully again with no parameters
        collection_w.load(replica_number=replica_number)
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_load_with_replicas_and_rgs_num(self, with_growing):
        """
        Method：
        1. prepare a collection with multi segments
        2. create rgA and rgB
        3. transfer 2 nodes to rgA and 3 nodes to rgB
        4. load 3 replicas in rgA and rgB
        verify load fail with msg
        5. load 1 replica in rgA and rgB
        verify load fail with msg
        6. load 2 replica in rgA and rgB
        verify load succ and each replica occupies one rg
        7. verify load successfully again with no parameters
        """
        # prepare a collection with segments
        dim = 128
        collection_w = self.init_collection_wrap(shards_num=2)
        insert_ids = []
        nb = 3000
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        # create rgA and rgB
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(name=rgA_name)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(name=rgB_name)
        # transfer 2 nodes to rgA and 3 nodes to rgB
        num_nodes_rgA = 2
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=num_nodes_rgA)
        num_nodes_rgB = 3
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgB_name,
                                        num_node=num_nodes_rgB)
        # load 3 replicas in rgA and rgB
        replica_number = 3
        error = {ct.err_code: 999,
                 ct.err_msg: 'failed to load collection, err=failed to spawn replica for collection[resource group num can only be 0, 1 or same as replica number]'}
        collection_w.load(replica_number=replica_number,
                          _resource_groups=[rgA_name, rgB_name],
                          check_task=CheckTasks.err_res,
                          check_items=error)

        # load 1 replica in rgA and rgB
        replica_number = 1
        collection_w.load(replica_number=replica_number,
                          _resource_groups=[rgA_name, rgB_name],
                          check_task=CheckTasks.err_res,
                          check_items=error)

        # load 2 replica in rgA and rgB
        replica_number = 2
        collection_w.load(replica_number=replica_number,
                          _resource_groups=[rgA_name, rgB_name])
        # verify replica state: each replica occupies one rg
        replicas = collection_w.get_replicas()
        assert len(replicas[0].groups) == replica_number
        for rep in replicas[0].groups:
            assert rep.num_outbound_node == {}
            assert rep.resource_group in [rgA_name, rgB_name]
            if rep.resource_group == rgA_name:
                assert len(rep.group_nodes) == num_nodes_rgA   # one replica for each rg
            else:
                assert len(rep.group_nodes) == num_nodes_rgB   # one replica for each rg

        # verify get segment info return not empty
        segments_info = self.utility_wrap.get_query_segment_info(collection_name=collection_w.name)[0]
        assert len(segments_info) > 0

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=200, dim=dim, start=8 * nb))
            insert_ids.extend(res.primary_keys)

        # verify search succ
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # verify load successfully again with no parameters
        collection_w.load(replica_number=2)
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_transfer_multi_replicas_from_to_default_rg(self, with_growing):
        """
        Method：
        1. prepare a collection with multi segments
        2. create rgA with 4 nodes
        3. reload 3 replicas into default rg
        4. transfer 3 replicas from default rg to rgA
        5. release and reload 3 replicas into rgA
        6. transfer 3 replicas from rgA to default rg
        """
        self._connect()
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(name=rgA_name)

        # transfer 4 nodes to rgA
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=4)

        dim = 128
        collection_w = self.init_collection_wrap(shards_num=4)
        insert_ids = []
        nb = 500
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)

        #  release collection and reload 2 replicas into rgA
        num_replica = 3
        collection_w.load(replica_number=num_replica, _resource_groups=[ct.default_resource_group_name])

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        # transfer 1/2/3 replicas from default rg to rgA
        self.utility_wrap.transfer_replica(source=ct.default_resource_group_name, target=rgA_name,
                                           collection_name=collection_w.name,
                                           num_replica=num_replica)
        # verify search succ
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # release and reload 3 replicas into rgA
        collection_w.release()
        collection_w.load(replica_number=num_replica, _resource_groups=[rgA_name])

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=7 * nb))
            insert_ids.extend(res.primary_keys)

        # transfer 1/2/3 replicas from rgA to default rg
        self.utility_wrap.transfer_replica(source=rgA_name, target=ct.default_resource_group_name,
                                           collection_name=collection_w.name,
                                           num_replica=num_replica)
        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_transfer_multi_replicas_into_one_rg(self, with_growing):
        """
        Method：
        1. prepare a collection with multi segments
        2. create rgA with 2 nodes and rgB with 3 nodes
        7. reload 2 replicas into rgA
        8. transfer 2 replicas from rgA to rgB
        """
        self._connect()
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(name=rgA_name)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(name=rgB_name)

        # transfer 2 nodes to rgA, 3 nodes to rgB
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=2)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgB_name,
                                        num_node=3)

        dim = 128
        collection_w = self.init_collection_wrap(shards_num=4)
        insert_ids = []
        nb = 500
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)

        #  release collection and reload 2 replicas into rgA
        num_replica = 2
        collection_w.load(replica_number=num_replica, _resource_groups=[rgA_name])

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        # verify replicas
        replicas = collection_w.get_replicas()
        assert len(replicas[0].groups) == num_replica
        for rep in replicas[0].groups:
            assert rep.resource_group in [rgA_name]
            assert rep.num_outbound_node == {}
            assert len(rep.group_nodes) == 1  # 1 node for each replica

        #  transfer 2 replicas from rgA to rgB
        self.utility_wrap.transfer_replica(source=rgA_name, target=rgB_name,
                                           collection_name=collection_w.name,
                                           num_replica=num_replica)

        # verify replicas
        replicas = collection_w.get_replicas()
        assert len(replicas[0].groups) == num_replica
        for rep in replicas[0].groups:
            assert rep.resource_group in [rgB_name]
            assert rep.num_outbound_node == {rgA_name: 1}

        # verify rg state
        rgA_info = {"name": rgA_name,
                    "capacity": 2,
                    "num_available_node": 2,
                    "num_loaded_replica": {},
                    "num_outgoing_node": {},
                    "num_incoming_node": {collection_w.name: 2}
                    }
        self.utility_wrap.describe_resource_group(name=rgA_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rgA_info)
        rgB_info = {"name": rgB_name,
                    "capacity": 3,
                    "num_available_node": 3,
                    "num_loaded_replica": {collection_w.name: 2},
                    "num_outgoing_node": {collection_w.name: 2},
                    "num_incoming_node": {}
                    }
        self.utility_wrap.describe_resource_group(name=rgB_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rgB_info)

        # verify search succ
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # wait transfer completed for replicas
        time.sleep(120)
        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=7 * nb))
            insert_ids.extend(res.primary_keys)

        rgA_info = {"name": rgA_name,
                    "capacity": 2,
                    "num_available_node": 2,
                    "num_loaded_replica": {},
                    "num_outgoing_node": {},
                    "num_incoming_node": {}
                    }
        self.utility_wrap.describe_resource_group(name=rgA_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rgA_info)
        rgB_info = {"name": rgB_name,
                    "capacity": 3,
                    "num_available_node": 3,
                    "num_loaded_replica": {collection_w.name: 2},
                    "num_outgoing_node": {},
                    "num_incoming_node": {}
                    }
        self.utility_wrap.describe_resource_group(name=rgB_name,
                                                  check_task=CheckTasks.check_rg_property,
                                                  check_items=rgB_info)

        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # verify replicas
        replicas = collection_w.get_replicas()
        assert len(replicas[0].groups) == num_replica
        for rep in replicas[0].groups:
            assert rep.resource_group in [rgB_name]
            assert rep.num_outbound_node == {}
            assert 1 <= len(rep.group_nodes) <= 2  # 1-2 node for each replica

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_transfer_replica_into_same_rg(self, with_growing):
        """
        Method：
        1. prepare a collection with multi segments
        2. create rgA and rgB
        3. transfer 1 node to rgA and 2 nodes to rgB
        4. load 2 replicas in rgA and rgB
        5. transfer 1 replica from rgB to rgA
        verify fail with error
        6. transfer 1 replica from rgA to rgB
        verify fail with error
        7. release collection and reload 2 replicas into rgA
        8. transfer 2 replicas from rgA to rgB
        """
        self._connect()
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(name=rgA_name)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(name=rgB_name)

        # transfer 1 nodes to rgA, 2 nodes to rgB
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=2)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgB_name,
                                        num_node=3)

        dim = 128
        collection_w = self.init_collection_wrap(shards_num=4)
        insert_ids = []
        nb = 500
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)

        collection_w.load(replica_number=2, _resource_groups=[rgA_name, rgB_name])

        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # transfer 1 replica from rgB to rgA
        error = {ct.err_code: 999,
                 ct.err_msg: f'found [1] replicas of same collection in target resource group[{rgA_name}], dynamically increase replica num is unsupported'}
        self.utility_wrap.transfer_replica(source=rgB_name, target=rgA_name,
                                           collection_name=collection_w.name, num_replica=1,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)
        # transfer 1 replica from rgA to rgB
        error = {ct.err_code: 999,
                 ct.err_msg: f'found [1] replicas of same collection in target resource group[{rgB_name}], dynamically increase replica num is unsupported'}
        self.utility_wrap.transfer_replica(source=rgA_name, target=rgB_name,
                                           collection_name=collection_w.name, num_replica=1,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_load_multi_rgs_with_default_rg(self):
        """
        Method:
        1. prepare a rgA with 2 nodes
        2. create a collection
        3. load the collection into rgA and default rg
        verify fail with error msg
        4. load the collection into rgA and rgB
        verify search succ
        5. transfer replica from rgB to default rg
        verify fail
        6. transfer node from rgA to default rgA
        verify succ
        7. transfer node from rgB to default rgB
        8. verify search succ
        """
        self._connect()
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(name=rgA_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=2)
        dim = 128
        collection_w = self.init_collection_wrap(shards_num=1)
        insert_ids = []
        nb = 500
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_flat_index)

        # load the collection with rgA and default rg
        error = {ct.err_code: 999,
                 ct.err_msg: f"failed to load collection, err=load operation can't use default resource group and other resource group together"}
        collection_w.load(replica_number=2,
                          _resource_groups=[rgA_name, ct.default_resource_group_name],
                          check_task=CheckTasks.err_res,
                          check_items=error)

        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(name=rgB_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgB_name,
                                        num_node=2)

        # load the collection into rgA and rgB
        collection_w.load(replica_number=2,
                          _resource_groups=[rgA_name, rgB_name])
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        # transfer the replica from rgB to default rg
        error = {ct.err_code: 999,
                 ct.err_msg: "transfer replica will cause replica loaded in both default rg and other rg"}
        self.utility_wrap.transfer_replica(source=rgB_name,
                                           target=ct.default_resource_group_name,
                                           collection_name=collection_w.name,
                                           num_replica=1,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)

        time.sleep(1)
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # transfer all nodes back to default rg
        self.utility_wrap.transfer_node(source=rgA_name,
                                        target=ct.default_resource_group_name,
                                        num_node=2)
        self.utility_wrap.transfer_node(source=rgB_name,
                                        target=ct.default_resource_group_name,
                                        num_node=2)
        time.sleep(1)
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )
        collection_w.get_replicas()

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_transfer_replica_not_enough_replicas_to_transfer(self):
        """
        Method：
        1. prepare a collection with 1 replica loaded
        2. create rgA
        3. transfer 3 nodes to rgA
        4. transfer 2 replicas to rgA
        verify fail with error
        """
        self._connect()
        dim = ct.default_dim
        # create a collectionA and insert data
        collection_w, _, _, insert_ids, _ = self.init_collection_general(insert_data=True, dim=dim)
        # create a rgA
        rg_name = cf.gen_unique_str('rg')
        self.init_resource_group(rg_name)
        # transfer 3 nodes to rgA
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rg_name,
                                        num_node=3)

        # transfer 2 replicas to rgA
        error = {ct.err_code: 5,
                 ct.err_msg: f'only found [1] replicas in source resource group[{ct.default_resource_group_name}]'}
        self.utility_wrap.transfer_replica(source=ct.default_resource_group_name, target=rg_name,
                                           collection_name=collection_w.name, num_replica=2,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    def test_transfer_replica_non_existing_rg(self):
        """
        Method:
        1. prepare a collection ready for searching
        2. transfer the replica from default rg to non_existing rg
        """
        self._connect()
        dim = ct.default_dim
        # create a collectionA and insert data
        collection_w, _, _, insert_ids, _ = self.init_collection_general(insert_data=True, dim=dim)

        # transfer replica to a non_existing rg
        rg_name = "non_existing"
        error = {ct.err_code: 999,
                 ct.err_msg: f"the target resource group[{rg_name}] doesn't exist, err=resource group doesn't exist"}
        self.utility_wrap.transfer_replica(source=ct.default_resource_group_name,
                                           target=rg_name,
                                           collection_name=collection_w.name,
                                           num_replica=1,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)

        # transfer replica from a non_existing rg
        error = {ct.err_code: 999,
                 ct.err_msg: f"the source resource group[{rg_name}] doesn't exist, err=resource group doesn't exist"}
        self.utility_wrap.transfer_replica(source=rg_name,
                                           target=ct.default_resource_group_name,
                                           collection_name=collection_w.name,
                                           num_replica=1,
                                           check_task=CheckTasks.err_res,
                                           check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_transfer_node_when_there_same_collection(self, with_growing):
        """
        Method:
        1. prepare a collection
        2. prepare rgA and rgB and rgC, both with 2 node
        3. load 3 replicas into rgA, rgB and rgC
        verify search succ, replica info, rg state
        4. transfer 1 node from rgA to rgB
        verify fail with error
        """
        dim = 128
        collection_w = self.init_collection_wrap(shards_num=4)
        insert_ids = []
        nb = 500
        for i in range(5):
            res, _ = collection_w.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w.flush()
            insert_ids.extend(res.primary_keys)
        collection_w.create_index(ct.default_float_vec_field_name, ct.default_index)

        num_node =2
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(rgA_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=num_node)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(rgB_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgB_name,
                                        num_node=num_node)
        rgC_name = cf.gen_unique_str('rgC')
        self.init_resource_group(rgC_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgC_name,
                                        num_node=num_node)
        # make growing
        if with_growing:
            res, _ = collection_w.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        # load 2 replicas into rgA and rgB
        replica_num = 3
        collection_w.load(replica_number=replica_num, _resource_groups=[rgA_name, rgB_name, rgC_name])

        replicas = collection_w.get_replicas()
        assert len(replicas[0].groups) == replica_num
        for rep in replicas[0].groups:
            assert rep.resource_group in [rgA_name, rgB_name, rgC_name]
            assert rep.num_outbound_node == {}
            assert len(rep.group_nodes) == num_node  # 2 node for each replica

        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        # verify search succ
        collection_w.search(vectors[:nq],
                            ct.default_float_vec_field_name,
                            ct.default_search_params,
                            ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"nq": nq,
                                         "ids": insert_ids.copy(),
                                         "limit": ct.default_limit}
                            )

        # transfer node from rgA to rgB
        error = {ct.err_code: 999,
                 ct.err_msg: f"can't transfer node, cause the resource group[{rgA_name}] and the resource group[{rgB_name}] loaded same collection, err=resource group doesn't exist"}
        self.utility_wrap.transfer_node(source=rgA_name, target=rgB_name,
                                        num_node=1,
                                        check_task=CheckTasks.err_res,
                                        check_items=error)

    @pytest.mark.tags(CaseLabel.MultiQueryNodes)
    @pytest.mark.parametrize("with_growing", [True, False])
    def test_transfer_node_n_replicas_to_default_rg(self, with_growing):
        """
        Method:
        1. create 2 collections: c1 and c2
        2. create 2 rgs: rgA(1 node) and rgB(0 node)
        3. load c1 and c2 with 1 replica into default rg
        4. transfer 1 node from default rg to rgB and transfer replica of c2 from default to rgB
        5. transfer replica of c1 from default to rgB
        """
        dim = 128
        collection_w_a = self.init_collection_wrap(shards_num=4)
        collection_w_b = self.init_collection_wrap(shards_num=4)
        insert_ids = []
        nb = 500
        for i in range(5):
            res, _ = collection_w_a.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w_a.flush()
            res, _ = collection_w_b.insert(cf.gen_default_list_data(nb=nb, dim=dim, start=i * nb))
            collection_w_b.flush()
            insert_ids.extend(res.primary_keys)
        collection_w_a.create_index(ct.default_float_vec_field_name, ct.default_index)
        collection_w_b.create_index(ct.default_float_vec_field_name, ct.default_index)

        # prepare rg
        num_node = 2
        rgA_name = cf.gen_unique_str('rgA')
        self.init_resource_group(rgA_name)
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgA_name,
                                        num_node=num_node)
        rgB_name = cf.gen_unique_str('rgB')
        self.init_resource_group(rgB_name)

        # load collections
        collection_w_a.load()
        collection_w_b.load()

        # make growing
        if with_growing:
            res, _ = collection_w_a.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            res, _ = collection_w_b.insert(cf.gen_default_list_data(nb, dim=dim, start=6 * nb))
            insert_ids.extend(res.primary_keys)

        # transfer 1 node from default rg to rgB and transfer replica of c2 from default to rgB
        self.utility_wrap.transfer_node(source=ct.default_resource_group_name,
                                        target=rgB_name,
                                        num_node=1)
        self.utility_wrap.transfer_replica(source=ct.default_resource_group_name,
                                           target=rgB_name,
                                           collection_name=collection_w_b.name,
                                           num_replica=1)

        # transfer replica of c1 from default to rgB
        self.utility_wrap.transfer_replica(source=ct.default_resource_group_name,
                                           target=rgB_name,
                                           collection_name=collection_w_a.name,
                                           num_replica=1)

        # verify search succ
        nq = 5
        vectors = [[random.random() for _ in range(dim)] for _ in range(nq)]
        collection_w_a.search(vectors[:nq],
                              ct.default_float_vec_field_name,
                              ct.default_search_params,
                              ct.default_limit,
                              check_task=CheckTasks.check_search_results,
                              check_items={"nq": nq,
                                           "ids": insert_ids.copy(),
                                           "limit": ct.default_limit}
                              )
        collection_w_b.search(vectors[:nq],
                              ct.default_float_vec_field_name,
                              ct.default_search_params,
                              ct.default_limit,
                              check_task=CheckTasks.check_search_results,
                              check_items={"nq": nq,
                                           "ids": insert_ids.copy(),
                                           "limit": ct.default_limit}
                              )
