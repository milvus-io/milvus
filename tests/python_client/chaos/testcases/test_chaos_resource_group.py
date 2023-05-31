import pytest

from base.client_base import TestcaseBase
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from utils.util_log import test_log as log

# customer rg
rg_name_0 = "RG_0"
rg_name_1 = "RG_1"

# coll name
coll_name_1 = "ResourceGroup_111"
coll_name_2 = "ResourceGroup_222"

# resource group info of 4 qns
resource_group_info = [
    {"name": rg_name_0, "available_node": 1, "capacity": 1, "loaded_replica": {coll_name_1: 1}},
    {"name": rg_name_1, "available_node": 1, "capacity": 1, "loaded_replica": {coll_name_1: 1}},
    {"name": ct.default_resource_group_name, "available_node": 2,
     "capacity": ct.default_resource_group_capacity, "loaded_replica": {coll_name_2: 2}}
]


class TestChaosRG(TestcaseBase):
    """ Test case of end to end"""

    def teardown_method(self, method):
        log.info(("*" * 35) + " teardown " + ("*" * 35))
        log.info("[teardown_method] Start teardown test case %s..." %
                 method.__name__)
        log.info("skip drop collection")

    @pytest.mark.tags(CaseLabel.L3)
    def test_milvus_resource_group(self):
        nb = 10000
        # collection rg map
        collection_rg_map = {
            coll_name_1: {"resource_groups": [rg_name_0, rg_name_1], "replica_number": 2},
            coll_name_2: {"resource_groups": [ct.default_resource_group_name], "replica_number": 2}
        }

        self._connect()
        # create RG_0, RG_1, transfer 1 node to RG_0, 1 node to RG_1

        for rg_info in resource_group_info:
            rg_name = rg_info["name"]
            if rg_name != ct.default_resource_group_name:
                _, create_rg_res = self.utility_wrap.create_resource_group(rg_name)
                assert create_rg_res
                log.info(f"[ResourceGroup] Create rg {rg_name} done")
                self.utility_wrap.transfer_node(source=ct.default_resource_group_name, target=rg_name,
                                                num_node=rg_info["available_node"])
                log.info(
                    f'[ResourceGroup] Transfer {rg_info["available_node"]} nodes from {ct.default_resource_group_name} to {rg_name} done')

        # verify RGs
        resource_groups, _ = self.utility_wrap.list_resource_groups()
        assert len(resource_groups) == len(resource_group_info)
        assert all([rg_info["name"] in resource_groups for rg_info in resource_group_info])
        for rg_info in resource_group_info:
            rg_info = {"name": rg_info["name"],
                       "capacity": rg_info["capacity"],
                       "num_available_node": rg_info["available_node"],
                       "num_loaded_replica": {},
                       "num_outgoing_node": {},
                       "num_incoming_node": {}
                       }
            desc_rg_info, _ = self.utility_wrap.describe_resource_group(name=rg_info["name"],
                                                                        check_task=ct.CheckTasks.check_rg_property,
                                                                        check_items=rg_info)
            log.info(f'[ResourceGroup] Rg of {rg_info["name"]} info is: {desc_rg_info}')

        # prepare collection C1, C2
        # create
        data = cf.gen_default_dataframe_data(nb=nb)
        index_params = {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 48, "efConstruction": 500}}

        for coll_name in coll_name_1, coll_name_2:
            # create
            collection_w = self.init_collection_wrap(name=coll_name, active_trace=True)
            log.info(f"create collection {collection_w.name} done")
            entities = collection_w.num_entities

            # insert
            _, res = collection_w.insert(data)
            assert res
            log.info(f"insert {nb} entities done")

            # flush
            _, check_result = collection_w.flush(timeout=180)
            assert check_result
            assert collection_w.num_entities == nb + entities
            entities = collection_w.num_entities
            log.info(f"flush done with entities: {entities}")

            # index
            index, _ = collection_w.create_index(field_name=ct.default_float_vec_field_name,
                                                 index_params=index_params,
                                                 index_name=cf.gen_unique_str())
            index, _ = collection_w.create_index(field_name=ct.default_string_field_name,
                                                 index_params={},
                                                 index_name=cf.gen_unique_str())
            index_infos = [index.to_dict() for index in collection_w.indexes]
            log.info(f"index info: {index_infos}")

            # load coll_rg_a, 2 replicas -> RG_0, RG_1
            # load coll_rg_b, 2 replicas -> default_RG
            collection_w.load(replica_number=collection_rg_map[coll_name]["replica_number"],
                              _resource_groups=collection_rg_map[coll_name]["resource_groups"])

            # show query segment info
            segment_info, _ = self.utility_wrap.get_query_segment_info(collection_w.name)
            log.info(f"{collection_w.name} segment info: {segment_info}")

            # show replicas info
            replicas, _ = collection_w.get_replicas()
            log.info(f"{collection_w.name} replica info: {replicas}")

            # search
            search_vectors = cf.gen_vectors(ct.default_nq, ct.default_dim)
            search_params = {"metric_type": "L2", "params": {"ef": 64}}
            search_res, _ = collection_w.search(data=search_vectors,
                                                anns_field=ct.default_float_vec_field_name,
                                                param=search_params, limit=ct.default_limit, expr="int64 >= 0")
            assert len(search_res) == ct.default_nq
            assert len(search_res[0]) == ct.default_limit

            # query and delete
            term_expr = f'{ct.default_int64_field_name} < 100'
            query_res, _ = collection_w.query(term_expr)
            assert len(query_res) == 100

            delete_expr = f'{ct.default_int64_field_name} in {[i for i in range(100)]}'
            collection_w.delete(delete_expr)
            collection_w.query(term_expr, check_task=ct.CheckTasks.check_query_empty)

        # verify rg replica info
        for rg_info in resource_group_info:
            rg_info = {"name": rg_info["name"],
                       "capacity": rg_info["capacity"],
                       "num_available_node": rg_info["available_node"],
                       "num_loaded_replica": rg_info["loaded_replica"],
                       "num_outgoing_node": {},
                       "num_incoming_node": {}
                       }
            desc_rg_info_2, _ = self.utility_wrap.describe_resource_group(name=rg_info["name"],
                                                                          check_task=ct.CheckTasks.check_rg_property,
                                                                          check_items=rg_info)
            log.info(f'[ResourceGroup] Rg of {rg_info["name"]} info is: {desc_rg_info_2}')

    @pytest.mark.tags(CaseLabel.L3)
    def test_verify_milvus_resource_group(self):
        self._connect()

        # verify collection exist
        all_collections, _ = self.utility_wrap.list_collections()
        assert all(coll_name in all_collections for coll_name in [coll_name_1, coll_name_2])

        # verify resource groups
        for rg_info in resource_group_info:
            rg_info = {"name": rg_info["name"],
                       "capacity": rg_info["capacity"],
                       "num_available_node": rg_info["available_node"],
                       "num_loaded_replica": rg_info["loaded_replica"],
                       "num_outgoing_node": {},
                       "num_incoming_node": {}
                       }
            desc_rg_info, _ = self.utility_wrap.describe_resource_group(name=rg_info["name"],
                                                                        check_task=ct.CheckTasks.check_rg_property,
                                                                        check_items=rg_info)
            log.info(f'[ResourceGroup] Rg of {rg_info["name"]} info is: {desc_rg_info}')

        # search
        for coll_name in coll_name_2, coll_name_1:
            # get query segment info
            segment, _ = self.utility_wrap.get_query_segment_info(coll_name)
            log.info(f"{coll_name} query segment info: {segment}")

            # get replicas
            collection_w = self.init_collection_wrap(name=coll_name, active_trace=True)
            replicas, _ = collection_w.get_replicas(check_task=ct.CheckTasks.check_nothing)
            log.info(f"{coll_name} replicas: {replicas}")

            # search
            for i in range(100):
                search_vectors = cf.gen_vectors(ct.default_nq, ct.default_dim)
                search_params = {"metric_type": "L2", "params": {"ef": 64}}
                search_res, _ = collection_w.search(data=search_vectors,
                                                    anns_field=ct.default_float_vec_field_name,
                                                    param=search_params, limit=ct.default_limit, expr="int64 >= 0")
                assert len(search_res) == ct.default_nq
                assert len(search_res[0]) == ct.default_limit

            # show query segment info finally
            segment_2, _ = self.utility_wrap.get_query_segment_info(coll_name)
            log.info(f"{coll_name} query segment info: {segment_2}")

            # show replicas finally
            replicas_2, _ = collection_w.get_replicas()
            log.info(f"{coll_name} replicas: {replicas_2}")
