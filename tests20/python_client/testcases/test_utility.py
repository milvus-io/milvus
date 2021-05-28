import copy
import pytest
from pymilvus_orm import FieldSchema
from base.client_request import ApiReq
from base.collection import ApiCollection
from base.partition import ApiPartition
from base.index import ApiIndex
from base.utility import ApiUtility
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel

prefix = "utility"
default_schema = cf.gen_default_collection_schema()
default_field_name = ct.default_float_vec_field_name
default_index_params = {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}


class TestUtilityParams(ApiReq):
    """ Test case of index interface """

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_collection_name(self, request):
        yield request.param

    @pytest.fixture(
        scope="function",
        params=ct.get_invalid_strs
    )
    def get_invalid_partition_name(self, request):
        yield request.param

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_collection_name_invalid(self, get_invalid_collection_name):
        """
        target: test has_collection with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        ut = ApiUtility()
        c_name = get_invalid_collection_name
        ex, _ = ut.has_collection(c_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition_collection_name_invalid(self, get_invalid_collection_name):
        """
        target: test has_partition with error collection name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        ut = ApiUtility()
        c_name = get_invalid_collection_name
        p_name = cf.gen_unique_str(prefix)
        ex, _ = ut.has_partition(c_name, p_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition_name_invalid(self, get_invalid_partition_name):
        """
        target: test has_partition with error partition name
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        p_name = get_invalid_partition_name
        ex, _ = ut.has_partition(c_name, p_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_collections_using_invalid(self):
        """
        target: test list_collections with invalid using
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        using = "empty"
        ut = ApiUtility(using=using)
        ex, _ = ut.list_collections()
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_invalid_name(self, get_invalid_collection_name):
        """
        target: test building_process
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_collection_name
        ut = ApiUtility()
        ex, _ = ut.index_building_progress(c_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    # TODO: not support index name
    @pytest.mark.tags(CaseLabel.L1)
    def _test_index_process_invalid_index_name(self, get_invalid_index_name):
        """
        target: test building_process
        method: input invalid index name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        index_name = get_invalid_index_name
        ut = ApiUtility()
        ex, _ = ut.index_building_progress(c_name, index_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_invalid_name(self, get_invalid_collection_name):
        """
        target: test wait_index
        method: input invalid name
        expected: raise exception
        """
        self._connect()
        c_name = get_invalid_collection_name
        ut = ApiUtility()
        ex, _ = ut.wait_for_index_building_complete(c_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_invalid_index_name(self, get_invalid_index_name):
        """
        target: test wait_index
        method: input invalid index name
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        index_name = get_invalid_index_name
        ut = ApiUtility()
        ex, _ = ut.wait_for_index_building_complete(c_name, index_name)
        log.error(str(ex))
        assert "invalid" or "illegal" in str(ex)


class TestUtilityBase(ApiReq):
    """ Test case of index interface """

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_collection(self):
        """
        target: test has_collection with collection name
        method: input collection name created before
        expected: True
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        res, _ = ut.has_collection(c_name)
        assert res is True

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_collection_not_created(self):
        """
        target: test has_collection with collection name which is not created
        method: input random collection name
        expected: False
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        self._collection()
        res, _ = ut.has_collection(c_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_collection_after_drop(self):
        """
        target: test has_collection with collection name droped before
        method: input random collection name
        expected: False
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        res, _ = ut.has_collection(c_name)
        assert res is True
        self.collection.drop()
        res, _ = ut.has_collection(c_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition(self):
        """
        target: test has_partition with partition name
        method: input collection name and partition name created before
        expected: True
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str()
        collection = self._collection(c_name)
        api_p = ApiPartition()
        api_p.partition_init(collection, p_name)
        res, _ = ut.has_partition(c_name, p_name)
        assert res is True

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition_not_created(self):
        """
        target: test has_partition with partition name
        method: input collection name, and partition name not created before
        expected: True
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str()
        self._collection(c_name)
        res, _ = ut.has_partition(c_name, p_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_has_partition_after_drop(self):
        """
        target: test has_partition with partition name
        method: input collection name, and partition name dropped
        expected: True
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        p_name = cf.gen_unique_str()
        collection = self._collection(c_name)
        api_p = ApiPartition()
        api_p.partition_init(collection, p_name)
        res, _ = ut.has_partition(c_name, p_name)
        assert res is True
        api_p.drop()
        res, _ = ut.has_partition(c_name, p_name)
        assert res is False

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_collections(self):
        """
        target: test list_collections
        method: create collection, list_collections
        expected: in the result
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        res, _ = ut.list_collections()
        assert c_name in res

    # TODO: make sure all collections deleted
    @pytest.mark.tags(CaseLabel.L1)
    def _test_list_collections_no_collection(self):
        """
        target: test list_collections
        method: no collection created, list_collections
        expected: length of the result equals to 0
        """
        self._connect()
        ut = ApiUtility()
        res, _ = ut.list_collections()
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_not_existed(self):
        """
        target: test building_process
        method: input collection not created before
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        ut = ApiUtility()
        ex, _ = ut.index_building_progress(c_name)
        log.error(str(ex))
        assert "exist" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_empty(self):
        """
        target: test building_process
        method: input empty collection
        expected: no exception raised
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        ut = ApiUtility()
        res, _ = ut.index_building_progress(c_name)
        assert "num_indexed_entities" in res
        assert res["num_indexed_entities"] == 0
        assert "num_total_entities" in res
        assert res["num_total_entities"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_insert_no_index(self):
        """
        target: test building_process
        method: insert 1 entity, no index created
        expected: no exception raised
        """
        nb = 1
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        ut = ApiUtility()
        res, _ = ut.index_building_progress(c_name)
        assert "num_indexed_entities" in res
        assert res["num_indexed_entities"] == 0
        assert "num_total_entities" in res
        assert res["num_total_entities"] == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_insert_index_not_enough(self):
        """
        target: test building_process
        method: insert 1 entity, no index created
        expected: no exception raised
        """
        nb = 1
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        self.collection.create_index(default_field_name, default_index_params)
        ut = ApiUtility()
        ut.wait_for_index_building_complete(c_name)
        res, _ = ut.index_building_progress(c_name)
        assert "num_indexed_entities" in res
        assert res["num_indexed_entities"] == 0
        assert "num_total_entities" in res
        assert res["num_total_entities"] == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_index(self):
        """
        target: test building_process
        method: insert 1200 entities, build and call building_process
        expected: 1200 entity indexed
        """
        nb = 1200
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        self.collection.create_index(default_field_name, default_index_params)
        ut = ApiUtility()
        ut.wait_for_index_building_complete(c_name)
        res, _ = ut.index_building_progress(c_name)
        assert "num_indexed_entities" in res
        assert res["num_indexed_entities"] == nb
        assert "num_total_entities" in res
        assert res["num_total_entities"] == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_index_process_collection_indexing(self):
        """
        target: test building_process
        method: call building_process during building
        expected: 1200 entity indexed
        """
        nb = 1200
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        self.collection.create_index(default_field_name, default_index_params)
        ut = ApiUtility()
        res, _ = ut.index_building_progress(c_name)
        for _ in range(2):
            assert "num_indexed_entities" in res
            assert res["num_indexed_entities"] <= nb
            assert res["num_indexed_entities"] >= 0
            assert "num_total_entities" in res
            assert res["num_total_entities"] == nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_collection_not_existed(self):
        """
        target: test wait_index
        method: input collection not created before
        expected: raise exception
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        ut = ApiUtility()
        ex, _ = ut.wait_for_index_building_complete(c_name)
        log.error(str(ex))
        assert "exist" in str(ex)

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_collection_empty(self):
        """
        target: test wait_index
        method: input empty collection
        expected: no exception raised
        """
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        ut = ApiUtility()
        res, _ = ut.wait_for_index_building_complete(c_name)
        assert res is None

    @pytest.mark.tags(CaseLabel.L1)
    def test_wait_index_collection_index(self):
        """
        target: test wait_index
        method: insert 1200 entities, build and call wait_index
        expected: 1200 entity indexed
        """
        nb = 1200
        self._connect()
        c_name = cf.gen_unique_str(prefix)
        self._collection(c_name)
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        self.collection.create_index(default_field_name, default_index_params)
        ut = ApiUtility()
        res, _ = ut.wait_for_index_building_complete(c_name)
        assert res is None
        res, _ = ut.index_building_progress(c_name)
        assert res["num_indexed_entities"] == nb


class TestUtilityAdvanced(ApiReq):
    """ Test case of index interface """

    @pytest.mark.tags(CaseLabel.L2)
    def test_has_collection_multi_collections(self):
        """
        target: test has_collection with collection name
        method: input collection name created before
        expected: True
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        self._collection(c_name)
        api_c = ApiCollection()
        api_c.collection_init(c_name_2)
        for name in [c_name, c_name_2]:
            res, _ = ut.has_collection(name)
            assert res is True

    @pytest.mark.tags(CaseLabel.L2)
    def test_list_collections_multi_collection(self):
        """
        target: test list_collections
        method: create collection, list_collections
        expected: in the result
        """
        self._connect()
        ut = ApiUtility()
        c_name = cf.gen_unique_str(prefix)
        c_name_2 = cf.gen_unique_str(prefix)
        self._collection(c_name)
        api_c = ApiCollection()
        api_c.collection_init(c_name_2)
        res, _ = ut.list_collections()
        for name in [c_name, c_name_2]:
            assert name in res
