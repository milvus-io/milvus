import copy
import pytest
from pymilvus_orm import FieldSchema
from base.client_request import ApiReq
from base.collection import ApiCollection
from base.partition import ApiPartition
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
