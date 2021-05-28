import pytest

from base.client_request import ApiReq
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel

prefix = "insert"
default_schema = cf.gen_default_collection_schema()
default_binary_schema = cf.gen_default_binary_collection_schema()


class TestInsertParams(ApiReq):
    """ Test case of Insert interface """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5302")
    def test_insert_dataframe_data(self):
        """
        target: test insert DataFrame data
        method: 1.create 2.insert dataframe data
        expected: assert num entities
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection()
        df = cf.gen_default_dataframe_data(nb)
        self.collection.insert(data=df)
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5470")
    def test_insert_list_data(self):
        """
        target: test insert list-like data
        method: 1.create 2.insert list data
        expected: assert num entities
        """
        conn = self._connect()
        log.info(type(conn))
        nb = ct.default_nb
        collection = self._collection()
        data = cf.gen_default_list_data(nb)
        self.collection.insert(data=data)
        conn.flush([collection.name])
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_empty_data(self):
        """
        target: test insert empty data
        method: insert empty
        expected: raise exception
        """
        self._collection()
        ex, _ = self.collection.insert(data=[])
        assert "Column cnt not match with schema" in str(ex)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5445")
    def test_insert_none(self):
        """
        target: test insert None
        method: data is None
        expected: raise exception
        """
        self._collection()
        ex, _ = self.collection.insert(data=None)
        log.info(str(ex))

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5421")
    def test_insert_numpy_data(self):
        """
        target: test insert numpy.ndarray data
        method: 1.create by schema 2.insert data
        expected: assert num_entities
        """
        self._connect()
        nb = 10
        self._collection()
        data = cf.gen_numpy_data(nb)
        ex, _ = self.collection.insert(data=data)
        log.error(str(ex))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #5302")
    def test_insert_binary_dataframe(self):
        """
        target: test insert binary dataframe
        method: 1. create by schema 2. insert dataframe
        expected: assert num_entities
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection(schema=default_binary_schema)
        df = cf.gen_default_binary_dataframe_data(nb)
        self.collection.insert(data=df)
        assert collection.num_entities == nb

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5414")
    def test_insert_binary_data(self):
        """
        target: test insert list-like binary data
        method: 1. create by schema 2. insert data
        expected: assert num_entities
        """
        self._connect()
        nb = ct.default_nb
        collection = self._collection(schema=default_binary_schema)
        data = cf.gen_default_binary_list_data(nb)
        self.collection.insert(data=data)
        assert collection.num_entities == nb


class TestInsertOperation(ApiReq):
    """
    ******************************************************************
      The following cases are used to test insert interface operations
    ******************************************************************
    """

    def teardown_method(self):
        if self.collection is not None and self.collection.collection is not None:
            self.collection.drop()

    def setup_method(self):
        pass

    def test_insert_without_connection(self):
        """
        target: test insert without connection
        method: insert after remove connection
        expected: raise exception
        """
        self._collection()
        self.connection.remove_connection(ct.default_alias)
        res_list, _ = self.connection.list_connections()
        assert ct.default_alias not in res_list
        data = cf.gen_default_list_data(10)
        ex, check = self.collection.insert(data=data)
        assert "There is no connection with alias '{}'".format(ct.default_alias) in str(ex)
        assert self.collection.is_empty()

    def test_insert_drop_collection(self):
        """
        target: test insert and drop
        method: insert data and drop collection
        expected: verify collection if exist
        """
        collection = self._collection()
        collection_list, _ = self.utility.list_collections()
        assert collection.name in collection_list
        self.collection.drop()
        collection_list, _ = self.utility.list_collections()
        assert collection.name not in collection_list

    def test_insert_create_index(self):
        """
        target: test insert and create index
        method: 1. insert 2. create index
        expected: verify num entities and index
        """
        pass

    def test_insert_after_create_index(self):
        """
        target: test insert after create index
        method: 1. create index 2. insert data
        expected: verify index and num entities
        """
        pass

    def test_insert_search(self):
        """
        target: test insert and search
        method: 1. insert data 2. search
        expected: verify search result
        """
        pass
