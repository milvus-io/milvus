import pytest

from base.client_base import TestcaseBase
from base.collection_wrapper import ApiCollectionWrapper
from common.common_type import CheckTasks, CaseLabel
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log

prefix = "db"


@pytest.mark.tags(CaseLabel.RBAC)
class TestDatabaseParams(TestcaseBase):
    """ Test case of database """

    def teardown_method(self, method):
        """
        teardown method: drop collection and db
        """
        log.info("[database_teardown_method] Start teardown database test cases ...")

        self._connect()

        # clear db
        for db in self.database_wrap.list_database()[0]:
            # using db
            self.database_wrap.using_database(db)

            # drop db collections
            colls, _ = self.utility_wrap.list_collections()
            for coll in colls:
                cw = ApiCollectionWrapper()
                cw.init_collection(coll)
                cw.drop()

            # drop db
            if db != ct.default_db:
                self.database_wrap.drop_database(db)

        dbs, _ = self.database_wrap.list_database()
        assert dbs == [ct.default_db]

        super().teardown_method(method)

    @pytest.fixture(scope="function", params=ct.get_invalid_strs)
    def get_invalid_string(self, request):
        """
        get invalid string
        :param request:
        :type request:
        """
        yield request.param

    def test_db_default(self):
        """
        target: test normal db interface
        method: 1. connect with default db
                2. create a new db
                3. list db and verify db created successfully
                4. using new db and create collection without specifying a name
                4. using default db
                5. create new collection and specify db name
                6. list collection in the new db
                7. drop db, collections will also be dropped
        expected: 1. all db interface
                  2. using db can change the default db
                  3. drop databases also drop collections
        """
        self._connect()

        # using default db and create collection
        collection_w_default = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # create db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # list db and verify db
        dbs, _ = self.database_wrap.list_database()
        assert db_name in dbs

        # using db and create collection
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # using default db
        self.database_wrap.using_database(ct.default_db)
        collections_default, _ = self.utility_wrap.list_collections()
        assert collection_w_default.name in collections_default
        assert collection_w.name not in collections_default

        # using db
        self.database_wrap.using_database(db_name)
        collections, _ = self.utility_wrap.list_collections()
        assert collection_w.name in collections
        assert collection_w_default.name not in collections

        # drop collection and drop db
        collection_w.drop()
        self.database_wrap.drop_database(db_name=db_name)
        dbs_afrer_drop, _ = self.database_wrap.list_database()
        assert db_name not in dbs_afrer_drop

    def test_create_db_invalid_name(self, get_invalid_string):
        """
        target: test create db with invalid name
        method: create db with invalid name
        expected: error
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "Invalid database name"}
        self.database_wrap.create_database(db_name=get_invalid_string, check_task=CheckTasks.err_res,
                                           check_items=error)

    def test_create_db_without_connection(self):
        """
        target: test create db without connection
        method: create db without connection
        expected: exception
        """
        self.connection_wrap.disconnect(ct.default_alias)
        error = {ct.err_code: 1, ct.err_msg: "should create connect first"}
        self.database_wrap.create_database(cf.gen_unique_str(), check_task=CheckTasks.err_res,
                                           check_items=error)

    def test_create_default_db(self):
        """
        target: test create db with default db name "default"
        method: create db with name "default"
        expected: exception
        """
        self._connect()
        error = {ct.err_code: 1, ct.err_msg: "database already exist: default"}
        self.database_wrap.create_database(ct.default_db, check_task=CheckTasks.err_res, check_items=error)

    def test_drop_db_invalid_name(self, get_invalid_string):
        """
        target: test drop db with invalid name
        method: drop db with invalid name
        expected: exception
        """
        self._connect()

        # create db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # drop db
        self.database_wrap.drop_database(db_name=get_invalid_string, check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1, ct.err_msg: "is illegal"})

        # created db is exist
        self.database_wrap.create_database(db_name, check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1, ct.err_msg: "db existed"})

        self.database_wrap.drop_database(db_name)
        dbs, _ = self.database_wrap.list_database()
        assert db_name not in dbs

    def test_list_db_not_existed_connection_using(self):
        """
        target: test list db with a not existed connection using
        method: list db with a random using
        expected: exception
        """
        # connect with default alias using
        self._connect()

        # list db with not existed using
        self.database_wrap.list_database(using="random", check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1, ct.err_msg: "should create connect first."})

    @pytest.mark.parametrize("timeout", ["", -1, 0])
    def test_list_db_with_invalid_timeout(self, timeout):
        """
        target: test lst db with invalid timeout
        method: list db with invalid timeout
        expected: exception
        """
        # connect with default alias using
        self._connect()

        # list db with not existed using
        self.database_wrap.list_database(timeout=timeout, check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1,
                                                      ct.err_msg: "rpc deadline exceeded: Retry timeout"})

    @pytest.mark.parametrize("invalid_db_name", [(), [], 1, [1, "2", 3], (1,), {1: 1}])
    def test_using_invalid_db(self, invalid_db_name):
        """
        target: test using with invalid db name
        method: using invalid db
        expected: exception
        """
        # connect with default alias using
        self._connect()

        # create collection in default db
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # using db with invalid name
        self.database_wrap.using_database(db_name=invalid_db_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: "db existed"})

        # verify using db is default db
        collections, _ = self.utility_wrap.list_collections()
        assert collection_w.name in collections

    @pytest.mark.parametrize("invalid_db_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_using_invalid_db_2(self, invalid_db_name):
        # connect with default alias using
        self._connect()

        # create collection in default db
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # using db with invalid name
        self.database_wrap.using_database(db_name=invalid_db_name, check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: "database not exist"})


@pytest.mark.tags(CaseLabel.RBAC)
class TestDatabaseOperation(TestcaseBase):

    def teardown_method(self, method):
        """
        teardown method: drop collection and db
        """
        log.info("[database_teardown_method] Start teardown database test cases ...")

        self._connect()

        # clear db
        for db in self.database_wrap.list_database()[0]:
            # using db
            self.database_wrap.using_database(db)

            # drop db collections
            colls, _ = self.utility_wrap.list_collections()
            for coll in colls:
                cw = ApiCollectionWrapper()
                cw.init_collection(coll)
                cw.drop()

            # drop db
            if db != ct.default_db:
                self.database_wrap.drop_database(db)

        dbs, _ = self.database_wrap.list_database()
        assert dbs == [ct.default_db]

        super().teardown_method(method)

    def test_create_db_name_existed(self):
        """
        target: create db with a existed db name
        method: create db repeatedly
        expected: exception
        """
        # create db
        self._connect()
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # create existed db again
        error = {ct.err_code: 1, ct.err_msg: "database already exist"}
        self.database_wrap.create_database(db_name, check_task=CheckTasks.err_res, check_items=error)

    def test_create_db_exceeds_max_num(self):
        """
        target: test db num exceeds max num
        method: create many dbs and exceeds max
        expected: exception
        """
        self._connect()
        dbs, _ = self.database_wrap.list_database()

        # because max num 64 not include default
        for i in range(ct.max_database_num + 1 - len(dbs)):
            self.database_wrap.create_database(cf.gen_unique_str(prefix))

        # there are ct.max_database_num-1 dbs (default is not included)
        error = {ct.err_code: 1,
                 ct.err_msg: f"database number ({ct.max_database_num + 1}) exceeds max configuration ({ct.max_database_num})"}
        self.database_wrap.create_database(cf.gen_unique_str(prefix), check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/24182")
    def test_create_collection_exceeds_per_db(self):
        """
        target: test limit collection num per db
        method: 1. create collections in the db and exceeds perDbCollections
        expected: exception
        """
        self._connect()
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)
        self.database_wrap.using_database(db_name)

        # create collections
        collections, _ = self.utility_wrap.list_collections()
        for i in range(ct.max_collections_per_db - len(collections)):
            self.init_collection_wrap(cf.gen_unique_str(prefix))

        error = {ct.err_code: 1,
                 ct.err_msg: f"failed to create collection, maxCollectionNumPerDB={ct.max_collections_per_db}, exceeded the limit number of "
                             f"collections per DB)"}
        self.collection_wrap.init_collection(cf.gen_unique_str(prefix), cf.gen_default_collection_schema(),
                                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/24182")
    def test_create_db_collections_exceeds_max_num(self):
        """
        target: test create collection in different db and each db's colelction within max,
                but total exceeds max collection num
        method: 1. create a db and create 10 collections in db
                2. create another db and create collection larger than (max - 10) but less than collectionPerDb
        expected: exception
        """
        self._connect()

        # create and using db_a
        db_a = cf.gen_unique_str("a")
        self.database_wrap.create_database(db_a)
        self.database_wrap.using_database(db_a)

        # create 50 collections in db_a
        collection_num_a = 50
        for i in range(collection_num_a):
            self.init_collection_wrap(cf.gen_unique_str(prefix))

        # create and using db_b
        db_b = cf.gen_unique_str("b")
        self.database_wrap.create_database(db_b)
        self.database_wrap.using_database(db_b)

        dbs, _ = self.database_wrap.list_database()
        exist_coll_num = 0
        for db in dbs:
            self.database_wrap.using_database(db)
            exist_coll_num += len(self.utility_wrap.list_collections()[0])

        # create collection so that total collection num exceed maxCollectionNum
        self.database_wrap.using_database(db_b)
        log.debug(f'exist collection num: {exist_coll_num}')
        collections, _ = self.utility_wrap.list_collections()
        for i in range(ct.max_collection_num - exist_coll_num):
            self.init_collection_wrap(cf.gen_unique_str(prefix))

        log.debug(f'db_b collection num: {len(self.utility_wrap.list_collections()[0])}')

        dbs, _ = self.database_wrap.list_database()
        total_coll_num = 0
        for db in dbs:
            self.database_wrap.using_database(db)
            total_coll_num += len(self.utility_wrap.list_collections()[0])

        log.debug(f'total collection num: {total_coll_num}')
        error = {ct.err_code: 1,
                 ct.err_msg: f"failed to create collection, maxCollectionNum={ct.max_collection_num}, exceeded the limit number of"}
        self.collection_wrap.init_collection(cf.gen_unique_str(prefix), cf.gen_default_collection_schema(),
                                             check_task=CheckTasks.err_res, check_items=error)

    def test_create_collection_name_same_db(self):
        """
        target: test create collection in db and collection name same sa db name
        method: 1.create a db
                2.create collection in the db and collection name same as db name
        expected: no error
        """
        self._connect()
        coll_db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(coll_db_name)

        self.database_wrap.using_database(coll_db_name)

        collection_w = self.init_collection_wrap(name=coll_db_name)

        collection_w.insert(cf.gen_default_dataframe_data())
        assert collection_w.num_entities == ct.default_nb

        colls, _ = self.utility_wrap.list_collections()
        assert coll_db_name in colls

        self.database_wrap.using_database(ct.default_db)
        coll_default, _ = self.utility_wrap.list_collections()
        assert coll_db_name not in coll_default

    def test_different_db_same_collection_name(self):
        """
        target: test create same collection name in different db
        method: 1. create 2 dbs
                2. create same collection name in the 2 dbs
        expected: verify db isolate collection
        """
        self._connect()

        # create a db
        db_a = cf.gen_unique_str("a")
        self.database_wrap.create_database(db_a)

        # create b db
        db_b = cf.gen_unique_str("b")
        self.database_wrap.create_database(db_b)

        # create same collection name in db_a and db_b
        same_coll_name = cf.gen_unique_str(prefix)

        # create and insert in db_a
        self.database_wrap.using_database(db_a)
        collection_w_a = self.init_collection_wrap(name=same_coll_name)
        collection_w_a.insert(cf.gen_default_dataframe_data(nb=100))
        assert collection_w_a.num_entities == 100
        collections_a, _ = self.utility_wrap.list_collections()
        assert same_coll_name in collections_a

        # create and insert in db_b
        self.database_wrap.using_database(db_b)
        collection_w_b = self.init_collection_wrap(name=same_coll_name)
        collection_w_b.insert(cf.gen_default_dataframe_data(nb=200))
        assert collection_w_b.num_entities == 200
        collections_a, _ = self.utility_wrap.list_collections()
        assert same_coll_name in collections_a

    def test_drop_default_db(self):
        """
        target: test drop default db
        method: drop default db
        expected: exception
        """
        self._connect()

        # drop default db
        self.database_wrap.drop_database(db_name=ct.default_db, check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1, ct.err_msg: "can not drop default database"})

        dbs, _ = self.database_wrap.list_database()
        assert ct.default_db in dbs

    def test_drop_db_has_collections(self):
        """
        target: test drop the db that still has collections
        method: drop db that still has some collections
        expected: exception
        """
        self._connect()

        # create db and using db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)
        self.database_wrap.using_database(db_name)

        # create collection in db
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str())

        # drop db
        self.database_wrap.drop_database(db_name, check_task=CheckTasks.err_res,
                                         check_items={ct.err_code: 1, ct.err_msg: "can not drop default database"})

        # drop collection and drop db
        collection_w.drop()
        self.database_wrap.drop_database(db_name)

    def test_drop_not_existed_db(self):
        """
        target: test drop not existed db
        method: drop a db repeatedly
        expected: exception
        """
        self._connect()

        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # drop a not existed db
        self.database_wrap.drop_database(cf.gen_unique_str(prefix))

        # drop db
        self.database_wrap.drop_database(db_name)
        self.database_wrap.drop_database(db_name)

    def test_drop_using_db(self):
        """
        target: drop the db in use
        method: drop the using db
        expected: operation in the db gets exception, need to using other db
        """
        # create db
        self._connect()

        # create collection in default db
        collection_w_default = self.init_collection_wrap(name=cf.gen_unique_str(prefix), db_name=ct.default_db)

        # create db
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # using db
        self.database_wrap.using_database(db_name)
        # collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # drop using db
        self.database_wrap.drop_database(db_name)

        # verify current db
        self.utility_wrap.list_collections(check_task=CheckTasks.err_res,
                                           check_items={ct.err_code: 1, ct.err_msg: "database not exist:"})
        self.database_wrap.list_database()
        self.database_wrap.using_database(ct.default_db)

        using_collections, _ = self.utility_wrap.list_collections()
        assert collection_w_default.name in using_collections

    def test_using_db_not_existed(self):
        """
        target: test using a not existed db
        method: using a not existed db
        expected: exception
        """
        # create db
        self._connect()
        collection_w = self.init_collection_wrap(cf.gen_unique_str(prefix))

        # list collection with not exist using db -> exception
        self.database_wrap.using_database(db_name=cf.gen_unique_str(), check_task=CheckTasks.err_res,
                                          check_items={ct.err_code: 1, ct.err_msg: "database not found"})

        # change using to default and list collections
        self.database_wrap.using_database(db_name=ct.default_db)
        colls, _ = self.utility_wrap.list_collections()
        assert collection_w.name in colls


@pytest.mark.tags(CaseLabel.RBAC)
class TestDatabaseOtherApi(TestcaseBase):
    """ test other interface that has db_name params"""

    def teardown_method(self, method):
        """
        teardown method: drop collection and db
        """
        log.info("[database_teardown_method] Start teardown database test cases ...")

        self._connect()

        # clear db
        for db in self.database_wrap.list_database()[0]:
            # using db
            self.database_wrap.using_database(db)

            # drop db collections
            colls, _ = self.utility_wrap.list_collections()
            for coll in colls:
                cw = ApiCollectionWrapper()
                cw.init_collection(coll)
                cw.drop()

            # drop db
            if db != ct.default_db:
                self.database_wrap.drop_database(db)

        dbs, _ = self.database_wrap.list_database()
        assert dbs == [ct.default_db]

        super().teardown_method(method)

    @pytest.mark.parametrize("invalid_db_name", [(), [], 1, [1, "2", 3], (1,), {1: 1}])
    def test_connect_invalid_db_name(self, host, port, invalid_db_name):
        """
        target: test conenct with invalid db name
        method: connect with invalid db name
        expected: connect fail
        """
        # connect with invalid db
        self.connection_wrap.connect(host=host, port=port, db_name=invalid_db_name,
                                     user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure,
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 1, ct.err_msg: "is illegal"})

    @pytest.mark.parametrize("invalid_db_name", ["12-s", "12 s", "(mn)", "中文", "%$#"])
    def test_connect_invalid_db_name_2(self, host, port, invalid_db_name):
        # connect with invalid db
        self.connection_wrap.connect(host=host, port=port, db_name=invalid_db_name,
                                     user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure,
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 2, ct.err_msg: "Fail connecting to server"})

    def test_connect_not_existed_db(self, host, port):
        """
        target: test connect with not existed db succ
        method: 1.connect with not existed db
                2.list collection and gets exception
                3.create db and create collection in the db
                3.using default db
                4.list collections succ
        expected: parameters db_name is not validated when connecting
        """
        # connect with not existed db
        db_name = cf.gen_unique_str(prefix)
        self.connection_wrap.connect(host=host, port=port, db_name=db_name,
                                     user=ct.default_user, password=ct.default_password,
                                     secure=cf.param_info.param_secure,
                                     check_task=CheckTasks.err_res,
                                     check_items={ct.err_code: 2, ct.err_msg: "database not found"})

    def test_connect_db(self, host, port):
        """
        target: test connect with db
        method: 1.create db and create collection in db
                2.disconnect and connect with db
                3.list collections
        expected: verify connect db is the using db
        """
        # create db
        self._connect()
        db_name = cf.gen_unique_str(prefix)
        self.database_wrap.create_database(db_name)

        # create collection
        self.database_wrap.using_database(db_name)
        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix))

        # re-connect with db name
        self.connection_wrap.disconnect(ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, db_name=db_name, user=ct.default_user,
                                     password=ct.default_password, secure=cf.param_info.param_secure)

        # verify connect db_name is the specify db
        collections_db, _ = self.utility_wrap.list_collections()
        assert collection_w.name in collections_db

        # verify db's collection not in default db
        self.connection_wrap.disconnect(ct.default_alias)
        self.connection_wrap.connect(host=host, port=port, db_name=ct.default_db, user=ct.default_user,
                                     password=ct.default_password, secure=cf.param_info.param_secure)

        collections_default, _ = self.utility_wrap.list_collections()
        assert collection_w.name not in collections_default
