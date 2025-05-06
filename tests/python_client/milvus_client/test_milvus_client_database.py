import pytest

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *
from common.constants import *
from pymilvus import DataType

prefix = "client_search"
partition_prefix = "client_partition"
db_prefix = "client_database"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_int32_array_field_name = ct.default_int32_array_field_name
default_string_array_field_name = ct.default_string_array_field_name

class TestMilvusClientDatabaseInvalid(TestMilvusClientV2Base):
    """ Test case of database """

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("db_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "  "])
    def test_milvus_client_create_database_invalid_db_name(self, db_name):
        """
        target: test fast create database with invalid db name
        method: create database with invalid db name
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        error = {ct.err_code: 802, ct.err_msg: f"the first character of a database name must be an underscore or letter: "
                                               f"invalid database name[database={db_name}]"}
        self.create_database(client, db_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_database_name_over_max_length(self):
        """
        target: test fast create database with over max db name length
        method: create database with over max db name length
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = "a".join("a" for i in range(256))
        error = {ct.err_code: 802, ct.err_msg: f"the length of a database name must be less than 255 characters"}
        self.create_database(client, db_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_database_name_with_default(self):
        """
        target: test fast create db name with default
        method: create db name with default
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = "default"
        error = {ct.err_code: 65535, ct.err_msg: f"database already exist: {db_name}"}
        self.create_database(client, db_name, default_dim,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_create_database_with_existed_name(self):
        """
        target: test fast create db name with existed name
        method: create db name with existed name
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        self.create_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        error = {ct.err_code: 65535, ct.err_msg: f"database already exist: {db_name}"}
        self.create_database(client, db_name, default_dim,
                               check_task=CheckTasks.err_res, check_items=error)
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="pymilvus issue 2683")
    @pytest.mark.parametrize("properties", ["hhh", []])
    def test_milvus_client_create_database_with_invalid_properties(self, properties):
        """
        target: test fast create db name with invalid properties
        method: create db name with invalid properties
        expected: raise exception
        actual: Currently such errors are not very readable, 
                and entries of numeric types such as 1.11, 111 are not blocked
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        error = {ct.err_code: 1, ct.err_msg: f"Unexpected error, message=<unsupported operand type(s) for +: 'float' and '{type(properties).__name__}'>"}
        self.create_database(client, db_name, properties,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("properties", [{"database.rep.number": 3}])
    @pytest.mark.skip("A param that does not currently exist will simply have no effect, "
                     "but it would be better if an error were reported.")
    def test_milvus_client_create_database_with_nonexistent_property_params(self, properties):
        """
        target: test fast create db name with nonexistent property params
        method: create db name with nonexistent property params
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        error = {ct.err_code: 1, ct.err_msg: f""}
        self.create_database(client, db_name, properties=properties,
                            check_task=CheckTasks.err_res, check_items=error)
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("db_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "  "])
    def test_milvus_client_drop_database_invalid_db_name(self, db_name):
        """
        target: test drop database with invalid db name
        method: drop database with invalid db name
        expected: raise exception
        """
        client = self._client()
        error = {ct.err_code: 802, ct.err_msg: f"the first character of a database name must be an underscore or letter: "
                                               f"invalid database name[database={db_name}]"}
        self.drop_database(client, db_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("db_name", ["nonexistent"])
    @pytest.mark.skip("Deleting a db that does not exist does not report an error, "
                     "but it would be better if an error were reported.")
    def test_milvus_client_drop_database_nonexistent_db_name(self, db_name):
        """
        target: test drop database with nonexistent db name
        method: drop database with nonexistent db name
        expected: raise exception
        """
        client = self._client()
        error = {ct.err_code: 802, ct.err_msg: f""}
        self.drop_database(client, db_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_database_has_collections(self):
        """
        target: test drop database which has collections
        method: drop database which has collections
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        self.create_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        # 2. create collection
        self.use_database(client, db_name)
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        #3. drop database
        error = {ct.err_code: 65535, ct.err_msg: f"{db_name} not empty, must drop all collections before drop database"}
        self.drop_database(client, db_name,
                           check_task=CheckTasks.err_res, check_items=error)
        self.drop_collection(client, collection_name)
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("db_name", ["default"])
    def test_milvus_client_list_databases_with_params(self, db_name):
        """
        target: test list database with params
        method: list database with params
        expected: raise exception
        """
        client = self._client()
        error = {ct.err_code: 1, ct.err_msg: f"Unexpected error, message=<GrpcHandler.list_database() "
                                             f"got an unexpected keyword argument 'db_name'"}
        self.list_databases(client, db_name=db_name,
                            check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("db_name", ["12-s", "12 s", "(mn)", "中文", "%$#", "  ", "nonexistent"])
    def test_milvus_client_describe_database_invalid_db_name(self, db_name):
        """
        target: test describe database with invalid db name
        method: describe database with invalid db name
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        error = {ct.err_code: 800, ct.err_msg: f"database not found[database={db_name}]"}
        self.describe_database(client, db_name,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("db_name", ["%$#", "test", " "])
    def test_milvus_client_alter_database_properties_nonexistent_db_name(self, db_name):
        """
        target: test alter database properties with nonexistent db name
        method: alter database properties with nonexistent db name
        expected: raise exception
        """
        client = self._client()
        # alter database properties
        properties = {"database.replica.number": 2}
        error = {ct.err_code: 800, ct.err_msg: f"database not found[database={db_name}]"}
        self.alter_database_properties(client, db_name, properties,
                                       check_task=CheckTasks.err_res,
                                       check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("properties", ["tt"])
    def test_milvus_client_alter_database_properties_invalid_format(self, properties):
        """
        target: test alter database properties with invalid properties format
        method: alter database properties with invalid properties format
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        self.create_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        error = {ct.err_code: 1, ct.err_msg: f"'str' object has no attribute 'items'"}
        self.alter_database_properties(client, db_name, properties,
                                     check_task=CheckTasks.err_res,
                                     check_items=error)
        self.drop_database(client, db_name)
    
    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_alter_database_properties_invalid_params(self):
        """
        target: test describe database with invalid db name
        method: describe database with invalid db name
        expected: raise exception
        actual: run successfully
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        properties = {"database.force.deny.writing": "true",
                      "database.replica.number": "3"}
        self.create_database(client, db_name, properties=properties)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"db_name": db_name,
                                            "database.force.deny.writing": "true",
                                            "database.replica.number": "3"})
        alter_properties = {"data.replica.number": 2}
        self.alter_database_properties(client, db_name, properties=alter_properties)
        describe = self.describe_database(client, db_name)[0]
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("db_name", ["%$#", "test", " "])
    def test_milvus_client_drop_database_properties_nonexistent_db_name(self, db_name):
        """
        target: test drop database properties with nonexistent db name
        method: drop database properties with nonexistent db name
        expected: raise exception
        """
        client = self._client()
        # alter database properties
        properties = {"data.replica.number": 2}
        error = {ct.err_code: 800, ct.err_msg: f"database not found[database={db_name}]"}
        self.drop_database_properties(client, db_name, properties,
                                      check_task=CheckTasks.err_res,
                                      check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("properties", ["", {}, []])
    def test_milvus_client_drop_database_properties_invalid_format(self, properties):
        """
        target: test drop database properties with invalid properties format
        method: drop database properties with invalid properties format
        expected: raise exception
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        self.create_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        error = {ct.err_code: 65535, ct.err_msg: f"alter database requires either properties or deletekeys to modify or delete keys, both cannot be empty"}
        self.drop_database_properties(client, db_name, property_keys=properties,
                                      check_task=CheckTasks.err_res,
                                      check_items=error)
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_milvus_client_drop_database_properties_invalid_params(self):
        """
        target: test drop database properties with invalid properties
        method: drop database properties with invalid properties
        expected: raise exception
        actual: case success, nothing changed
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        properties = {"database.force.deny.writing": "true",
                      "database.replica.number": "3"}
        self.create_database(client, db_name, properties=properties)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"db_name": db_name,
                                            "database.force.deny.writing": "true",
                                            "database.replica.number": "3"})
        drop_properties = {"data.replica.number": 2}
        self.drop_database_properties(client, db_name, property_keys=drop_properties)
        describe = self.describe_database(client, db_name)[0]
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("db_name", ["nonexistent"])
    def test_milvus_client_use_database_nonexistent_db_name(self, db_name):
        """
        target: test use database with nonexistent db name
        method: use database with nonexistent db name
        expected: raise exception
        """
        client = self._client()
        error = {ct.err_code: 800, ct.err_msg: f"database not found[database={db_name}]"}
        self.use_database(client, db_name,
                           check_task=CheckTasks.err_res, check_items=error)
        self.using_database(client, db_name,
                           check_task=CheckTasks.err_res, check_items=error)

class TestMilvusClientDatabaseValid(TestMilvusClientV2Base):
    """ Test case of database interface """

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_create_drop_database_default(self):
        """
        target: test create and drop database normal case
        method: 1. create database 2. create collection 3. insert data 4. search & query 5. drop collection & database
        expected: run successfully
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        self.create_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        self.using_database(client, db_name)
        # 2. create collection
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": dim,
                                              "consistency_level": 0})
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": default_limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 6. drop action
        self.drop_collection(client, collection_name)
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_create_database_with_properties(self):
        """
        target: test create database with properties
        method: 1. create database 2. create collection 3. insert data 4. search & query 5. drop collection & database
        expected: run successfully
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        properties = {"database.force.deny.writing": "false",
                      "database.replica.number": "3"}
        self.create_database(client, db_name, properties=properties)
        describe = self.describe_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"db_name": db_name,
                                            "database.force.deny.writing": "false",
                                            "database.replica.number": "3"})
        self.using_database(client, db_name)
        # 2. create collection
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        self.describe_collection(client, collection_name,
                                 check_task=CheckTasks.check_describe_collection_property,
                                 check_items={"collection_name": collection_name,
                                              "dim": dim,
                                              "consistency_level": 0})
        # 3. insert
        rng = np.random.default_rng(seed=19530)
        rows = [{default_primary_key_field_name: i, default_vector_field_name: list(rng.random((1, default_dim))[0]),
                 default_float_field_name: i * 1.0, default_string_field_name: str(i)} for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        # 4. search
        vectors_to_search = rng.random((1, default_dim))
        self.search(client, collection_name, vectors_to_search,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": len(vectors_to_search),
                                 "limit": default_limit})
        # 5. query
        self.query(client, collection_name, filter=default_search_exp,
                   check_task=CheckTasks.check_query_results,
                   check_items={exp_res: rows,
                                "with_vec": True,
                                "pk_name": default_primary_key_field_name})
        # 6. drop action
        self.drop_collection(client, collection_name)
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_alter_database_properties_default(self):
        """
        target: test alter database with properties
        method: 1. create database 2. alter database properties
        expected: run successfully
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        properties = {"database.force.deny.writing": "true",
                      "database.replica.number": "3"}
        self.create_database(client, db_name, properties=properties)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"db_name": db_name,
                                            "database.force.deny.writing": "true",
                                            "database.replica.number": "3"})
        self.using_database(client, db_name)
        alter_properties = {"database.replica.number": "2",
                            "database.force.deny.reading": "true"}
        self.alter_database_properties(client, db_name, properties=alter_properties)
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"db_name": db_name,
                                            "database.force.deny.writing": "true",
                                            "database.force.deny.reading": "true",
                                            "database.replica.number": "2"})
        # 6. drop action
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_drop_database_properties_default(self):
        """
        target: test drop database with properties
        method: 1. create database 2. drop database properties
        expected: run successfully
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        properties = {"database.force.deny.writing": "true",
                      "database.force.deny.reading": "true",
                      "database.replica.number": "3",
                      "database.max.collections": 100,
                      "database.diskQuota.mb": 10240}
        self.create_database(client, db_name, properties=properties)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items=properties)
        self.using_database(client, db_name)
        drop1 = {"database.replica.number"}
        self.drop_database_properties(client, db_name, property_keys=drop1)
        describe = self.describe_database(client, db_name)[0]
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"database.replica.number": "Missing"})
        drop2 = ["database.force.deny.writing", "database.force.deny.reading"]
        self.drop_database_properties(client, db_name, property_keys=drop2)
        describe = self.describe_database(client, db_name)[0]
        self.describe_database(client, db_name,
                               check_task=CheckTasks.check_describe_database_property,
                               check_items={"database.force.deny.writing": "Missing",
                                           "database.force.deny.reading": "Missing",
                                           "properties_length": 3})
        # drop3 = "database.max.collections"
        # self.drop_database_properties(client, db_name, property_keys=drop3)
        # it doesn't work, but no error reported
        
        # 6. drop action
        self.drop_database(client, db_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_milvus_client_use_database_default(self):
        """
        target: test use_database
        method: 1. create another database 2. create collection in defalut db & another db 3. list collections
        expected: run successfully
        """
        client = self._client()
        # 1. create database
        db_name = cf.gen_unique_str(db_prefix)
        self.create_database(client, db_name)
        dbs = self.list_databases(client)[0]
        assert db_name in dbs
        collection_name_default_db = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name_default_db, default_dim)
        collections_default_db = self.list_collections(client)[0]
        assert collection_name_default_db in collections_default_db
        self.use_database(client, db_name)
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        collections = self.list_collections(client)[0]
        assert collection_name in collections
        assert collections_default_db not in collections
        
        # 6. drop action
        self.drop_collection(client, collection_name)
        self.drop_database(client, db_name)
        self.use_database(client, "default")
        self.drop_collection(client, collection_name_default_db)