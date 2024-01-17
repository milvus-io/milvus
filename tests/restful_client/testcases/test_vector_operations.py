import random
from sklearn import preprocessing
import numpy as np
import sys
import json
import time
from utils import constant
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from utils.utils import (get_data_by_payload, get_common_fields_by_data)


@pytest.mark.L0
class TestInsertVector(TestBase):

    @pytest.mark.parametrize("insert_round", [2, 1])
    @pytest.mark.parametrize("nb", [100, 10, 1])
    @pytest.mark.parametrize("dim", [32, 128])
    @pytest.mark.parametrize("primary_field", ["id", "url"])
    @pytest.mark.parametrize("vector_field", ["vector", "embedding"])
    @pytest.mark.parametrize("db_name", ["prod", "default"])
    def test_insert_vector_with_simple_payload(self, db_name, vector_field, primary_field, nb, dim, insert_round):
        """
        Insert a vector with a simple payload
        """
        self.update_database(db_name=db_name)
        # create a collection
        name = gen_collection_name()
        collection_payload = {
            "collectionName": name,
            "dimension": dim,
            "primaryField": primary_field,
            "vectorField": vector_field,
        }
        rsp = self.collection_client.collection_create(collection_payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 200
        # insert data
        for i in range(insert_round):
            data = get_data_by_payload(collection_payload, nb)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 200
            assert rsp['data']['insertCount'] == nb
        logger.info("finished")

    @pytest.mark.L0
    @pytest.mark.parametrize("insert_round", [10])
    def test_insert_vector_with_multi_round(self, insert_round):
        """
        Insert a vector with a simple payload
        """
        # create a collection
        name = gen_collection_name()
        collection_payload = {
            "collectionName": name,
            "dimension": 768,
        }
        rsp = self.collection_client.collection_create(collection_payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        logger.info(f"rsp: {rsp}")
        assert rsp['code'] == 200
        # insert data
        nb = 300
        for i in range(insert_round):
            data = get_data_by_payload(collection_payload, nb)
            payload = {
                "collectionName": name,
                "data": data,
            }
            body_size = sys.getsizeof(json.dumps(payload))
            logger.info(f"body size: {body_size / 1024 / 1024} MB")
            rsp = self.vector_client.vector_insert(payload)
            assert rsp['code'] == 200
            assert rsp['data']['insertCount'] == nb


@pytest.mark.L1
class TestInsertVectorNegative(TestBase):
    def test_insert_vector_with_invalid_api_key(self):
        """
        Insert a vector with invalid api key
        """
        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 200
        # insert data
        nb = 10
        data = [
            {
                "vector": [np.float64(random.random()) for _ in range(dim)],
            } for _ in range(nb)
        ]
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        client = self.vector_client
        client.api_key = "invalid_api_key"
        rsp = client.vector_insert(payload)
        assert rsp['code'] == 1800

    def test_insert_vector_with_invalid_collection_name(self):
        """
        Insert a vector with an invalid collection name
        """

        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 200
        # insert data
        nb = 100
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": "invalid_collection_name",
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 1

    def test_insert_vector_with_invalid_database_name(self):
        """
        Insert a vector with an invalid database name
        """
        # create a collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 200
        # insert data
        nb = 10
        data = get_data_by_payload(payload, nb)
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        success = False
        rsp = self.vector_client.vector_insert(payload, db_name="invalid_database")
        assert rsp['code'] == 800

    def test_insert_vector_with_mismatch_dim(self):
        """
        Insert a vector with mismatch dim
        """
        # create a collection
        name = gen_collection_name()
        dim = 32
        payload = {
            "collectionName": name,
            "dimension": dim,
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 200
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 200
        # insert data
        nb = 1
        data = [
            {
                "vector": [np.float64(random.random()) for _ in range(dim + 1)],
            } for i in range(nb)
        ]
        payload = {
            "collectionName": name,
            "data": data,
        }
        body_size = sys.getsizeof(json.dumps(payload))
        logger.info(f"body size: {body_size / 1024 / 1024} MB")
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 1804
        assert rsp['message'] == "fail to deal the insert data"


@pytest.mark.L0
class TestSearchVector(TestBase):

    @pytest.mark.parametrize("metric_type", ["IP", "L2"])
    def test_search_vector_with_simple_payload(self, metric_type):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, metric_type=metric_type)

        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        limit = int(payload.get("limit", 100))
        assert len(res) == limit
        ids = [item['id'] for item in res]
        assert len(ids) == len(set(ids))
        distance = [item['distance'] for item in res]
        if metric_type == "L2":
            assert distance == sorted(distance)
        if metric_type == "IP":
            assert distance == sorted(distance, reverse=True)

    @pytest.mark.parametrize("sum_limit_offset", [16384, 16385])
    @pytest.mark.xfail(reason="")
    def test_search_vector_with_exceed_sum_limit_offset(self, sum_limit_offset):
        """
        Search a vector with a simple payload
        """
        max_search_sum_limit_offset = constant.MAX_SUM_OFFSET_AND_LIMIT
        name = gen_collection_name()
        self.name = name
        nb = sum_limit_offset + 2000
        metric_type = "IP"
        limit = 100
        self.init_collection(name, metric_type=metric_type, nb=nb, batch_size=2000)

        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "limit": limit,
            "offset": sum_limit_offset - limit,
        }
        rsp = self.vector_client.vector_search(payload)
        if sum_limit_offset > max_search_sum_limit_offset:
            assert rsp['code'] == 65535
            return
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        limit = int(payload.get("limit", 100))
        assert len(res) == limit
        ids = [item['id'] for item in res]
        assert len(ids) == len(set(ids))
        distance = [item['distance'] for item in res]
        if metric_type == "L2":
            assert distance == sorted(distance)
        if metric_type == "IP":
            assert distance == sorted(distance, reverse=True)

    @pytest.mark.parametrize("level", [0, 1, 2])
    @pytest.mark.parametrize("offset", [0, 10, 100])
    @pytest.mark.parametrize("limit", [1, 100])
    @pytest.mark.parametrize("metric_type", ["L2", "IP"])
    def test_search_vector_with_complex_payload(self, limit, offset, level, metric_type):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = limit + offset + 100
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb, metric_type=metric_type)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": limit,
            "offset": offset,
        }
        rsp = self.vector_client.vector_search(payload)
        if offset + limit > constant.MAX_SUM_OFFSET_AND_LIMIT:
            assert rsp['code'] == 90126
            return
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) == limit
        for item in res:
            assert item.get("uid") >= 0
            for field in output_fields:
                assert field in item

    @pytest.mark.parametrize("filter_expr", ["uid >= 0", "uid >= 0 and uid < 100", "uid in [1,2,3]"])
    def test_search_vector_with_complex_int_filter(self, filter_expr):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            uid = item.get("uid")
            eval(filter_expr)

    @pytest.mark.parametrize("filter_expr", ["name > \"placeholder\"", "name like \"placeholder%\""])
    def test_search_vector_with_complex_varchar_filter(self, filter_expr):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            name = item.get("name")
            logger.info(f"name: {name}")
            if ">" in filter_expr:
                assert name > prefix
            if "like" in filter_expr:
                assert name.startswith(prefix)

    @pytest.mark.parametrize("filter_expr", ["uid < 100 and name > \"placeholder\"",
                                             "uid < 100 and name like \"placeholder%\""
                                             ])
    def test_search_vector_with_complex_int64_varchar_and_filter(self, filter_expr):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            uid = item.get("uid")
            name = item.get("name")
            logger.info(f"name: {name}")
            uid_expr = filter_expr.split("and")[0]
            assert eval(uid_expr) is True
            varchar_expr = filter_expr.split("and")[1]
            if ">" in varchar_expr:
                assert name > prefix
            if "like" in varchar_expr:
                assert name.startswith(prefix)


@pytest.mark.L1
class TestSearchVectorNegative(TestBase):
    @pytest.mark.parametrize("limit", [0, 16385])
    def test_search_vector_with_invalid_limit(self, limit):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim)
        vector_field = schema_payload.get("vectorField")
        # search data
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": limit,
            "offset": 0,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 1

    @pytest.mark.parametrize("offset", [-1, 100_001])
    def test_search_vector_with_invalid_offset(self, offset):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim)
        vector_field = schema_payload.get("vectorField")
        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        output_fields = get_common_fields_by_data(data, exclude_fields=[vector_field])
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
            "outputFields": output_fields,
            "filter": "uid >= 0",
            "limit": 100,
            "offset": offset,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 1

    def test_search_vector_with_illegal_api_key(self):
        """
        Search a vector with an illegal api key
        """
        pass

    def test_search_vector_with_invalid_collection_name(self):
        """
        Search a vector with an invalid collection name
        """
        pass

    def test_search_vector_with_invalid_output_field(self):
        """
        Search a vector with an invalid output field
        """
        pass

    @pytest.mark.parametrize("invalid_expr", ["invalid_field > 0", "12-s", "中文", "a", " "])
    def test_search_vector_with_invalid_expression(self, invalid_expr):
        """
        Search a vector with an invalid expression
        """
        pass

    def test_search_vector_with_invalid_vector_field(self):
        """
        Search a vector with an invalid vector field for ann search
        """
        pass

    @pytest.mark.parametrize("dim_offset", [1, -1])
    def test_search_vector_with_mismatch_vector_dim(self, dim_offset):
        """
        Search a vector with a mismatch vector dim
        """
        pass


@pytest.mark.L0
class TestQueryVector(TestBase):

    @pytest.mark.parametrize("expr", ["10+20 <= uid < 20+30", "uid in [1,2,3,4]",
                                      "uid > 0", "uid >= 0", "uid > 0",
                                      "uid > -100 and uid < 100"])
    @pytest.mark.parametrize("include_output_fields", [True, False])
    @pytest.mark.parametrize("partial_fields", [True, False])
    def test_query_vector_with_int64_filter(self, expr, include_output_fields, partial_fields):
        """
        Query a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        schema_payload, data = self.init_collection(name)
        output_fields = get_common_fields_by_data(data)
        if partial_fields:
            output_fields = output_fields[:len(output_fields) // 2]
            if "uid" not in output_fields:
                output_fields.append("uid")
        else:
            output_fields = output_fields

        # query data
        payload = {
            "collectionName": name,
            "filter": expr,
            "limit": 100,
            "offset": 0,
            "outputFields": output_fields
        }
        if not include_output_fields:
            payload.pop("outputFields")
            if 'vector' in output_fields:
                output_fields.remove("vector")
        time.sleep(5)
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        for r in res:
            uid = r['uid']
            assert eval(expr) is True
            for field in output_fields:
                assert field in r

    @pytest.mark.parametrize("filter_expr", ["name > \"placeholder\"", "name like \"placeholder%\""])
    @pytest.mark.parametrize("include_output_fields", [True, False])
    def test_query_vector_with_varchar_filter(self, filter_expr, include_output_fields):
        """
        Query a vector with a complex payload
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        # search data
        output_fields = get_common_fields_by_data(data)
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": 0,
        }
        if not include_output_fields:
            payload.pop("outputFields")
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            name = item.get("name")
            logger.info(f"name: {name}")
            if ">" in filter_expr:
                assert name > prefix
            if "like" in filter_expr:
                assert name.startswith(prefix)

    @pytest.mark.parametrize("sum_of_limit_offset", [16384])
    def test_query_vector_with_large_sum_of_limit_offset(self, sum_of_limit_offset):
        """
        Query a vector with sum of limit and offset larger than max value
        """
        max_sum_of_limit_offset = 16384
        name = gen_collection_name()
        filter_expr = "name > \"placeholder\""
        self.name = name
        nb = 200
        dim = 128
        limit = 100
        offset = sum_of_limit_offset - limit
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        names = []
        for item in data:
            names.append(item.get("name"))
        names.sort()
        logger.info(f"names: {names}")
        mid = len(names) // 2
        prefix = names[mid][0:2]
        # search data
        output_fields = get_common_fields_by_data(data)
        filter_expr = filter_expr.replace("placeholder", prefix)
        logger.info(f"filter_expr: {filter_expr}")
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": filter_expr,
            "limit": limit,
            "offset": offset,
        }
        rsp = self.vector_client.vector_query(payload)
        if sum_of_limit_offset > max_sum_of_limit_offset:
            assert rsp['code'] == 1
            return
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        assert len(res) <= limit
        for item in res:
            name = item.get("name")
            logger.info(f"name: {name}")
            if ">" in filter_expr:
                assert name > prefix
            if "like" in filter_expr:
                assert name.startswith(prefix)


@pytest.mark.L0
class TestGetVector(TestBase):

    def test_get_vector_with_simple_payload(self):
        """
        Search a vector with a simple payload
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name)

        # search data
        dim = 128
        vector_to_search = preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
        payload = {
            "collectionName": name,
            "vector": vector_to_search,
        }
        rsp = self.vector_client.vector_search(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        limit = int(payload.get("limit", 100))
        assert len(res) == limit
        ids = [item['id'] for item in res]
        assert len(ids) == len(set(ids))
        payload = {
            "collectionName": name,
            "outputFields": ["*"],
            "id": ids[0],
        }
        rsp = self.vector_client.vector_get(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {res}")
        logger.info(f"res: {len(res)}")
        for item in res:
            assert item['id'] == ids[0]

    @pytest.mark.L0
    @pytest.mark.parametrize("id_field_type", ["list", "one"])
    @pytest.mark.parametrize("include_invalid_id", [True, False])
    @pytest.mark.parametrize("include_output_fields", [True, False])
    def test_get_vector_complex(self, id_field_type, include_output_fields, include_invalid_id):
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"uid in {uids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        ids = []
        for r in res:
            ids.append(r['id'])
        logger.info(f"ids: {len(ids)}")
        id_to_get = None
        if id_field_type == "list":
            id_to_get = ids
        if id_field_type == "one":
            id_to_get = ids[0]
        if include_invalid_id:
            if isinstance(id_to_get, list):
                id_to_get[-1] = 0
            else:
                id_to_get = 0
        # get by id list
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "id": id_to_get
        }
        rsp = self.vector_client.vector_get(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        if isinstance(id_to_get, list):
            if include_invalid_id:
                assert len(res) == len(id_to_get) - 1
            else:
                assert len(res) == len(id_to_get)
        else:
            if include_invalid_id:
                assert len(res) == 0
            else:
                assert len(res) == 1
        for r in rsp['data']:
            if isinstance(id_to_get, list):
                assert r['id'] in id_to_get
            else:
                assert r['id'] == id_to_get
            if include_output_fields:
                for field in output_fields:
                    assert field in r


@pytest.mark.L0
class TestDeleteVector(TestBase):

    @pytest.mark.parametrize("include_invalid_id", [True, False])
    @pytest.mark.parametrize("id_field_type", ["list", "one"])
    def test_delete_vector_default(self, id_field_type, include_invalid_id):
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        time.sleep(1)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"uid in {uids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        ids = []
        for r in res:
            ids.append(r['id'])
        logger.info(f"ids: {len(ids)}")
        id_to_get = None
        if id_field_type == "list":
            id_to_get = ids
        if id_field_type == "one":
            id_to_get = ids[0]
        if include_invalid_id:
            if isinstance(id_to_get, list):
                id_to_get.append(0)
            else:
                id_to_get = 0
        if isinstance(id_to_get, list):
            if len(id_to_get) >= 100:
                id_to_get = id_to_get[-100:]
        # delete by id list
        payload = {
            "collectionName": name,
            "id": id_to_get
        }
        rsp = self.vector_client.vector_delete(payload)
        assert rsp['code'] == 200
        logger.info(f"delete res: {rsp}")

        # verify data deleted
        if not isinstance(id_to_get, list):
            id_to_get = [id_to_get]
        payload = {
            "collectionName": name,
            "filter": f"id in {id_to_get}",
        }
        time.sleep(5)
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        assert len(rsp['data']) == 0


@pytest.mark.L1
class TestDeleteVector(TestBase):
    def test_delete_vector_with_invalid_api_key(self):
        """
        Delete a vector with an invalid api key
        """
        name = gen_collection_name()
        self.name = name
        nb = 200
        dim = 128
        schema_payload, data = self.init_collection(name, dim=dim, nb=nb)
        output_fields = get_common_fields_by_data(data)
        uids = []
        for item in data:
            uids.append(item.get("uid"))
        payload = {
            "collectionName": name,
            "outputFields": output_fields,
            "filter": f"uid in {uids}",
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        ids = []
        for r in res:
            ids.append(r['id'])
        logger.info(f"ids: {len(ids)}")
        id_to_get = ids
        # delete by id list
        payload = {
            "collectionName": name,
            "id": id_to_get
        }
        client = self.vector_client
        client.api_key = "invalid_api_key"
        rsp = client.vector_delete(payload)
        assert rsp['code'] == 1800

    def test_delete_vector_with_invalid_collection_name(self):
        """
        Delete a vector with an invalid collection name
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, dim=128, nb=3000)

        # query data
        # expr = f"id in {[i for i in range(10)]}".replace("[", "(").replace("]", ")")
        expr = "id > 0"
        payload = {
            "collectionName": name,
            "filter": expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        id_list = [r['id'] for r in res]
        delete_expr = f"id in {[i for i in id_list[:10]]}"
        # query data before delete
        payload = {
            "collectionName": name,
            "filter": delete_expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        # delete data
        payload = {
            "collectionName": name + "_invalid",
            "filter": delete_expr,
        }
        rsp = self.vector_client.vector_delete(payload)
        assert rsp['code'] == 1

    def test_delete_vector_with_non_primary_key(self):
        """
        Delete a vector with a non-primary key, expect no data were deleted
        """
        name = gen_collection_name()
        self.name = name
        self.init_collection(name, dim=128, nb=300)
        expr = "uid > 0"
        payload = {
            "collectionName": name,
            "filter": expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        logger.info(f"res: {len(res)}")
        id_list = [r['uid'] for r in res]
        delete_expr = f"uid in {[i for i in id_list[:10]]}"
        # query data before delete
        payload = {
            "collectionName": name,
            "filter": delete_expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        rsp = self.vector_client.vector_query(payload)
        assert rsp['code'] == 200
        res = rsp['data']
        num_before_delete = len(res)
        logger.info(f"res: {len(res)}")
        # delete data
        payload = {
            "collectionName": name,
            "filter": delete_expr,
        }
        rsp = self.vector_client.vector_delete(payload)
        # query data after delete
        payload = {
            "collectionName": name,
            "filter": delete_expr,
            "limit": 3000,
            "offset": 0,
            "outputFields": ["id", "uid"]
        }
        time.sleep(1)
        rsp = self.vector_client.vector_query(payload)
        assert len(rsp["data"]) == num_before_delete
