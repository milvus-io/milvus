import pytest
import requests
from api.milvus import AliasClient, CollectionClient, DatabaseClient, RoleClient, UserClient, VectorClient
from utils.constant import CaseLabel
from utils.utils import gen_unique_str

BASE_ID = 2**53 + 1
TIMEOUT_CODE = 10001
PRIVILEGE_NOT_PERMITTED = 1401


def search_request(collection, **fields):
    return {
        "collectionName": collection,
        "data": [[1.0, 0.0], [0.0, 1.0]],
        "annsField": "vector",
        "limit": 3,
        "outputFields": ["marker"],
        "consistencyLevel": "Strong",
        **fields,
    }


def post_search(endpoint, token, operation, body, **headers):
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return requests.post(f"{endpoint}/v2/vectordb/entities/{operation}", json=body, headers=headers, timeout=30)


def run_batch(endpoint, token, searches, options=None, **headers):
    body = {"requests": [{"search": item} for item in searches], **(options or {})}
    response = post_search(endpoint, token, "multi_search", body, **headers)
    assert response.status_code == 200, response.text
    batch = response.json()
    assert batch["code"] == 0, batch
    assert len(batch["data"]) == len(searches), batch
    return [item["search"] for item in batch["data"]]


@pytest.fixture(scope="class")
def search_targets(endpoint, token, request):
    db = gen_unique_str("multisearch_db")
    shared = gen_unique_str("multisearch_shared")
    other = gen_unique_str("multisearch_other")
    alias = gen_unique_str("multisearch_alias")
    database = DatabaseClient(endpoint, token)
    collections = CollectionClient(endpoint, token)
    vectors = VectorClient(endpoint, token)
    aliases = AliasClient(endpoint, token)
    rsp = database.database_create({"dbName": db})
    assert rsp["code"] == 0, rsp
    request.addfinalizer(lambda: database.database_drop({"dbName": db}))

    for db_name, collection, pk in [("default", shared, BASE_ID), ("default", other, 21), (db, shared, 41)]:
        rsp = collections.collection_create(
            {
                "collectionName": collection,
                "schema": {
                    "autoId": False,
                    "fields": [
                        {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                        {"fieldName": "marker", "dataType": "Int64"},
                        {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": "2"}},
                    ],
                },
                "indexParams": [{"fieldName": "vector", "indexName": "flat", "indexType": "FLAT", "metricType": "IP"}],
            },
            db_name=db_name,
        )
        assert rsp["code"] == 0, rsp
        request.addfinalizer(
            lambda name=collection, target_db=db_name: collections.collection_drop(
                {"collectionName": name}, db_name=target_db
            )
        )
        rsp = vectors.vector_insert(
            {
                "collectionName": collection,
                "data": [
                    {"id": pk, "marker": BASE_ID, "vector": [1.0, 0.0]},
                    {"id": pk + 1, "marker": BASE_ID + 1, "vector": [0.0, 1.0]},
                ],
            },
            db_name=db_name,
        )
        assert rsp["code"] == 0, rsp
        collections.wait_load_completed(collection, db_name=db_name, timeout=90)

    for db_name in ("default", db):
        rsp = aliases.create_alias({"dbName": db_name, "collectionName": shared, "aliasName": alias})
        assert rsp["code"] == 0, rsp
        request.addfinalizer(lambda target_db=db_name: aliases.drop_alias({"dbName": target_db, "aliasName": alias}))
    return db, shared, other, alias


@pytest.mark.tags(CaseLabel.L0)
class TestMultiSearch:
    @pytest.mark.parametrize("allow_int64", ["true", "false"])
    def test_ordered_native_results(self, endpoint, token, search_targets, allow_int64):
        _, shared, other, _ = search_targets
        searches = [
            search_request(shared, filter=f"id == {BASE_ID}"),
            search_request(other),
            search_request(shared, filter=f"id == {BASE_ID + 1}", data=[[0.0, 1.0]]),
            search_request(shared, filter="id < 0"),
            {},
            search_request(shared, data="invalid"),
        ]
        headers = {"Accept-Type-Allow-Int64": allow_int64}
        results = run_batch(endpoint, token, searches, {"maxConcurrency": 3}, **headers)
        for query, item in zip(searches[:4], results[:4]):
            ordinary = post_search(endpoint, token, "search", query, **headers).json()
            assert item["code"] == ordinary["code"] == 0, (item, ordinary)
            assert item["data"] == ordinary["data"], (item, ordinary)
            assert item.get("topks") == ordinary.get("topks"), (item, ordinary)
            assert item.keys() == ordinary.keys(), (item, ordinary)
        assert results[0]["topks"] == [1, 1]
        expected_id = BASE_ID if allow_int64 == "true" else str(BASE_ID)
        assert results[0]["data"][0]["id"] == expected_id
        assert results[0]["data"][0]["marker"] == expected_id
        assert results[1]["topks"] == [2, 2]
        assert results[3]["data"] == []
        assert all(item["code"] != 0 and "data" not in item for item in results[4:])
        assert all(item["code"] != 0 for item in run_batch(endpoint, token, searches[4:]))

    def test_database_precedence(self, endpoint, token, search_targets):
        db, shared, _, alias = search_targets
        headers = {"DB-Name": db}
        results = run_batch(
            endpoint,
            token,
            [search_request(alias), search_request(shared, dbName="default"), search_request(alias, dbName="")],
            **headers,
        )
        assert [result["code"] for result in results] == [0, 0, 0], results
        assert [int(result["data"][0]["id"]) for result in results] == [41, BASE_ID, 41]
        results = run_batch(endpoint, token, [search_request(alias, dbName=db)], **{"DB-Name": f"{db}_missing"})
        assert results[0]["code"] == 0, results
        assert int(results[0]["data"][0]["id"]) == 41
        results = run_batch(endpoint, token, [search_request(alias)])
        assert results[0]["code"] == 0, results
        assert int(results[0]["data"][0]["id"]) == BASE_ID

    def test_cached_targets_still_require_search_privilege(self, endpoint, token, search_targets, request):
        db, shared, _, _ = search_targets
        user, role, password = gen_unique_str("searcher"), gen_unique_str("search_role"), "searcher123"
        users, roles = UserClient(endpoint, token), RoleClient(endpoint, token)
        rsp = users.user_create({"userName": user, "password": password})
        assert rsp["code"] == 0, rsp
        request.addfinalizer(lambda: users.user_drop({"userName": user}))
        rsp = roles.role_create({"roleName": role})
        assert rsp["code"] == 0, rsp
        request.addfinalizer(lambda: roles.role_drop({"roleName": role}))
        grant = {
            "roleName": role,
            "objectType": "Collection",
            "objectName": shared,
            "privilege": "Search",
            "dbName": "default",
        }
        rsp = roles.role_grant(grant)
        assert rsp["code"] == 0, rsp
        request.addfinalizer(lambda: roles.role_revoke(grant))
        binding = {"userName": user, "roleName": role}
        rsp = users.user_grant(binding)
        assert rsp["code"] == 0, rsp
        request.addfinalizer(lambda: users.user_revoke(binding))

        searches = [search_request(shared, dbName=db), search_request(shared, dbName="default")]
        warm = run_batch(endpoint, token, searches)
        assert [item["code"] for item in warm] == [0, 0]
        results = run_batch(endpoint, f"{user}:{password}", searches, {"maxConcurrency": 2})
        assert [item["code"] for item in results] == [PRIVILEGE_NOT_PERMITTED, 0], results
        assert int(results[1]["data"][0]["id"]) == BASE_ID

    def test_structure_and_expired_budget(self, endpoint, token, search_targets):
        _, shared, _, _ = search_targets
        valid_search = {"search": search_request(shared)}
        for payload in [
            {"requests": []},
            {"requests": [valid_search, {"query": {}}]},
            {"requests": [valid_search, {}]},
            {"requests": [valid_search], "timeoutMs": -1},
        ]:
            response = post_search(endpoint, token, "multi_search", payload)
            assert response.json()["code"] != 0, response.text
            assert not response.json().get("data"), response.text
        response = post_search(endpoint, "unknown:bad", "multi_search", {"requests": [valid_search]})
        assert response.json()["code"] != 0, response.text
        assert not response.json().get("data"), response.text

        results = run_batch(
            endpoint, token, [search_request(shared), search_request(shared)], {"timeoutMs": 0, "maxConcurrency": 1}
        )
        assert [item["code"] for item in results] == [TIMEOUT_CODE, TIMEOUT_CODE], results
        assert all("data" not in item for item in results)
        assert run_batch(endpoint, token, [search_request(shared)], {"timeoutMs": 10000})[0]["code"] == 0
