import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

DIM = 4


def _vec(seed):
    return [float(seed), 0.0, 0.0, 0.0]


def _regex_rows():
    return [
        {
            "id": 1,
            "vec": _vec(1),
            "text": "ERROR E1001: connection timeout",
            "email": "alice@gmail.com",
            "url": "/api/v1/users/123",
            "level": "ERROR",
            "metadata": {
                "level": "ERROR",
                "version": "v1.2",
                "trace": "abc-123",
                "version_num": 12,
                "enabled": True,
                "nested": {"x": "abc"},
                "arr": ["abc"],
            },
            "tags": ["release-v1", "prod"],
        },
        {
            "id": 2,
            "vec": _vec(2),
            "text": "WARN W2002: retry later",
            "email": "bob@example.com",
            "url": "/api/v2/orders/456",
            "level": "WARN",
            "metadata": {
                "level": "WARN",
                "version": "v2.0",
                "trace": "def-456",
                "version_num": 20,
                "enabled": False,
                "nested": {"x": "def"},
                "arr": ["def"],
            },
            "tags": ["release-v2", "staging"],
        },
        {
            "id": 3,
            "vec": _vec(3),
            "text": "DEBUG cache hit",
            "email": "carol@GMAIL.com",
            "url": "/api/v10/users/789",
            "level": "DEBUG",
            "metadata": {
                "level": "DEBUG",
                "version": "v10.1",
                "trace": "ghi-789",
                "version_num": 101,
                "enabled": True,
                "nested": {"x": "ghi"},
                "arr": ["ghi"],
            },
            "tags": ["debug", "dev"],
        },
        {
            "id": 4,
            "vec": _vec(4),
            "text": "cn log error code 555-1234",
            "email": None,
            "url": "/static/index.html",
            "level": "INFO",
            "metadata": {
                "level": "INFO",
                "version": "alpha",
                "trace": None,
                "version_num": 0,
                "enabled": False,
                "nested": {"x": "cn"},
                "arr": ["cn"],
            },
            "tags": ["cn", "release-alpha"],
        },
        {
            "id": 5,
            "vec": _vec(5),
            "text": "multi\nline c\nd pattern",
            "email": "dave@gmail.com",
            "url": "/api/v1/users/search",
            "level": "ERROR",
            "metadata": {
                "level": "ERROR",
                "version": "v1.3",
                "trace": "jkl-000",
                "version_num": 13,
                "enabled": True,
                "nested": {"x": "jkl"},
                "arr": ["jkl"],
            },
            "tags": ["release-v1-hotfix", "prod"],
        },
        {
            "id": 6,
            "vec": _vec(6),
            "text": "",
            "email": "empty@gmail.com",
            "url": "",
            "level": "",
            "metadata": {
                "level": "",
                "version": "",
                "trace": "",
                "version_num": None,
                "enabled": False,
                "nested": {},
                "arr": [],
            },
            "tags": ["", "empty"],
        },
        {
            "id": 7,
            "vec": _vec(7),
            "text": "status OK deploy success",
            "email": "emo@dev.io",
            "url": "/api/health",
            "level": "INFO",
            "metadata": {
                "level": "INFO",
                "version": "v1.0",
                "trace": "emoji-001",
                "version_num": 10,
                "enabled": True,
                "nested": {"x": "ok"},
                "arr": ["ok"],
            },
            "tags": ["emoji", "dev"],
        },
    ]


class TestRegexFilter(TestBase):
    def setup_class(self):
        self.collection_name = self.__class__.__name__ + gen_collection_name()

    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_regex_collection(self, request, init_class_config):
        collection_client, vector_client = self._class_scope_clients()

        def teardown():
            collection_client.collection_drop({"collectionName": self.collection_name})

        request.addfinalizer(teardown)
        self._create_regex_collection(self.collection_name, collection_client, vector_client)

    def _create_regex_collection(self, name, collection_client, vector_client):
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": False,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vec", "dataType": "FloatVector", "elementTypeParams": {"dim": str(DIM)}},
                    {"fieldName": "text", "dataType": "VarChar", "elementTypeParams": {"max_length": "512"}},
                    {
                        "fieldName": "email",
                        "dataType": "VarChar",
                        "nullable": True,
                        "elementTypeParams": {"max_length": "256"},
                    },
                    {"fieldName": "url", "dataType": "VarChar", "elementTypeParams": {"max_length": "512"}},
                    {"fieldName": "level", "dataType": "VarChar", "elementTypeParams": {"max_length": "32"}},
                    {"fieldName": "metadata", "dataType": "JSON"},
                    {
                        "fieldName": "tags",
                        "dataType": "Array",
                        "elementDataType": "VarChar",
                        "elementTypeParams": {"max_capacity": "8", "max_length": "128"},
                    },
                ],
            },
            "indexParams": [{"fieldName": "vec", "indexName": "vec_index", "metricType": "L2"}],
        }
        rsp = collection_client.collection_create(payload)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(name, timeout=60)

        rows = _regex_rows()
        rsp = vector_client.vector_insert({"collectionName": name, "data": rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(rows)
        collection_client.flush(name)

    def _shared_collection(self):
        return self.collection_name

    def _query_ids(self, name, filter_expr, timeout=1):
        rsp = self.vector_client.vector_query(
            {"collectionName": name, "filter": filter_expr, "outputFields": ["id"], "limit": 100},
            timeout=timeout,
        )
        assert rsp["code"] == 0, rsp
        return sorted(row["id"] for row in rsp.get("data", []))

    @pytest.mark.tags(CaseLabel.L0)
    def test_regex_query_basic_semantics(self):
        """
        target: verify REST query supports core regex =~ semantics
        method: query VarChar field with substring, anchors, classes, dot-newline, and empty pattern
        expected: returned ids match PyMilvus regex filter semantics
        """
        name = self._shared_collection()

        cases = [
            ('text =~ "timeout"', [1]),
            ('text =~ "ERROR"', [1]),
            ('text =~ "^ERROR"', [1]),
            ('text =~ "hit$"', [3]),
            ('text =~ "^DEBUG cache hit$"', [3]),
            (r'text =~ "E[0-9]{4}:"', [1]),
            (r'text =~ "[0-9]{3}-[0-9]{4}"', [4]),
            ('text =~ "c.d"', [4, 5]),
            ('text =~ "(?-s)c.d"', [4]),
            ('text =~ ""', [1, 2, 3, 4, 5, 6, 7]),
            ('text =~ "^$"', [6]),
        ]
        for filter_expr, expected in cases:
            assert self._query_ids(name, filter_expr) == expected

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_query_negation_and_boolean_expression(self):
        """
        target: verify REST query supports !~, not(=~), and boolean combinations
        method: compare negated regex and combined scalar predicates
        expected: !~ is equivalent to not(=~) and boolean precedence is respected
        """
        name = self._shared_collection()

        assert self._query_ids(name, 'text !~ "^DEBUG"') == [1, 2, 4, 5, 6, 7]
        assert self._query_ids(name, 'not (text =~ "^DEBUG")') == [1, 2, 4, 5, 6, 7]
        assert self._query_ids(name, 'text =~ "ERROR" and id > 1') == []
        assert self._query_ids(name, 'text =~ "(?i)error" and id > 1') == [4]
        assert self._query_ids(name, 'text =~ "^WARN" or text =~ "^DEBUG"') == [2, 3]

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_query_json_array_and_nullable_paths(self):
        """
        target: verify REST query supports regex on JSON paths, Array<Varchar> elements, and nullable fields
        method: query JSON string paths, array element paths, and nullable VarChar with =~ and !~
        expected: string paths match, non-string/missing paths return empty, null values are excluded unless explicit
        """
        name = self._shared_collection()

        assert self._query_ids(name, r'metadata["version"] =~ "^v[0-9]+\.[0-9]+$"') == [1, 2, 3, 5, 7]
        assert self._query_ids(name, 'metadata["nested"]["x"] =~ "^abc$"') == [1]
        assert self._query_ids(name, 'metadata["nested"]["x"] !~ "^abc$"') == [2, 3, 4, 5, 7]
        assert self._query_ids(name, r'metadata["trace"] =~ "[a-z]+-[0-9]+"') == [1, 2, 3, 5, 7]
        assert self._query_ids(name, 'metadata["version_num"] =~ "1"', timeout=0) == []
        assert self._query_ids(name, 'metadata["nested"]["missing"] =~ ".*"', timeout=0) == []

        assert self._query_ids(name, 'tags[0] =~ "^release-v[0-9]+"') == [1, 2, 5]
        assert self._query_ids(name, 'tags[0] !~ "^release"') == [3, 4, 6, 7]
        assert self._query_ids(name, 'tags[10] =~ ".*"', timeout=0) == []
        assert self._query_ids(name, 'tags[0] =~ "^$"') == [6]

        assert self._query_ids(name, 'email =~ "gmail"') == [1, 5, 6]
        assert self._query_ids(name, 'email !~ "gmail"') == [2, 3, 7]
        assert self._query_ids(name, 'email !~ "gmail" or email is null') == [2, 3, 4, 7]

    @pytest.mark.tags(CaseLabel.L0)
    def test_regex_search_filter(self):
        """
        target: verify REST vector search supports regex filters
        method: run unfiltered search then filtered search with regex on url field
        expected: filtered search only returns ids with /api/v[0-9]+/users urls
        """
        name = self._shared_collection()

        unfiltered = self.vector_client.vector_search(
            {
                "collectionName": name,
                "data": [_vec(1)],
                "annsField": "vec",
                "limit": 7,
                "outputFields": ["id"],
                "searchParams": {"metricType": "L2", "params": {}},
            }
        )
        assert unfiltered["code"] == 0, unfiltered
        assert {row["id"] for row in unfiltered["data"]} == {1, 2, 3, 4, 5, 6, 7}

        filtered = self.vector_client.vector_search(
            {
                "collectionName": name,
                "data": [_vec(1)],
                "annsField": "vec",
                "filter": 'url =~ "^/api/v[0-9]+/users"',
                "limit": 3,
                "outputFields": ["id", "url"],
                "searchParams": {"metricType": "L2", "params": {}},
            }
        )
        assert filtered["code"] == 0, filtered
        assert {row["id"] for row in filtered["data"]} == {1, 3, 5}
        assert all(row["url"].startswith("/api/v") for row in filtered["data"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_delete_filter(self):
        """
        target: verify REST delete supports regex filters
        method: delete rows where text starts with DEBUG, then query remaining ids
        expected: id 3 is removed and other rows remain visible
        """
        name = gen_collection_name()
        self.name = name
        self._create_regex_collection(name, self.collection_client, self.vector_client)

        rsp = self.vector_client.vector_delete({"collectionName": name, "filter": 'text =~ "^DEBUG"'})
        assert rsp["code"] == 0, rsp

        assert self._query_ids(name, "id > 0") == [1, 2, 4, 5, 6, 7]

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_invalid_expressions(self):
        """
        target: verify REST rejects invalid regex expressions
        method: send invalid pattern, unsupported field type, direct array field, and non-string RHS
        expected: each request fails with a clear validation error
        """
        name = self._shared_collection()

        cases = [
            ('text =~ "(unclosed"', "missing closing"),
            ('text =~ "(a)\\\\1"', "invalid regex pattern"),
            ('id =~ "1"', "regex match on non-string or non-json field"),
            ('tags =~ "prod"', "can not comparisons array fields directly"),
            ("text =~ 123", "string literal or template variable"),
            ('unknown_field =~ "test"', "field unknown_field not exist"),
        ]
        for filter_expr, message in cases:
            rsp = self.vector_client.vector_query(
                {"collectionName": name, "filter": filter_expr, "outputFields": ["id"], "limit": 10}
            )
            assert rsp["code"] != 0, rsp
            assert message in rsp["message"], rsp
