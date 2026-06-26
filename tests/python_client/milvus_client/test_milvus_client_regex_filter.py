import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import AnnSearchRequest, DataType, WeightedRanker
from pymilvus.client.embedding_list import EmbeddingList

prefix = "regex_filter"
default_dim = ct.default_dim


def gen_regex_test_schema(client, dim=default_dim):
    schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
    schema.add_field("text", DataType.VARCHAR, max_length=512)
    schema.add_field("email", DataType.VARCHAR, max_length=256, nullable=True)
    schema.add_field("url", DataType.VARCHAR, max_length=512)
    schema.add_field("level", DataType.VARCHAR, max_length=32)
    schema.add_field(
        "metadata",
        DataType.JSON,
    )
    schema.add_field(
        "tags",
        DataType.ARRAY,
        element_type=DataType.VARCHAR,
        max_capacity=8,
        max_length=128,
    )
    return schema


def gen_regex_test_data():
    return [
        {
            "id": 1,
            "vec": [random.random() for _ in range(default_dim)],
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
            "vec": [random.random() for _ in range(default_dim)],
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
            "vec": [random.random() for _ in range(default_dim)],
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
            "vec": [random.random() for _ in range(default_dim)],
            "text": "中文日志 error code 555-1234",
            "email": None,
            "url": "/static/index.html",
            "level": "INFO",
            "metadata": {
                "level": "INFO",
                "version": "alpha",
                "trace": None,
                "version_num": 0,
                "enabled": False,
                "nested": {"x": "中文"},
                "arr": ["cn"],
            },
            "tags": ["cn", "release-alpha"],
        },
        {
            "id": 5,
            "vec": [random.random() for _ in range(default_dim)],
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
            "vec": [random.random() for _ in range(default_dim)],
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
            "vec": [random.random() for _ in range(default_dim)],
            "text": "status OK ✅ deploy success 🚀",
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


def setup_regex_collection(client, collection_name, dim=default_dim, data=None):
    schema = gen_regex_test_schema(client, dim)
    client.create_collection(collection_name, schema=schema, consistency_level="Strong")
    if data is None:
        data = gen_regex_test_data()
    client.insert(collection_name=collection_name, data=data)
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="FLAT", metric_type="L2")
    client.create_index(collection_name, index_params)
    client.load_collection(collection_name)
    return collection_name


def drain_iterator(iterator):
    rows = []
    while True:
        batch = iterator.next()
        if not batch:
            break
        rows.extend(batch)
    iterator.close()
    return rows


def gen_struct_array_regex_schema(client, dim=default_dim):
    schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
    struct_schema = client.create_struct_field_schema()
    struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)
    struct_schema.add_field("name", DataType.VARCHAR, max_length=128)
    struct_schema.add_field("status", DataType.VARCHAR, max_length=64)
    struct_schema.add_field("code", DataType.INT64)
    schema.add_field(
        "events",
        datatype=DataType.ARRAY,
        element_type=DataType.STRUCT,
        struct_schema=struct_schema,
        max_capacity=4,
        nullable=True,
    )
    return schema


def setup_struct_array_regex_collection(client, collection_name, create_scalar_index=False):
    schema = gen_struct_array_regex_schema(client)
    client.create_collection(collection_name, schema=schema, consistency_level="Strong")
    data = [
        {
            "id": 1,
            "vec": [0.01] + [0.0] * (default_dim - 1),
            "events": [
                {
                    "embedding": [0.01] + [0.0] * (default_dim - 1),
                    "name": "login-error-timeout",
                    "status": "ERROR",
                    "code": 500,
                },
                {
                    "embedding": [0.02] + [0.0] * (default_dim - 1),
                    "name": "login-ok",
                    "status": "OK",
                    "code": 200,
                },
            ],
        },
        {
            "id": 2,
            "vec": [0.02] + [0.0] * (default_dim - 1),
            "events": [
                {
                    "embedding": [0.03] + [0.0] * (default_dim - 1),
                    "name": "checkout-warning",
                    "status": "WARN",
                    "code": 300,
                },
                {
                    "embedding": [0.04] + [0.0] * (default_dim - 1),
                    "name": "",
                    "status": "EMPTY",
                    "code": 0,
                },
            ],
        },
        {
            "id": 3,
            "vec": [0.03] + [0.0] * (default_dim - 1),
            "events": [
                {
                    "embedding": [0.05] + [0.0] * (default_dim - 1),
                    "name": "deploy-success",
                    "status": "OK",
                    "code": 200,
                }
            ],
        },
        {
            "id": 4,
            "vec": [0.04] + [0.0] * (default_dim - 1),
            "events": [],
        },
    ]
    client.insert(collection_name=collection_name, data=data)
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
    index_params.add_index(field_name="events[embedding]", index_type="HNSW", metric_type="MAX_SIM_L2")
    if create_scalar_index:
        index_params.add_index(field_name="events[name]", index_type="INVERTED")
    client.create_index(collection_name, index_params)
    client.load_collection(collection_name)
    return data


class RegexFilterSharedWideBase(TestMilvusClientV2Base):
    """Shared read-only regex dataset.

    Add tests here only when they are read-only and do not require custom schema/data,
    reload, or index lifecycle behavior.
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = self.__class__.__name__ + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_shared_wide_collection(self, request):
        client = self._client()

        def teardown():
            client.drop_collection(self.collection_name)

        request.addfinalizer(teardown)
        setup_regex_collection(client, self.collection_name)

    def _shared_collection(self):
        return self._client(), self.collection_name


class RegexFilterStructArraySharedBase(TestMilvusClientV2Base):
    """Shared read-only StructArray regex dataset.

    Add tests here only when they are read-only and do not require custom schema/data,
    reload, or index lifecycle behavior.
    """

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = self.__class__.__name__ + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_struct_array_collection(self, request):
        client = self._client()

        def teardown():
            client.drop_collection(self.collection_name)

        request.addfinalizer(teardown)
        setup_struct_array_regex_collection(client, self.collection_name, create_scalar_index=True)

    def _shared_collection(self):
        return self._client(), self.collection_name


@pytest.mark.xdist_group("TestRegexFilterBasicSemantic")
class TestRegexFilterBasicSemantic(RegexFilterSharedWideBase):
    @pytest.mark.tags(CaseLabel.L0)
    def test_regex_substring_match(self):
        """
        target: verify default regex behavior is substring matching
        expected: text =~ "timeout" returns id [1]; text =~ "ERROR" returns [1] not full-string
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "timeout"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f"expected [1], got {result}"

        res = client.query(collection_name, filter='text =~ "ERROR"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f"expected [1], got {result}"

        res = client.query(collection_name, filter='text =~ "^ERROR"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f"^ERROR: expected [1], got {result}"

        res = client.query(collection_name, filter='text =~ "hit$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [3], f"hit$: expected [3], got {result}"

        res = client.query(collection_name, filter='text =~ "^DEBUG cache hit$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [3], f"^DEBUG cache hit$: expected [3], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_classes_and_quantifiers(self):
        """
        target: verify RE2 character classes and quantifiers
        expected: E[0-9]{4}: -> [1], [0-9]{3}-[0-9]{4} -> [4]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter=r'text =~ "E[0-9]{4}:"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f"E[0-9]{{4}}: expected [1], got {result}"

        res = client.query(collection_name, filter=r'text =~ "[0-9]{3}-[0-9]{4}"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [4], f"[0-9]{{3}}-[0-9]{{4}}: expected [4], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_negation(self):
        """
        target: verify !~ is equivalent to NOT(field =~ pattern)
        expected: text !~ "^DEBUG" returns [1,2,4,5,6,7]
        """
        client, collection_name = self._shared_collection()

        expected = [1, 2, 4, 5, 6, 7]

        res = client.query(collection_name, filter='text !~ "^DEBUG"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == expected, f"!~ ^DEBUG: expected {expected}, got {result}"

        res = client.query(collection_name, filter='not (text =~ "^DEBUG")', output_fields=["id"])
        result_not = sorted([r["id"] for r in res])
        assert result_not == expected, f"not(=~): expected {expected}, got {result_not}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_case_insensitive(self):
        """
        target: verify inline flag (?i) for case-insensitive matching
        expected: email =~ "(?i)gmail\\.com$" returns [1,3,5,6]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter=r'email =~ "(?i)gmail\.com$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 3, 5, 6], f"expected [1,3,5,6], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_dot_matches_newline(self):
        """
        target: verify dot_nl=true (default) and (?-s) to disable
        expected: "c.d" -> [4,5]; "(?-s)c.d" -> [4]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "c.d"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [4, 5], f'"c.d": expected [4,5], got {result}'

        res = client.query(collection_name, filter='text =~ "(?-s)c.d"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [4], f'"(?-s)c.d": expected [4], got {result}'

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_unicode(self):
        """
        target: verify Unicode class and emoji literal matching
        expected: \\p{Han}+ -> [4], ✅ -> [7], 🚀 -> [7]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "\\p{Han}+"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [4], f"\\p{{Han}}+: expected [4], got {result}"

        res = client.query(collection_name, filter='text =~ "✅"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [7], f"✅: expected [7], got {result}"

        res = client.query(collection_name, filter='text =~ "🚀"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [7], f"🚀: expected [7], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_named_group(self):
        """
        target: verify RE2 (?P<name>...) named capture group does not affect matching.
                RE2/20230301 (used by Milvus) only supports (?P<name>...) syntax,
                not (?<name>...) which was added in later RE2 releases.
        expected: (?P<code>E[0-9]{4}) -> [1], (?P<level>ERROR|WARN) -> [1,2]
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter=r'text =~ "(?P<code>E[0-9]{4})"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1], f"(?P<code>E[0-9]{{4}}): expected [1], got {result}"

        res = client.query(
            collection_name,
            filter=r'text =~ "(?P<level>ERROR|WARN)"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2], f"(?P<level>ERROR|WARN): expected [1,2], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_control_characters(self):
        """
        target: verify regex matching on data containing \\t and \\n
        expected: \\t -> [201], \\n -> [202], ^line1 -> [201,202]
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        data = [
            {
                "id": 201,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "line1\ttabbed",
                "email": "a@test.com",
                "url": "/a",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 202,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "line1\nline2",
                "email": "b@test.com",
                "url": "/b",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 203,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "normal text",
                "email": "c@test.com",
                "url": "/c",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
        ]
        setup_regex_collection(client, collection_name, data=data)

        res = client.query(collection_name, filter='text =~ "\\t"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [201], f"\\t: expected [201], got {result}"

        res = client.query(collection_name, filter='text =~ "\\n"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [202], f"\\n: expected [202], got {result}"

        res = client.query(collection_name, filter='text =~ "^line1"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [201, 202], f"^line1: expected [201,202], got {result}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_empty_pattern(self):
        """
        target: verify empty pattern matches all non-NULL strings
        expected: text =~ "" -> [1,2,3,4,5,6,7]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ ""', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3, 4, 5, 6, 7], f"expected [1,2,3,4,5,6,7], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_no_match_returns_empty_list(self):
        """
        target: verify regex returns empty list when no match, not None or error
        expected: text =~ "NO_SUCH_PATTERN_98765" -> []
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter='text =~ "NO_SUCH_PATTERN_98765"',
            output_fields=["id"],
        )
        assert res == [], f"expected [], got {res}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_nested_quantifier_smoke(self):
        """
        target: verify nested quantifier (a+)+b works correctly under RE2
        expected: (a+)+b -> [1,3]
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        data = [
            {
                "id": 1,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "aaaaab",
                "email": "a@t.com",
                "url": "/a",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 2,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "aaaaaaaaaa",
                "email": "b@t.com",
                "url": "/b",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 3,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "xaaab",
                "email": "c@t.com",
                "url": "/c",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 4,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "ccccc",
                "email": "d@t.com",
                "url": "/d",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
        ]
        setup_regex_collection(client, collection_name, data=data)

        res = client.query(collection_name, filter='text =~ "(a+)+b"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 3], f"expected [1,3], got {result}"

        self.drop_collection(client, collection_name)


@pytest.mark.xdist_group("TestRegexFilterFieldAccess")
class TestRegexFilterFieldAccess(RegexFilterSharedWideBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_varchar_field(self):
        """
        target: verify regex on basic VarChar field (url)
        expected: url =~ "/api/v[0-9]+/users" -> [1,3,5]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='url =~ "/api/v[0-9]+/users"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 3, 5], f"expected [1,3,5], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_json_string_path(self):
        """
        target: verify regex on JSON string path
        expected: metadata["version"] =~ "^v[0-9]+\\.[0-9]+$" -> [1,2,3,5,7]
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter=r'metadata["version"] =~ "^v[0-9]+\.[0-9]+$"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3, 5, 7], f"expected [1,2,3,5,7], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_json_nested_path(self):
        """
        target: verify regex and !~ on nested JSON string paths
        expected: metadata["nested"]["x"] =~ "^abc$" -> [1], !~ "^abc$" -> [2,3,4,5,7]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='metadata["nested"]["x"] =~ "^abc$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f'nested =~ "^abc$": expected [1], got {result}'

        res = client.query(collection_name, filter='metadata["nested"]["x"] !~ "^abc$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2, 3, 4, 5, 7], f'nested !~ "^abc$": expected [2,3,4,5,7], got {result}'

        res = client.query(collection_name, filter='metadata["nested"]["missing"] =~ ".*"', output_fields=["id"])
        assert res == [], f"missing nested key: expected [], got {res}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_json_null_path(self):
        """
        target: verify JSON null value and empty string do not match regex
        expected: metadata["trace"] =~ "[a-z]+-[0-9]+" -> [1,2,3,5,7]
        id 4 trace is null (excluded), id 6 trace is "" (excluded)
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter=r'metadata["trace"] =~ "[a-z]+-[0-9]+"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3, 5, 7], f"expected [1,2,3,5,7], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_array_index_out_of_range(self):
        """
        target: verify array index out of range returns empty without error
        expected: tags[10] =~ ".*" -> []
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='tags[10] =~ ".*"', output_fields=["id"])
        assert res == [], f"expected [], got {[r['id'] for r in res]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_array_does_not_match_any_element_implicitly(self):
        """
        target: verify regex on array field without index raises error
        expected: tags =~ "prod" -> error "can not comparisons array fields directly"
        """
        client, collection_name = self._shared_collection()

        error = {
            ct.err_code: 1100,
            ct.err_msg: "can not comparisons array fields directly",
        }
        self.query(
            client,
            collection_name,
            filter='tags =~ "prod"',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_json_root_object(self):
        """
        target: verify regex on JSON root object returns empty without error.
                Design: JSON does not error on per-row type mismatch because other rows
                may have matching types for the same key.
        expected: metadata =~ "ERROR" -> []
                  metadata["level"] =~ "ERROR" -> [1,5]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='metadata =~ "ERROR"', output_fields=["id"])
        assert res == [], f'root object =~ "ERROR": expected [], got {res}'

        res = client.query(collection_name, filter='metadata["level"] =~ "ERROR"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 5], f'metadata["level"] =~ "ERROR": expected [1,5], got {result}'

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_array_varchar_element(self):
        """
        target: verify regex on individual ARRAY VARCHAR element by index
        expected: tags[0] =~ "^release-v[0-9]+" -> [1,2,5]
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter='tags[0] =~ "^release-v[0-9]+"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 5], f"expected [1,2,5], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_null_scalar_value(self):
        """
        target: verify NULL scalar values are excluded by both =~ and !~
        expected: email =~ "gmail" -> [1,5,6], email !~ "gmail" -> [2,3,7]
        id 4 has email=None, excluded by both =~ and !~
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='email =~ "gmail"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 5, 6], f'=~ "gmail": expected [1,5,6], got {result}'

        res = client.query(collection_name, filter='email !~ "gmail"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2, 3, 7], f'!~ "gmail": expected [2,3,7], got {result}'

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_null_negation_equivalence(self):
        """
        target: verify !~ and not(=~) behave identically on NULL values
        expected: email !~ "gmail" == not (email =~ "gmail") -> [2,3,7]
        """
        client, collection_name = self._shared_collection()

        res_a = client.query(collection_name, filter='email !~ "gmail"', output_fields=["id"])
        result_a = sorted([r["id"] for r in res_a])

        res_b = client.query(collection_name, filter='not (email =~ "gmail")', output_fields=["id"])
        result_b = sorted([r["id"] for r in res_b])

        assert result_a == result_b == [2, 3, 7], f"!~={result_a}, not(=~)={result_b}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_null_explicitly_included(self):
        """
        target: verify !~ or is null explicitly includes NULL rows
        expected: email !~ "gmail" or email is null -> [2,3,4,7]
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter='email !~ "gmail" or email is null',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [2, 3, 4, 7], f"expected [2,3,4,7], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_array_empty_element(self):
        """
        target: verify regex matches empty string in array element
        expected: tags[0] =~ "^$" -> [6]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='tags[0] =~ "^$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [6], f"expected [6], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_array_element_negation(self):
        """
        target: verify !~ on ARRAY<VARCHAR> element paths
        expected: tags[0] !~ "^release" -> [3,4,6,7], out-of-range !~ includes all rows
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='tags[0] !~ "^release"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [3, 4, 6, 7], f'tags[0] !~ "^release": expected [3,4,6,7], got {result}'

        res = client.query(collection_name, filter='tags[10] !~ ".*"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3, 4, 5, 6, 7], f"out-of-range !~: expected all rows, got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_dynamic_json_field_paths(self):
        """
        target: regex on dynamic JSON field paths is explicit and stable
        expected: supported dynamic paths match strings; non-string and missing paths return empty
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_dim)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")
        data = [
            {"id": 1, "vec": [0.1] * default_dim, "dyn_text": "ERROR dynamic timeout", "dyn_num": 10},
            {"id": 2, "vec": [0.2] * default_dim, "dyn_text": "INFO dynamic ok", "dyn_num": 20},
            {"id": 3, "vec": [0.3] * default_dim, "dyn_other": "missing dyn_text", "dyn_num": 30},
        ]
        client.insert(collection_name=collection_name, data=data)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        client.create_index(collection_name, index_params)
        client.load_collection(collection_name)

        res = client.query(collection_name, filter='$meta["dyn_text"] =~ "ERROR.*timeout"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f"dynamic dyn_text regex expected [1], got {result}"

        res = client.query(collection_name, filter='$meta["dyn_text"] !~ "ERROR"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2], f"dynamic dyn_text !~ ERROR expected [2], got {result}"

        res = client.query(collection_name, filter='$meta["dyn_num"] =~ "10"', output_fields=["id"])
        assert res == [], f"dynamic non-string regex expected [], got {res}"

        res = client.query(collection_name, filter='$meta["missing"] =~ ".*"', output_fields=["id"])
        assert res == [], f"dynamic missing regex expected [], got {res}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_invalid_pattern(self):
        """
        target: verify invalid regex pattern is rejected
        expected: text =~ "(unclosed" -> error "missing closing"
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "missing closing"}
        self.query(
            client,
            collection_name,
            filter='text =~ "(unclosed"',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_json_non_string_path(self):
        """
        target: verify JSON non-string values (int/bool/object/array) silently return empty
        expected: all return [] without error
        """
        client, collection_name = self._shared_collection()

        for expr, label in [
            ('metadata["version_num"] =~ "1"', "int"),
            ('metadata["enabled"] =~ "true"', "bool"),
            ('metadata["nested"] =~ "abc"', "object"),
            ('metadata["arr"] =~ "abc"', "array"),
        ]:
            res = client.query(collection_name, filter=expr, output_fields=["id"])
            assert res == [], f"{label} {expr}: expected [], got {[r['id'] for r in res]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_on_missing_json_path(self):
        """
        target: verify regex on missing JSON key returns empty without error
        expected: metadata["nonexistent_key"] =~ ".*" -> []
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter='metadata["nonexistent_key"] =~ ".*"',
            output_fields=["id"],
        )
        assert res == [], f"expected [], got {[r['id'] for r in res]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_unsupported_backreference(self):
        """
        target: verify RE2 backreference is rejected
        expected: text =~ "(a)\\1" -> error "invalid regex pattern"
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "invalid regex pattern"}
        self.query(
            client,
            collection_name,
            filter=r'text =~ "(a)\\1"',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_unsupported_lookahead(self):
        """
        target: verify RE2 lookahead is rejected
        expected: text =~ "ERROR(?= E1001)" -> error "invalid regex pattern"
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "invalid regex pattern"}
        self.query(
            client,
            collection_name,
            filter='text =~ "ERROR(?= E1001)"',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_on_numeric_field(self):
        """
        target: verify regex on numeric field is rejected
        expected: count =~ "42" -> error "regex match on non-string or non-json field"
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("count", DataType.INT64)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")
        client.insert(collection_name, [{"id": 1, "vec": [0.1] * default_dim, "count": 42}])
        ip = client.prepare_index_params()
        ip.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        client.create_index(collection_name, ip)
        client.load_collection(collection_name)

        error = {
            ct.err_code: 1100,
            ct.err_msg: "regex match on non-string or non-json field",
        }
        self.query(
            client,
            collection_name,
            filter='count =~ "42"',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_right_operand_must_be_string_literal(self):
        """
        target: verify =~ right operand must be string literal
        expected: text =~ 123 -> error about regex pattern requiring string literal or template variable
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "string literal or template variable"}
        self.query(
            client,
            collection_name,
            filter="text =~ 123",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_escaped_quotes_and_backslash(self):
        """
        target: verify regex can match escaped quotes and backslash in pattern
        expected: text =~ "a\"b" -> [301], text =~ "a\\\\b" -> [302]
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        data = [
            {
                "id": 301,
                "vec": [0.1] * default_dim,
                "text": 'a"b',
                "email": "a@test.com",
                "url": "/a",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 302,
                "vec": [0.1] * default_dim,
                "text": "a\\b",
                "email": "b@test.com",
                "url": "/b",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
            {
                "id": 303,
                "vec": [0.1] * default_dim,
                "text": "normal",
                "email": "c@test.com",
                "url": "/c",
                "level": "INFO",
                "metadata": {},
                "tags": [],
            },
        ]
        setup_regex_collection(client, collection_name, data=data)

        # Match literal double quote: filter parser supports \" escape
        res = client.query(collection_name, filter=r'text =~ "a\"b"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [301], f"escaped quote: expected [301], got {result}"

        # Match literal backslash: need \\\\ in filter to produce \\ in RE2
        res = client.query(collection_name, filter=r'text =~ "a\\\\b"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [302], f"escaped backslash: expected [302], got {result}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_empty_field_value(self):
        """
        target: verify empty string field matches ^$ and is matched by .*
        expected: text =~ "^$" -> [6], text !~ ".*" -> [1,2,3,4,5,7]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "^$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [6], f'=~ "^$": expected [6], got {result}'

        res = client.query(collection_name, filter='text !~ ".*"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [], f'!~ ".*": expected [], got {result}'

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_invalid_escape(self):
        """
        target: verify invalid escape sequences are rejected
        expected: \\k, \\x, \\p{UnknownClass} -> error "invalid"
        """
        client, collection_name = self._shared_collection()

        for pattern, label in [
            (r"\k", "backslash-k"),
            (r"\x", "backslash-x"),
            (r"\p{UnknownClass}", "unknown-unicode-class"),
        ]:
            error = {ct.err_code: 1100, ct.err_msg: "invalid"}
            self.query(
                client,
                collection_name,
                filter=f'text =~ "{pattern}"',
                check_task=CheckTasks.err_res,
                check_items=error,
            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_template_param_invalid_regex_negation(self):
        """
        target: verify invalid regex template parameter is rejected for !~
        expected: text !~ {pattern} raises regex/parse related error
        """
        client, collection_name = self._shared_collection()

        with pytest.raises(Exception) as exc_info:
            client.query(
                collection_name,
                filter="text !~ {pattern}",
                filter_params={"pattern": "["},
                output_fields=["id"],
            )
        err = str(exc_info.value).lower()
        assert any(keyword in err for keyword in ["regex", "invalid", "parse", "missing"]), (
            f"expected invalid regex error for !~ template parameter, got: {err}"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_unknown_field(self):
        """
        target: verify regex on unknown field raises error
        expected: unknown_field =~ "test" -> error "field unknown_field not exist"
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "field unknown_field not exist"}
        self.query(
            client,
            collection_name,
            filter='unknown_field =~ "test"',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_right_operand_cannot_be_field(self):
        """
        target: verify right operand cannot be a field reference
        expected: text =~ email -> error about regex pattern requiring string literal or template variable
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "string literal or template variable"}
        self.query(
            client,
            collection_name,
            filter="text =~ email",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_right_operand_unsupported_literal_types(self):
        """
        target: verify right operand cannot be bool/array
        expected: text =~ true -> error, text =~ [1,2] -> error about regex pattern requiring string literal or template variable
        """
        client, collection_name = self._shared_collection()

        error = {ct.err_code: 1100, ct.err_msg: "string literal or template variable"}
        self.query(
            client,
            collection_name,
            filter="text =~ true",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        self.query(
            client,
            collection_name,
            filter="text =~ [1,2]",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_long_pattern_and_large_alternation(self):
        """
        target: verify long pattern and large alternation do not crash
        expected: both return [] without error
        """
        client, collection_name = self._shared_collection()

        long_pattern = "a" * 10000
        res = client.query(collection_name, filter=f'text =~ "{long_pattern}"', output_fields=["id"])
        assert res == [], f"long pattern: expected [], got {[r['id'] for r in res]}"

        alt_pattern = "|".join([f"alt_{i}" for i in range(1000)])
        res = client.query(collection_name, filter=f'text =~ "{alt_pattern}"', output_fields=["id"])
        assert res == [], f"large alternation: expected [], got {[r['id'] for r in res]}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_not_precedence(self):
        """
        target: verify operator precedence: not, !~, and, or, parentheses
        expected:
          not text =~ "ERROR" and id > 3  -> [4,5,6,7] (not binds regex, then and)
          text !~ "ERROR" and id > 3      -> [4,5,6,7] (control, same as above)
          text =~ "ERROR" or text =~ "WARN" and id > 3  -> [1] (and binds tighter than or)
          (text =~ "ERROR" or text =~ "WARN") and id > 3 -> [] (parens force or first)
          not text =~ "ERROR" or text =~ "WARN"  -> [2,3,4,5,6,7] (not binds regex)
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter='not text =~ "ERROR" and id > 3',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [4, 5, 6, 7], f'not text =~ "ERROR" and id > 3: expected [4,5,6,7], got {result}'

        res = client.query(collection_name, filter='text !~ "ERROR" and id > 3', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [4, 5, 6, 7], f'text !~ "ERROR" and id > 3: expected [4,5,6,7], got {result}'

        res = client.query(
            collection_name,
            filter='text =~ "ERROR" or text =~ "WARN" and id > 3',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1], f'text =~ "ERROR" or text =~ "WARN" and id > 3: expected [1], got {result}'

        res = client.query(
            collection_name,
            filter='(text =~ "ERROR" or text =~ "WARN") and id > 3',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [], f'(text =~ "ERROR" or text =~ "WARN") and id > 3: expected [], got {result}'

        res = client.query(
            collection_name,
            filter='not text =~ "ERROR" or text =~ "WARN"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [2, 3, 4, 5, 6, 7], (
            f'not text =~ "ERROR" or text =~ "WARN": expected [2,3,4,5,6,7], got {result}'
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_delete_expression_smoke(self):
        """
        target: verify delete supports regex filter expression
        expected: delete text =~ "^DEBUG" removes id 3, remaining [1,2,4,5,6,7]
                  if delete does not support regex, expect a clear error
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self.delete(client, collection_name, filter='text =~ "^DEBUG"')

        res = client.query(collection_name, filter="id > 0", output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 4, 5, 6, 7], f"after delete: expected [1,2,4,5,6,7], got {result}"

        self.drop_collection(client, collection_name)


@pytest.mark.xdist_group("TestRegexFilterOptimization")
class TestRegexFilterOptimization(RegexFilterSharedWideBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_optimize_pure_literal_as_inner_match(self):
        """
        target: pure literal pattern optimized to inner match, same as like "%...%"
        expected: text =~ "ERROR" -> [1], text like "%ERROR%" -> [1]
        """
        client, collection_name = self._shared_collection()

        res_regex = client.query(collection_name, filter='text =~ "ERROR"', output_fields=["id"])
        result_regex = sorted([r["id"] for r in res_regex])

        res_like = client.query(collection_name, filter='text like "%ERROR%"', output_fields=["id"])
        result_like = sorted([r["id"] for r in res_like])

        assert result_regex == [1], f"regex: expected [1], got {result_regex}"
        assert result_regex == result_like, f"regex {result_regex} != like {result_like}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_optimize_prefix(self):
        """
        target: ^ERROR optimized to prefix match, same as like "ERROR%"
        expected: text =~ "^ERROR" -> [1], text like "ERROR%" -> [1]
        """
        client, collection_name = self._shared_collection()

        res_regex = client.query(collection_name, filter='text =~ "^ERROR"', output_fields=["id"])
        result_regex = sorted([r["id"] for r in res_regex])

        res_like = client.query(collection_name, filter='text like "ERROR%"', output_fields=["id"])
        result_like = sorted([r["id"] for r in res_like])

        assert result_regex == [1], f"regex: expected [1], got {result_regex}"
        assert result_regex == result_like, f"regex {result_regex} != like {result_like}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_optimize_postfix(self):
        """
        target: hit$ optimized to postfix match, same as like "%hit"
        expected: text =~ "hit$" -> [3], text like "%hit" -> [3]
        """
        client, collection_name = self._shared_collection()

        res_regex = client.query(collection_name, filter='text =~ "hit$"', output_fields=["id"])
        result_regex = sorted([r["id"] for r in res_regex])

        res_like = client.query(collection_name, filter='text like "%hit"', output_fields=["id"])
        result_like = sorted([r["id"] for r in res_like])

        assert result_regex == [3], f"regex: expected [3], got {result_regex}"
        assert result_regex == result_like, f"regex {result_regex} != like {result_like}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_optimize_full_equal(self):
        """
        target: ^...$ optimized to full equal, does not match substring
        expected: text =~ "^DEBUG cache hit$" -> [3], text == "DEBUG cache hit" -> [3]
                  "DEBUG cache hitttt" is NOT matched by ^...$
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        extra_data = [
            {
                "id": 401,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "DEBUG cache hitttt",
                "email": "sub@dev.io",
                "url": "/test",
                "level": "DEBUG",
                "metadata": {
                    "level": "DEBUG",
                    "version": "v1",
                    "trace": "sub",
                    "version_num": 1,
                    "enabled": True,
                    "nested": {"x": "sub"},
                    "arr": ["sub"],
                },
                "tags": ["test"],
            },
            {
                "id": 402,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "xDEBUG cache hit",
                "email": "pre@dev.io",
                "url": "/test2",
                "level": "DEBUG",
                "metadata": {
                    "level": "DEBUG",
                    "version": "v1",
                    "trace": "pre",
                    "version_num": 1,
                    "enabled": True,
                    "nested": {"x": "pre"},
                    "arr": ["pre"],
                },
                "tags": ["test"],
            },
        ]
        setup_regex_collection(client, collection_name, data=gen_regex_test_data() + extra_data)

        res_regex = client.query(collection_name, filter='text =~ "^DEBUG cache hit$"', output_fields=["id"])
        result_regex = sorted([r["id"] for r in res_regex])

        res_eq = client.query(collection_name, filter='text == "DEBUG cache hit"', output_fields=["id"])
        result_eq = sorted([r["id"] for r in res_eq])

        assert result_regex == [3], f"regex: expected [3], got {result_regex}"
        assert result_regex == result_eq, f"regex {result_regex} != == {result_eq}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_metacharacters_not_optimized_incorrectly(self):
        """
        target: metacharacters like []{} are not treated as literal
        expected: text =~ "E[0-9]{4}" -> [1] (matches E1001), not [] (literal)
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "E[0-9]{4}"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1], f"E[0-9]{{4}}: expected [1], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_filter_like_relationship(self):
        """
        target: LIKE semantics unaffected, regex vs LIKE difference is clear
        expected: text like "ERROR" -> [] (no wildcard, full-string only)
                  text like "%ERROR%" -> [1] (with wildcards, substring)
                  text =~ "ERROR" -> [1] (regex substring)
        """
        client, collection_name = self._shared_collection()

        res_like_full = client.query(collection_name, filter='text like "ERROR"', output_fields=["id"])
        result_like_full = sorted([r["id"] for r in res_like_full])
        assert result_like_full == [], f'like "ERROR": expected [], got {result_like_full}'

        res_like_sub = client.query(collection_name, filter='text like "%ERROR%"', output_fields=["id"])
        result_like_sub = sorted([r["id"] for r in res_like_sub])

        res_regex = client.query(collection_name, filter='text =~ "ERROR"', output_fields=["id"])
        result_regex = sorted([r["id"] for r in res_regex])

        assert result_like_sub == [1], f'like "%%ERROR%%": expected [1], got {result_like_sub}'
        assert result_regex == [1], f"regex: expected [1], got {result_regex}"
        assert result_regex == result_like_sub, f"regex {result_regex} != like {result_like_sub}"


class TestRegexFilterIndexPath(TestMilvusClientV2Base):
    TEXT_PATTERN_SUITE = [
        ('text =~ "ERROR"', [1]),
        ('text =~ "^WARN"', [2]),
        ('text =~ "hit$"', [3]),
        ('text =~ "E[0-9]{4}:"', [1]),
        ('text =~ "\\p{Han}+"', [4]),
        ('text =~ "c.d"', [4, 5]),
        ('text =~ "(?-s)c.d"', [4]),
        ('text =~ ""', [1, 2, 3, 4, 5, 6, 7]),
        ('text =~ "^$"', [6]),
        ('text =~ "(?i)error"', [1, 4]),
        ('text =~ "✅"', [7]),
        ('text =~ "🚀"', [7]),
        ('text =~ "ERROR.*timeout"', [1]),
        ('text =~ "[a-z]+"', [1, 2, 3, 4, 5, 7]),
        ('text =~ "E.*1.*:"', [1]),
    ]

    def _run_pattern_suite(self, client, collection_name):
        for filter_expr, expected in self.TEXT_PATTERN_SUITE:
            res = client.query(collection_name, filter=filter_expr, output_fields=["id"])
            result = sorted([r["id"] for r in res])
            assert result == expected, f"{filter_expr}: expected {expected}, got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_no_index_baseline(self):
        """
        target: raw data path baseline, no scalar index on text
        expected: all patterns return correct id sets
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self._run_pattern_suite(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_with_ngram_index(self):
        """
        target: ngram index two-phase path has no false negative
        expected: all simple and complex regex patterns match the no-index baseline
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self.release_collection(client, collection_name)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="text", index_type="NGRAM", params={"min_gram": 2, "max_gram": 4})
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        self._run_pattern_suite(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_with_inverted_index(self):
        """
        target: inverted index regex/fallback path matches the no-index baseline
        expected: all patterns, especially c.d and (?-s)c.d, return correct id sets
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self.release_collection(client, collection_name)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="text", index_type="INVERTED")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        self._run_pattern_suite(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_with_sort_index(self):
        """
        target: string STL_SORT index unique-value iteration path matches the no-index baseline
        expected: literal, prefix, postfix, real regex, and negation return correct id sets
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self.release_collection(client, collection_name)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="text", index_type="STL_SORT")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        self._run_pattern_suite(client, collection_name)
        res = client.query(collection_name, filter='text !~ "ERROR"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2, 3, 4, 5, 6, 7], f'text !~ "ERROR": expected [2,3,4,5,6,7], got {result}'

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_with_bitmap_index(self):
        """
        target: bitmap index on low-cardinality VARCHAR field matches the no-index baseline
        expected: level =~ "ERR|WARN" -> [1,2,5], level !~ "DEBUG" -> [1,2,4,5,6,7]
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self.release_collection(client, collection_name)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="level", index_type="BITMAP")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        res = client.query(collection_name, filter='level =~ "ERR|WARN"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 5], f'level =~ "ERR|WARN": expected [1,2,5], got {result}'

        res = client.query(collection_name, filter='level !~ "DEBUG"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 4, 5, 6, 7], f'level !~ "DEBUG": expected [1,2,4,5,6,7], got {result}'

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_with_marisa_index(self):
        """
        target: trie/marisa string index path works for regex filters on url
        expected: url =~ "^/api/v[0-9]+/users" -> [1,3,5], url =~ "search$" -> [5]
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_regex_collection(client, collection_name)

        self.release_collection(client, collection_name)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="url", index_type="Trie")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        res = client.query(collection_name, filter='url =~ "^/api/v[0-9]+/users"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 3, 5], f'url =~ "^/api/v[0-9]+/users": expected [1,3,5], got {result}'

        res = client.query(collection_name, filter='url =~ "search$"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [5], f'url =~ "search$": expected [5], got {result}'

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_index_after_flush_and_reload(self):
        """
        target: sealed segment with scalar index remains stable after release/load
        expected: pattern suite matches baseline before and after reload
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = gen_regex_test_schema(client)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")
        client.insert(collection_name=collection_name, data=gen_regex_test_data())
        self.flush(client, collection_name)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        index_params.add_index(field_name="text", index_type="INVERTED")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        self._run_pattern_suite(client, collection_name)

        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        self._run_pattern_suite(client, collection_name)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_scalar_index_paths_with_sealed_data(self):
        """
        target: scalar index regex paths match expected ids on sealed data with non-trivial value distribution
        expected: INVERTED, NGRAM, STL_SORT, Trie, and BITMAP return expected ids without false negatives
        """
        client = self._client()
        rows = []
        expected_error_timeout = []
        expected_level = []
        expected_url = []
        for i in range(5000):
            is_error_timeout = i % 20 == 0
            level = "ERROR" if i % 3 == 0 else "INFO"
            text = f"ERROR sealed row {i} timeout" if is_error_timeout else f"INFO sealed row {i} ok"
            url = f"/api/v{i % 10}/users/{i}" if i % 4 == 0 else f"/static/{i}"
            if is_error_timeout:
                expected_error_timeout.append(i)
            if level == "ERROR":
                expected_level.append(i)
            if url.startswith("/api/v"):
                expected_url.append(i)
            rows.append(
                {
                    "id": i,
                    "vec": [float(i) / 1000] + [0.0] * (default_dim - 1),
                    "text_inverted": text,
                    "text_ngram": text,
                    "text_sort": text,
                    "email": f"sealed-{i}@example.com",
                    "url": url,
                    "level": level,
                    "metadata": {"level": level, "version": f"v{i % 10}.0", "trace": f"sealed-{i}"},
                    "tags": [level.lower()],
                }
            )

        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("text_inverted", DataType.VARCHAR, max_length=512)
        schema.add_field("text_ngram", DataType.VARCHAR, max_length=512)
        schema.add_field("text_sort", DataType.VARCHAR, max_length=512)
        schema.add_field("email", DataType.VARCHAR, max_length=256)
        schema.add_field("url", DataType.VARCHAR, max_length=512)
        schema.add_field("level", DataType.VARCHAR, max_length=32)
        schema.add_field("metadata", DataType.JSON)
        schema.add_field("tags", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=8, max_length=128)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")
        client.insert(collection_name=collection_name, data=rows)
        self.flush(client, collection_name)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        index_params.add_index(field_name="text_inverted", index_type="INVERTED")
        index_params.add_index(field_name="text_ngram", index_type="NGRAM", params={"min_gram": 2, "max_gram": 4})
        index_params.add_index(field_name="text_sort", index_type="STL_SORT")
        index_params.add_index(field_name="url", index_type="Trie")
        index_params.add_index(field_name="level", index_type="BITMAP")
        client.create_index(collection_name, index_params)
        client.load_collection(collection_name)

        for label, filter_expr, expected in [
            ("INVERTED", 'text_inverted =~ "ERROR.*timeout"', expected_error_timeout),
            ("NGRAM", 'text_ngram =~ "ERROR.*timeout"', expected_error_timeout),
            ("STL_SORT", 'text_sort =~ "ERROR.*timeout"', expected_error_timeout),
            ("Trie", 'url =~ "^/api/v[0-9]+/users"', expected_url),
            ("BITMAP", 'level =~ "^ERROR$"', expected_level),
        ]:
            res = client.query(collection_name, filter=filter_expr, output_fields=["id"], limit=len(expected) + 10)
            result = sorted([r["id"] for r in res])
            assert result == expected, f"{label} {filter_expr}: expected {expected}, got {result}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_growing_and_sealed_segments(self):
        """
        target: regex and negated regex cover both sealed and growing segments
        expected: =~ returns [1,2,101,102], !~ returns [3,4,5,6,7,103] with no duplicates or misses
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = gen_regex_test_schema(client)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")

        client.insert(collection_name=collection_name, data=gen_regex_test_data())
        self.flush(client, collection_name)

        extra_data = [
            {
                "id": 101,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "ERROR E9001: growing segment",
                "email": "grow-error@dev.io",
                "url": "/api/v9/users/101",
                "level": "ERROR",
                "metadata": {
                    "level": "ERROR",
                    "version": "v9",
                    "trace": "grow-101",
                    "version_num": 9,
                    "enabled": True,
                    "nested": {"x": "grow"},
                    "arr": ["grow"],
                },
                "tags": ["growing"],
            },
            {
                "id": 102,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "WARN W9002: growing segment",
                "email": "grow-warn@dev.io",
                "url": "/api/v9/users/102",
                "level": "WARN",
                "metadata": {
                    "level": "WARN",
                    "version": "v9",
                    "trace": "grow-102",
                    "version_num": 9,
                    "enabled": False,
                    "nested": {"x": "grow"},
                    "arr": ["grow"],
                },
                "tags": ["growing"],
            },
            {
                "id": 103,
                "vec": [random.random() for _ in range(default_dim)],
                "text": "INFO I9003: growing segment",
                "email": "grow-info@dev.io",
                "url": "/api/v9/users/103",
                "level": "INFO",
                "metadata": {
                    "level": "INFO",
                    "version": "v9",
                    "trace": "grow-103",
                    "version_num": 9,
                    "enabled": True,
                    "nested": {"x": "grow"},
                    "arr": ["grow"],
                },
                "tags": ["growing"],
            },
        ]
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        index_params.add_index(field_name="text", index_type="INVERTED")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        client.insert(collection_name=collection_name, data=extra_data)

        res = client.query(collection_name, filter='text =~ "ERROR|WARN"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 101, 102], f'text =~ "ERROR|WARN": expected [1,2,101,102], got {result}'

        res = client.query(collection_name, filter='text !~ "ERROR|WARN"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [3, 4, 5, 6, 7, 103], f'text !~ "ERROR|WARN": expected [3,4,5,6,7,103], got {result}'

        self.drop_collection(client, collection_name)


@pytest.mark.xdist_group("TestRegexFilterQuerySearch")
class TestRegexFilterQuerySearch(RegexFilterSharedWideBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_query_output_fields(self):
        """
        target: query with regex filter returns requested scalar output fields
        expected: text =~ "timeout" returns id 1 with text and email fields
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter='text =~ "timeout"',
            output_fields=["id", "text", "email"],
        )
        assert len(res) == 1, f"expected 1 row, got {len(res)}: {res}"
        assert res[0]["id"] == 1, f"expected id 1, got {res[0]['id']}"
        assert res[0]["text"] == "ERROR E1001: connection timeout", f"unexpected text: {res[0]['text']}"
        assert res[0]["email"] == "alice@gmail.com", f"unexpected email: {res[0]['email']}"

    @pytest.mark.tags(CaseLabel.L0)
    def test_regex_vector_search(self):
        """
        target: vector search supports regex filter and excludes non-matching rows
        expected: unfiltered search can return non-url-matching ids, filtered search only returns ids in [1,3,5]
        """
        client, collection_name = self._shared_collection()
        data = gen_regex_test_data()

        query_vector = data[0]["vec"]
        unfiltered = client.search(
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            limit=7,
            output_fields=["id"],
        )
        unfiltered_ids = [hit["id"] for hit in unfiltered[0]]
        assert set(unfiltered_ids) == {1, 2, 3, 4, 5, 6, 7}, f"unfiltered expected all ids, got {unfiltered_ids}"
        assert any(i not in {1, 3, 5} for i in unfiltered_ids), (
            f"unfiltered result did not include non-matching ids: {unfiltered_ids}"
        )

        filtered = client.search(
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            filter='url =~ "^/api/v[0-9]+/users"',
            limit=3,
            output_fields=["id"],
        )
        filtered_ids = [hit["id"] for hit in filtered[0]]
        assert set(filtered_ids) == {1, 3, 5}, f"filtered expected ids [1,3,5], got {filtered_ids}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_search_nullable_and_json_paths(self):
        """
        target: search supports regex filters on nullable fields and JSON string paths
        expected: =~/!~/or is null all return only rows satisfying the predicate
        """
        client, collection_name = self._shared_collection()
        data = gen_regex_test_data()
        query_vector = data[0]["vec"]

        for filter_expr, expected in [
            ('email =~ "gmail"', {1, 5, 6}),
            ('email !~ "gmail"', {2, 3, 7}),
            ('email !~ "gmail" or email is null', {2, 3, 4, 7}),
            ('metadata["version"] =~ "^v[0-9]+\\.[0-9]+$"', {1, 2, 3, 5, 7}),
            ('metadata["nested"]["x"] !~ "^abc$"', {2, 3, 4, 5, 7}),
        ]:
            res = client.search(
                collection_name=collection_name,
                data=[query_vector],
                anns_field="vec",
                search_params={"metric_type": "L2", "params": {"ef": 64}},
                filter=filter_expr,
                limit=len(expected),
                output_fields=["id", "email", "metadata"],
            )
            result = {hit["id"] for hit in res[0]}
            assert result == expected, f"{filter_expr}: expected {expected}, got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_hybrid_boolean_and(self):
        """
        target: regex filter combines correctly with scalar AND expression
        expected: text =~ "ERROR" and id > 1 -> [], text =~ "(?i)error" and id > 1 -> [4]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "ERROR" and id > 1', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [], f'text =~ "ERROR" and id > 1: expected [], got {result}'

        res = client.query(
            collection_name,
            filter='text =~ "(?i)error" and id > 1',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [4], f'text =~ "(?i)error" and id > 1: expected [4], got {result}'

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_hybrid_boolean_or(self):
        """
        target: regex filters combine correctly with OR expression
        expected: ^WARN -> [2], ^DEBUG -> [3], OR combines them to [2,3]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='text =~ "^WARN"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2], f'text =~ "^WARN": expected [2], got {result}'

        res = client.query(collection_name, filter='text =~ "^DEBUG"', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [3], f'text =~ "^DEBUG": expected [3], got {result}'

        res = client.query(
            collection_name,
            filter='text =~ "^WARN" or text =~ "^DEBUG"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [2, 3], f'text =~ "^WARN" or text =~ "^DEBUG": expected [2,3], got {result}'

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_with_limit_offset(self):
        """
        target: regex filter works with query order_by, limit, and offset
        expected: url =~ "^/api" matches [1,2,3,5,7]; order by id asc + limit 2 offset 1 returns [2,3]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='url =~ "^/api"', output_fields=["id"])
        all_result = sorted([r["id"] for r in res])
        assert all_result == [1, 2, 3, 5, 7], f'url =~ "^/api": expected [1,2,3,5,7], got {all_result}'

        res = client.query(
            collection_name,
            filter='url =~ "^/api"',
            output_fields=["id"],
            order_by_fields=[{"field": "id", "order": "asc"}],
            limit=2,
            offset=1,
        )
        result = [r["id"] for r in res]
        assert result == [2, 3], f"order by id asc limit 2 offset 1: expected [2,3], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_hybrid_in_expression(self):
        """
        target: regex filter combines correctly with IN expression
        expected: IN returns [1,2,5], regex returns [1,2], AND returns their intersection [1,2]
        """
        client, collection_name = self._shared_collection()

        res = client.query(collection_name, filter='level in ["ERROR", "WARN"]', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 5], f'level in ["ERROR", "WARN"]: expected [1,2,5], got {result}'

        res = client.query(
            collection_name,
            filter='text =~ "E[0-9]{4}|W[0-9]{4}"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2], f'text =~ "E[0-9]{{4}}|W[0-9]{{4}}": expected [1,2], got {result}'

        res = client.query(
            collection_name,
            filter='level in ["ERROR", "WARN"] and text =~ "E[0-9]{4}|W[0-9]{4}"',
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2], f"IN and regex: expected [1,2], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_milvus_hybrid_search_filter(self):
        """
        target: Milvus hybrid_search supports different expr filters in different sub-searches
        expected: regex expr sub-search contributes api ids [1,3,5]; scalar expr sub-search contributes block ids [2,4]
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 4
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("vec2", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("text", DataType.VARCHAR, max_length=512)
        schema.add_field("url", DataType.VARCHAR, max_length=512)
        schema.add_field("level", DataType.VARCHAR, max_length=32)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")

        data = [
            {
                "id": 1,
                "vec": [0.01, 0.0, 0.0, 0.0],
                "vec2": [10.0, 10.0, 10.0, 10.0],
                "text": "api user one",
                "url": "/api/v1/users/1",
                "level": "ALLOW",
            },
            {
                "id": 2,
                "vec": [10.0, 10.0, 10.0, 10.0],
                "vec2": [0.0, 0.0, 0.0, 0.0],
                "text": "blocked two",
                "url": "/blocked/internal/2",
                "level": "BLOCK",
            },
            {
                "id": 3,
                "vec": [0.02, 0.0, 0.0, 0.0],
                "vec2": [10.0, 10.0, 10.0, 10.0],
                "text": "api user three",
                "url": "/api/v2/users/3",
                "level": "ALLOW",
            },
            {
                "id": 4,
                "vec": [10.0, 10.0, 10.0, 10.0],
                "vec2": [0.005, 0.0, 0.0, 0.0],
                "text": "blocked four",
                "url": "/blocked/internal/4",
                "level": "BLOCK",
            },
            {
                "id": 5,
                "vec": [0.03, 0.0, 0.0, 0.0],
                "vec2": [10.0, 10.0, 10.0, 10.0],
                "text": "api user five",
                "url": "/api/v3/users/5",
                "level": "ALLOW",
            },
            {
                "id": 6,
                "vec": [20.0, 20.0, 20.0, 20.0],
                "vec2": [20.0, 20.0, 20.0, 20.0],
                "text": "static six",
                "url": "/static/index.html",
                "level": "OTHER",
            },
        ]
        client.insert(collection_name=collection_name, data=data)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        index_params.add_index(field_name="vec2", index_type="HNSW", metric_type="L2")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)

        query_vector = [[0.0, 0.0, 0.0, 0.0]]
        req_vec = AnnSearchRequest(query_vector, "vec", {"metric_type": "L2", "params": {"ef": 64}}, 6)
        req_vec2 = AnnSearchRequest(query_vector, "vec2", {"metric_type": "L2", "params": {"ef": 64}}, 6)
        ranker = WeightedRanker(0.5, 0.5)

        unfiltered = client.hybrid_search(
            collection_name,
            [req_vec, req_vec2],
            ranker,
            limit=6,
            output_fields=["id", "url", "level"],
        )
        unfiltered_ids = [hit["id"] for hit in unfiltered[0]]
        assert set(unfiltered_ids) == {1, 2, 3, 4, 5, 6}, f"unfiltered expected all ids, got {unfiltered_ids}"

        regex_req = AnnSearchRequest(
            query_vector,
            "vec",
            {"metric_type": "L2", "params": {"ef": 64}},
            3,
            expr='url =~ "^/api/v[0-9]+/users"',
        )
        block_req = AnnSearchRequest(
            query_vector,
            "vec2",
            {"metric_type": "L2", "params": {"ef": 64}},
            2,
            expr='level == "BLOCK"',
        )
        filtered = client.hybrid_search(
            collection_name,
            [regex_req, block_req],
            ranker,
            limit=5,
            output_fields=["id", "url", "level"],
        )
        filtered_ids = [hit["id"] for hit in filtered[0]]
        assert set(filtered_ids) == {1, 2, 3, 4, 5}, (
            f"different expr hybrid_search expected [1,2,3,4,5], got {filtered_ids}"
        )
        assert {1, 3, 5}.issubset(set(filtered_ids)), f"regex expr ids missing from result: {filtered_ids}"
        assert {2, 4}.issubset(set(filtered_ids)), f"scalar expr ids missing from result: {filtered_ids}"
        assert 6 not in filtered_ids, f"id 6 matches neither expr but appeared in result: {filtered_ids}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_hybrid_search_filter_variants(self):
        """
        target: hybrid_search supports single/both sub-search regex filters, !~, nullable, JSON path, and template params
        expected: filtered hybrid_search results are constrained by each AnnSearchRequest expr
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 4
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("vec2", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("text", DataType.VARCHAR, max_length=128)
        schema.add_field("email", DataType.VARCHAR, max_length=128, nullable=True)
        schema.add_field("metadata", DataType.JSON)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")
        data = [
            {
                "id": 1,
                "vec": [0.01, 0.0, 0.0, 0.0],
                "vec2": [10.0, 10.0, 10.0, 10.0],
                "text": "ERROR alpha timeout",
                "email": "a@gmail.com",
                "metadata": {"trace": "api-001", "nested": {"x": "alpha"}},
            },
            {
                "id": 2,
                "vec": [10.0, 10.0, 10.0, 10.0],
                "vec2": [0.01, 0.0, 0.0, 0.0],
                "text": "WARN beta retry",
                "email": "b@gmail.com",
                "metadata": {"trace": "api-002", "nested": {"x": "beta"}},
            },
            {
                "id": 3,
                "vec": [0.02, 0.0, 0.0, 0.0],
                "vec2": [10.0, 10.0, 10.0, 10.0],
                "text": "INFO gamma ok",
                "email": "c@example.com",
                "metadata": {"trace": "ops-003", "nested": {"x": "gamma"}},
            },
            {
                "id": 4,
                "vec": [10.0, 10.0, 10.0, 10.0],
                "vec2": [0.02, 0.0, 0.0, 0.0],
                "text": "ERROR delta fail",
                "email": "d@gmail.com",
                "metadata": {"trace": "ops-004", "nested": {"x": "delta"}},
            },
            {
                "id": 5,
                "vec": [10.0, 10.0, 10.0, 10.0],
                "vec2": [0.03, 0.0, 0.0, 0.0],
                "text": "INFO nullable email",
                "email": None,
                "metadata": {"trace": "ops-005", "nested": {"x": "nullable"}},
            },
        ]
        client.insert(collection_name=collection_name, data=data)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        index_params.add_index(field_name="vec2", index_type="HNSW", metric_type="L2")
        client.create_index(collection_name, index_params)
        self.load_collection(client, collection_name)
        query_vector = [[0.0, 0.0, 0.0, 0.0]]
        ranker = WeightedRanker(0.5, 0.5)

        regex_req = AnnSearchRequest(
            query_vector,
            "vec",
            {"metric_type": "L2", "params": {"ef": 64}},
            2,
            expr='text =~ "ERROR"',
        )
        warn_req = AnnSearchRequest(
            query_vector,
            "vec2",
            {"metric_type": "L2", "params": {"ef": 64}},
            1,
            expr='text =~ "WARN"',
        )
        res = client.hybrid_search(collection_name, [regex_req, warn_req], ranker, limit=3, output_fields=["id"])
        result = {hit["id"] for hit in res[0]}
        assert result == {1, 2, 4}, f"regex and WARN sub-searches should return only {{1,2,4}}, got {result}"

        json_req = AnnSearchRequest(
            query_vector,
            "vec",
            {"metric_type": "L2", "params": {"ef": 64}},
            2,
            expr='metadata["trace"] =~ {pattern}',
            expr_params={"pattern": r"^api-"},
        )
        nullable_req = AnnSearchRequest(
            query_vector,
            "vec2",
            {"metric_type": "L2", "params": {"ef": 64}},
            2,
            expr='email !~ "gmail" or email is null',
        )

        res = client.hybrid_search(collection_name, [json_req], WeightedRanker(1.0), limit=2, output_fields=["id"])
        result = {hit["id"] for hit in res[0]}
        assert result == {1, 2}, f"JSON template regex request should return only {{1,2}}, got {result}"

        res = client.hybrid_search(collection_name, [nullable_req], WeightedRanker(1.0), limit=2, output_fields=["id"])
        result = {hit["id"] for hit in res[0]}
        assert result == {3, 5}, f"nullable regex request should return only {{3,5}}, got {result}"

        res = client.hybrid_search(collection_name, [json_req, nullable_req], ranker, limit=4, output_fields=["id"])
        result = {hit["id"] for hit in res[0]}
        assert result == {1, 2, 3, 5}, f"both-regex/template/nullable hybrid expected {{1,2,3,5}}, got {result}"
        assert 4 not in result, f"id 4 should match neither request but appeared: {result}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_search_no_match_returns_empty_list(self):
        """
        target: vector search with regex filter no match returns empty list
        expected: unfiltered search has results; filtered search with no matching regex returns []
        """
        client, collection_name = self._shared_collection()
        data = gen_regex_test_data()

        query_vector = data[0]["vec"]
        unfiltered = client.search(
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            limit=3,
            output_fields=["id"],
        )
        assert len(unfiltered[0]) > 0, f"unfiltered search expected non-empty result, got {unfiltered}"

        filtered = client.search(
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            filter='text =~ "NO_SUCH_PATTERN_98765"',
            limit=10,
            output_fields=["id"],
        )
        assert filtered[0] == [], f"no-match regex search expected [], got {filtered[0]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_partition_query_search_delete(self):
        """
        target: partition query/search/delete honor regex filters and partition boundaries
        expected: regex results stay within requested partition and delete affects only that partition
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = gen_regex_test_schema(client)
        client.create_collection(collection_name, schema=schema, consistency_level="Strong")
        self.create_partition(client, collection_name, "p_error")
        self.create_partition(client, collection_name, "p_warn")
        data = gen_regex_test_data()
        client.insert(collection_name=collection_name, data=[data[0], data[4]], partition_name="p_error")
        client.insert(collection_name=collection_name, data=[data[1], data[2]], partition_name="p_warn")
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2")
        client.create_index(collection_name, index_params)
        client.load_collection(collection_name)

        res = client.query(
            collection_name, filter='level =~ "ERROR"', partition_names=["p_error"], output_fields=["id"]
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 5], f"query p_error expected [1,5], got {result}"

        res = client.query(collection_name, filter='level =~ "ERROR"', partition_names=["p_warn"], output_fields=["id"])
        assert res == [], f"query p_warn should not leak ERROR rows, got {res}"

        search_res = client.search(
            collection_name=collection_name,
            data=[data[0]["vec"]],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            filter='level =~ "ERROR"',
            partition_names=["p_error"],
            limit=2,
            output_fields=["id", "level"],
        )
        result = {hit["id"] for hit in search_res[0]}
        assert result == {1, 5}, f"search p_error expected {{1,5}}, got {result}"

        self.delete(client, collection_name, filter='text =~ "timeout"', partition_name="p_error")
        res = client.query(collection_name, filter="id > 0", partition_names=["p_error"], output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [5], f"delete in p_error should leave only [5], got {result}"

        res = client.query(collection_name, filter="id > 0", partition_names=["p_warn"], output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2, 3], f"delete in p_error leaked into p_warn, got {result}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_query_and_search_iterator(self):
        """
        target: query_iterator and search_iterator support regex filters
        expected: iterators return all matching rows without duplicate PKs
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        rows = []
        for i in range(200):
            level = "ERROR" if i % 4 == 0 else "INFO"
            rows.append(
                {
                    "id": i,
                    "vec": [float(i) / 1000] + [0.0] * (default_dim - 1),
                    "text": f"{level} iterator row {i}",
                    "email": f"user{i}@example.com",
                    "url": f"/iter/{i}",
                    "level": level,
                    "metadata": {"level": level, "version": f"v{i % 10}.0", "trace": f"iter-{i}"},
                    "tags": [level.lower()],
                }
            )
        setup_regex_collection(client, collection_name, data=rows)

        expected = {i for i in range(200) if i % 4 == 0}
        iterator = client.query_iterator(
            collection_name,
            batch_size=17,
            filter='text =~ "^ERROR"',
            output_fields=["id", "text"],
        )
        query_rows = drain_iterator(iterator)
        query_ids = [r["id"] for r in query_rows]
        assert set(query_ids) == expected, f"query_iterator expected {expected}, got {set(query_ids)}"
        assert len(query_ids) == len(set(query_ids)), f"query_iterator duplicated ids: {query_ids}"
        assert all(r["text"].startswith("ERROR") for r in query_rows), (
            f"query_iterator returned non-matching rows: {query_rows}"
        )

        iterator = client.search_iterator(
            collection_name,
            data=[rows[0]["vec"]],
            batch_size=13,
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            filter='text =~ "^ERROR"',
            limit=len(expected),
            output_fields=["id", "text"],
        )
        search_hits = drain_iterator(iterator)
        search_ids = [hit["id"] for hit in search_hits]
        assert set(search_ids) == expected, f"search_iterator expected {expected}, got {set(search_ids)}"
        assert len(search_ids) == len(set(search_ids)), f"search_iterator duplicated ids: {search_ids}"
        assert all(hit["text"].startswith("ERROR") for hit in search_hits), (
            f"search_iterator returned non-matching rows: {search_hits}"
        )

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_concurrent_correctness(self):
        """
        target: concurrent regex query/search requests remain correct and service stays healthy
        expected: legal requests return expected ids; invalid regex returns error; later query still succeeds
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        data = gen_regex_test_data()
        setup_regex_collection(client, collection_name, data=data)
        query_vector = data[0]["vec"]

        def query_ids(filter_expr, expected):
            res = client.query(collection_name, filter=filter_expr, output_fields=["id"])
            result = sorted([r["id"] for r in res])
            assert result == expected, f"{filter_expr}: expected {expected}, got {result}"

        def search_ids(filter_expr, expected_set):
            res = client.search(
                collection_name=collection_name,
                data=[query_vector],
                anns_field="vec",
                search_params={"metric_type": "L2", "params": {"ef": 64}},
                filter=filter_expr,
                limit=len(expected_set),
                output_fields=["id"],
            )
            result = {hit["id"] for hit in res[0]}
            assert result == expected_set, f"{filter_expr}: expected {expected_set}, got {result}"

        def invalid_regex():
            try:
                client.query(collection_name, filter='text =~ "(unclosed"', output_fields=["id"])
            except Exception as e:
                assert "missing closing" in str(e), f"unexpected invalid regex error: {e}"
                return
            raise AssertionError("invalid regex unexpectedly succeeded")

        tasks = [
            lambda: query_ids('text =~ "ERROR"', [1]),
            lambda: query_ids('text =~ "^WARN"', [2]),
            lambda: query_ids('text =~ "E[0-9]{4}"', [1]),
            lambda: query_ids('text !~ "DEBUG"', [1, 2, 4, 5, 6, 7]),
            lambda: search_ids('url =~ "^/api/v[0-9]+/users"', {1, 3, 5}),
            invalid_regex,
        ]

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(task) for _ in range(3) for task in tasks]
            for future in as_completed(futures):
                future.result()

        res = client.query(collection_name, filter="id > 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 7, f"post-concurrency health query expected count 7, got {res}"

        self.drop_collection(client, collection_name)


@pytest.mark.xdist_group("TestRegexFilterStructArray")
class TestRegexFilterStructArray(RegexFilterStructArraySharedBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_struct_array_scalar_field_query(self):
        """
        target: regex filtering on scalar fields inside StructArray elements
        expected: MATCH_ANY covers =~, !~, empty array, and non-string values
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name, filter='MATCH_ANY(events, $[name] =~ "error.*timeout")', output_fields=["id"]
        )
        result = sorted([r["id"] for r in res])
        assert result == [1], f"struct name =~ error.*timeout: expected [1], got {result}"

        res = client.query(collection_name, filter='MATCH_ANY(events, $[status] !~ "ERROR")', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3], f"struct status !~ ERROR: expected [1,2,3], got {result}"

        res = client.query(collection_name, filter='MATCH_ANY(events, $[name] =~ "^$")', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [2], f"struct empty name =~ ^$: expected [2], got {result}"

        res = client.query(collection_name, filter='MATCH_ANY(events, $[name] =~ ".*")', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3], f"struct name =~ .* should exclude empty/null arrays, got {result}"

        res = client.query(collection_name, filter='MATCH_ANY(events, $[name] !~ ".*")', output_fields=["id"])
        assert res == [], f"struct name !~ .* should exclude empty/null arrays, got {res}"

        # StructArray elements are strongly typed, so element-level missing required fields cannot be inserted.
        # Use an unknown StructArray sub-field to cover missing-path expression validation for =~ and !~.
        for filter_expr in [
            'MATCH_ANY(events, $[unknown] =~ ".*")',
            'MATCH_ANY(events, $[unknown] !~ ".*")',
        ]:
            error = {ct.err_code: 1100, ct.err_msg: "unknown"}
            self.query(
                client,
                collection_name,
                filter=filter_expr,
                check_task=CheckTasks.err_res,
                check_items=error,
            )

        error = {ct.err_code: 1100, ct.err_msg: "regex match on non-string or non-json field"}
        self.query(
            client,
            collection_name,
            filter='MATCH_ANY(events, $[code] =~ "500")',
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_struct_array_nullable_null_field_query(self):
        """
        target: regex filtering excludes null StructArray fields when nullable StructArray insert is supported
        expected: events=None is inserted successfully; =~ and !~ both exclude the null StructArray row
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        setup_struct_array_regex_collection(client, collection_name)
        client.insert(
            collection_name=collection_name,
            data=[{"id": 5, "vec": [0.05] + [0.0] * (default_dim - 1), "events": None}],
        )

        res = client.query(collection_name, filter='MATCH_ANY(events, $[name] =~ ".*")', output_fields=["id"])
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3], f"struct name =~ .* should exclude empty/null arrays, got {result}"

        res = client.query(collection_name, filter='MATCH_ANY(events, $[name] !~ ".*")', output_fields=["id"])
        assert res == [], f"struct name !~ .* should exclude empty/null arrays, got {res}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_struct_array_scalar_index_path(self):
        """
        target: indexed StructArray scalar field regex path returns the same result as raw path
        expected: INVERTED index on events[name] has no false negatives for regex filtering
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name, filter='MATCH_ANY(events, $[name] =~ "error.*timeout")', output_fields=["id"]
        )
        result = sorted([r["id"] for r in res])
        assert result == [1], f"indexed struct name =~ error.*timeout: expected [1], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_struct_array_hybrid_search(self):
        """
        target: hybrid_search supports regex filters on StructArray scalar paths
        expected: one request filters struct name =~ error.*timeout; another uses struct status !~ ERROR
        """
        client, collection_name = self._shared_collection()
        query_vector = EmbeddingList()
        query_vector.add([0.035] + [0.0] * (default_dim - 1))
        req_unfiltered = AnnSearchRequest(
            [query_vector],
            "events[embedding]",
            {"metric_type": "MAX_SIM_L2", "params": {"ef": 64}},
            1,
        )
        req_name = AnnSearchRequest(
            [query_vector],
            "events[embedding]",
            {"metric_type": "MAX_SIM_L2", "params": {"ef": 64}},
            1,
            expr='MATCH_ANY(events, $[name] =~ "error.*timeout")',
        )
        req_status = AnnSearchRequest(
            [query_vector],
            "events[embedding]",
            {"metric_type": "MAX_SIM_L2", "params": {"ef": 64}},
            1,
            expr='MATCH_ANY(events, $[status] =~ "WARN")',
        )

        res = client.hybrid_search(
            collection_name, [req_unfiltered], WeightedRanker(1.0), limit=1, output_fields=["id"]
        )
        result = {hit["id"] for hit in res[0]}
        assert result == {2}, f"unfiltered struct embedding top1 should be non-matching id 2, got {result}"

        res = client.hybrid_search(collection_name, [req_name], WeightedRanker(1.0), limit=1, output_fields=["id"])
        result = {hit["id"] for hit in res[0]}
        assert result == {1}, f"struct name regex request should return only id 1, got {result}"

        res = client.hybrid_search(
            collection_name, [req_name, req_status], WeightedRanker(0.5, 0.5), limit=2, output_fields=["id"]
        )
        result = {hit["id"] for hit in res[0]}
        assert result == {1, 2}, f"struct hybrid name/status regex should return only {{1,2}}, got {result}"


@pytest.mark.xdist_group("TestRegexFilterTemplateExpression")
class TestRegexFilterTemplateExpression(RegexFilterSharedWideBase):
    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_template_param_basic(self):
        """
        target: verify regex filter supports template parameter on query path
        expected: text/json/array regex template params behave the same as inline string literals
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter="text =~ {pattern}",
            filter_params={"pattern": "timeout"},
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1], f"text =~ {{pattern}} timeout: expected [1], got {result}"

        res = client.query(
            collection_name,
            filter='metadata["version"] =~ {pattern}',
            filter_params={"pattern": r"^v[0-9]+\.[0-9]+$"},
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 3, 5, 7], f"json version template: expected [1,2,3,5,7], got {result}"

        res = client.query(
            collection_name,
            filter="tags[0] =~ {pattern}",
            filter_params={"pattern": r"^release-v[0-9]+"},
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [1, 2, 5], f"array element template: expected [1,2,5], got {result}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_template_param_negation(self):
        """
        target: verify negated regex supports template parameter on query path
        expected: !~ template params match literal !~ semantics and still exclude NULL values
        """
        client, collection_name = self._shared_collection()

        res = client.query(
            collection_name,
            filter="level !~ {pattern}",
            filter_params={"pattern": r"^ERROR$"},
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [2, 3, 4, 6, 7], f"level !~ {{pattern}}: expected [2,3,4,6,7], got {result}"

        res = client.query(
            collection_name,
            filter="email !~ {pattern}",
            filter_params={"pattern": r"(?i)gmail\.com$"},
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [2, 7], f"email !~ {{pattern}} excluding NULL: expected [2,7], got {result}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_template_param_invalid_regex(self):
        """
        target: verify invalid regex passed through template parameter is rejected
        expected: invalid regex raises an error with regex/parse related message
        """
        client, collection_name = self._shared_collection()

        with pytest.raises(Exception) as exc_info:
            client.query(
                collection_name,
                filter="text =~ {pattern}",
                filter_params={"pattern": "["},
                output_fields=["id"],
            )
        err = str(exc_info.value).lower()
        assert any(keyword in err for keyword in ["regex", "invalid", "parse", "missing", "bracket"]), (
            f"expected invalid regex error, got: {err}"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_template_param_type_mismatch(self):
        """
        target: verify regex template parameter rejects non-string pattern values
        expected: non-string regex pattern parameter raises type/parameter related error
        """
        client, collection_name = self._shared_collection()

        with pytest.raises(Exception) as exc_info:
            client.query(
                collection_name,
                filter="text =~ {pattern}",
                filter_params={"pattern": 123},
                output_fields=["id"],
            )
        err = str(exc_info.value).lower()
        assert any(
            keyword in err
            for keyword in [
                "string",
                "type",
                "template",
                "regex",
                "parameter",
                "unsupported",
            ]
        ), f"expected non-string regex template parameter error, got: {err}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_regex_search_template_param_basic(self):
        """
        target: verify vector search supports regex template parameter in filter
        expected: search filter url =~ {pattern} returns only ids [1,3,5]
        """
        client, collection_name = self._shared_collection()
        query_vector = [0.0] * default_dim
        unfiltered = client.search(
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            limit=7,
            output_fields=["id"],
        )
        unfiltered_ids = [hit["id"] for hit in unfiltered[0]]
        assert set(unfiltered_ids) == {1, 2, 3, 4, 5, 6, 7}, f"unfiltered expected all ids, got {unfiltered_ids}"

        filtered = client.search(
            collection_name=collection_name,
            data=[query_vector],
            anns_field="vec",
            search_params={"metric_type": "L2", "params": {"ef": 64}},
            filter="url =~ {pattern}",
            filter_params={"pattern": r"^/api/v[0-9]+/users"},
            limit=3,
            output_fields=["id"],
        )
        filtered_ids = [hit["id"] for hit in filtered[0]]
        assert set(filtered_ids) == {1, 3, 5}, f"filtered expected ids [1,3,5], got {filtered_ids}"


class TestRegexFilterTemplateExpressionIndependent(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L2)
    def test_regex_template_param_injection_safety(self):
        """
        target: verify regex template parameter cannot inject filter expression grammar
        expected: expression-like parameter value is treated as regex pattern data only
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        data = gen_regex_test_data()
        injection_pattern = 'timeout" or id > 0 or text =~ "'
        data.extend(
            [
                {
                    "id": 201,
                    "vec": [random.random() for _ in range(default_dim)],
                    "text": f"literal probe {injection_pattern}",
                    "email": "inject-literal@example.com",
                    "url": "/inject/literal",
                    "level": "ERROR",
                    "metadata": {"level": "ERROR", "version": "inject-literal"},
                    "tags": ["inject", "literal"],
                },
                {
                    "id": 202,
                    "vec": [random.random() for _ in range(default_dim)],
                    "text": "ordinary timeout probe",
                    "email": "inject-ordinary@example.com",
                    "url": "/inject/ordinary",
                    "level": "ERROR",
                    "metadata": {"level": "ERROR", "version": "inject-ordinary"},
                    "tags": ["inject", "ordinary"],
                },
                {
                    "id": 203,
                    "vec": [random.random() for _ in range(default_dim)],
                    "text": "non error injection guard",
                    "email": "inject-guard@example.com",
                    "url": "/inject/guard",
                    "level": "WARN",
                    "metadata": {"level": "WARN", "version": "inject-guard"},
                    "tags": ["inject", "guard"],
                },
            ]
        )
        setup_regex_collection(client, collection_name, data=data)

        res = client.query(
            collection_name,
            filter='text =~ {pattern} and level == "ERROR"',
            filter_params={"pattern": injection_pattern},
            output_fields=["id"],
        )
        result = sorted([r["id"] for r in res])
        assert result == [201], f"regex template parameter must not inject filter grammar, got {result}"

        self.drop_collection(client, collection_name)
