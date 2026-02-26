import io
import time
import threading

import pytest
from pymilvus import DataType, Function, FunctionType

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

prefix = "file_resource"

# MinIO object paths (relative to bucket root, without rootPath prefix)
JIEBA_DICT_PATH = "jieba/jieba_dict.txt"
SYNONYMS_PATH = "synonyms/synonyms.txt"
STOPWORDS_PATH = "stopwords/stop_words.txt"
DECOMPOUNDER_PATH = "decompounder/decompounder_dict.txt"


class FileResourceTestBase(TestMilvusClientV2Base):
    """Base class with shared helpers for file resource tests."""

    @pytest.fixture(autouse=True)
    def init_minio_client(self, minio_host):
        """Initialise a MinIO client reusing the existing --minio_host option."""
        from minio import Minio
        self._minio_client = Minio(
            f"{minio_host}:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

    def setup_method(self, method):
        super().setup_method(method)
        self._file_resources_to_cleanup = []
        self._minio_objects_to_cleanup = []
        self._teardown_client = None

    def teardown_method(self, method):
        # Phase 1: drop collections first (releases file resource references)
        super().teardown_method(method)
        # Phase 2: clean up file resources via MilvusClient independent gRPC channel
        client = self._teardown_client
        if client and self._file_resources_to_cleanup:
            for name in self._file_resources_to_cleanup:
                try:
                    client.remove_file_resource(name=name)
                except Exception:
                    pass
        # Phase 3: clean up MinIO temporary objects (large file tests only)
        minio = self._minio_client
        if minio and self._minio_objects_to_cleanup:
            for bucket, path in self._minio_objects_to_cleanup:
                try:
                    minio.remove_object(bucket, path)
                except Exception:
                    pass

    def _client(self, active_trace=False, **kwargs):
        client = super()._client(active_trace=active_trace, **kwargs)
        self._teardown_client = client
        return client

    def add_file_resource(self, client, name, path, **kwargs):
        result = super().add_file_resource(client, name, path, **kwargs)
        # Only track successful additions (skip expected-error calls)
        if kwargs.get("check_task") != CheckTasks.err_res:
            if name not in self._file_resources_to_cleanup:
                self._file_resources_to_cleanup.append(name)
        return result

    def remove_file_resource(self, client, name, **kwargs):
        result = super().remove_file_resource(client, name, **kwargs)
        # Unregister on successful removal (skip expected-error calls)
        if kwargs.get("check_task") != CheckTasks.err_res:
            if name in self._file_resources_to_cleanup:
                self._file_resources_to_cleanup.remove(name)
        return result

    def create_bm25_collection(self, client, col_name, analyzer_params, **kwargs):
        """Create a collection with a VARCHAR text field + BM25 function."""
        schema, _ = self.create_schema(client)
        self.add_field(schema, "id", DataType.INT64, is_primary=True, auto_id=True)
        self.add_field(
            schema,
            "text",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            analyzer_params=analyzer_params,
        )
        self.add_field(schema, "sparse_vector", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["sparse_vector"],
            )
        )
        self.create_collection(client, col_name, schema=schema, **kwargs)

    def create_bm25_collection_with_stop_filter(
        self, client, col_name, res_name, **kwargs
    ):
        """Shortcut: BM25 collection with a remote stop-words filter."""
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": res_name,
                            "file_name": "stop_words.txt",
                        },
                    }
                ],
            },
            **kwargs,
        )

    def build_and_load_bm25(self, client, col_name):
        """Build sparse index and load collection (no data insertion)."""
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(
            field_name="sparse_vector",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
        )
        self.create_index(client, col_name, index_params)
        self.load_collection(client, col_name)
        time.sleep(1)

    def insert_and_build_bm25(self, client, col_name, texts):
        """Insert rows, build sparse index, load collection."""
        data = [{"text": t} for t in texts]
        self.insert(client, col_name, data)
        time.sleep(2)
        self.build_and_load_bm25(client, col_name)


class TestMilvusClientFileResourceAdd(FileResourceTestBase):
    """Test case of add file resource"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_file_resource_valid(self, file_resource_env):
        """
        target: add a valid file resource
        method: upload file to MinIO then add_file_resource
        expected: no exception
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_file_resource_path_not_exist(self, file_resource_env):
        """
        target: add a file resource whose path does not exist in MinIO
        method: add_file_resource with non-existent path
        expected: raise Exception
        """
        client = self._client()
        # 1. add file resource with non-existent path
        error = {ct.err_code: 65535, ct.err_msg: "file resource path not exist"}
        self.add_file_resource(
            client,
            cf.gen_unique_str(prefix),
            "not/exist.txt",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_file_resource_idempotent_same_name_path(self, file_resource_env):
        """
        target: add is idempotent when name+path are identical
        method: call add_file_resource twice with same args
        expected: both succeed
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource twice with same args
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_file_resource_same_name_different_path(self, file_resource_env):
        """
        target: add with same name but different path should fail
        method: add(name="x", path="a") then add(name="x", path="b")
        expected: second call raises Exception
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. add same name with different path should fail
        error = {ct.err_code: 65535, ct.err_msg: "already exists"}
        self.add_file_resource(
            client,
            res_name,
            SYNONYMS_PATH,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_file_resource_empty_name(self, file_resource_env):
        """
        target: add with empty name
        method: add_file_resource(name="", ...)
        expected: server may accept empty name (no validation); cleanup after
        """
        client = self._client()
        # 1. add file resource with empty name
        self.add_file_resource(client, "", JIEBA_DICT_PATH)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_file_resource_empty_path(self, file_resource_env):
        """
        target: add with empty path
        method: add_file_resource(name=..., path="")
        expected: raise Exception
        """
        client = self._client()
        # 1. add file resource with empty path
        error = {ct.err_code: 1001, ct.err_msg: "Object name cannot be empty"}
        self.add_file_resource(
            client,
            cf.gen_unique_str(prefix),
            "",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_multiple_file_resources(self, file_resource_env):
        """
        target: add multiple different file resources
        method: add 3 resources with distinct names/paths
        expected: all succeed
        """
        client = self._client()
        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        # 1. add 3 file resources
        for n, p in zip(names, paths):
            self.add_file_resource(client, n, p)


class TestMilvusClientFileResourceRemove(FileResourceTestBase):
    """Test case of remove file resource"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_remove_existing_resource(self, file_resource_env):
        """
        target: remove an existing resource
        method: add then remove
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. remove file resource
        self.remove_file_resource(client, res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_nonexistent_resource(self, file_resource_env):
        """
        target: remove non-existent resource (idempotent)
        method: remove a name that was never added
        expected: no error
        """
        client = self._client()
        # 1. remove non-existent resource
        self.remove_file_resource(client, cf.gen_unique_str(prefix))

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_idempotent(self, file_resource_env):
        """
        target: double remove is idempotent
        method: add -> remove -> remove
        expected: both remove calls succeed
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. remove twice
        self.remove_file_resource(client, res_name)
        self.remove_file_resource(client, res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_resource_in_use_by_collection(self, file_resource_env):
        """
        target: cannot remove a resource still referenced by a collection
        method: add -> create collection using it -> remove
        expected: raise Exception
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. create collection referencing it
        self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
        # 3. remove should fail
        error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
        self.remove_file_resource(
            client, res_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_then_readd_same_name(self, file_resource_env):
        """
        target: re-add with same name after removal
        method: add -> remove -> add (possibly different path)
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. remove
        self.remove_file_resource(client, res_name)
        # 3. re-add with different path
        self.add_file_resource(client, res_name, SYNONYMS_PATH)


class TestMilvusClientFileResourceList(FileResourceTestBase):
    """Test case of list file resources"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_empty(self, file_resource_env):
        """
        target: list when no resources exist
        method: ensure clean state then call list
        expected: empty list
        """
        client = self._client()
        # 1. best-effort cleanup
        res, _ = self.list_file_resources(client)
        for r in res:
            try:
                self.remove_file_resource(client, r.name)
            except Exception:
                pass
        # 2. list should be empty
        res, _ = self.list_file_resources(client)
        assert len(res) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_after_add(self, file_resource_env):
        """
        target: list shows added resources
        method: add 3 resources -> list
        expected: all 3 names present
        """
        client = self._client()
        # 1. best-effort cleanup
        res, _ = self.list_file_resources(client)
        for r in res:
            try:
                self.remove_file_resource(client, r.name)
            except Exception:
                pass
        # 2. add 3 resources
        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        for n, p in zip(names, paths):
            self.add_file_resource(client, n, p)
        # 3. list and verify
        res, _ = self.list_file_resources(client)
        listed = {r.name for r in res}
        for n in names:
            assert n in listed, f"{n} missing from list"

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_after_remove(self, file_resource_env):
        """
        target: list updates after removal
        method: add 3 -> remove 1 -> list
        expected: removed resource absent, other 2 present
        """
        client = self._client()
        # 1. best-effort cleanup
        res, _ = self.list_file_resources(client)
        for r in res:
            try:
                self.remove_file_resource(client, r.name)
            except Exception:
                pass
        # 2. add 3 resources
        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        for n, p in zip(names, paths):
            self.add_file_resource(client, n, p)
        # 3. remove the first one
        self.remove_file_resource(client, names[0])
        # 4. list and verify
        res, _ = self.list_file_resources(client)
        listed = {r.name for r in res}
        assert names[0] not in listed
        assert names[1] in listed
        assert names[2] in listed


class TestMilvusClientFileResourceJieba(FileResourceTestBase):
    """Test case of jieba tokenizer with remote dict"""

    def _jieba_analyzer_params(self, res_name):
        return {
            "tokenizer": {
                "type": "jieba",
                "extra_dict_file": {
                    "type": "remote",
                    "resource_name": res_name,
                    "file_name": "jieba_dict.txt",
                },
            }
        }

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_remote_dict_bm25_search(self, file_resource_env):
        """
        target: BM25 search with remote jieba dict returns correct top hit
        method: add resource -> create collection -> insert 4 docs -> search
        expected: first hit is the doc containing the query term
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. create collection with jieba analyzer
        self.create_bm25_collection(
            client, col_name, self._jieba_analyzer_params(res_name)
        )
        # 3. insert and build index
        docs = [
            "向量数据库是新一代的数据管理系统",
            "语义搜索可以理解用户意图",
            "全文检索是传统的文本搜索方式",
            "milvus 支持混合搜索",
        ]
        self.insert_and_build_bm25(client, col_name, docs)
        # 4. search and verify
        results, _ = self.search(
            client,
            col_name,
            data=["向量数据库"],
            anns_field="sparse_vector",
            limit=10,
            output_fields=["text"],
        )
        assert len(results) > 0 and len(results[0]) > 0, (
            "Search should return at least one result"
        )
        top_text = results[0][0]["entity"]["text"]
        assert "向量数据库" in top_text, (
            f"Top hit should contain '向量数据库', got '{top_text}'"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_remote_dict_all_custom_words(self, file_resource_env):
        """
        target: all custom dict words recognized as single tokens
        method: run_analyzer for each custom word via collection context
        expected: custom words each appear as a single token
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. create collection with jieba analyzer
        self.create_bm25_collection(
            client, col_name, self._jieba_analyzer_params(res_name)
        )
        self.build_and_load_bm25(client, col_name)
        # 3. verify each custom word is recognized as a single token
        test_cases = [
            ("向量数据库是新一代的数据管理系统", "向量数据库"),
            ("语义搜索可以理解用户意图", "语义搜索"),
            ("全文检索是传统的文本搜索方式", "全文检索"),
        ]
        for text, expected_token in test_cases:
            res, _ = self.run_analyzer(
                client, text, None, collection_name=col_name, field_name="text"
            )
            tokens = list(res.tokens)
            assert expected_token in tokens, (
                f"Custom dict word '{expected_token}' should be a token, got {tokens}"
            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_jieba_default_vs_custom_comparison(self, file_resource_env):
        """
        target: compare default jieba vs custom dict tokenization
        method: run_analyzer with default jieba and verify custom words are NOT tokens
        expected: custom words NOT in default jieba tokens
        """
        client = self._client()
        # 1. verify default jieba does not produce custom words as single tokens
        res, _ = self.run_analyzer(client, "向量数据库是新一代", {"tokenizer": "jieba"})
        tokens_default = list(res.tokens)
        assert "向量数据库" not in tokens_default, (
            f"Default jieba should NOT produce '向量数据库' as token, got {tokens_default}"
        )
        # 2. verify another custom word
        res, _ = self.run_analyzer(client, "语义搜索可以理解", {"tokenizer": "jieba"})
        tokens_default2 = list(res.tokens)
        assert "语义搜索" not in tokens_default2, (
            f"Default jieba should NOT produce '语义搜索' as token, got {tokens_default2}"
        )


class TestMilvusClientFileResourceSynonym(FileResourceTestBase):
    """Test case of synonym filter with remote file"""

    def _synonym_analyzer_params(self, res_name, expand=True):
        return {
            "tokenizer": "jieba",
            "filter": [
                {
                    "type": "synonym",
                    "expand": expand,
                    "synonyms_file": {
                        "type": "remote",
                        "resource_name": res_name,
                        "file_name": "synonyms.txt",
                    },
                }
            ],
        }

    @pytest.mark.tags(CaseLabel.L1)
    def test_synonym_remote_file_search(self, file_resource_env):
        """
        target: synonym expansion enables cross-synonym search
        method: insert doc with one synonym, search with another
        expected: the doc is found via synonym match
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, SYNONYMS_PATH)
        # 2. create collection with synonym analyzer
        self.create_bm25_collection(
            client, col_name, self._synonym_analyzer_params(res_name, expand=True)
        )
        # 3. insert and build index
        docs = [
            "搜索引擎可以帮助用户查找信息",
            "数据库管理系统支持SQL查询",
            "分布式系统的设计原则",
        ]
        self.insert_and_build_bm25(client, col_name, docs)
        # 4. search with synonym and verify
        results, _ = self.search(
            client,
            col_name,
            data=["检索"],
            anns_field="sparse_vector",
            limit=10,
            output_fields=["text"],
        )
        assert len(results) > 0 and len(results[0]) > 0, (
            "Synonym search should return results"
        )
        hit_texts = [r["entity"]["text"] for r in results[0]]
        assert any("搜索" in t for t in hit_texts), (
            f"Doc with '搜索' should match synonym query '检索', got {hit_texts}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_synonym_expand_true_all_synonyms(self, file_resource_env):
        """
        target: expand=true produces all synonyms for each input token
        method: run_analyzer on synonym words with expand=true
        expected: all synonym variants appear in tokens
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, SYNONYMS_PATH)
        # 2. create collection with synonym analyzer (expand=true)
        self.create_bm25_collection(
            client, col_name, self._synonym_analyzer_params(res_name, expand=True)
        )
        self.build_and_load_bm25(client, col_name)
        # 3. verify "搜索" expands to all synonyms
        res, _ = self.run_analyzer(
            client, "搜索", None, collection_name=col_name, field_name="text"
        )
        tokens = set(res.tokens)
        expected_synonyms = {"搜索", "检索", "查询"}
        assert expected_synonyms.issubset(tokens), (
            f"expand=true on '搜索' should produce {expected_synonyms}, got {tokens}"
        )
        # 4. verify "向量" expands to all synonyms
        res, _ = self.run_analyzer(
            client, "向量", None, collection_name=col_name, field_name="text"
        )
        tokens = set(res.tokens)
        expected_synonyms = {"向量", "矢量", "vector"}
        assert expected_synonyms.issubset(tokens), (
            f"expand=true on '向量' should produce {expected_synonyms}, got {tokens}"
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_synonym_expand_false_maps_to_canonical(self, file_resource_env):
        """
        target: expand=false maps non-canonical forms to the first form
        method: run_analyzer on non-canonical synonyms with expand=false
        expected: non-canonical forms map to canonical form
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, SYNONYMS_PATH)
        # 2. create collection with synonym analyzer (expand=false)
        self.create_bm25_collection(
            client, col_name, self._synonym_analyzer_params(res_name, expand=False)
        )
        self.build_and_load_bm25(client, col_name)
        # 3. verify "检索" maps to canonical "搜索"
        res, _ = self.run_analyzer(
            client, "检索", None, collection_name=col_name, field_name="text"
        )
        tokens = list(res.tokens)
        assert tokens == ["搜索"], (
            f"expand=false: '检索' should map to ['搜索'], got {tokens}"
        )
        # 4. verify "查询" maps to canonical "搜索"
        res, _ = self.run_analyzer(
            client, "查询", None, collection_name=col_name, field_name="text"
        )
        tokens = list(res.tokens)
        assert tokens == ["搜索"], (
            f"expand=false: '查询' should map to ['搜索'], got {tokens}"
        )
        # 5. verify "矢量" maps to canonical "向量"
        res, _ = self.run_analyzer(
            client, "矢量", None, collection_name=col_name, field_name="text"
        )
        tokens = list(res.tokens)
        assert tokens == ["向量"], (
            f"expand=false: '矢量' should map to ['向量'], got {tokens}"
        )
        # 6. verify canonical "搜索" remains unchanged
        res, _ = self.run_analyzer(
            client, "搜索", None, collection_name=col_name, field_name="text"
        )
        tokens = list(res.tokens)
        assert tokens == ["搜索"], (
            f"expand=false: canonical '搜索' should stay ['搜索'], got {tokens}"
        )


class TestMilvusClientFileResourceStopWords(FileResourceTestBase):
    """Test case of stop words filter with remote file"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_stop_words_remote_file(self, file_resource_env):
        """
        target: stop words filtered via remote file and comparison with baseline
        method: run_analyzer with stop filter and without, compare token sets
        expected: stop words absent with filter, present without
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        text = "这是一个在测试的文本和数据在这里了"
        stop_words = {"的", "是", "在", "了", "和"}
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. create collection with stop words filter
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "jieba",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": res_name,
                            "file_name": "stop_words.txt",
                        },
                    }
                ],
            },
        )
        self.build_and_load_bm25(client, col_name)
        # 3. verify stop words are filtered
        res, _ = self.run_analyzer(
            client, text, None, collection_name=col_name, field_name="text"
        )
        tokens_filtered = set(res.tokens)
        found = stop_words & tokens_filtered
        assert len(found) == 0, (
            f"Stop words should be filtered, but found {found} in {tokens_filtered}"
        )
        # 4. verify baseline (no filter) contains stop words
        res_baseline, _ = self.run_analyzer(client, text, {"tokenizer": "jieba"})
        tokens_baseline = set(res_baseline.tokens)
        baseline_stop = stop_words & tokens_baseline
        assert len(baseline_stop) > 0, (
            f"Baseline (no filter) should contain stop words, but got {tokens_baseline}"
        )
        # 5. verify content tokens still present after filtering
        content_tokens = tokens_filtered - stop_words
        assert len(content_tokens) > 0, (
            f"Filtered output should still have content tokens, got {tokens_filtered}"
        )


class TestMilvusClientFileResourceDecompounder(FileResourceTestBase):
    """Test case of decompounder filter with remote file"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_decompounder_remote_file(self, file_resource_env):
        """
        target: decompounder splits compound words via remote dictionary
        method: run_analyzer on compound words with decompounder
        expected: compound words are split into sub-components
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, DECOMPOUNDER_PATH)
        # 2. create collection with decompounder filter
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "decompounder",
                        "word_list_file": {
                            "type": "remote",
                            "resource_name": res_name,
                            "file_name": "decompounder_dict.txt",
                        },
                    }
                ],
            },
        )
        self.build_and_load_bm25(client, col_name)
        # 3. verify compound words are decomposed
        res, _ = self.run_analyzer(
            client,
            "banknote firework",
            None,
            collection_name=col_name,
            field_name="text",
        )
        tokens = list(res.tokens)
        for expected in ["bank", "note", "fire", "work"]:
            assert expected in tokens, (
                f"Decompounder should produce '{expected}', got {tokens}"
            )
        # 4. verify compound words are replaced
        assert "banknote" not in tokens, (
            f"Compound 'banknote' should be decomposed, got {tokens}"
        )
        assert "firework" not in tokens, (
            f"Compound 'firework' should be decomposed, got {tokens}"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_decompounder_baseline_without_filter(self, file_resource_env):
        """
        target: without decompounder filter, compound words remain intact
        method: run_analyzer with standard tokenizer only
        expected: compound words are single tokens
        """
        client = self._client()
        # 1. verify compound words remain intact without decompounder
        res, _ = self.run_analyzer(
            client, "banknote firework", {"tokenizer": "standard"}
        )
        tokens = list(res.tokens)
        assert "banknote" in tokens, (
            f"Without decompounder, 'banknote' should be a single token, got {tokens}"
        )
        assert "firework" in tokens, (
            f"Without decompounder, 'firework' should be a single token, got {tokens}"
        )
        assert "bank" not in tokens and "note" not in tokens, (
            f"Without decompounder, sub-tokens should not appear, got {tokens}"
        )


class TestMilvusClientFileResourceInvalidConfig(FileResourceTestBase):
    """Test case of invalid remote file resource configurations"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_missing_resource_name(self, file_resource_env):
        """
        target: remote config without resource_name
        method: create collection with missing resource_name in analyzer config
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        # 1. create collection with missing resource_name should fail
        error = {ct.err_code: 1100, ct.err_msg: "resource name of remote file"}
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "file_name": "stop_words.txt",
                        },
                    }
                ],
            },
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_missing_file_name(self, file_resource_env):
        """
        target: remote config without file_name
        method: create collection with missing file_name in analyzer config
        expected: error on create_collection
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. create collection with missing file_name should fail
        error = {ct.err_code: 1100, ct.err_msg: "file name of remote file"}
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": res_name,
                        },
                    }
                ],
            },
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_invalid_type(self, file_resource_env):
        """
        target: type="unknown" in file resource config
        method: create collection with invalid type in analyzer config
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        # 1. create collection with invalid type should fail
        error = {ct.err_code: 1100, ct.err_msg: "unsupported file type"}
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "unknown",
                            "resource_name": "r",
                            "file_name": "f.txt",
                        },
                    }
                ],
            },
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_nonexistent_resource_name(self, file_resource_env):
        """
        target: resource_name references a resource that was never added
        method: create collection with non-existent resource_name
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        # 1. create collection with non-existent resource should fail
        error = {ct.err_code: 1100, ct.err_msg: "not found in local resource list"}
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": "nonexist_resource",
                            "file_name": "stop_words.txt",
                        },
                    }
                ],
            },
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestMilvusClientFileResourceRefCount(FileResourceTestBase):
    """Test case of reference counting and deletion protection"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_single_ref_cannot_delete(self, file_resource_env):
        """
        target: resource referenced by 1 collection cannot be removed
        method: add -> create col -> remove
        expected: raise Exception
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. create collection referencing the resource
        self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
        # 3. remove should fail
        error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
        self.remove_file_resource(
            client, res_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_multi_ref_partial_drop(self, file_resource_env):
        """
        target: resource referenced by 2 collections; dropping 1 still blocks removal
        method: add -> create col1 + col2 -> drop col1 -> remove
        expected: raise Exception (col2 still references)
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col1 = cf.gen_unique_str(prefix)
        col2 = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. create two collections referencing the resource
        self.create_bm25_collection_with_stop_filter(client, col1, res_name)
        self.create_bm25_collection_with_stop_filter(client, col2, res_name)
        # 3. drop one collection
        self.drop_collection(client, col1)
        time.sleep(1)
        # 4. remove should still fail (col2 still references)
        error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
        self.remove_file_resource(
            client, res_name, check_task=CheckTasks.err_res, check_items=error
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_all_refs_dropped_can_delete(self, file_resource_env):
        """
        target: after all referencing collections are dropped, resource can be removed
        method: add -> create col -> drop col -> remove
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. create and drop collection
        self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
        self.drop_collection(client, col_name)
        time.sleep(1)
        # 3. remove should succeed
        self.remove_file_resource(client, res_name)


class TestMilvusClientFileResourceSyncCheck(FileResourceTestBase):
    """Test case of synchronization during collection creation"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_then_immediate_create_collection(self, file_resource_env):
        """
        target: add is synchronous — immediate collection creation should work
        method: add_file_resource -> create collection
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. immediately create collection
        self.create_bm25_collection_with_stop_filter(client, col_name, res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_without_add(self, file_resource_env):
        """
        target: create collection referencing a resource that was never added
        method: create collection with non-existent resource_name
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        # 1. create collection without adding resource should fail
        error = {ct.err_code: 1100, ct.err_msg: "not found in local resource list"}
        self.create_bm25_collection_with_stop_filter(
            client,
            col_name,
            "never_added_resource",
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestMilvusClientFileResourceIdempotency(FileResourceTestBase):
    """Test case of idempotent add and remove"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_idempotent(self, file_resource_env):
        """
        target: add same name+path twice is idempotent
        method: call add_file_resource twice with same args
        expected: no error
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource twice
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_idempotent(self, file_resource_env):
        """
        target: double remove is idempotent
        method: add -> remove -> remove
        expected: no error
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. remove twice
        self.remove_file_resource(client, res_name)
        self.remove_file_resource(client, res_name)


class TestMilvusClientFileResourceConcurrency(FileResourceTestBase):
    """Test case of concurrent file resource operations"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_add_different_resources(self, file_resource_env):
        """
        target: concurrent adds of distinct resources
        method: add 4 resources in parallel threads
        expected: all succeed
        """
        client = self._client()
        resources = [
            (cf.gen_unique_str(prefix), JIEBA_DICT_PATH),
            (cf.gen_unique_str(prefix), SYNONYMS_PATH),
            (cf.gen_unique_str(prefix), STOPWORDS_PATH),
            (cf.gen_unique_str(prefix), DECOMPOUNDER_PATH),
        ]
        # Register all resources for teardown cleanup
        for n, _ in resources:
            self._file_resources_to_cleanup.append(n)
        errors = []

        def _add(name, path):
            try:
                client.add_file_resource(name=name, path=path)
            except Exception as e:
                errors.append((name, str(e)))

        # 1. concurrent add
        threads = [threading.Thread(target=_add, args=(n, p)) for n, p in resources]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, f"Concurrent add failed: {errors}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_add_same_resource(self, file_resource_env):
        """
        target: concurrent add of same name+path (idempotent)
        method: add same resource 5 times in parallel
        expected: all succeed
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # Register for teardown cleanup
        self._file_resources_to_cleanup.append(res_name)
        errors = []

        def _add():
            try:
                client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
            except Exception as e:
                errors.append(str(e))

        # 1. concurrent add
        threads = [threading.Thread(target=_add) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, f"Concurrent idempotent add failed: {errors}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_then_create_collection(self, file_resource_env):
        """
        target: add resource then immediately create collection
        method: add -> create collection
        expected: success (add is synchronous)
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, STOPWORDS_PATH)
        # 2. immediately create collection
        self.create_bm25_collection_with_stop_filter(client, col_name, res_name)


class TestMilvusClientFileResourceLargeFile(FileResourceTestBase):
    """Test case of large file resource"""

    @staticmethod
    def _generate_large_jieba_dict(target_size_mb):
        """Generate a jieba-format dictionary of approximately target_size_mb."""
        import random

        rng = random.Random(42)
        lines = []
        current_size = 0
        target_bytes = target_size_mb * 1024 * 1024
        word_id = 0
        while current_size < target_bytes:
            length = rng.randint(2, 4)
            word = "".join(chr(rng.randint(0x4E00, 0x9FFF)) for _ in range(length))
            freq = rng.randint(1, 50000)
            pos = rng.choice(["n", "v", "a", "d", "nr", "ns", "nt", "nz"])
            line = f"{word} {freq} {pos}\n"
            lines.append(line)
            current_size += len(line.encode("utf-8"))
            word_id += 1
        return "".join(lines)

    @staticmethod
    def _generate_large_stopwords(target_size_mb):
        """Generate a stop words file of approximately target_size_mb."""
        import random

        rng = random.Random(42)
        chunks = []
        current_size = 0
        target_bytes = target_size_mb * 1024 * 1024
        chunk_lines = []
        flush_every = 10000
        while current_size < target_bytes:
            length = rng.randint(1, 2)
            word = "".join(chr(rng.randint(0x4E00, 0x9FFF)) for _ in range(length))
            line = f"{word}\n"
            chunk_lines.append(line)
            current_size += len(line.encode("utf-8"))
            if len(chunk_lines) >= flush_every:
                chunks.append("".join(chunk_lines))
                chunk_lines = []
        if chunk_lines:
            chunks.append("".join(chunk_lines))
        return "".join(chunks)

    def _upload_and_add(self, client, bucket, remote_path, content):
        """Upload content to MinIO and register for teardown cleanup."""
        self._minio_objects_to_cleanup.append((bucket, remote_path))
        content_bytes = content.encode("utf-8")
        self._minio_client.put_object(
            bucket, remote_path, io.BytesIO(content_bytes), len(content_bytes)
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_large_jieba_dict_5mb(self, file_resource_env):
        """
        target: add and use a ~5MB jieba dictionary
        method: generate ~5MB dict -> upload -> add -> create collection -> search
        expected: add/create/search all succeed
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        remote_path = "large_test/jieba_5mb.txt"
        # 1. generate and upload large dict
        content = self._generate_large_jieba_dict(5)
        self._upload_and_add(client, bucket, remote_path, content)
        # 2. add file resource
        self.add_file_resource(client, res_name, remote_path)
        # 3. create collection with jieba analyzer
        analyzer_params = {
            "tokenizer": {
                "type": "jieba",
                "extra_dict_file": {
                    "type": "remote",
                    "resource_name": res_name,
                    "file_name": "jieba_5mb.txt",
                },
            }
        }
        self.create_bm25_collection(client, col_name, analyzer_params)
        # 4. insert and search
        docs = [
            "向量数据库是新一代的数据管理系统",
            "语义搜索可以理解用户意图",
            "全文检索是传统的文本搜索方式",
        ]
        self.insert_and_build_bm25(client, col_name, docs)
        results, _ = self.search(
            client,
            col_name,
            data=["数据管理"],
            anns_field="sparse_vector",
            limit=10,
            output_fields=["text"],
        )
        assert len(results) > 0 and len(results[0]) > 0, (
            "Search with 5MB dict should return results"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_large_jieba_dict_50mb(self, file_resource_env):
        """
        target: stress test with ~50MB jieba dictionary
        method: generate ~50MB dict -> upload -> add -> create collection -> search
        expected: add/create/search all succeed
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        remote_path = "large_test/jieba_50mb.txt"
        # 1. generate and upload large dict
        content = self._generate_large_jieba_dict(50)
        self._upload_and_add(client, bucket, remote_path, content)
        # 2. add file resource
        self.add_file_resource(client, res_name, remote_path)
        # 3. create collection with jieba analyzer
        analyzer_params = {
            "tokenizer": {
                "type": "jieba",
                "extra_dict_file": {
                    "type": "remote",
                    "resource_name": res_name,
                    "file_name": "jieba_50mb.txt",
                },
            }
        }
        self.create_bm25_collection(client, col_name, analyzer_params)
        # 4. insert and search
        docs = [
            "向量数据库是新一代的数据管理系统",
            "语义搜索可以理解用户意图",
        ]
        self.insert_and_build_bm25(client, col_name, docs)
        results, _ = self.search(
            client,
            col_name,
            data=["数据管理"],
            anns_field="sparse_vector",
            limit=10,
            output_fields=["text"],
        )
        assert len(results) > 0 and len(results[0]) > 0, (
            "Search with 50MB dict should return results"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_large_stopwords_5mb(self, file_resource_env):
        """
        target: add and use a ~5MB stop words file
        method: generate ~5MB stop words -> upload -> add -> create collection -> verify
        expected: add/create succeed, stop words filtering works
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        remote_path = "large_test/stopwords_5mb.txt"
        # 1. generate and upload large stop words
        real_stops = "的\n是\n在\n了\n和\n"
        generated = self._generate_large_stopwords(5)
        content = real_stops + generated
        self._upload_and_add(client, bucket, remote_path, content)
        # 2. add file resource
        self.add_file_resource(client, res_name, remote_path)
        # 3. create collection with stop words filter
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "jieba",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": res_name,
                            "file_name": "stopwords_5mb.txt",
                        },
                    }
                ],
            },
        )
        self.build_and_load_bm25(client, col_name)
        # 4. verify stop words are filtered
        text = "这是一个在测试的文本和数据在这里了"
        res, _ = self.run_analyzer(
            client, text, None, collection_name=col_name, field_name="text"
        )
        tokens = set(res.tokens)
        stop_words = {"的", "是", "在", "了", "和"}
        found = stop_words & tokens
        assert len(found) == 0, (
            f"Stop words should be filtered with 5MB file, but found {found}"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_large_stopwords_50mb(self, file_resource_env):
        """
        target: stress test with ~50MB stop words file
        method: generate ~50MB stop words -> upload -> add -> create collection -> verify
        expected: add/create succeed, stop words filtering works
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        remote_path = "large_test/stopwords_50mb.txt"
        # 1. generate and upload large stop words
        real_stops = "的\n是\n在\n了\n和\n"
        generated = self._generate_large_stopwords(50)
        content = real_stops + generated
        self._upload_and_add(client, bucket, remote_path, content)
        # 2. add file resource
        self.add_file_resource(client, res_name, remote_path)
        # 3. create collection with stop words filter
        self.create_bm25_collection(
            client,
            col_name,
            {
                "tokenizer": "jieba",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": res_name,
                            "file_name": "stopwords_50mb.txt",
                        },
                    }
                ],
            },
        )
        self.build_and_load_bm25(client, col_name)
        # 4. verify stop words are filtered
        text = "这是一个在测试的文本和数据在这里了"
        res, _ = self.run_analyzer(
            client, text, None, collection_name=col_name, field_name="text"
        )
        tokens = set(res.tokens)
        stop_words = {"的", "是", "在", "了", "和"}
        found = stop_words & tokens
        assert len(found) == 0, (
            f"Stop words should be filtered with 50MB file, but found {found}"
        )


class TestMilvusClientFileResourceUpdate(FileResourceTestBase):
    """Test case of resource update scenarios"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_readd_different_path(self, file_resource_env):
        """
        target: remove then re-add with a different path
        method: add -> remove -> add with different path
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        # 1. add file resource
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        # 2. remove
        self.remove_file_resource(client, res_name)
        # 3. re-add with different path
        self.add_file_resource(client, res_name, SYNONYMS_PATH)

    @pytest.mark.tags(CaseLabel.L2)
    def test_overwrite_file_content_same_path(self, file_resource_env):
        """
        target: overwrite MinIO file then re-add (same name+path)
        method: upload v1 -> add -> upload v2 -> add again (idempotent)
        expected: no error
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        remote_path = "update_test/custom_stop.txt"
        # 1. upload v1, register MinIO cleanup, and add
        self._minio_objects_to_cleanup.append((bucket, remote_path))
        content1 = "的\n是\n"
        content1_bytes = content1.encode("utf-8")
        self._minio_client.put_object(
            bucket, remote_path, io.BytesIO(content1_bytes), len(content1_bytes)
        )
        self.add_file_resource(client, res_name, remote_path)
        # 2. overwrite with v2 and re-add (idempotent)
        content2 = "的\n是\n在\n了\n和\n"
        content2_bytes = content2.encode("utf-8")
        self._minio_client.put_object(
            bucket, remote_path, io.BytesIO(content2_bytes), len(content2_bytes)
        )
        self.add_file_resource(client, res_name, remote_path)
