import io
import threading
import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, Function, FunctionType

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

    @pytest.fixture(autouse=True)
    def _force_clean_stale_resources(self, request):
        """Before each test, sweep any leftover file resources from earlier runs.

        Server has a known bug where a resource whose MinIO object is missing
        blocks every subsequent add/remove with 'node sync failed' until it is
        finally purged. Retrying removal a few times usually drains it.
        """
        host = request.config.getoption("--host")
        port = request.config.getoption("--port")
        user = request.config.getoption("--user")
        password = request.config.getoption("--password")
        try:
            from pymilvus import MilvusClient

            sweeper = MilvusClient(
                uri=f"http://{host}:{port}", user=user, password=password, timeout=10
            )
            for _ in range(5):
                leftovers = sweeper.list_file_resources()
                if not leftovers:
                    break
                for r in leftovers:
                    try:
                        sweeper.remove_file_resource(name=r.name)
                    except Exception:
                        pass
            sweeper.close()
        except Exception:
            # Non-fatal — real connectivity issues will surface in the actual test.
            pass
        yield

    def setup_method(self, method):
        super().setup_method(method)
        self._file_resources_to_cleanup = []
        self._minio_objects_to_cleanup = []
        self._teardown_client = None

    def teardown_method(self, method):
        # Phase 1: drop collections first (releases file resource references)
        super().teardown_method(method)
        # Phase 2: clean up file resources via MilvusClient independent gRPC channel.
        # Retry a few times: server has a known bug where remove returns
        # "node sync failed" on first call but succeeds on a subsequent attempt.
        client = self._teardown_client
        if client and self._file_resources_to_cleanup:
            pending = list(self._file_resources_to_cleanup)
            for _ in range(3):
                if not pending:
                    break
                still = []
                for name in pending:
                    try:
                        client.remove_file_resource(name=name)
                    except Exception:
                        still.append(name)
                pending = still
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

    def create_bm25_collection_with_stop_filter(self, client, col_name, res_name, **kwargs):
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
            index_name="sparse_vector",
        )
        self.create_index(client, col_name, index_params)
        # Poll until the sparse index finishes building before loading.
        assert self.wait_for_index_ready(client, col_name, "sparse_vector"), (
            f"sparse_vector index not ready within timeout for {col_name}"
        )
        self.load_collection(client, col_name)

    def insert_and_build_bm25(self, client, col_name, texts):
        """Insert rows, flush to make segments visible, then build and load."""
        data = [{"text": t} for t in texts]
        self.insert(client, col_name, data)
        # flush is synchronous and waits for segments to be persisted —
        # replaces the previous arbitrary time.sleep(2).
        self.flush(client, col_name)
        self.build_and_load_bm25(client, col_name)

    def wait_until(self, condition, timeout=30, interval=1):
        """Poll `condition` callable until it returns truthy or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return True
            time.sleep(interval)
        return False


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
    def test_remove_idempotent_double_remove(self, file_resource_env):
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
        self.remove_file_resource(client, res_name, check_task=CheckTasks.err_res, check_items=error)

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
        target: removed resources no longer appear in list
        method: add 2 resources -> remove both -> list
        expected: neither name appears in the list (concurrent-safe)
        """
        client = self._client()
        # 1. add 2 resources
        names = [cf.gen_unique_str(prefix) for _ in range(2)]
        for n in names:
            self.add_file_resource(client, n, JIEBA_DICT_PATH)
        # 2. remove them
        for n in names:
            self.remove_file_resource(client, n)
        # 3. verify neither name appears in list
        res, _ = self.list_file_resources(client)
        listed = {r.name for r in res}
        for n in names:
            assert n not in listed, f"{n} should not appear after removal"

    @pytest.mark.skip(reason="https://github.com/milvus-io/milvus/issues/48612")
    @pytest.mark.tags(CaseLabel.L1)
    def test_list_force_cleanup_all_resources(self, file_resource_env):
        """
        target: reproduce panic when force-removing in-use resources under concurrency
        method: list all resources -> force remove each (including those in use by
                other parallel workers) -> list again
        expected: with -n 6, removing in-use resources may trigger server panic
        """
        client = self._client()
        # 1. force remove all existing resources (may fail for in-use ones)
        res, _ = self.list_file_resources(client)
        for r in res:
            try:
                client.remove_file_resource(name=r.name)
            except Exception:
                pass
        # 2. list remaining resources (may not be empty under concurrency)
        res, _ = self.list_file_resources(client)

    @pytest.mark.tags(CaseLabel.L1)
    def test_list_after_add(self, file_resource_env):
        """
        target: list shows added resources
        method: add 3 resources -> list
        expected: all 3 names present
        """
        client = self._client()
        # 1. add 3 resources
        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        for n, p in zip(names, paths):
            self.add_file_resource(client, n, p)
        # 2. list and verify
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
        # 1. add 3 resources
        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        for n, p in zip(names, paths):
            self.add_file_resource(client, n, p)
        # 2. remove the first one
        self.remove_file_resource(client, names[0])
        # 3. list and verify
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
        self.create_bm25_collection(client, col_name, self._jieba_analyzer_params(res_name))
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
        assert len(results) > 0 and len(results[0]) > 0, "Search should return at least one result"
        top_text = results[0][0]["entity"]["text"]
        assert "向量数据库" in top_text, f"Top hit should contain '向量数据库', got '{top_text}'"

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
        self.create_bm25_collection(client, col_name, self._jieba_analyzer_params(res_name))
        self.build_and_load_bm25(client, col_name)
        # 3. verify each custom word is recognized as a single token
        test_cases = [
            ("向量数据库是新一代的数据管理系统", "向量数据库"),
            ("语义搜索可以理解用户意图", "语义搜索"),
            ("全文检索是传统的文本搜索方式", "全文检索"),
        ]
        for text, expected_token in test_cases:
            res, _ = self.run_analyzer(client, text, None, collection_name=col_name, field_name="text")
            tokens = list(res.tokens)
            assert expected_token in tokens, f"Custom dict word '{expected_token}' should be a token, got {tokens}"

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
        self.create_bm25_collection(client, col_name, self._synonym_analyzer_params(res_name, expand=True))
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
        assert len(results) > 0 and len(results[0]) > 0, "Synonym search should return results"
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
        self.create_bm25_collection(client, col_name, self._synonym_analyzer_params(res_name, expand=True))
        self.build_and_load_bm25(client, col_name)
        # 3. verify "搜索" expands to all synonyms
        res, _ = self.run_analyzer(client, "搜索", None, collection_name=col_name, field_name="text")
        tokens = set(res.tokens)
        expected_synonyms = {"搜索", "检索", "查询"}
        assert expected_synonyms.issubset(tokens), (
            f"expand=true on '搜索' should produce {expected_synonyms}, got {tokens}"
        )
        # 4. verify "向量" expands to all synonyms
        res, _ = self.run_analyzer(client, "向量", None, collection_name=col_name, field_name="text")
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
        self.create_bm25_collection(client, col_name, self._synonym_analyzer_params(res_name, expand=False))
        self.build_and_load_bm25(client, col_name)
        # 3. verify "检索" maps to canonical "搜索"
        res, _ = self.run_analyzer(client, "检索", None, collection_name=col_name, field_name="text")
        tokens = list(res.tokens)
        assert tokens == ["搜索"], f"expand=false: '检索' should map to ['搜索'], got {tokens}"
        # 4. verify "查询" maps to canonical "搜索"
        res, _ = self.run_analyzer(client, "查询", None, collection_name=col_name, field_name="text")
        tokens = list(res.tokens)
        assert tokens == ["搜索"], f"expand=false: '查询' should map to ['搜索'], got {tokens}"
        # 5. verify "矢量" maps to canonical "向量"
        res, _ = self.run_analyzer(client, "矢量", None, collection_name=col_name, field_name="text")
        tokens = list(res.tokens)
        assert tokens == ["向量"], f"expand=false: '矢量' should map to ['向量'], got {tokens}"
        # 6. verify canonical "搜索" remains unchanged
        res, _ = self.run_analyzer(client, "搜索", None, collection_name=col_name, field_name="text")
        tokens = list(res.tokens)
        assert tokens == ["搜索"], f"expand=false: canonical '搜索' should stay ['搜索'], got {tokens}"


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
        res, _ = self.run_analyzer(client, text, None, collection_name=col_name, field_name="text")
        tokens_filtered = set(res.tokens)
        found = stop_words & tokens_filtered
        assert len(found) == 0, f"Stop words should be filtered, but found {found} in {tokens_filtered}"
        # 4. verify baseline (no filter) contains stop words
        res_baseline, _ = self.run_analyzer(client, text, {"tokenizer": "jieba"})
        tokens_baseline = set(res_baseline.tokens)
        baseline_stop = stop_words & tokens_baseline
        assert len(baseline_stop) > 0, f"Baseline (no filter) should contain stop words, but got {tokens_baseline}"
        # 5. verify content tokens still present after filtering
        content_tokens = tokens_filtered - stop_words
        assert len(content_tokens) > 0, f"Filtered output should still have content tokens, got {tokens_filtered}"


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
            assert expected in tokens, f"Decompounder should produce '{expected}', got {tokens}"
        # 4. verify compound words are replaced
        assert "banknote" not in tokens, f"Compound 'banknote' should be decomposed, got {tokens}"
        assert "firework" not in tokens, f"Compound 'firework' should be decomposed, got {tokens}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_decompounder_baseline_without_filter(self, file_resource_env):
        """
        target: without decompounder filter, compound words remain intact
        method: run_analyzer with standard tokenizer only
        expected: compound words are single tokens
        """
        client = self._client()
        # 1. verify compound words remain intact without decompounder
        res, _ = self.run_analyzer(client, "banknote firework", {"tokenizer": "standard"})
        tokens = list(res.tokens)
        assert "banknote" in tokens, f"Without decompounder, 'banknote' should be a single token, got {tokens}"
        assert "firework" in tokens, f"Without decompounder, 'firework' should be a single token, got {tokens}"
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
        self.remove_file_resource(client, res_name, check_task=CheckTasks.err_res, check_items=error)

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
        # 3. drop one collection (drop_collection is synchronous at meta layer)
        self.drop_collection(client, col1)
        # 4. remove should still fail (col2 still references)
        error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
        self.remove_file_resource(client, res_name, check_task=CheckTasks.err_res, check_items=error)

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

        # 3. remove should succeed — poll until async refcount release lands.
        def _remove_ok():
            try:
                self.remove_file_resource(client, res_name)
                return True
            except Exception as exc:
                if "is still in use" in str(exc):
                    return False
                raise

        assert self.wait_until(_remove_ok, timeout=30, interval=1), (
            f"{res_name} still marked in-use after all referencing collections dropped"
        )


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
        self._minio_client.put_object(bucket, remote_path, io.BytesIO(content_bytes), len(content_bytes))

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
        assert len(results) > 0 and len(results[0]) > 0, "Search with 5MB dict should return results"

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
        assert len(results) > 0 and len(results[0]) > 0, "Search with 50MB dict should return results"

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
        res, _ = self.run_analyzer(client, text, None, collection_name=col_name, field_name="text")
        tokens = set(res.tokens)
        stop_words = {"的", "是", "在", "了", "和"}
        found = stop_words & tokens
        assert len(found) == 0, f"Stop words should be filtered with 5MB file, but found {found}"

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
        res, _ = self.run_analyzer(client, text, None, collection_name=col_name, field_name="text")
        tokens = set(res.tokens)
        stop_words = {"的", "是", "在", "了", "和"}
        found = stop_words & tokens
        assert len(found) == 0, f"Stop words should be filtered with 50MB file, but found {found}"


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
        target: overwrite MinIO file then re-add (same name+path) and confirm
                the new content is actually picked up by BM25 tokenization.
        method: upload v1 -> add -> create col_v1 -> run_analyzer (v1 stop set)
                -> upload v2 -> re-add -> create col_v2 -> run_analyzer (v2 set)
        expected: col_v1 filters only v1 stop words; col_v2 also filters the
                  v2-only words (在/了/和) proving the overwrite took effect
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        remote_path = "update_test/custom_stop.txt"
        text = "这是一个在测试的文本和数据在这里了"
        v1_only_stop = {"的", "是"}
        v2_only_stop = {"在", "了", "和"}
        # analyzer_params factory — references the remote resource by name.
        analyzer_params = {
            "tokenizer": "jieba",
            "filter": [
                {
                    "type": "stop",
                    "stop_words_file": {
                        "type": "remote",
                        "resource_name": res_name,
                        "file_name": "custom_stop.txt",
                    },
                }
            ],
        }

        # 1. upload v1 (only 的/是) and add resource
        self._minio_objects_to_cleanup.append((bucket, remote_path))
        content1_bytes = "的\n是\n".encode()
        self._minio_client.put_object(bucket, remote_path, io.BytesIO(content1_bytes), len(content1_bytes))
        self.add_file_resource(client, res_name, remote_path)

        # 2. first collection uses v1 content — 在/了/和 should still appear as tokens
        col_v1 = cf.gen_unique_str(prefix)
        self.create_bm25_collection(client, col_v1, analyzer_params)
        res_v1, _ = self.run_analyzer(client, text, None, collection_name=col_v1, field_name="text")
        tokens_v1 = set(res_v1.tokens)
        assert not (v1_only_stop & tokens_v1), f"v1 stop words should be filtered, got {tokens_v1}"
        assert v2_only_stop & tokens_v1, (
            f"v2-only stop words should NOT be filtered under v1 content, tokens={tokens_v1}"
        )

        # 3. overwrite MinIO object with v2 (adds 在/了/和) and re-add (idempotent)
        content2_bytes = "的\n是\n在\n了\n和\n".encode()
        self._minio_client.put_object(bucket, remote_path, io.BytesIO(content2_bytes), len(content2_bytes))
        self.add_file_resource(client, res_name, remote_path)

        # 4. fresh collection should read v2 content — 在/了/和 now filtered too
        col_v2 = cf.gen_unique_str(prefix)
        self.create_bm25_collection(client, col_v2, analyzer_params)
        res_v2, _ = self.run_analyzer(client, text, None, collection_name=col_v2, field_name="text")
        tokens_v2 = set(res_v2.tokens)
        all_stop = v1_only_stop | v2_only_stop
        assert not (all_stop & tokens_v2), f"All stop words (v1+v2) should be filtered after overwrite, got {tokens_v2}"


# ---------------------------------------------------------------------------
# A. Name / Path / Content boundary cases
# ---------------------------------------------------------------------------
class TestMilvusClientFileResourceNameBoundary(FileResourceTestBase):
    """Name-dimension boundary cases.

    Server has no explicit length/charset validation on resource name —
    we verify observed behavior here (accept or reject) to lock it down.
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_name_long_255_chars(self, file_resource_env):
        """
        target: 255-char resource name works end to end
        method: pad a unique prefix to exactly 255 chars -> add -> list -> remove
        expected: no error
        """
        client = self._client()
        base = cf.gen_unique_str(prefix)
        res_name = base + "a" * (255 - len(base))
        assert len(res_name) == 255
        self.add_file_resource(client, res_name, JIEBA_DICT_PATH)
        res, _ = self.list_file_resources(client)
        assert res_name in {r.name for r in res}
        self.remove_file_resource(client, res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_name_special_characters(self, file_resource_env):
        """
        target: non-ASCII and punctuation characters accepted in resource name
        method: add names with cn chars, dashes, dots, underscores
        expected: each name treated as distinct, round-trip through list
        """
        client = self._client()
        base = cf.gen_unique_str(prefix)
        names = [
            f"{base}-dash",
            f"{base}.dot",
            f"{base}_under",
            f"{base}中文",
        ]
        for n in names:
            self.add_file_resource(client, n, JIEBA_DICT_PATH)
        res, _ = self.list_file_resources(client)
        listed = {r.name for r in res}
        for n in names:
            assert n in listed, f"{n!r} missing from list {listed}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_name_case_sensitivity(self, file_resource_env):
        """
        target: resource names are case-sensitive — upper/lower treated as distinct
        method: add `Foo` and `foo` separately, both should succeed
        expected: both exist simultaneously in list
        """
        client = self._client()
        base = cf.gen_unique_str(prefix)
        lower = f"{base}_case"
        upper = lower.upper()
        assert lower != upper
        self.add_file_resource(client, lower, JIEBA_DICT_PATH)
        self.add_file_resource(client, upper, JIEBA_DICT_PATH)
        res, _ = self.list_file_resources(client)
        listed = {r.name for r in res}
        assert lower in listed and upper in listed, f"case-sensitive names both missing, listed={listed}"


class TestMilvusClientFileResourcePathBoundary(FileResourceTestBase):
    """Path-dimension boundary cases.

    Server validates only Exist(path) on MinIO — no ../ stripping,
    no absolute/relative rules, path is used as raw object key.
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_path_with_dotdot_rejected(self, file_resource_env):
        """
        target: path containing `..` is not accepted — server either returns
                path-not-exist or MinIO rejects the literal key (400 Bad Request)
        method: add with 'foo/../jieba/jieba_dict.txt'
        expected: raise Exception (either "path not exist" or "IO failed")
        """
        client = self._client()
        error = {ct.err_code: 1001, ct.err_msg: "IO failed"}
        self.add_file_resource(
            client,
            cf.gen_unique_str(prefix),
            "foo/../" + JIEBA_DICT_PATH,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_path_deeply_nested(self, file_resource_env):
        """
        target: deeply nested path works when object exists at that key
        method: upload to nested path, add, then remove — verify round-trip
        expected: no error
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        nested = "a/b/c/d/e/f/deep_dict.txt"
        self._minio_objects_to_cleanup.append((bucket, nested))
        payload = b"deeply nested word 5 n\n"
        self._minio_client.put_object(bucket, nested, io.BytesIO(payload), len(payload))
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, nested)
        self.remove_file_resource(client, res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_path_with_spaces(self, file_resource_env):
        """
        target: whitespace in path is preserved as literal object key
        method: upload to 'with space/dict file.txt', add, remove
        expected: no error
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        remote = "with space/dict file.txt"
        self._minio_objects_to_cleanup.append((bucket, remote))
        payload = b"word 5 n\n"
        self._minio_client.put_object(bucket, remote, io.BytesIO(payload), len(payload))
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, remote)
        self.remove_file_resource(client, res_name)


class TestMilvusClientFileResourceContent(FileResourceTestBase):
    """File-content boundary cases (encoding / BOM / empty).

    Server does not read or validate content — add just checks existence.
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_empty_file_accepted(self, file_resource_env):
        """
        target: empty file (0 bytes) can be registered as a file resource
        method: put empty object -> add -> list -> remove
        expected: add succeeds
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        remote = "empty/zero.txt"
        self._minio_objects_to_cleanup.append((bucket, remote))
        self._minio_client.put_object(bucket, remote, io.BytesIO(b""), 0)
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, remote)
        res, _ = self.list_file_resources(client)
        assert res_name in {r.name for r in res}

    @pytest.mark.tags(CaseLabel.L2)
    def test_non_utf8_encoded_file_accepted(self, file_resource_env):
        """
        target: non-UTF-8 (GBK) bytes are accepted at add time
        method: upload GBK bytes -> add -> remove
        expected: add succeeds (server does not validate encoding on add)
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        remote = "encoding/gbk_stop.txt"
        self._minio_objects_to_cleanup.append((bucket, remote))
        payload = "的\n是\n".encode("gbk")
        self._minio_client.put_object(bucket, remote, io.BytesIO(payload), len(payload))
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, remote)
        self.remove_file_resource(client, res_name)

    @pytest.mark.xfail(
        reason="Known: UTF-8 BOM is not stripped from first line of stop-words file, "
        "so the first stop word (`\\ufeff的`) fails to match `的` and leaks through.",
        strict=False,
    )
    @pytest.mark.tags(CaseLabel.L2)
    def test_file_with_bom_and_crlf(self, file_resource_env):
        """
        target: BOM + CRLF line endings are tolerated when used by BM25 analyzer
        method: upload stop-words with UTF-8 BOM + CRLF, run_analyzer inline
                (no collection) to verify tokenization skips BOM prefix
        expected: stop word filter still filters 的/是 (BOM does not leak into dict)
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        remote = "encoding/bom_crlf_stop.txt"
        self._minio_objects_to_cleanup.append((bucket, remote))
        payload = "﻿的\r\n是\r\n".encode()
        self._minio_client.put_object(bucket, remote, io.BytesIO(payload), len(payload))
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, remote)

        analyzer_params = {
            "tokenizer": "jieba",
            "filter": [
                {
                    "type": "stop",
                    "stop_words_file": {
                        "type": "remote",
                        "resource_name": res_name,
                        "file_name": "bom_crlf_stop.txt",
                    },
                }
            ],
        }
        res, _ = self.run_analyzer(client, "这是的测试", analyzer_params)
        tokens = set(res.tokens)
        assert "的" not in tokens and "是" not in tokens, (
            f"BOM/CRLF stop words should still be stripped, got tokens={tokens}"
        )


# ---------------------------------------------------------------------------
# B. Advanced lifecycle — multi-resource, multi-reference, MinIO side-effects
# ---------------------------------------------------------------------------
class TestMilvusClientFileResourceLifecycleAdvanced(FileResourceTestBase):
    """Reference-counting and lifecycle scenarios beyond the basic RefCount suite."""

    @pytest.mark.xfail(
        reason="Known server bug: combining remote jieba dict + stop + synonym filters "
        "can panic the datanode — tokenizer.h:31 assert `res.result_->success` fails "
        "with 'No such file or directory' when the analyzer loads before file sync completes.",
        strict=False,
    )
    @pytest.mark.tags(CaseLabel.L1)
    def test_multi_resource_same_collection(self, file_resource_env):
        """
        target: one collection can reference 4 file resources simultaneously
        method: create BM25 collection with jieba dict + stop + synonym + decompounder
                -> drop collection -> all 4 resources become removable
        expected: no "is still in use" after drop, all 4 removable
        """
        client = self._client()
        jieba_name = cf.gen_unique_str(prefix + "_jieba")
        stop_name = cf.gen_unique_str(prefix + "_stop")
        synonym_name = cf.gen_unique_str(prefix + "_syn")
        decomp_name = cf.gen_unique_str(prefix + "_decomp")
        self.add_file_resource(client, jieba_name, JIEBA_DICT_PATH)
        self.add_file_resource(client, stop_name, STOPWORDS_PATH)
        self.add_file_resource(client, synonym_name, SYNONYMS_PATH)
        self.add_file_resource(client, decomp_name, DECOMPOUNDER_PATH)

        col = cf.gen_unique_str(prefix)
        analyzer_params = {
            "tokenizer": {
                "type": "jieba",
                "extra_dict_file": {
                    "type": "remote",
                    "resource_name": jieba_name,
                    "file_name": "jieba_dict.txt",
                },
            },
            "filter": [
                {
                    "type": "stop",
                    "stop_words_file": {
                        "type": "remote",
                        "resource_name": stop_name,
                        "file_name": "stop_words.txt",
                    },
                },
                {
                    "type": "synonym",
                    "synonyms_file": {
                        "type": "remote",
                        "resource_name": synonym_name,
                        "file_name": "synonyms.txt",
                    },
                },
            ],
        }
        self.create_bm25_collection(client, col, analyzer_params)

        # while referenced, none of the 4 can be removed
        error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
        for name in (jieba_name, stop_name, synonym_name):
            self.remove_file_resource(client, name, check_task=CheckTasks.err_res, check_items=error)

        # after drop all refs released
        self.drop_collection(client, col)
        for name in (jieba_name, stop_name, synonym_name, decomp_name):

            def _ok(n=name):
                try:
                    self.remove_file_resource(client, n)
                    return True
                except Exception as exc:
                    if "is still in use" in str(exc):
                        return False
                    raise

            assert self.wait_until(_ok, timeout=30, interval=1), f"{name} still in-use after collection drop"

    @pytest.mark.tags(CaseLabel.L2)
    def test_same_resource_multi_collection(self, file_resource_env):
        """
        target: same resource referenced by N collections — remove requires all dropped
        method: add 1 resource -> create 3 collections using it -> drop 2 -> remove
                should still fail -> drop 3rd -> remove succeeds
        expected: refcount tracks collection-level references
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, STOPWORDS_PATH)

        cols = [cf.gen_unique_str(prefix) for _ in range(3)]
        for c in cols:
            self.create_bm25_collection_with_stop_filter(client, c, res_name)

        # drop two, one still references -> remove must fail
        self.drop_collection(client, cols[0])
        self.drop_collection(client, cols[1])
        error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
        self.remove_file_resource(client, res_name, check_task=CheckTasks.err_res, check_items=error)

        # drop the last -> remove eventually succeeds
        self.drop_collection(client, cols[2])

        def _ok():
            try:
                self.remove_file_resource(client, res_name)
                return True
            except Exception as exc:
                if "is still in use" in str(exc):
                    return False
                raise

        assert self.wait_until(_ok, timeout=30, interval=1), (
            f"{res_name} still in use after all 3 collections dropped"
        )

    @pytest.mark.xfail(
        reason="Known server bug: deleting a MinIO object that an active file "
        "resource points to puts the resource meta into a state where every "
        "subsequent add/remove_file_resource returns 'node sync failed', "
        "cascading failures across the whole instance until the orphan is "
        "purged manually. This test also leaves the orphan behind so it is "
        "skipped in the normal suite.",
        strict=False,
    )
    @pytest.mark.tags(CaseLabel.L2)
    def test_minio_object_deleted_while_loaded(self, file_resource_env):
        """
        target: deleting MinIO object after collection is loaded does not break
                in-memory analyzer — server cached file locally
        method: add resource -> build & load collection with BM25 stop filter
                -> analyze -> delete MinIO object -> analyze again on the same
                loaded collection
        expected: second analyze still returns filtered tokens (cached)
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        remote = "cache_test/stop.txt"
        self._minio_objects_to_cleanup.append((bucket, remote))
        payload = "的\n是\n".encode()
        self._minio_client.put_object(bucket, remote, io.BytesIO(payload), len(payload))
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, remote)

        col = cf.gen_unique_str(prefix)
        self.create_bm25_collection(
            client,
            col,
            {
                "tokenizer": "standard",
                "filter": [
                    {
                        "type": "stop",
                        "stop_words_file": {
                            "type": "remote",
                            "resource_name": res_name,
                            "file_name": "stop.txt",
                        },
                    }
                ],
            },
        )
        self.build_and_load_bm25(client, col)
        text = "这是的测试文本"
        res_before, _ = self.run_analyzer(client, text, None, collection_name=col, field_name="text")
        tokens_before = set(res_before.tokens)

        # delete underlying MinIO object
        self._minio_client.remove_object(bucket, remote)

        # run_analyzer on the same (already-loaded) collection — cached
        res_after, _ = self.run_analyzer(client, text, None, collection_name=col, field_name="text")
        tokens_after = set(res_after.tokens)
        assert tokens_before == tokens_after, (
            f"analyzer output should be stable with cached file; before={tokens_before} after={tokens_after}"
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_drop_database_releases_refcount(self, file_resource_env):
        """
        target: drop database cascades collection drops and releases file refs
        method: create db -> switch to it -> create collection using resource
                -> drop db -> resource becomes removable
        expected: removable after drop_database
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, STOPWORDS_PATH)

        db_name = cf.gen_unique_str(prefix + "_db")
        self.create_database(client, db_name)
        try:
            self.using_database(client, db_name)
            col = cf.gen_unique_str(prefix)
            self.create_bm25_collection_with_stop_filter(client, col, res_name)

            # refcount should block removal
            error = {ct.err_code: 65535, ct.err_msg: "is still in use"}
            self.remove_file_resource(client, res_name, check_task=CheckTasks.err_res, check_items=error)

            # drop collection in that db first (drop_database usually requires empty db)
            self.drop_collection(client, col)
            self.using_database(client, "default")
            self.drop_database(client, db_name)
        finally:
            self.using_database(client, "default")

        # removable after all refs gone
        def _ok():
            try:
                self.remove_file_resource(client, res_name)
                return True
            except Exception as exc:
                if "is still in use" in str(exc):
                    return False
                raise

        assert self.wait_until(_ok, timeout=30, interval=1), f"{res_name} still in-use after drop_database"


# ---------------------------------------------------------------------------
# C. run_analyzer runtime scenarios
# ---------------------------------------------------------------------------
class TestMilvusClientFileResourceRunAnalyzer(FileResourceTestBase):
    """run_analyzer scenarios that don't depend on an existing collection."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_run_analyzer_remote_without_collection(self, file_resource_env):
        """
        target: run_analyzer with remote stop-words file works without any collection
        method: add resource -> call run_analyzer with inline analyzer_params
                referencing the remote stop_words_file
        expected: tokens filtered per the stop list
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        self.add_file_resource(client, res_name, STOPWORDS_PATH)

        analyzer_params = {
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
        }
        text = "the apple 的 是 hello"
        res, _ = self.run_analyzer(client, text, analyzer_params)
        tokens = set(res.tokens)
        assert "的" not in tokens and "是" not in tokens, f"remote stop-words should have been applied, got {tokens}"

    @pytest.mark.xfail(
        reason="Known server bug: see TestMilvusClientFileResourceLifecycleAdvanced::"
        "test_multi_resource_same_collection — same tokenizer.h:31 panic.",
        strict=False,
    )
    @pytest.mark.tags(CaseLabel.L2)
    def test_combined_remote_filters(self, file_resource_env):
        """
        target: jieba(remote dict) + stop(remote) + synonym(remote) chained in one
                analyzer config all work together
        method: build analyzer_params combining 3 remote files, run_analyzer on
                a Chinese sentence
        expected: custom jieba word split out, stop words removed,
                  synonym filter applied (canonical form present)
        """
        client = self._client()
        jieba_name = cf.gen_unique_str(prefix + "_j")
        stop_name = cf.gen_unique_str(prefix + "_s")
        synonym_name = cf.gen_unique_str(prefix + "_y")
        self.add_file_resource(client, jieba_name, JIEBA_DICT_PATH)
        self.add_file_resource(client, stop_name, STOPWORDS_PATH)
        self.add_file_resource(client, synonym_name, SYNONYMS_PATH)

        analyzer_params = {
            "tokenizer": {
                "type": "jieba",
                "extra_dict_file": {
                    "type": "remote",
                    "resource_name": jieba_name,
                    "file_name": "jieba_dict.txt",
                },
            },
            "filter": [
                {
                    "type": "stop",
                    "stop_words_file": {
                        "type": "remote",
                        "resource_name": stop_name,
                        "file_name": "stop_words.txt",
                    },
                },
                {
                    "type": "synonym",
                    "synonyms_file": {
                        "type": "remote",
                        "resource_name": synonym_name,
                        "file_name": "synonyms.txt",
                    },
                },
            ],
        }
        # "向量数据库" — custom jieba word; "的" — stop; "向量" — synonym
        text = "向量数据库是向量搜索的基础"
        res, _ = self.run_analyzer(client, text, analyzer_params)
        tokens = set(res.tokens)
        # jieba custom tokenizer should emit the compound word
        assert "向量数据库" in tokens, f"custom jieba word missing, got {tokens}"
        # stop filter should drop 是/的
        assert "是" not in tokens and "的" not in tokens, f"stop words present: {tokens}"
