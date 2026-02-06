"""
FileResource E2E Tests

Test the FileResource feature which allows managing file resources (e.g. custom
jieba dictionaries, synonym tables, stop word lists) stored in MinIO/S3 and
referencing them in text analyzer configurations.

Classes:
    - TestFileResourceAdd: AddFileResource API tests
    - TestFileResourceRemove: RemoveFileResource API tests
    - TestFileResourceList: ListFileResources API tests
    - TestFileResourceJiebaAnalyzer: jieba tokenizer + remote dict integration
    - TestFileResourceSynonymAnalyzer: synonym filter + remote file integration
    - TestFileResourceStopWordsAnalyzer: stop words filter + remote file integration
    - TestFileResourceDecompounderAnalyzer: decompounder filter + remote file
    - TestFileResourceInvalidAnalyzerConfig: invalid remote file configs
    - TestFileResourceRefCount: reference counting & deletion protection
    - TestFileResourceSyncCheck: synchronization during collection creation
    - TestFileResourceIdempotency: idempotent add / remove operations
    - TestFileResourceConcurrency: concurrent operations
    - TestFileResourceUpdate: resource update scenarios

Known issues:
    - ListFileResources may return empty list on master (PR #47568 not merged).
      Tests relying on list are marked accordingly.
"""

import io
import time
import threading

import pytest
from pymilvus import DataType, Function, FunctionType, MilvusException

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

    def create_bm25_collection(self, client, col_name, analyzer_params):
        """Create a collection with a VARCHAR text field + BM25 function."""
        schema, _ = self.create_schema(client)
        self.add_field(schema, "id", DataType.INT64, is_primary=True, auto_id=True)
        self.add_field(schema, "text", DataType.VARCHAR, max_length=65535,
                       enable_analyzer=True, analyzer_params=analyzer_params)
        self.add_field(schema, "sparse_vector", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(Function(
            name="bm25",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["sparse_vector"],
        ))
        self.create_collection(client, col_name, schema=schema)

    def create_bm25_collection_with_stop_filter(self, client, col_name, res_name):
        """Shortcut: BM25 collection with a remote stop-words filter."""
        self.create_bm25_collection(client, col_name, {
            "tokenizer": "standard",
            "filter": [{
                "type": "stop",
                "stop_words_file": {
                    "type": "remote",
                    "resource_name": res_name,
                    "file_name": "stop_words.txt",
                }
            }]
        })

    def build_and_load_bm25(self, client, col_name):
        """Build sparse index and load collection (no data insertion)."""
        index_params, _ = self.prepare_index_params(client)
        index_params.add_index(field_name="sparse_vector",
                               index_type="SPARSE_INVERTED_INDEX",
                               metric_type="BM25")
        self.create_index(client, col_name, index_params)
        self.load_collection(client, col_name)
        time.sleep(1)

    def insert_and_build_bm25(self, client, col_name, texts):
        """Insert rows, build sparse index, load collection."""
        data = [{"text": t} for t in texts]
        self.insert(client, col_name, data)
        time.sleep(2)
        self.build_and_load_bm25(client, col_name)


# ===================================================================
# Module 1 — CRUD API
# ===================================================================

class TestFileResourceAdd(FileResourceTestBase):
    """AddFileResource API tests"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_file_resource_valid(self, file_resource_env):
        """
        target: add a valid file resource
        method: upload file to MinIO then add_file_resource
        expected: no exception
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        # cleanup
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_file_resource_path_not_exist(self, file_resource_env):
        """
        target: add a file resource whose path does not exist in MinIO
        method: add_file_resource with non-existent path
        expected: MilvusException
        """
        client = self._client()
        with pytest.raises(MilvusException):
            client.add_file_resource(name=cf.gen_unique_str(prefix),
                                     path="not/exist.txt")

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_file_resource_idempotent_same_name_path(self, file_resource_env):
        """
        target: add is idempotent when name+path are identical
        method: call add_file_resource twice with same args
        expected: both succeed
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_file_resource_same_name_different_path(self, file_resource_env):
        """
        target: add with same name but different path should fail
        method: add(name="x", path="a") then add(name="x", path="b")
        expected: second call raises MilvusException
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        try:
            with pytest.raises(MilvusException):
                client.add_file_resource(name=res_name, path=SYNONYMS_PATH)
        finally:
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_file_resource_empty_name(self, file_resource_env):
        """
        target: add with empty name
        method: add_file_resource(name="", ...)
        expected: server may accept empty name (no validation); cleanup after
        """
        client = self._client()
        # Server does not reject empty name currently
        client.add_file_resource(name="", path=JIEBA_DICT_PATH)
        client.remove_file_resource(name="")

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_file_resource_empty_path(self, file_resource_env):
        """
        target: add with empty path
        method: add_file_resource(name=..., path="")
        expected: MilvusException
        """
        client = self._client()
        with pytest.raises(MilvusException):
            client.add_file_resource(name=cf.gen_unique_str(prefix), path="")

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
        for n, p in zip(names, paths):
            client.add_file_resource(name=n, path=p)
        for n in names:
            client.remove_file_resource(name=n)


class TestFileResourceRemove(FileResourceTestBase):
    """RemoveFileResource API tests"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_remove_existing_resource(self, file_resource_env):
        """
        target: remove an existing resource
        method: add then remove
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_nonexistent_resource(self, file_resource_env):
        """
        target: remove non-existent resource (idempotent)
        method: remove a name that was never added
        expected: no error
        """
        client = self._client()
        client.remove_file_resource(name=cf.gen_unique_str(prefix))

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_idempotent(self, file_resource_env):
        """
        target: double remove is idempotent
        method: add -> remove -> remove
        expected: both remove calls succeed
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_resource_in_use_by_collection(self, file_resource_env):
        """
        target: cannot remove a resource still referenced by a collection
        method: add -> create collection using it -> remove
        expected: MilvusException
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
            with pytest.raises(MilvusException):
                client.remove_file_resource(name=res_name)
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_then_readd_same_name(self, file_resource_env):
        """
        target: re-add with same name after removal
        method: add -> remove -> add (possibly different path)
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)
        client.add_file_resource(name=res_name, path=SYNONYMS_PATH)
        client.remove_file_resource(name=res_name)


class TestFileResourceList(FileResourceTestBase):
    """ListFileResources API tests

    NOTE: list may return empty on master before PR #47568 merges.
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_list_empty(self, file_resource_env):
        """
        target: list when no resources exist
        method: ensure clean state then call list
        expected: empty list
        """
        client = self._client()
        # best-effort cleanup
        for r in client.list_file_resources():
            try:
                client.remove_file_resource(name=r.name)
            except Exception:
                pass
        assert len(client.list_file_resources()) == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="Known bug: ListFileResources returns empty (PR #47568 not merged)")
    def test_list_after_add(self, file_resource_env):
        """
        target: list shows added resources
        method: add 3 resources -> list
        expected: all 3 names present
        """
        client = self._client()
        for r in client.list_file_resources():
            try:
                client.remove_file_resource(name=r.name)
            except Exception:
                pass

        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        for n, p in zip(names, paths):
            client.add_file_resource(name=n, path=p)

        listed = {r.name for r in client.list_file_resources()}
        for n in names:
            assert n in listed, f"{n} missing from list"

        for n in names:
            client.remove_file_resource(name=n)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="Known bug: ListFileResources returns empty (PR #47568 not merged)")
    def test_list_after_remove(self, file_resource_env):
        """
        target: list updates after removal
        method: add 3 -> remove 1 -> list
        expected: removed resource absent, other 2 present
        """
        client = self._client()
        for r in client.list_file_resources():
            try:
                client.remove_file_resource(name=r.name)
            except Exception:
                pass

        names = [cf.gen_unique_str(prefix) for _ in range(3)]
        paths = [JIEBA_DICT_PATH, SYNONYMS_PATH, STOPWORDS_PATH]
        for n, p in zip(names, paths):
            client.add_file_resource(name=n, path=p)

        client.remove_file_resource(name=names[0])

        listed = {r.name for r in client.list_file_resources()}
        assert names[0] not in listed
        assert names[1] in listed
        assert names[2] in listed

        for n in names[1:]:
            client.remove_file_resource(name=n)


# ===================================================================
# Module 2 — Analyzer integration
# ===================================================================

class TestFileResourceJiebaAnalyzer(FileResourceTestBase):
    """Jieba tokenizer + remote extra_dict_file

    Custom dict: 向量数据库/语义搜索/全文检索
    Default jieba does NOT produce "向量数据库" or "语义搜索" as single tokens.
    With the custom dict, all three should appear as single tokens.
    """

    def _jieba_analyzer_params(self, res_name):
        return {
            "tokenizer": {
                "type": "jieba",
                "extra_dict_file": {
                    "type": "remote",
                    "resource_name": res_name,
                    "file_name": "jieba_dict.txt",
                }
            }
        }

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_remote_dict_bm25_search(self, file_resource_env):
        """
        target: BM25 search with remote jieba dict returns correct top hit
        method: add resource -> create collection -> insert 4 docs -> search "向量数据库"
        expected: first hit is the doc containing "向量数据库"
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        try:
            self.create_bm25_collection(client, col_name,
                                        self._jieba_analyzer_params(res_name))
            docs = [
                "向量数据库是新一代的数据管理系统",
                "语义搜索可以理解用户意图",
                "全文检索是传统的文本搜索方式",
                "milvus 支持混合搜索",
            ]
            self.insert_and_build_bm25(client, col_name, docs)
            results, _ = self.search(client, col_name, data=["向量数据库"],
                                     anns_field="sparse_vector", limit=10,
                                     output_fields=["text"])
            assert len(results) > 0 and len(results[0]) > 0, \
                "Search should return at least one result"
            top_text = results[0][0]["entity"]["text"]
            assert "向量数据库" in top_text, \
                f"Top hit should contain '向量数据库', got '{top_text}'"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_remote_dict_all_custom_words(self, file_resource_env):
        """
        target: all custom dict words recognized as single tokens
        method: run_analyzer for each custom word via collection context
        expected: "向量数据库", "语义搜索", "全文检索" each appear as a single token
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        try:
            self.create_bm25_collection(client, col_name,
                                        self._jieba_analyzer_params(res_name))
            self.build_and_load_bm25(client, col_name)

            # Verify each custom word is recognized as a single token
            test_cases = [
                ("向量数据库是新一代的数据管理系统", "向量数据库"),
                ("语义搜索可以理解用户意图", "语义搜索"),
                ("全文检索是传统的文本搜索方式", "全文检索"),
            ]
            for text, expected_token in test_cases:
                res, _ = self.run_analyzer(client, text, None,
                                           collection_name=col_name,
                                           field_name="text")
                tokens = list(res.tokens)
                assert expected_token in tokens, \
                    f"Custom dict word '{expected_token}' should be a token, got {tokens}"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_jieba_default_vs_custom_comparison(self, file_resource_env):
        """
        target: compare default jieba vs custom dict tokenization
        method: run_analyzer with default jieba and verify custom words are NOT tokens,
                confirming that the custom dict is what makes the difference
        expected: "向量数据库" and "语义搜索" NOT in default jieba tokens
        """
        client = self._client()
        # Default jieba baseline: "向量数据库" should NOT be a single token
        res, _ = self.run_analyzer(client, "向量数据库是新一代",
                                   {"tokenizer": "jieba"})
        tokens_default = list(res.tokens)
        assert "向量数据库" not in tokens_default, \
            f"Default jieba should NOT produce '向量数据库' as token, got {tokens_default}"

        # Default jieba baseline: "语义搜索" should NOT be a single token
        res, _ = self.run_analyzer(client, "语义搜索可以理解",
                                   {"tokenizer": "jieba"})
        tokens_default2 = list(res.tokens)
        assert "语义搜索" not in tokens_default2, \
            f"Default jieba should NOT produce '语义搜索' as token, got {tokens_default2}"


class TestFileResourceSynonymAnalyzer(FileResourceTestBase):
    """Synonym filter + remote synonyms_file

    Synonym file rules:
      向量, 矢量, vector
      搜索, 检索, 查询
      数据库, DB
    """

    def _synonym_analyzer_params(self, res_name, expand=True):
        return {
            "tokenizer": "jieba",
            "filter": [{
                "type": "synonym",
                "expand": expand,
                "synonyms_file": {
                    "type": "remote",
                    "resource_name": res_name,
                    "file_name": "synonyms.txt",
                }
            }]
        }

    @pytest.mark.tags(CaseLabel.L1)
    def test_synonym_remote_file_search(self, file_resource_env):
        """
        target: synonym expansion enables cross-synonym search
        method: insert doc with "搜索", search with synonym "检索"
        expected: the doc containing "搜索" is in search results
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=SYNONYMS_PATH)
        try:
            self.create_bm25_collection(client, col_name,
                                        self._synonym_analyzer_params(res_name, expand=True))
            docs = [
                "搜索引擎可以帮助用户查找信息",
                "数据库管理系统支持SQL查询",
                "分布式系统的设计原则",
            ]
            self.insert_and_build_bm25(client, col_name, docs)
            results, _ = self.search(client, col_name, data=["检索"],
                                     anns_field="sparse_vector", limit=10,
                                     output_fields=["text"])
            assert len(results) > 0 and len(results[0]) > 0, \
                "Synonym search should return results"
            hit_texts = [r["entity"]["text"] for r in results[0]]
            assert any("搜索" in t for t in hit_texts), \
                f"Doc with '搜索' should match synonym query '检索', got {hit_texts}"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_synonym_expand_true_all_synonyms(self, file_resource_env):
        """
        target: expand=true produces all synonyms for each input token
        method: run_analyzer on "搜索" and "向量" with expand=true
        expected: "搜索" -> {搜索, 检索, 查询}; "向量" -> {向量, 矢量, vector}
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=SYNONYMS_PATH)
        try:
            self.create_bm25_collection(client, col_name,
                                        self._synonym_analyzer_params(res_name, expand=True))
            self.build_and_load_bm25(client, col_name)

            # "搜索" should expand to all synonyms: 搜索, 检索, 查询
            res, _ = self.run_analyzer(client, "搜索", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = set(res.tokens)
            expected_synonyms = {"搜索", "检索", "查询"}
            assert expected_synonyms.issubset(tokens), \
                f"expand=true on '搜索' should produce {expected_synonyms}, got {tokens}"

            # "向量" should expand to: 向量, 矢量, vector
            res, _ = self.run_analyzer(client, "向量", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = set(res.tokens)
            expected_synonyms = {"向量", "矢量", "vector"}
            assert expected_synonyms.issubset(tokens), \
                f"expand=true on '向量' should produce {expected_synonyms}, got {tokens}"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_synonym_expand_false_maps_to_canonical(self, file_resource_env):
        """
        target: expand=false maps non-canonical forms to the first (canonical) form
        method: run_analyzer on "检索", "查询", "矢量" with expand=false
        expected: "检索"/"查询" -> "搜索"; "矢量" -> "向量"
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=SYNONYMS_PATH)
        try:
            self.create_bm25_collection(client, col_name,
                                        self._synonym_analyzer_params(res_name, expand=False))
            self.build_and_load_bm25(client, col_name)

            # "检索" should map to canonical "搜索"
            res, _ = self.run_analyzer(client, "检索", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = list(res.tokens)
            assert tokens == ["搜索"], \
                f"expand=false: '检索' should map to ['搜索'], got {tokens}"

            # "查询" should also map to canonical "搜索"
            res, _ = self.run_analyzer(client, "查询", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = list(res.tokens)
            assert tokens == ["搜索"], \
                f"expand=false: '查询' should map to ['搜索'], got {tokens}"

            # "矢量" should map to canonical "向量"
            res, _ = self.run_analyzer(client, "矢量", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = list(res.tokens)
            assert tokens == ["向量"], \
                f"expand=false: '矢量' should map to ['向量'], got {tokens}"

            # Canonical form "搜索" should remain unchanged
            res, _ = self.run_analyzer(client, "搜索", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = list(res.tokens)
            assert tokens == ["搜索"], \
                f"expand=false: canonical '搜索' should stay ['搜索'], got {tokens}"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)


class TestFileResourceStopWordsAnalyzer(FileResourceTestBase):
    """Stop words filter + remote stop_words_file

    Stop words file: 的/是/在/了/和
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_stop_words_remote_file(self, file_resource_env):
        """
        target: stop words filtered via remote file and comparison with baseline
        method: run_analyzer with stop filter and without, compare token sets
        expected: stop words (的/是/在/了/和) absent with filter, present without
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        text = "这是一个在测试的文本和数据在这里了"
        stop_words = {"的", "是", "在", "了", "和"}
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            self.create_bm25_collection(client, col_name, {
                "tokenizer": "jieba",
                "filter": [{
                    "type": "stop",
                    "stop_words_file": {
                        "type": "remote",
                        "resource_name": res_name,
                        "file_name": "stop_words.txt",
                    }
                }]
            })
            self.build_and_load_bm25(client, col_name)

            # With stop filter: stop words should be absent
            res, _ = self.run_analyzer(client, text, None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens_filtered = set(res.tokens)
            found = stop_words & tokens_filtered
            assert len(found) == 0, \
                f"Stop words should be filtered, but found {found} in {tokens_filtered}"

            # Baseline without filter: stop words should be present
            res_baseline, _ = self.run_analyzer(client, text,
                                                {"tokenizer": "jieba"})
            tokens_baseline = set(res_baseline.tokens)
            baseline_stop = stop_words & tokens_baseline
            assert len(baseline_stop) > 0, \
                f"Baseline (no filter) should contain stop words, but got {tokens_baseline}"

            # Non-stop-word content tokens should still be present after filtering
            content_tokens = tokens_filtered - stop_words
            assert len(content_tokens) > 0, \
                f"Filtered output should still have content tokens, got {tokens_filtered}"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)


class TestFileResourceDecompounderAnalyzer(FileResourceTestBase):
    """Decompounder filter + remote word_list_file

    Dictionary words: bank, note, fire, work
    "banknote" -> ["bank", "note"], "firework" -> ["fire", "work"]
    Without decompounder: "banknote" and "firework" remain as-is.
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_decompounder_remote_file(self, file_resource_env):
        """
        target: decompounder splits compound words via remote dictionary
        method: run_analyzer on "banknote firework" with decompounder
        expected: "banknote" -> ["bank","note"], "firework" -> ["fire","work"]
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=DECOMPOUNDER_PATH)
        try:
            self.create_bm25_collection(client, col_name, {
                "tokenizer": "standard",
                "filter": [{
                    "type": "decompounder",
                    "word_list_file": {
                        "type": "remote",
                        "resource_name": res_name,
                        "file_name": "decompounder_dict.txt",
                    }
                }]
            })
            self.build_and_load_bm25(client, col_name)

            # "banknote firework" should be split into sub-components
            res, _ = self.run_analyzer(client, "banknote firework", None,
                                       collection_name=col_name,
                                       field_name="text")
            tokens = list(res.tokens)
            for expected in ["bank", "note", "fire", "work"]:
                assert expected in tokens, \
                    f"Decompounder should produce '{expected}', got {tokens}"

            # Verify compound words are replaced (not kept alongside sub-tokens)
            assert "banknote" not in tokens, \
                f"Compound 'banknote' should be decomposed, got {tokens}"
            assert "firework" not in tokens, \
                f"Compound 'firework' should be decomposed, got {tokens}"
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_decompounder_baseline_without_filter(self, file_resource_env):
        """
        target: without decompounder filter, compound words remain intact
        method: run_analyzer with standard tokenizer only (no decompounder)
        expected: "banknote" and "firework" are single tokens
        """
        client = self._client()
        res, _ = self.run_analyzer(client, "banknote firework",
                                   {"tokenizer": "standard"})
        tokens = list(res.tokens)
        assert "banknote" in tokens, \
            f"Without decompounder, 'banknote' should be a single token, got {tokens}"
        assert "firework" in tokens, \
            f"Without decompounder, 'firework' should be a single token, got {tokens}"
        assert "bank" not in tokens and "note" not in tokens, \
            f"Without decompounder, sub-tokens should not appear, got {tokens}"


class TestFileResourceInvalidAnalyzerConfig(FileResourceTestBase):
    """Invalid remote file resource configurations in analyzer"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_missing_resource_name(self, file_resource_env):
        """
        target: remote config without resource_name
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        with pytest.raises(Exception):
            self.create_bm25_collection(client, col_name, {
                "tokenizer": "standard",
                "filter": [{"type": "stop", "stop_words_file": {
                    "type": "remote", "file_name": "stop_words.txt"}}]
            })

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_missing_file_name(self, file_resource_env):
        """
        target: remote config without file_name
        expected: error on create_collection
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            with pytest.raises(Exception):
                self.create_bm25_collection(client, col_name, {
                    "tokenizer": "standard",
                    "filter": [{"type": "stop", "stop_words_file": {
                        "type": "remote", "resource_name": res_name}}]
                })
        finally:
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_invalid_type(self, file_resource_env):
        """
        target: type="unknown" in file resource config
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        with pytest.raises(Exception):
            self.create_bm25_collection(client, col_name, {
                "tokenizer": "standard",
                "filter": [{"type": "stop", "stop_words_file": {
                    "type": "unknown", "resource_name": "r", "file_name": "f.txt"}}]
            })

    @pytest.mark.tags(CaseLabel.L2)
    def test_remote_nonexistent_resource_name(self, file_resource_env):
        """
        target: resource_name references a resource that was never added
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        with pytest.raises(Exception):
            self.create_bm25_collection(client, col_name, {
                "tokenizer": "standard",
                "filter": [{"type": "stop", "stop_words_file": {
                    "type": "remote",
                    "resource_name": "nonexist_resource",
                    "file_name": "stop_words.txt"}}]
            })


# ===================================================================
# Module 3 — Consistency & edge cases
# ===================================================================

class TestFileResourceRefCount(FileResourceTestBase):
    """Reference counting & deletion protection"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_single_ref_cannot_delete(self, file_resource_env):
        """
        target: resource referenced by 1 collection cannot be removed
        method: add -> create col -> remove
        expected: MilvusException
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
            with pytest.raises(MilvusException):
                client.remove_file_resource(name=res_name)
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_multi_ref_partial_drop(self, file_resource_env):
        """
        target: resource referenced by 2 collections; dropping 1 still blocks removal
        method: add -> create col1 + col2 -> drop col1 -> remove
        expected: MilvusException (col2 still references)
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col1 = cf.gen_unique_str(prefix)
        col2 = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            self.create_bm25_collection_with_stop_filter(client, col1, res_name)
            self.create_bm25_collection_with_stop_filter(client, col2, res_name)
            self.drop_collection(client, col1)
            time.sleep(1)
            with pytest.raises(MilvusException):
                client.remove_file_resource(name=res_name)
        finally:
            for c in [col1, col2]:
                try:
                    self.drop_collection(client, c)
                except Exception:
                    pass
            client.remove_file_resource(name=res_name)

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
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
        self.drop_collection(client, col_name)
        time.sleep(1)
        client.remove_file_resource(name=res_name)


class TestFileResourceSyncCheck(FileResourceTestBase):
    """Synchronization checks during collection creation"""

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
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_without_add(self, file_resource_env):
        """
        target: create collection referencing a resource that was never added
        expected: error on create_collection
        """
        client = self._client()
        col_name = cf.gen_unique_str(prefix)
        with pytest.raises(Exception):
            self.create_bm25_collection_with_stop_filter(
                client, col_name, "never_added_resource")


class TestFileResourceIdempotency(FileResourceTestBase):
    """Idempotency of add and remove"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_idempotent(self, file_resource_env):
        """
        target: add same name+path twice is idempotent
        expected: no error
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_remove_idempotent(self, file_resource_env):
        """
        target: double remove is idempotent
        expected: no error
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)
        client.remove_file_resource(name=res_name)


class TestFileResourceConcurrency(FileResourceTestBase):
    """Concurrent file resource operations"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_add_different_resources(self, file_resource_env):
        """
        target: concurrent adds of distinct resources
        expected: all succeed
        """
        client = self._client()
        resources = [
            (cf.gen_unique_str(prefix), JIEBA_DICT_PATH),
            (cf.gen_unique_str(prefix), SYNONYMS_PATH),
            (cf.gen_unique_str(prefix), STOPWORDS_PATH),
            (cf.gen_unique_str(prefix), DECOMPOUNDER_PATH),
        ]
        errors = []

        def _add(name, path):
            try:
                client.add_file_resource(name=name, path=path)
            except Exception as e:
                errors.append((name, str(e)))

        threads = [threading.Thread(target=_add, args=(n, p)) for n, p in resources]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, f"Concurrent add failed: {errors}"
        for n, _ in resources:
            client.remove_file_resource(name=n)

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_add_same_resource(self, file_resource_env):
        """
        target: concurrent add of same name+path (idempotent)
        expected: all succeed
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        errors = []

        def _add():
            try:
                client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=_add) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors, f"Concurrent idempotent add failed: {errors}"
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_add_then_create_collection(self, file_resource_env):
        """
        target: add resource then immediately create collection
        expected: success (add is synchronous)
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        col_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=STOPWORDS_PATH)
        try:
            self.create_bm25_collection_with_stop_filter(client, col_name, res_name)
        finally:
            self.drop_collection(client, col_name)
            client.remove_file_resource(name=res_name)


class TestFileResourceUpdate(FileResourceTestBase):
    """Resource update / re-add scenarios"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_delete_readd_different_path(self, file_resource_env):
        """
        target: remove then re-add with a different path
        expected: success
        """
        client = self._client()
        res_name = cf.gen_unique_str(prefix)
        client.add_file_resource(name=res_name, path=JIEBA_DICT_PATH)
        client.remove_file_resource(name=res_name)
        client.add_file_resource(name=res_name, path=SYNONYMS_PATH)
        client.remove_file_resource(name=res_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_overwrite_file_content_same_path(self, file_resource_env, minio_client):
        """
        target: overwrite MinIO file then re-add (same name+path)
        method: upload v1 -> add -> upload v2 -> add again (idempotent)
        expected: no error
        """
        client = self._client()
        bucket = file_resource_env["bucket"]
        res_name = cf.gen_unique_str(prefix)
        remote_path = "update_test/custom_stop.txt"

        content1 = "的\n是\n"
        content1_bytes = content1.encode("utf-8")
        minio_client.put_object(bucket, remote_path,
                                io.BytesIO(content1_bytes), len(content1_bytes))
        try:
            client.add_file_resource(name=res_name, path=remote_path)
            # overwrite with new content
            content2 = "的\n是\n在\n了\n和\n"
            content2_bytes = content2.encode("utf-8")
            minio_client.put_object(bucket, remote_path,
                                    io.BytesIO(content2_bytes), len(content2_bytes))
            # idempotent re-add
            client.add_file_resource(name=res_name, path=remote_path)
        finally:
            client.remove_file_resource(name=res_name)
            try:
                minio_client.remove_object(bucket, remote_path)
            except Exception:
                pass
