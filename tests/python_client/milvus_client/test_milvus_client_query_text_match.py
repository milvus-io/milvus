import time

import pandas as pd
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from faker import Faker
from pymilvus import DataType
from utils.util_log import test_log as log

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)


class TestQueryTextMatchIndependent(TestMilvusClientV2Base):
    """Independent MilvusClient query tests for text_match and text_match_fuzzy."""

    TEXT_FIELDS = ["word", "sentence", "paragraph", "text"]

    def _gen_fuzzy_vector(self, seed, dim=8):
        values = [float(seed + i + 1) for i in range(dim)]
        norm = sum(v * v for v in values) ** 0.5
        return [v / norm for v in values]

    def _setup_text_match_fuzzy_collection(self, client, load_before_insert=False):
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=512,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params={"tokenizer": "standard"},
        )
        schema.add_field(
            "plain_text",
            DataType.VARCHAR,
            max_length=512,
            enable_analyzer=True,
            enable_match=False,
            analyzer_params={"tokenizer": "standard"},
        )
        # Regression coverage for #51077: this option name remains usable as a scalar field.
        schema.add_field("max_edit_distance", DataType.INT64)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        if load_before_insert:
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name="float32_emb", index_type="FLAT", metric_type="L2", params={})
            self.create_index(client, collection_name, index_params=idx)
            self.load_collection(client, collection_name)

        texts = [
            "allergy season",
            "allergic reaction",
            "apple juice",
            "football game",
            "music focus work",
            "coffee paper smell",
            "vector database search",
        ]
        distances = [5, 1, 2, 3, 0, 4, 6]
        data = [
            {
                "id": i,
                "float32_emb": self._gen_fuzzy_vector(i, dim),
                "text": text,
                "plain_text": text,
                "max_edit_distance": distances[i],
            }
            for i, text in enumerate(texts)
        ]
        self.insert(client, collection_name, data=data)

        if not load_before_insert:
            self.flush(client, collection_name)
            idx = self.prepare_index_params(client)[0]
            idx.add_index(field_name="float32_emb", index_type="FLAT", metric_type="L2", params={})
            self.create_index(client, collection_name, index_params=idx)
            self.load_collection(client, collection_name)

        return collection_name, dim

    def _setup_text_match_fuzzy_nullable_collection(self, client):
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=512,
            nullable=True,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params={"tokenizer": "standard"},
        )
        schema.add_field("age", DataType.INT64)
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        data = [
            {"id": 0, "float32_emb": self._gen_fuzzy_vector(0, dim), "text": "allergy season", "age": 10},
            {"id": 1, "float32_emb": self._gen_fuzzy_vector(1, dim), "text": "allergy archive", "age": 20},
            {"id": 2, "float32_emb": self._gen_fuzzy_vector(2, dim), "text": None, "age": 30},
            {"id": 3, "float32_emb": self._gen_fuzzy_vector(3, dim), "text": "", "age": 40},
            {"id": 4, "float32_emb": self._gen_fuzzy_vector(4, dim), "text": "apple juice", "age": 50},
            {"id": 5, "float32_emb": self._gen_fuzzy_vector(5, dim), "text": "music focus", "age": 60},
        ]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name="float32_emb", index_type="FLAT", metric_type="L2", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        return collection_name, dim

    def _search_id_set(self, client, collection_name, dim, expr, filter_params=None):
        res, _ = self.search(
            client,
            collection_name,
            data=[self._gen_fuzzy_vector(100, dim)],
            anns_field="float32_emb",
            search_params={"metric_type": "L2"},
            limit=20,
            filter=expr,
            filter_params=filter_params or {},
            output_fields=["id", "text"],
        )
        hits = res[0] if res else []
        return {hit["entity"]["id"] for hit in hits}

    def _flatten_search_ids(self, res):
        return [hit["entity"]["id"] for hits in res for hit in hits]

    def _setup_text_match_collection(self, client, tokenizer, data_size=3000):
        language = "zh" if tokenizer == "jieba" else "en"
        fake = fake_zh if tokenizer == "jieba" else fake_en

        dim = 128
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in self.TEXT_FIELDS:
            schema.add_field(
                field_name,
                DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params={"tokenizer": tokenizer},
            )
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        float_vectors = cf.gen_vectors(data_size, dim)
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "float32_emb": float_vectors[i],
            }
            for i in range(data_size)
        ]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name="float32_emb", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 500}
        )
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        df = pd.DataFrame(data)
        wf_map = {field: cf.analyze_documents(df[field].tolist(), language=language) for field in self.TEXT_FIELDS}
        df_split = cf.split_dataframes(df, self.TEXT_FIELDS, language=language)
        return collection_name, df_split, wf_map

    def _query_id_set(self, client, collection_name, expr, filter_params=None, output_fields=None):
        res, _ = self.query(
            client,
            collection_name,
            filter=expr,
            filter_params=filter_params or {},
            output_fields=output_fields or ["id", "text", "max_edit_distance"],
        )
        return {row["id"] for row in res}

    def _wait_for_query_ids(self, client, collection_name, expr, expected_ids, timeout=30):
        for _ in range(timeout):
            actual_ids = self._query_id_set(client, collection_name, expr)
            if actual_ids == expected_ids:
                return
            time.sleep(1)
        assert actual_ids == expected_ids

    def _osa_distance(self, left, right):
        if left == right:
            return 0
        if not left:
            return len(right)
        if not right:
            return len(left)

        rows = [[0] * (len(right) + 1) for _ in range(len(left) + 1)]
        for i in range(len(left) + 1):
            rows[i][0] = i
        for j in range(len(right) + 1):
            rows[0][j] = j

        for i in range(1, len(left) + 1):
            for j in range(1, len(right) + 1):
                cost = 0 if left[i - 1] == right[j - 1] else 1
                rows[i][j] = min(
                    rows[i - 1][j] + 1,
                    rows[i][j - 1] + 1,
                    rows[i - 1][j - 1] + cost,
                )
                if i > 1 and j > 1 and left[i - 1] == right[j - 2] and left[i - 2] == right[j - 1]:
                    rows[i][j] = min(rows[i][j], rows[i - 2][j - 2] + 1)

        return rows[-1][-1]

    def _mutate_token(self, token, edit_count, existing_tokens):
        candidates = [token[:-1], token[1:], token[:1] + "x" + token[1:], token + "x"]
        if edit_count == 2:
            candidates = [token[:-2], token[2:], token[:1] + token[3:], token + "xy"]
        for candidate in candidates:
            if candidate and candidate not in existing_tokens:
                return candidate
        raise AssertionError(f"failed to build a {edit_count}-edit typo for token {token}")

    def _pick_fuzzy_token(self, wf_map, field, exclude_tokens=None):
        exclude_tokens = exclude_tokens or set()
        existing_tokens = set(wf_map[field].keys())
        for token, _ in wf_map[field].most_common():
            if token not in exclude_tokens and token.isascii() and token.isalpha() and len(token) >= 6:
                return (
                    token,
                    self._mutate_token(token, 1, existing_tokens),
                    self._mutate_token(token, 2, existing_tokens),
                )
        raise AssertionError(f"failed to pick an English fuzzy token from field {field}")

    def _fuzzy_expected_ids(self, df_split, field, query, max_edit_distance):
        query_tokens = list(cf.analyze_documents([query], language="en").keys())
        row_tokens = {token for tokens in df_split[field] for token in tokens}
        matched_tokens = {
            token
            for token in row_tokens
            if any(self._osa_distance(token, query_token) <= max_edit_distance for query_token in query_tokens)
        }
        return {
            int(row["id"]) for _, row in df_split.iterrows() if any(token in matched_tokens for token in row[field])
        }

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_text_and_phrase_match_filter_template(self):
        """
        target: verify text_match and phrase_match query strings support filter templating
        method: create analyzer-enabled data and compare literal filters with template filters
        expected: template filters return the same IDs as equivalent literal filters
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params={"tokenizer": "standard"},
        )
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        data = [
            {"id": 0, "text": "vector database search", "float32_emb": [0.1] * dim},
            {"id": 1, "text": "database vector search", "float32_emb": [0.2] * dim},
            {"id": 2, "text": "hybrid sparse retrieval", "float32_emb": [0.3] * dim},
        ]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name="float32_emb", index_type="FLAT", metric_type="L2", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        cases = [
            ('text_match(text, "vector")', "text_match(text, {query})", {"query": "vector"}),
            (
                'phrase_match(text, "vector database", 0)',
                "phrase_match(text, {query}, 0)",
                {"query": "vector database"},
            ),
        ]
        for literal_expr, template_expr, template_params in cases:
            literal_res, _ = self.query(client, collection_name, filter=literal_expr, output_fields=["id"])
            template_res, _ = self.query(
                client,
                collection_name,
                filter=template_expr,
                filter_params=template_params,
                output_fields=["id"],
            )
            assert {row["id"] for row in template_res} == {row["id"] for row in literal_res}

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_with_text_match_fuzzy_filter(self):
        """
        target: verify text_match_fuzzy query behavior
        method: check fuzzy/exact, K=0/1/2, transposition, multi-token OR, NOT, empty queries,
                template parameters, and growing/sealed data
        expected: query returns exactly the expected primary keys
        """
        client = self._client()
        collection_name, _ = self._setup_text_match_fuzzy_collection(client, load_before_insert=True)

        self._wait_for_query_ids(client, collection_name, 'text_match(text, "music")', {4})
        self._wait_for_query_ids(client, collection_name, 'text_match_fuzzy(text, "alergy", max_edit_distance=1)', {0})

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        cases = [
            ('text_match(text, "music")', None, {4}),
            ('text_match(text, "musc")', None, set()),
            ('text_match_fuzzy(text, "musc", max_edit_distance=1)', None, {4}),
            ('text_match_fuzzy(text, "musicx", max_edit_distance=1)', None, {4}),
            ('text_match_fuzzy(text, "mupic", max_edit_distance=1)', None, {4}),
            ('text_match_fuzzy(text, "musc", max_edit_distance=0)', None, set()),
            ('text_match_fuzzy(text, "music", max_edit_distance=0)', None, {4}),
            ('text_match_fuzzy(text, "fotbal", max_edit_distance=1)', None, set()),
            ('text_match_fuzzy(text, "fotbal", max_edit_distance=2)', None, {3}),
            ('text_match_fuzzy(text, "muisc", max_edit_distance=1)', None, {4}),
            ('text_match_fuzzy(text, "alergy aple", max_edit_distance=1)', None, {0, 2}),
            ("text_match_fuzzy(text, {query}, max_edit_distance=1)", {"query": "alergy"}, {0}),
            ('text_match_fuzzy(text, "", max_edit_distance=1)', None, set()),
            ('text_match_fuzzy(text, "...", max_edit_distance=1)', None, set()),
            ('not text_match_fuzzy(text, "alergy", max_edit_distance=1)', None, {1, 2, 3, 4, 5, 6}),
            (
                'text_match_fuzzy(text, "alergy aple", max_edit_distance=1) '
                'and not text_match_fuzzy(text, "alergy", max_edit_distance=1)',
                None,
                {2},
            ),
        ]
        for expr, filter_params, expected_ids in cases:
            assert self._query_id_set(client, collection_name, expr, filter_params) == expected_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_match_fuzzy_soft_keyword(self):
        """
        target: verify max_edit_distance is a soft keyword for text_match_fuzzy
        method: use a scalar field named max_edit_distance and an uppercase option name
        expected: the field filter and case-insensitive option name both work
        """
        client = self._client()
        collection_name, _ = self._setup_text_match_fuzzy_collection(client)

        assert self._query_id_set(client, collection_name, "max_edit_distance > 1") == {0, 2, 3, 5, 6}
        assert self._query_id_set(
            client,
            collection_name,
            'text_match_fuzzy(text, "alergy", MAX_EDIT_DISTANCE=1)',
        ) == {0}

    @pytest.mark.tags(CaseLabel.L1)
    def test_text_match_fuzzy_invalid_params(self):
        """
        target: verify text_match_fuzzy rejects invalid options, distances, and unsupported fields
        method: run malformed text_match_fuzzy filters against a deterministic collection
        expected: each invalid expression returns an error with the expected message fragment
        """
        client = self._client()
        collection_name, _ = self._setup_text_match_fuzzy_collection(client)

        cases = [
            ('text_match_fuzzy(text, "alergy", fuzziness=1)', "expected max_edit_distance"),
            ('text_match_fuzzy(text, "alergy", max_edit_distance=-1)', "max_edit_distance should be in [0, 2]"),
            ('text_match_fuzzy(text, "alergy", max_edit_distance=3)', "max_edit_distance should be in [0, 2]"),
            ('text_match_fuzzy(text, "alergy", max_edit_distance=1.5)', "expecting IntegerConstant"),
            (
                'text_match_fuzzy(text, "alergy", max_edit_distance=9223372036854775808)',
                "invalid max_edit_distance value",
            ),
            ('text_match_fuzzy(text, "alergy")', "expecting ','"),
            (
                'text_match_fuzzy(text, 123, max_edit_distance=1)',
                "text_match_fuzzy query should be a string literal or template variable",
            ),
            ('text_match_fuzzy(not_exist, "alergy", max_edit_distance=1)', "not_exist"),
            ('text_match_fuzzy(max_edit_distance, "alergy", max_edit_distance=1)', "non-string"),
            ('text_match_fuzzy(plain_text, "alergy", max_edit_distance=1)', "does not enable match"),
        ]
        for expr, expected_msg in cases:
            self.query(
                client,
                collection_name,
                filter=expr,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1100, ct.err_msg: expected_msg},
            )

        with pytest.raises(Exception) as exc_info:
            client.query(
                collection_name=collection_name,
                filter="text_match_fuzzy(text, {query}, max_edit_distance=1)",
                filter_params={"query": 123},
                output_fields=["id"],
            )
        error = str(exc_info.value).lower()
        assert any(keyword in error for keyword in ["string", "template", "type", "parameter"]), error

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_fuzzy_with_custom_analyzers(self):
        """
        target: verify text_match_fuzzy uses each field's configured analyzer
        method: query fields using stop, lowercase, length, stemmer, ascii-folding, and ICU analyzers
        expected: fuzzy matching operates on analyzed terms and filtered terms never match
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = 8
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        analyzers = {
            "stop_text": {
                "tokenizer": "standard",
                "filter": [{"type": "stop", "stop_words": ["the"]}],
            },
            "lower_text": {"tokenizer": "standard", "filter": ["lowercase"]},
            "length_text": {
                "tokenizer": "standard",
                "filter": [{"type": "length", "max": 10}],
            },
            "stemmer_text": {
                "tokenizer": "standard",
                "filter": [{"type": "stemmer", "language": "english"}],
            },
            "ascii_text": {"tokenizer": "standard", "filter": ["asciifolding"]},
            "icu_text": {"tokenizer": "icu"},
        }
        for field_name, analyzer_params in analyzers.items():
            schema.add_field(
                field_name,
                DataType.VARCHAR,
                max_length=512,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
            )
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        data = [
            {
                "id": 0,
                "float32_emb": self._gen_fuzzy_vector(0, dim),
                "stop_text": "allergy the season",
                "lower_text": "ALLERGY season",
                "length_text": "allergy aaaaaaaaaaa",
                "stemmer_text": "play runner",
                "ascii_text": "café Möller",
                "icu_text": "database retrieval",
            },
            {
                "id": 1,
                "float32_emb": self._gen_fuzzy_vector(1, dim),
                "stop_text": "unrelated the archive",
                "lower_text": "ORANGES archive",
                "length_text": "unrelated bbbbbbbbbbb",
                "stemmer_text": "book reader",
                "ascii_text": "jalapeño menu",
                "icu_text": "storage compaction",
            },
        ]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name="float32_emb", index_type="FLAT", metric_type="L2", params={})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        cases = [
            ('text_match_fuzzy(stop_text, "alergy", max_edit_distance=1)', {0}),
            ('text_match_fuzzy(stop_text, "the", max_edit_distance=1)', set()),
            ('text_match_fuzzy(stop_text, "teh", max_edit_distance=1)', set()),
            ('text_match_fuzzy(lower_text, "ALERGY", max_edit_distance=1)', {0}),
            ('text_match_fuzzy(length_text, "alergy", max_edit_distance=1)', {0}),
            ('text_match_fuzzy(length_text, "aaaaaaaaaa", max_edit_distance=1)', set()),
            ('text_match_fuzzy(stemmer_text, "playing", max_edit_distance=0)', {0}),
            ('text_match_fuzzy(stemmer_text, "runer", max_edit_distance=1)', {0}),
            ('text_match_fuzzy(ascii_text, "caff", max_edit_distance=1)', {0}),
            ('text_match_fuzzy(icu_text, "databse", max_edit_distance=1)', {0}),
        ]
        for expr, expected_ids in cases:
            res, _ = self.query(client, collection_name, filter=expr, output_fields=["id"])
            assert {row["id"] for row in res} == expected_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_fuzzy_after_insert_delete_upsert_and_reload(self):
        """
        target: verify fuzzy text-index visibility through collection lifecycle operations
        method: insert after load, delete, upsert the matched text, then flush and reload
        expected: fuzzy results reflect every mutation before and after release/load
        """
        client = self._client()
        collection_name, _ = self._setup_text_match_fuzzy_collection(client)
        allergy_expr = 'text_match_fuzzy(text, "alergy", max_edit_distance=1)'
        apple_expr = 'text_match_fuzzy(text, "aple", max_edit_distance=1)'

        assert self._query_id_set(client, collection_name, allergy_expr) == {0}
        self.insert(
            client,
            collection_name,
            data=[
                {
                    "id": 7,
                    "float32_emb": self._gen_fuzzy_vector(7),
                    "text": "allergy update",
                    "plain_text": "allergy update",
                    "max_edit_distance": 7,
                }
            ],
        )
        self._wait_for_query_ids(client, collection_name, allergy_expr, {0, 7})

        self.delete(client, collection_name, ids=[0])
        self._wait_for_query_ids(client, collection_name, allergy_expr, {7})

        self.upsert(
            client,
            collection_name,
            data=[
                {
                    "id": 7,
                    "float32_emb": self._gen_fuzzy_vector(8),
                    "text": "apple update",
                    "plain_text": "apple update",
                    "max_edit_distance": 8,
                }
            ],
        )
        self._wait_for_query_ids(client, collection_name, allergy_expr, set())
        self._wait_for_query_ids(client, collection_name, apple_expr, {2, 7})

        self.flush(client, collection_name)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        assert self._query_id_set(client, collection_name, allergy_expr) == set()
        assert self._query_id_set(client, collection_name, apple_expr) == {2, 7}

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_text_match_fuzzy_with_combined_expression_nullable_and_empty_text(self):
        """
        target: verify text_match_fuzzy composes with scalar filters and ignores null/empty text rows
        method: query nullable analyzer text data with fuzzy filters, age predicates, and OR clauses
        expected: fuzzy matches only valid tokenized rows; null and empty strings do not produce false positives
        """
        client = self._client()
        collection_name, _ = self._setup_text_match_fuzzy_nullable_collection(client)
        output_fields = ["id", "text", "age"]

        cases = [
            ('text_match_fuzzy(text, "alergy", max_edit_distance=1)', {0, 1}),
            ('text_match_fuzzy(text, "alergy", max_edit_distance=1) and age > 15', {1}),
            ('text_match_fuzzy(text, "alergy", max_edit_distance=1) and age > 50', set()),
            (
                'text_match_fuzzy(text, "alergy", max_edit_distance=1) '
                'or text_match_fuzzy(text, "aple", max_edit_distance=1)',
                {0, 1, 4},
            ),
            ('id in [2, 3] and text_match_fuzzy(text, "alergy", max_edit_distance=1)', set()),
        ]
        for expr, expected_ids in cases:
            assert self._query_id_set(client, collection_name, expr, output_fields=output_fields) == expected_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_text_match_fuzzy_with_random_large_data(self):
        """
        target: verify text_match_fuzzy query works on non-fixed text data at thousands-row scale
        method: pick real corpus tokens, generate typo queries, and compare query ids with Python-calculated ids
        expected: query returns the Python-calculated id set
        """
        client = self._client()
        collection_name, df_split, wf_map = self._setup_text_match_collection(
            client, tokenizer="standard", data_size=3000
        )

        field = "text"
        base_token, one_edit_query, _ = self._pick_fuzzy_token(wf_map, field)
        another_token, another_one_edit_query, two_edit_query = self._pick_fuzzy_token(
            wf_map, field, exclude_tokens={base_token}
        )

        exact_typo_ids = self._fuzzy_expected_ids(df_split, field, one_edit_query, max_edit_distance=0)
        one_edit_ids = self._fuzzy_expected_ids(df_split, field, one_edit_query, max_edit_distance=1)
        two_edit_ids = self._fuzzy_expected_ids(df_split, field, two_edit_query, max_edit_distance=2)
        multi_token_query = f"{one_edit_query} {another_one_edit_query}"
        multi_token_ids = self._fuzzy_expected_ids(df_split, field, multi_token_query, max_edit_distance=1)

        log.info(
            "large fuzzy query data: base=%s one_edit=%s another=%s another_one_edit=%s two_edit=%s",
            base_token,
            one_edit_query,
            another_token,
            another_one_edit_query,
            two_edit_query,
        )

        assert exact_typo_ids == set()
        assert one_edit_ids
        assert two_edit_ids
        assert one_edit_ids <= self._fuzzy_expected_ids(df_split, field, one_edit_query, max_edit_distance=2)
        assert one_edit_ids <= multi_token_ids

        output_fields = ["id", field]
        assert (
            self._query_id_set(
                client, collection_name, f'text_match({field}, "{one_edit_query}")', output_fields=output_fields
            )
            == set()
        )
        assert (
            self._query_id_set(
                client,
                collection_name,
                f'text_match_fuzzy({field}, "{one_edit_query}", max_edit_distance=1)',
                output_fields=output_fields,
            )
            == one_edit_ids
        )
        assert (
            self._query_id_set(
                client,
                collection_name,
                f'text_match_fuzzy({field}, "{two_edit_query}", max_edit_distance=2)',
                output_fields=output_fields,
            )
            == two_edit_ids
        )
        assert (
            self._query_id_set(
                client,
                collection_name,
                f'text_match_fuzzy({field}, "{multi_token_query}", max_edit_distance=1)',
                output_fields=output_fields,
            )
            == multi_token_ids
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_with_text_match_fuzzy_filter(self):
        """
        target: verify text_match_fuzzy is accepted by the search filter path
        method: run a literal fuzzy filter and a template fuzzy filter through dense search
        expected: each search returns exactly the expected primary keys
        """
        client = self._client()
        collection_name, dim = self._setup_text_match_fuzzy_collection(client)

        cases = [
            ('text_match_fuzzy(text, "musc", max_edit_distance=1)', None, {4}),
            ("text_match_fuzzy(text, {query}, max_edit_distance=1)", {"query": "alergy"}, {0}),
        ]
        for expr, filter_params, expected_ids in cases:
            assert self._search_id_set(client, collection_name, dim, expr, filter_params) == expected_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_text_match_fuzzy_with_search_by_pk_and_offset(self):
        """
        target: verify text_match_fuzzy works with search-by-pk and offset pagination
        method: search by primary keys, then compare an offset page with the full search tail
        expected: the filter is applied and the offset result matches the expected slice
        """
        client = self._client()
        collection_name, dim = self._setup_text_match_fuzzy_collection(client)
        output_fields = ["id", "text"]
        expr = 'text_match_fuzzy(text, "alergy aple musc cofee", max_edit_distance=1)'
        expected_ids = {0, 2, 4, 5}

        by_pk_res, _ = self.search(
            client,
            collection_name,
            data=None,
            ids=[0, 1, 2, 3, 4, 5, 6],
            anns_field="float32_emb",
            search_params={"metric_type": "L2"},
            limit=10,
            filter=expr,
            output_fields=output_fields,
        )
        assert set(self._flatten_search_ids(by_pk_res)) == expected_ids

        full_res, _ = self.search(
            client,
            collection_name,
            data=[self._gen_fuzzy_vector(100, dim)],
            anns_field="float32_emb",
            search_params={"metric_type": "L2"},
            limit=4,
            filter=expr,
            output_fields=output_fields,
        )
        offset_res, _ = self.search(
            client,
            collection_name,
            data=[self._gen_fuzzy_vector(100, dim)],
            anns_field="float32_emb",
            search_params={"metric_type": "L2"},
            limit=2,
            offset=1,
            filter=expr,
            output_fields=output_fields,
        )

        full_ids = self._flatten_search_ids(full_res)
        offset_ids = self._flatten_search_ids(offset_res)
        assert set(full_ids) == expected_ids
        assert offset_ids == full_ids[1:3]
