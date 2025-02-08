import pandas as pd
import re
import jieba
from tantivy import SchemaBuilder, Document, Index, Query
from typing import List, Dict
import numpy as np

import pytest
import random
from pymilvus import FieldSchema, CollectionSchema, DataType
from utils.util_log import test_log as log
from common import common_func as cf
from base.client_base import TestcaseBase

prefix = "phrase_match"


class PhraseMatchTestGenerator:
    def __init__(self, language="en"):
        """
        Initialize the test data generator

        Args:
            language: Language for text generation ('en' for English, 'zh' for Chinese)
        """
        self.language = language
        self.index = None
        self.documents = []

        # English vocabulary
        self.en_activities = [
            "swimming",
            "football",
            "basketball",
            "tennis",
            "volleyball",
            "baseball",
            "golf",
            "rugby",
            "cricket",
            "boxing",
            "running",
            "cycling",
            "skating",
            "skiing",
            "surfing",
            "diving",
            "climbing",
            "yoga",
            "dancing",
            "hiking",
        ]

        self.en_verbs = [
            "love",
            "like",
            "enjoy",
            "play",
            "practice",
            "prefer",
            "do",
            "learn",
            "teach",
            "watch",
            "start",
            "begin",
            "continue",
            "finish",
            "master",
            "try",
        ]

        self.en_connectors = [
            "and",
            "or",
            "but",
            "while",
            "after",
            "before",
            "then",
            "also",
            "plus",
            "with",
        ]

        self.en_modifiers = [
            "very much",
            "a lot",
            "seriously",
            "casually",
            "professionally",
            "regularly",
            "often",
            "sometimes",
            "daily",
            "weekly",
        ]

        # Chinese vocabulary
        self.zh_activities = [
            "游泳",
            "足球",
            "篮球",
            "网球",
            "排球",
            "棒球",
            "高尔夫",
            "橄榄球",
            "板球",
            "拳击",
            "跑步",
            "骑行",
            "滑冰",
            "滑雪",
            "冲浪",
            "潜水",
            "攀岩",
            "瑜伽",
            "跳舞",
            "徒步",
        ]

        self.zh_verbs = [
            "喜欢",
            "热爱",
            "享受",
            "玩",
            "练习",
            "偏好",
            "做",
            "学习",
            "教",
            "观看",
            "开始",
            "开启",
            "继续",
            "完成",
            "掌握",
            "尝试",
        ]

        self.zh_connectors = [
            "和",
            "或者",
            "但是",
            "同时",
            "之后",
            "之前",
            "然后",
            "也",
            "加上",
            "跟",
        ]

        self.zh_modifiers = [
            "非常",
            "很多",
            "认真地",
            "随意地",
            "专业地",
            "定期地",
            "经常",
            "有时候",
            "每天",
            "每周",
        ]

        # Set vocabulary based on language
        self.activities = self.zh_activities if language == "zh" else self.en_activities
        self.verbs = self.zh_verbs if language == "zh" else self.en_verbs
        self.connectors = self.zh_connectors if language == "zh" else self.en_connectors
        self.modifiers = self.zh_modifiers if language == "zh" else self.en_modifiers

    def tokenize_text(self, text: str) -> List[str]:
        """Tokenize text using jieba tokenizer"""
        text = text.strip()
        text = re.sub(r"[^\w\s]", " ", text)
        text = text.replace("\n", " ")
        if self.language == "zh":
            text = text.replace(" ", "")
            return list(jieba.cut_for_search(text))
        else:
            return list(text.split())

    def generate_embedding(self, dim: int) -> List[float]:
        """Generate random embedding vector"""
        return list(np.random.random(dim))

    def generate_text_pattern(self) -> str:
        """Generate test document text with various patterns"""
        patterns = [
            # Simple pattern with two activities
            lambda: f"{random.choice(self.activities)} {random.choice(self.activities)}",
            # Pattern with connector between activities
            lambda: f"{random.choice(self.activities)} {random.choice(self.connectors)} {random.choice(self.activities)}",
            # Pattern with modifier between activities
            lambda: f"{random.choice(self.activities)} {random.choice(self.modifiers)} {random.choice(self.activities)}",
            # Complex pattern with verb and activities
            lambda: f"{random.choice(self.verbs)} {random.choice(self.activities)} {random.choice(self.activities)}",
            # Pattern with multiple gaps
            lambda: f"{random.choice(self.activities)} {random.choice(self.modifiers)} {random.choice(self.connectors)} {random.choice(self.activities)}",
        ]
        return random.choice(patterns)()

    def generate_test_data(self, num_documents: int, dim: int) -> List[Dict]:
        """
        Generate test documents with text and embeddings

        Args:
            num_documents: Number of documents to generate
            dim: Dimension of embedding vectors

        Returns:
            List of dictionaries containing document data
        """
        # Generate documents
        self.documents = []
        for i in range(num_documents):
            self.documents.append(
                {
                    "id": i,
                    "text": self.generate_text_pattern()
                    if self.language == "en"
                    else self.generate_text_pattern().replace(" ", ""),
                    "emb": self.generate_embedding(dim),
                }
            )

        # Initialize Tantivy index
        schema_builder = SchemaBuilder()

        schema_builder.add_text_field("text", stored=True)
        schema_builder.add_unsigned_field("doc_id", stored=True)
        schema = schema_builder.build()

        self.index = Index(schema=schema, path=None)

        writer = self.index.writer()

        # Index all documents
        for doc in self.documents:
            document = Document()
            new_text = " ".join(self.tokenize_text(doc["text"]))
            document.add_text("text", new_text)
            document.add_unsigned("doc_id", doc["id"])
            writer.add_document(document)

        writer.commit()
        self.index.reload()

        return self.documents

    def generate_test_queries(self, num_queries: int) -> List[Dict]:
        """
        Generate test queries with varying slop values

        Args:
            num_queries: Number of queries to generate

        Returns:
            List of dictionaries containing query information
        """
        queries = []
        slop_values = [0, 1, 2, 3]  # Common slop values

        for i in range(num_queries):
            # Randomly select two or three words for the query
            num_words = random.choice([2, 3])
            words = random.sample(self.activities, num_words)

            queries.append(
                {
                    "id": i,
                    "query": " ".join(words)
                    if self.language == "en"
                    else "".join(words),
                    "slop": random.choice(slop_values),
                    "type": f"{num_words}_words",
                }
            )

        return queries

    def get_query_results(self, query: str, slop: int) -> List[Dict]:
        """
        Get all documents that match the phrase query

        Args:
            query: Query phrase
            slop: Maximum allowed word gap

        Returns:
            List[Dict]: List of matching documents with their ids and texts
        """
        if self.index is None:
            raise RuntimeError("No documents indexed. Call generate_test_data first.")

        # Clean and normalize query

        query_terms = self.tokenize_text(query)

        # Create phrase query
        searcher = self.index.searcher()
        phrase_query = Query.phrase_query(self.index.schema, "text", query_terms, slop)

        # Search for matches
        results = searcher.search(phrase_query, limit=len(self.documents))

        # Extract all matching documents
        matched_docs = []
        for _, doc_address in results.hits:
            doc = searcher.doc(doc_address)
            doc_id = doc.to_dict()["doc_id"]
            matched_docs.extend(doc_id)

        return matched_docs


class TestQueryPhraseMatch(TestcaseBase):
    """
    Test cases for phrase match functionality using PhraseMatchTestGenerator

    Test Case Categories:
    1. Basic Functionality Tests
       - Different tokenizers (standard, jieba)
       - Different index configurations
       - Basic phrase matching

    2. Slop Parameter Tests
       - Different slop values
       - Edge cases for slop
       - Slop with different phrase lengths

    3. Language-specific Tests
       - English text
       - Chinese text
       - Mixed language text

    4. Query Pattern Tests
       - Different phrase patterns
       - Complex phrases
       - Edge cases

    5. Performance Tests
       - Large datasets
       - Complex queries
       - Multiple concurrent queries

    6. Error Handling Tests
       - Invalid inputs
       - Edge cases
       - Error conditions
    """

    def init_collection_schema(
        self, dim: int, tokenizer: str, enable_partition_key: bool
    ) -> CollectionSchema:
        """Initialize collection schema with specified parameters"""
        analyzer_params = {"tokenizer": tokenizer}
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        return CollectionSchema(
            fields=fields, description="phrase match test collection"
        )

    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("tokenizer", ["standard","jieba"])
    def test_query_phrase_match_default(
        self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: Test phrase match with generated test data
        method: 1. Generate test data using PhraseMatchTestGenerator
               2. Insert data into collection
               3. Create index and load collection
               4. Execute phrase match queries with different slop values
               5. Verify results using generator's expected results
        expected: Phrase match results should match the generator's expected results
        """
        # Initialize parameters
        dim = 128
        data_size = 3000
        num_queries = 10

        # Initialize generator based on tokenizer
        language = "zh" if tokenizer == "jieba" else "en"
        generator = PhraseMatchTestGenerator(language=language)

        # Create collection
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=self.init_collection_schema(dim, tokenizer, enable_partition_key),
        )

        # Generate test data
        test_data = generator.generate_test_data(data_size, dim)
        df = pd.DataFrame(test_data)
        log.info(f"Test data: {df['text']}")
        # Insert data into collection
        insert_data = [
            {"id": d["id"], "text": d["text"], "emb": d["emb"]} for d in test_data
        ]
        collection_w.insert(insert_data)
        collection_w.flush()

        # Create indexes
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )
        if enable_inverted_index:
            collection_w.create_index(
                "text", {"index_type": "INVERTED", "params": {"tokenizer": tokenizer}}
            )

        collection_w.load()

        # Generate and execute test queries
        test_queries = generator.generate_test_queries(num_queries)

        for query in test_queries:
            expr = f"phrase_match(text, '{query['query']}', {query['slop']})"
            log.info(f"Testing query: {expr}")

            # Execute query
            results, _ = collection_w.query(expr=expr, output_fields=["id", "text"])

            # Get expected matches using Tantivy
            expected_matches = generator.get_query_results(
                query["query"], query["slop"]
            )
            # Get actual matches from Milvus
            actual_matches = [r["id"] for r in results]
            if set(actual_matches) != set(expected_matches):
                log.info(f"collection schema: {collection_w.schema}")
                for match_id in expected_matches:
                    # query by id to get text
                    res, _ = collection_w.query(
                        expr=f"id == {match_id}", output_fields=["text"]
                    )
                    text = res[0]["text"]
                    log.info(f"Expected match: {match_id}, text: {text}")

                for match_id in actual_matches:
                    # query by id to get text
                    res, _ = collection_w.query(
                        expr=f"id == {match_id}", output_fields=["text"]
                    )
                    text = res[0]["text"]
                    log.info(f"Matched document: {match_id}, text: {text}")
            # Assert results match
            assert (
                set(actual_matches) == set(expected_matches)
            ), f"Mismatch in results for query '{query['query']}' with slop {query['slop']}"

    @pytest.mark.parametrize("slop_value", [0, 1, 2, 5, 10])
    def test_slop_parameter(
        self, slop_value
    ):
        """
        target: Test phrase matching with different slop values
        method:
            1. Create collection with standard tokenizer
            2. Insert data with known word gaps
            3. Test phrase matching with different slop values
        expected: Results should match phrases within the specified slop distance
        """
        dim = 128
        data_size = 3000
        num_queries = 2
        tokenizer = "standard"
        enable_partition_key = True
        # Initialize generator based on tokenizer
        language = "zh" if tokenizer == "jieba" else "en"
        generator = PhraseMatchTestGenerator(language=language)

        # Create collection
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=self.init_collection_schema(dim, tokenizer, enable_partition_key),
        )

        # Generate test data
        test_data = generator.generate_test_data(data_size, dim)
        df = pd.DataFrame(test_data)
        log.info(f"Test data: {df['text']}")
        # Insert data into collection
        insert_data = [
            {"id": d["id"], "text": d["text"], "emb": d["emb"]} for d in test_data
        ]
        collection_w.insert(insert_data)
        collection_w.flush()

        # Create indexes
        collection_w.create_index(
            "emb",
            {"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}},
        )

        collection_w.create_index(
            "text", {"index_type": "INVERTED", "params": {"tokenizer": tokenizer}}
        )

        collection_w.load()

        # Generate and execute test queries
        test_queries = generator.generate_test_queries(num_queries)

        for query in test_queries:
            expr = f"phrase_match(text, '{query['query']}', {slop_value})"
            log.info(f"Testing query: {expr}")

            # Execute query
            results, _ = collection_w.query(expr=expr, output_fields=["id", "text"])

            # Get expected matches using Tantivy
            expected_matches = generator.get_query_results(
                query["query"], slop_value
            )
            # Get actual matches from Milvus
            actual_matches = [r["id"] for r in results]
            if set(actual_matches) != set(expected_matches):
                log.info(f"collection schema: {collection_w.schema}")
                for match_id in expected_matches:
                    # query by id to get text
                    res, _ = collection_w.query(
                        expr=f"id == {match_id}", output_fields=["text"]
                    )
                    text = res[0]["text"]
                    log.info(f"Expected match: {match_id}, text: {text}")

                for match_id in actual_matches:
                    # query by id to get text
                    res, _ = collection_w.query(
                        expr=f"id == {match_id}", output_fields=["text"]
                    )
                    text = res[0]["text"]
                    log.info(f"Matched document: {match_id}, text: {text}")
            # Assert results match
            assert (
                set(actual_matches) == set(expected_matches)
            ), f"Mismatch in results for query '{query['query']}' with slop {slop_value}"

    def test_query_patterns(
        self
    ):
        """
        target: Test different phrase match query patterns
        method:
            1. Create collection
            2. Insert data with various phrase patterns
            3. Test different query patterns
        expected: Complex phrase patterns should be matched correctly
        """
        dim = 128
        collection_name = f"{prefix}_patterns"
        schema = self.init_collection_schema(dim, "standard", False)
        collection = self.init_collection_wrap(name=collection_name, schema=schema)

        # Generate data with various patterns
        generator = PhraseMatchTestGenerator(language="en")
        data = generator.generate_test_data(3000, dim)
        collection.insert(data)

        collection.create_index(
            field_name="text",
            index_params={"index_type": "INVERTED_INDEX"}
        )
        collection.create_index(
            field_name="emb",
            index_params={"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        )
        collection.load()

        # Test various patterns
        test_patterns = [
            ("love swimming and running", 0),  # Exact phrase
            ("enjoy very basketball", 1),  # Phrase with gap
            ("practice tennis seriously often", 2),  # Complex phrase
            ("swimming running cycling", 5)  # Multiple activities
        ]
        # generate documents that match the patterns
        

        for pattern, slop in test_patterns:
            results = collection.query(
                expr=f'phrase_match(text, "{pattern}", {slop})',
                output_fields=["text"]
            )
            log.info(f"Pattern '{pattern}' with slop {slop} found {len(results)} matches")

    @pytest.mark.parametrize("data_size", [1000, 5000])
    def test_performance(
        self, data_size
    ):
        """
        target: Test phrase matching performance with different data sizes
        method:
            1. Create collection with large dataset
            2. Test query performance
            3. Test concurrent queries
        expected: Queries should complete within reasonable time
        """
        dim = 128
        collection_name = f"{prefix}_perf_{data_size}"
        schema = self.init_collection_schema(dim, "standard", False)
        collection = self.init_collection_wrap(name=collection_name, schema=schema)

        # Generate large dataset
        generator = PhraseMatchTestGenerator(language="en")
        data = generator.generate_test_data(data_size, dim)
        collection.insert(data)

        collection.create_index(
            field_name="text",
            index_params={"index_type": "INVERTED_INDEX"}
        )
        collection.create_index(
            field_name="emb",
            index_params={"index_type": "IVF_SQ8", "metric_type": "L2", "params": {"nlist": 64}}
        )
        collection.load()

        # Generate test queries
        queries = generator.generate_test_queries(10)

        # Test query performance
        for query in queries:
            results = collection.query(
                expr=f'phrase_match(text, "{query["text"]}", {query["slop"]})',
                output_fields=["text"]
            )
            log.info(f"Query returned {len(results)} results from {data_size} documents")

    def test_error_handling(
        self
    ):
        """
        target: Test error handling in phrase matching
        method:
            1. Test invalid inputs
            2. Test edge cases
            3. Test error conditions
        expected: System should handle errors gracefully
        """
        dim = 128
        collection_name = f"{prefix}_error"
        schema = self.init_collection_schema(dim, "standard", False)
        collection = self.init_collection_wrap(name=collection_name, schema=schema)

        # Insert some test data
        generator = PhraseMatchTestGenerator(language="en")
        data = generator.generate_test_data(100, dim)
        collection.insert(data)

        collection.create_index(
            field_name="text",
            index_params={"index_type": "INVERTED_INDEX"}
        )
        collection.load()

        # Test invalid inputs
        invalid_cases = [
            ("", 0),  # Empty query
            ("test" * 1000, 0),  # Very long query
            ("valid query", -1),  # Negative slop
            ("valid query", 1000000)  # Very large slop
        ]

        for query, slop in invalid_cases:
            try:
                results = collection.query(
                    expr=f'phrase_match(text, "{query}", {slop})',
                    output_fields=["text"]
                )
                log.info(f"Query with '{query}' (slop={slop}) returned {len(results)} results")
            except Exception as e:
                log.info(f"Expected error for invalid case: {str(e)}")

