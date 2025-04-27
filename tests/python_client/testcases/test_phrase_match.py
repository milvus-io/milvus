from common.common_type import CaseLabel
from common.phrase_match_generator import PhraseMatchTestGenerator
import pytest
import pandas as pd
from pymilvus import FieldSchema, CollectionSchema, DataType

from common.common_type import CheckTasks
from utils.util_log import test_log as log
from common import common_func as cf
from base.client_base import TestcaseBase

prefix = "phrase_match"


def init_collection_schema(
        dim: int, tokenizer: str, enable_partition_key: bool
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
    return CollectionSchema(fields=fields, description="phrase match test collection")


@pytest.mark.tags(CaseLabel.L0)
class TestQueryPhraseMatch(TestcaseBase):
    """
    Test cases for phrase match functionality in Milvus using PhraseMatchTestGenerator.
    This class verifies the phrase matching capabilities with different configurations
    including various tokenizers, partition keys, and index settings.
    """

    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("tokenizer", ["standard", "jieba", "icu"])
    def test_query_phrase_match_with_different_tokenizer(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: Verify phrase match functionality with different tokenizers (standard, jieba)
        method: 1. Generate test data using PhraseMatchTestGenerator with language-specific content
               2. Create collection with appropriate schema (primary key, text field with analyzer, vector field)
               3. Build both vector (IVF_SQ8) and inverted indexes
               4. Execute phrase match queries with various slop values
               5. Compare results against Tantivy reference implementation
        expected: Milvus phrase match results should exactly match the reference implementation
                 results for all queries and slop values
        note: Test is marked to xfail for jieba tokenizer due to known issues
        """
        # Initialize parameters
        dim = 128
        data_size = 3000
        num_queries = 10
        analyzer_params = {"tokenizer": tokenizer}

        # Initialize generator based on tokenizer
        language = "zh" if tokenizer == "jieba" else "en"
        generator = PhraseMatchTestGenerator(language=language)

        # Create collection
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=init_collection_schema(dim, tokenizer, enable_partition_key),
        )

        # Generate test data
        test_data = generator.generate_test_data(data_size, dim)
        df = pd.DataFrame(test_data)
        log.info(f"Test data: \n{df['text']}")
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
            if tokenizer == "standard":
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

            else:
                log.info("Tokenizer is not standard, verify phrase match results by checking all query tokens in result")
                for result in results:
                    text = result["text"]
                    tokens = self.get_tokens_by_analyzer(query["query"], analyzer_params)
                    for token in tokens:
                        if token not in text:
                            log.info(f"Token {token} not in text {text}")
                            assert False

    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("tokenizer", ["standard"])
    def test_phrase_match_as_filter_in_vector_search(
            self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: Verify phrase match functionality when used as a filter in vector search
        method: 1. Generate test data with both text content and vector embeddings
               2. Create collection with vector field (128d) and text field
               3. Build both vector index (IVF_SQ8) and text inverted index
               4. Perform vector search with phrase match as a filter condition
               5. Verify the combined search results maintain accuracy
        expected: The system should correctly combine vector search with phrase match filtering
                 while maintaining both search accuracy and performance
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
            schema=init_collection_schema(dim, tokenizer, enable_partition_key),
        )

        # Generate test data
        test_data = generator.generate_test_data(data_size, dim)
        df = pd.DataFrame(test_data)
        log.info(f"Test data: \n{df['text']}")
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

            # Execute filter search
            data = [generator.generate_embedding(dim) for _ in range(10)]
            results, _ = collection_w.search(
                data,
                anns_field="emb",
                param={},
                limit=10,
                expr=expr,
                output_fields=["id", "text"],
            )

            # Get expected matches using Tantivy
            expected_matches = generator.get_query_results(
                query["query"], query["slop"]
            )
            # assert results satisfy the filter
            for hits in results:
                for hit in hits:
                    assert hit.id in expected_matches

    @pytest.mark.parametrize("slop_value", [0, 1, 2, 5, 10])
    def test_slop_parameter(self, slop_value):
        """
        target: Verify phrase matching behavior with varying slop values
        method: 1. Create collection with standard tokenizer
               2. Generate and insert data with controlled word gaps between terms
               3. Test phrase matching with specific slop values (0, 1, 2, etc.)
               4. Verify matches at different word distances
               5. Compare results with Tantivy reference implementation
        expected: Results should only match phrases where words are within the specified
                 slop distance, validating the slop parameter's distance control
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
            schema=init_collection_schema(dim, tokenizer, enable_partition_key),
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

        collection_w.create_index("text", {"index_type": "INVERTED"})

        collection_w.load()

        # Generate and execute test queries
        test_queries = generator.generate_test_queries(num_queries)

        for query in test_queries:
            expr = f"phrase_match(text, '{query['query']}', {slop_value})"
            log.info(f"Testing query: {expr}")

            # Execute query
            results, _ = collection_w.query(expr=expr, output_fields=["id", "text"])

            # Get expected matches using Tantivy
            expected_matches = generator.get_query_results(query["query"], slop_value)
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

    def test_query_phrase_match_with_different_patterns(self):
        """
        target: Verify phrase matching with various text patterns and complexities
        method: 1. Create collection with standard tokenizer
               2. Generate and insert data with diverse phrase patterns:
                  - Exact phrases ("love swimming and running")
                  - Phrases with gaps ("enjoy very basketball")
                  - Complex phrases ("practice tennis seriously often")
                  - Multiple term phrases ("swimming running cycling")
               3. Test each pattern with appropriate slop values
               4. Verify minimum match count for each pattern
        expected: System should correctly identify and match each pattern type
                 with the specified number of matches per pattern
        """
        dim = 128
        collection_name = f"{prefix}_patterns"
        schema = init_collection_schema(dim, "standard", False)
        collection = self.init_collection_wrap(name=collection_name, schema=schema)

        # Generate data with various patterns
        generator = PhraseMatchTestGenerator(language="en")
        data = generator.generate_test_data(3000, dim)
        collection.insert(data)
        # Test various patterns
        test_patterns = [
            ("love swimming and running", 0),  # Exact phrase
            ("enjoy very basketball", 1),  # Phrase with gap
            ("practice tennis seriously often", 2),  # Complex phrase
            ("swimming running cycling", 5),  # Multiple activities
        ]

        # Generate and insert documents that match the patterns
        num_docs_per_pattern = 100
        pattern_documents = generator.generate_pattern_documents(
            test_patterns, dim, num_docs_per_pattern=num_docs_per_pattern
        )
        collection.insert(pattern_documents)
        df = pd.DataFrame(pattern_documents)[["id", "text"]]
        log.info(f"Test data:\n {df}")
        collection.flush()
        collection.create_index(
            field_name="text", index_params={"index_type": "INVERTED"}
        )
        collection.create_index(
            field_name="emb",
            index_params={
                "index_type": "IVF_SQ8",
                "metric_type": "L2",
                "params": {"nlist": 64},
            },
        )
        collection.load()

        for pattern, slop in test_patterns:
            results, _ = collection.query(
                expr=f'phrase_match(text, "{pattern}", {slop})', output_fields=["text"]
            )
            log.info(
                f"Pattern '{pattern}' with slop {slop} found {len(results)} matches"
            )
            assert len(results) >= num_docs_per_pattern


@pytest.mark.tags(CaseLabel.L1)
class TestQueryPhraseMatchNegative(TestcaseBase):
    def test_query_phrase_match_with_invalid_slop(self):
        """
        target: Verify error handling for invalid slop values in phrase matching
        method: 1. Create collection with standard test data
               2. Test phrase matching with invalid slop values:
                  - Negative slop values (-1)
                  - Extremely large slop values (10^31)
               3. Verify error handling and response
        expected: System should:
                 1. Reject queries with invalid slop values
                 2. Return appropriate error responses
                 3. Maintain system stability after invalid queries
        """
        dim = 128
        collection_name = f"{prefix}_invalid_slop"
        schema = init_collection_schema(dim, "standard", False)
        collection = self.init_collection_wrap(name=collection_name, schema=schema)

        # Insert some test data
        generator = PhraseMatchTestGenerator(language="en")
        data = generator.generate_test_data(100, dim)
        collection.insert(data)

        collection.create_index(
            field_name="text", index_params={"index_type": "INVERTED"}
        )
        collection.create_index(
            field_name="emb",
            index_params={
                "index_type": "IVF_SQ8",
                "metric_type": "L2",
                "params": {"nlist": 64},
            },
        )
        collection.load()

        # Test invalid inputs
        invalid_cases = [
            ("valid query", -1),  # Negative slop
            ("valid query", 10 ** 31),  # Very large slop
        ]

        for query, slop in invalid_cases:
            res, result = collection.query(
                expr=f'phrase_match(text, "{query}", {slop})',
                output_fields=["text"],
                check_task=CheckTasks.check_nothing,
            )
            log.info(f"Query: '{query[:10]}' with slop {slop} returned {res}")
            assert result is False
