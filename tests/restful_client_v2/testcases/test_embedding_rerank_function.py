import pytest
import numpy as np
from faker import Faker
from base.testbase import TestBase
from utils.utils import gen_collection_name
from utils.util_log import test_log as logger

fake_en = Faker("en_US")

prefix = "text_embedding_search"


@pytest.mark.L0
class TestTextEmbeddingSearch(TestBase):
    """
    ******************************************************************
      The following cases are used to test text embedding function search via RESTful API
    ******************************************************************
    """

    def _create_basic_collection_payload(self, name, tei_endpoint, dim=768, with_bm25=False):
        """Helper method to create basic collection payload with TEI function"""
        fields = [
            {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
            {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
            {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
        ]
        
        functions = [{
            "name": "tei",
            "type": "TextEmbedding",
            "inputFieldNames": ["document"],
            "outputFieldNames": ["dense"],
            "params": {
                "provider": "TEI",
                "endpoint": tei_endpoint
            }
        }]
        
        if with_bm25:
            fields[1]["elementTypeParams"].update({
                "enable_analyzer": True,
                "analyzer_params": {"tokenizer": "standard"},
                "enable_match": True
            })
            fields.append({"fieldName": "sparse", "dataType": "SparseFloatVector"})
            functions.append({
                "name": "bm25_fn",
                "type": "BM25",
                "inputFieldNames": ["document"],
                "outputFieldNames": ["sparse"],
                "params": {}
            })
        
        return {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": fields,
                "functions": functions
            }
        }

    def _create_and_verify_collection(self, name, tei_endpoint, dim=768, with_bm25=False):
        """Helper method to create collection and verify creation"""
        payload = self._create_basic_collection_payload(name, tei_endpoint, dim, with_bm25)
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Verify collection was created
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0, f"Collection describe failed: {rsp}"
        assert rsp['data']['collectionName'] == name, f"Collection name mismatch: expected {name}, got {rsp['data']['collectionName']}"
        return payload

    def _insert_and_verify_data(self, name, data):
        """Helper method to insert data and verify insertion"""
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        assert rsp['data']['insertCount'] == len(data), f"Expected {len(data)} inserts, got {rsp['data']['insertCount']}"
        return rsp

    def _create_index_and_load(self, name, index_fields=None):
        """Helper method to create index and load collection"""
        if index_fields is None:
            index_fields = [{"fieldName": "dense", "indexName": "dense_index", "metricType": "COSINE"}]
        
        index_payload = {
            "collectionName": name,
            "indexParams": [
                {**field, "indexType": "AUTOINDEX", "params": {}} 
                for field in index_fields
            ]
        }
        rsp = self.index_client.index_create(index_payload)
        assert rsp['code'] == 0, f"Index creation failed: {rsp}"

        # Load collection
        rsp = self.collection_client.collection_load(collection_name=name)
        assert rsp['code'] == 0, f"Collection load failed: {rsp}"

    def test_simple_tei_text_embedding_workflow(self, tei_endpoint):
        """
        target: test simple TEI text embedding workflow
        method: create collection, insert data, create index, load, and search
        expected: all operations succeed
        """
        name = gen_collection_name(prefix)
        
        # Create collection with TEI text embedding function
        self._create_and_verify_collection(name, tei_endpoint)

        # Insert simple text data
        data = [
            {"id": 1, "document": "This is a test document"},
            {"id": 2, "document": "Another test document"}
        ]
        self._insert_and_verify_data(name, data)

        # Create index and load collection
        self._create_index_and_load(name)

        # Search
        search_payload = {
            "collectionName": name,
            "data": ["test document"],
            "limit": 2,
            "outputFields": ["id", "document"]
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] == 0, f"Search failed: {rsp}"
        assert len(rsp['data']) > 0, f"Search returned no results: {rsp['data']}"

    def test_create_collection_with_tei_text_embedding_function(self, tei_endpoint):
        """
        target: test create collection with TEI text embedding function via REST API (equivalent to ORM example)
        method: create collection with TEI text embedding function using RESTful API
        expected: create collection successfully
        """
        name = gen_collection_name(prefix)
        
        # Create collection with additional truncation parameters
        payload = self._create_basic_collection_payload(name, tei_endpoint)
        payload["schema"]["functions"][0]["params"].update({
            "truncate": True,
            "truncation_direction": "Right"
        })
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Verify collection was created with function
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0, f"Collection describe failed: {rsp}"
        assert rsp['data']['collectionName'] == name, f"Collection name mismatch: expected {name}, got {rsp['data']['collectionName']}"


    @pytest.mark.parametrize("truncate", [True, False])
    @pytest.mark.parametrize("truncation_direction", ["Left", "Right"])
    def test_insert_with_tei_text_embedding_truncation(self, tei_endpoint, truncate, truncation_direction):
        """
        target: test insert data with TEI text embedding function with truncation parameters
        method: insert long text data with different truncation settings
        expected: insert successfully and truncation works as expected
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with TEI text embedding function including truncation params
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint,
                            "truncate": truncate,
                            "truncation_direction": truncation_direction
                        }
                    }
                ]
            }
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Prepare test data with long text similar to ORM test
        left_text = " ".join([fake_en.word() for _ in range(512)])
        right_text = " ".join([fake_en.word() for _ in range(512)])
        data = [
            {
                "id": 0,
                "document": left_text + " " + right_text
            },
            {
                "id": 1,
                "document": left_text
            },
            {
                "id": 2,
                "document": right_text
            }
        ]
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        
        if not truncate:
            logger.info(f"Truncate is False, insertion result: {rsp}")
            return
        
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        assert rsp['data']['insertCount'] == len(data), f"Expected {len(data)} inserts, got {rsp['data']['insertCount']}"

        # Create index and load for similarity comparison
        index_payload = {
            "collectionName": name,
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index",
                    "metricType": "COSINE",
                    "indexType": "AUTOINDEX",
                    "params": {}
                }
            ]
        }
        rsp = self.index_client.index_create(index_payload)
        assert rsp['code'] == 0, f"Index creation failed: {rsp}"

        # Load collection
        rsp = self.collection_client.collection_load(collection_name=name)
        assert rsp['code'] == 0

        # Query to get embeddings for similarity comparison
        query_payload = {
            "collectionName": name,
            "filter": "id >= 0",
            "outputFields": ["id", "dense"],
            "limit": 10
        }
        
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0, f"Query failed: {rsp}"
        assert len(rsp['data']) == 3, f"Expected 3 results, got {len(rsp['data'])}"

        # Compare similarity between embeddings to verify truncation direction
        embeddings = {}
        for result in rsp['data']:
            embeddings[result['id']] = result['dense']
        
        # Calculate cosine similarity
        similarity_left = np.dot(embeddings[0], embeddings[1]) / (
            np.linalg.norm(embeddings[0]) * np.linalg.norm(embeddings[1])
        )
        similarity_right = np.dot(embeddings[0], embeddings[2]) / (
            np.linalg.norm(embeddings[0]) * np.linalg.norm(embeddings[2])
        )
        
        logger.info(f"Similarity with left: {similarity_left}, with right: {similarity_right}")
        
        if truncation_direction == "Left":
            # When truncating from left, the combined text should be more similar to right text
            assert similarity_left < similarity_right, (
                f"Left truncation failed: left_sim={similarity_left:.4f}, right_sim={similarity_right:.4f}"
            )
        else:  # Right truncation
            # When truncating from right, the combined text should be more similar to left text
            assert similarity_left > similarity_right, (
                f"Right truncation failed: left_sim={similarity_left:.4f}, right_sim={similarity_right:.4f}"
            )
        

    def test_insert_with_tei_text_embedding_function(self, tei_endpoint):
        """
        target: test insert data with TEI text embedding function via REST API
        method: insert text data, embeddings should be automatically generated by TEI
        expected: insert successfully and embeddings are generated
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with TEI text embedding function
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "tei", 
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    }
                ]
            }
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert text data without embedding vectors (they should be auto-generated by TEI)
        nb = 10
        data = []
        for i in range(nb):
            data.append({
                "id": i,
                "document": fake_en.text()
            })
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        assert rsp['data']['insertCount'] == nb, f"Expected {nb} inserts, got {rsp['data']['insertCount']}"

    def test_search_with_tei_text_embedding_function(self, tei_endpoint):
        """
        target: test search with TEI text embedding function via REST API
        method: 1. create collection with TEI text embedding function
                2. insert text data
                3. search with text query (should auto-generate embedding via TEI)
        expected: search successfully with relevant results
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with TEI text embedding function
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding", 
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index",
                    "metricType": "COSINE"
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert text data
        nb = 100
        documents = [
            "Machine learning is a subset of artificial intelligence",
            "Deep learning uses neural networks with multiple layers",
            "Natural language processing helps computers understand text",
            "Computer vision enables machines to interpret visual information",
            "Reinforcement learning trains agents through rewards and penalties"
        ]
        
        data = []
        for i in range(nb):
            data.append({
                "id": i,
                "document": documents[i % len(documents)] + f" Document {i}"
            })
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        # Search with text query (TEI will auto-generate embedding)
        search_payload = {
            "collectionName": name,
            "data": ["artificial intelligence and machine learning"],
            "limit": 10,
            "outputFields": ["id", "document"]
        }
        
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] == 0, f"Search failed: {rsp}"
        assert len(rsp['data']) > 0, f"Search returned no results"
        
        # Verify search results contain relevant documents
        found_relevant = any(
            "machine learning" in result.get('document', '').lower() or
            "artificial intelligence" in result.get('document', '').lower()
            for result in rsp['data']
        )
        assert found_relevant, f"Search should return relevant documents, got: {[r.get('document', '') for r in rsp['data']]}"
        

    def test_tei_and_bm25_collection_creation(self, tei_endpoint):
        """
        target: test create collection with both TEI and BM25 functions using correct format
        method: create collection with TEI text embedding and BM25 functions based on working example
        expected: collection creation succeeds
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with both TEI and BM25 functions using correct format
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "document", 
                        "dataType": "VarChar", 
                        "elementTypeParams": {
                            "max_length": "1000",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True
                        }
                    },
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "sparse", "dataType": "SparseFloatVector"}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    },
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["sparse"],
                        "params": {}
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index", 
                    "metricType": "COSINE"
                },
                {
                    "fieldName": "sparse",
                    "indexName": "sparse_index",
                    "metricType": "BM25",
                    "params": {"index_type": "SPARSE_INVERTED_INDEX"}
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert test data 
        data = []
        for i in range(10):
            data.append({
                "id": i,
                "document": fake_en.text().lower()
            })
        
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        assert rsp['data']['insertCount'] == 10, f"Expected 10 inserts, got {rsp['data']['insertCount']}"

        # Test search with BM25 (sparse vector)
        search_payload = {
            "collectionName": name,
            "data": [fake_en.text().lower()],
            "annsField": "sparse",
            "limit": 5,
            "outputFields": ["id", "document"]
        }
        
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] == 0, f"BM25 search failed: {rsp}"
        assert len(rsp['data']) > 0, f"BM25 search returned no results"
        
        # test search with dense vector
        search_payload = {
            "collectionName": name,
            "data": [fake_en.text().lower()],
            "annsField": "dense",
            "limit": 5,
            "outputFields": ["id", "document"]
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] == 0, f"Dense search failed: {rsp}"
        assert len(rsp['data']) > 0, f"Dense search returned no results"
        

    def test_hybrid_search_with_text_embedding_and_bm25(self, tei_endpoint):
        """
        target: test hybrid search combining text embedding and BM25 via REST API
        method: 1. create collection with both text embedding and BM25 functions
                2. insert text data
                3. perform hybrid search
        expected: hybrid search returns combined results
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with both text embedding and BM25 functions
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "document", 
                        "dataType": "VarChar", 
                        "elementTypeParams": {
                            "max_length": "65535",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True
                        }
                    },
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}},
                    {"fieldName": "sparse", "dataType": "SparseFloatVector"}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    },
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["sparse"],
                        "params": {}
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index", 
                    "metricType": "COSINE"
                },
                {
                    "fieldName": "sparse",
                    "indexName": "sparse_index",
                    "metricType": "BM25",
                    "params": {"index_type": "SPARSE_INVERTED_INDEX"}
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert diverse text data
        documents = [
            "Python is a popular programming language for data science",
            "JavaScript is widely used for web development",
            "Machine learning algorithms can predict future trends",
            "Database systems store and manage large amounts of data",
            "Cloud computing provides scalable infrastructure solutions",
            "Artificial intelligence transforms various industries",
            "Software engineering practices improve code quality",
            "Data visualization helps understand complex datasets",
            "Cybersecurity protects digital assets from threats",
            "Mobile applications provide convenient user experiences"
        ]
        
        data = []
        for i in range(50):
            data.append({
                "id": i,
                "document": documents[i % len(documents)] + f" Extended content {i}"
            })
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        # Perform hybrid search using advanced search
        hybrid_search_payload = {
            "collectionName": name,
            "search": [
                {
                    "data": ["programming language data science"],
                    "annsField": "dense",
                    "limit": 20
                },
                {
                    "data": ["programming language data science"],
                    "annsField": "sparse", 
                    "limit": 20
                }
            ],
            "rerank": {
                "strategy": "weighted",
                "params": {"weights": [0.7, 0.3]}
            },
            "limit": 10,
            "outputFields": ["id", "document"]
        }
        
        rsp = self.vector_client.vector_advanced_search(hybrid_search_payload)
        assert rsp['code'] == 0, f"Hybrid search failed: {rsp}"
        assert len(rsp['data']) > 0, f"Hybrid search returned no results"
        
        # Verify hybrid search results are relevant
        found_relevant = any(
            any(term in result.get('document', '').lower() for term in ['python', 'programming', 'data'])
            for result in rsp['data']
        )
        assert found_relevant, f"Hybrid search should return relevant documents, got: {[r.get('document', '') for r in rsp['data']]}"


@pytest.mark.L1  
class TestTextEmbeddingSearchAdvanced(TestBase):
    """
    ******************************************************************
      Advanced test cases for text embedding function search via RESTful API
    ******************************************************************
    """

    def test_search_with_filter_and_text_embedding(self, tei_endpoint):
        """
        target: test search with both text embedding and scalar filters
        method: 1. create collection with text embedding function and metadata fields
                2. insert text data with metadata
                3. search with text query and scalar filters
        expected: search returns filtered and relevant results
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with text embedding function and metadata fields
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "100"}},
                    {"fieldName": "year", "dataType": "Int64"},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index",
                    "metricType": "COSINE"
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert text data with metadata
        categories = ["technology", "science", "business", "education"]
        years = [2020, 2021, 2022, 2023, 2024]
        
        data = []
        for i in range(100):
            data.append({
                "id": i,
                "document": fake_en.text(),
                "category": categories[i % len(categories)],
                "year": years[i % len(years)]
            })
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        # Search with text query and filters
        search_payload = {
            "collectionName": name,
            "data": ["technology innovation"],
            "filter": "category == 'technology' and year >= 2022",
            "limit": 10,
            "outputFields": ["id", "document", "category", "year"]
        }
        
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] == 0
        
        # Verify all results match the filter criteria
        for result in rsp['data']:
            assert result['category'] == 'technology', f"Category mismatch: expected 'technology', got '{result['category']}'"
            assert result['year'] >= 2022, f"Year filter failed: expected >= 2022, got {result['year']}"
        

    def test_upsert_with_text_embedding_function(self, tei_endpoint):
        """
        target: test upsert operation with text embedding function
        method: 1. insert initial text data
                2. upsert with modified text content
                3. verify embeddings are updated
        expected: upsert successfully updates both text and embeddings
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with text embedding function
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False, 
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index",
                    "metricType": "COSINE"
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert initial data
        original_text = "The original document about machine learning"
        data = [{"id": 1, "document": original_text}]
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        # Query original embedding
        query_payload = {
            "collectionName": name,
            "filter": "id == 1",
            "outputFields": ["id", "document", "dense"],
            "limit": 10
        }
        
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0, f"Original query failed: {rsp}"
        assert len(rsp['data']) > 0, f"Original query returned no results"
        original_embedding = rsp['data'][0]['dense']

        # Upsert with modified text
        updated_text = "The updated document about deep learning and neural networks"
        upsert_data = [{"id": 1, "document": updated_text}]
        
        payload = {
            "collectionName": name,
            "data": upsert_data
        }
        
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0, f"Upsert failed: {rsp}"

        # Query updated embedding
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0, f"Updated query failed: {rsp}"
        assert len(rsp['data']) > 0, f"Updated query returned no results"
        updated_embedding = rsp['data'][0]['dense']
        
        # Verify text was updated
        assert rsp['data'][0]['document'] == updated_text, f"Text not updated: expected '{updated_text}', got '{rsp['data'][0]['document']}'"
        
        # Verify embedding was updated (embeddings should be different)
        similarity = np.dot(original_embedding, updated_embedding) / (
            np.linalg.norm(original_embedding) * np.linalg.norm(updated_embedding)
        )
        assert similarity < 0.99, f"Embedding should be significantly different after text update, similarity: {similarity:.4f}"


@pytest.mark.L2
class TestTextEmbeddingSearchNegative(TestBase):
    """
    ******************************************************************
      Negative test cases for text embedding function search via RESTful API  
    ******************************************************************
    """

    def test_create_collection_with_invalid_text_embedding_params(self):
        """
        target: test create collection with invalid text embedding function parameters
        method: create collection with invalid embedding provider/model
        expected: collection creation should fail with appropriate error
        """
        name = gen_collection_name(prefix)
        dim = 1024
        
        # Create collection with invalid text embedding function
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "text_embedding_fn",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "invalid_provider",
                            "model_name": "invalid_model",
                            "api_key": "invalid_key"
                        }
                    }
                ]
            }
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] != 0, f"Expected creation to fail with invalid provider, but got: {rsp}"

    def test_search_with_empty_query_text(self, tei_endpoint):
        """
        target: test search with empty text query
        method: 1. create collection with text embedding function
                2. insert data
                3. search with empty string
        expected: search should handle empty query appropriately
        """
        name = gen_collection_name(prefix)
        dim = 768
        
        # Create collection with text embedding function
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(dim)}}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    }
                ]
            }
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert sample data
        data = [{"id": i, "document": fake_en.text()} for i in range(10)]
        
        payload = {
            "collectionName": name,
            "data": data
        }
        
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        # Search with empty query
        search_payload = {
            "collectionName": name,
            "data": [""],
            "limit": 5,
            "outputFields": ["id", "document"]
        }
        
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] != 0, f"Expected search to fail with empty query, but got: {rsp}"


    def test_dimension_mismatch_with_text_embedding(self, tei_endpoint):
        """
        target: test dimension mismatch between text embedding function and vector field
        method: create collection with mismatched dimensions
        expected: collection creation should fail
        """
        name = gen_collection_name(prefix)
        wrong_dim = 512  # TEI produces 768-dim vectors
        
        # Create collection with mismatched dimensions
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": str(wrong_dim)}}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint  # This produces 768-dim vectors
                        }
                    }
                ]
            }
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] != 0, f"Expected creation to fail with dimension mismatch, but got: {rsp}"

class TestModelRerankFunction(TestBase):
    """
    ******************************************************************
      Test cases for model rerank function via RESTful API
    ******************************************************************
    """

    def _create_collection_with_all_vector_types(self, name, tei_endpoint):
        """Helper method to create collection with dense, sparse, and bm25 fields"""
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection with all vector types",
                "fields": [
                    {"fieldName": "doc_id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "document", 
                        "dataType": "VarChar", 
                        "elementTypeParams": {
                            "max_length": "65535",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True
                        }
                    },
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "768"}},
                    {"fieldName": "sparse", "dataType": "SparseFloatVector"},
                    {"fieldName": "bm25", "dataType": "SparseFloatVector"}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    },
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["bm25"],
                        "params": {}
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index", 
                    "metricType": "COSINE"
                },
                {
                    "fieldName": "sparse",
                    "indexName": "sparse_index",
                    "metricType": "IP",
                    "params": {"index_type": "SPARSE_INVERTED_INDEX"}
                },
                {
                    "fieldName": "bm25",
                    "indexName": "bm25_index",
                    "metricType": "BM25",
                    "params": {"index_type": "SPARSE_INVERTED_INDEX"}
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Insert sample data
        import random
        data = []
        for i in range(50):
            data.append({
                "doc_id": i,
                "document": fake_en.text(),
                "sparse": {random.randint(1, 10000): random.random() for _ in range(100)}
            })
        
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        
        return name

    @pytest.mark.parametrize("enable_truncate", [False, True])
    def test_single_vector_search_with_model_rerank(self, tei_endpoint, enable_truncate, 
                                                  tei_reranker_endpoint):
        """
        target: test single vector search with model rerank using RESTful API
        method: test dense/sparse/bm25 search with model reranker separately
        expected: search should succeed with model reranker
        """
        import random
        
        name = gen_collection_name(prefix)
        self._create_collection_with_all_vector_types(name, tei_endpoint)
        
        # Prepare search parameters for reranker
        nq = 2
        query_texts = [fake_en.text() for _ in range(nq)]
        if enable_truncate:
            # Make query texts larger for truncation test
            query_texts = [" ".join([fake_en.word() for _ in range(1024)]) for _ in range(nq)]
        
        # Prepare reranker parameters (functionScore format)
        reranker_params = {
            "functions": [{
                "name": "tei_reranker",
                "description": "",
                "type": "Rerank",
                "inputFieldNames": ["document"],
                "params": {
                    "reranker": "model",
                    "provider": "tei",
                    "queries": query_texts,
                    "endpoint": tei_reranker_endpoint,
                    "truncate": enable_truncate,
                    "truncation_direction": "Right"
                }
            }]
        }
        
        # Test different search types
        for search_type in ["dense", "sparse", "bm25"]:
            logger.info(f"Executing {search_type} search with model reranker")
            
            if search_type == "dense":
                # Dense vector search
                search_payload = {
                    "collectionName": name,
                    "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                    "annsField": "dense",
                    "limit": 10,
                    "outputFields": ["doc_id", "document"],
                    "functionScore": reranker_params
                }
                
            elif search_type == "sparse":
                # Sparse vector search
                search_payload = {
                    "collectionName": name,
                    "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                    "annsField": "sparse",
                    "limit": 10,
                    "outputFields": ["doc_id", "document"],
                    "functionScore": reranker_params
                }
                
            elif search_type == "bm25":
                # BM25 search
                search_payload = {
                    "collectionName": name,
                    "data": query_texts,
                    "annsField": "bm25",
                    "limit": 10,
                    "outputFields": ["doc_id", "document"],
                    "searchParams": {"metric_type": "BM25"},
                    "functionScore": reranker_params
                }
            
            rsp = self.vector_client.vector_search(search_payload)
            assert rsp['code'] == 0, f"{search_type} search with model reranker failed: {rsp}"
            assert len(rsp['data']) > 0, f"{search_type} search returned no results"
            logger.info(f"{search_type} search with TEI reranker succeeded")

    def test_hybrid_vector_search_with_model_rerank(self, tei_endpoint, 
                                                  tei_reranker_endpoint):
        """
        target: test hybrid vector search with model rerank using RESTful API
        method: test dense+sparse/dense+bm25/sparse+bm25 search with model reranker
        expected: hybrid search should succeed with model reranker
        """
        import random
        
        name = gen_collection_name(prefix)
        self._create_collection_with_all_vector_types(name, tei_endpoint)
        
        # Prepare search parameters for reranker
        nq = 2
        query_texts = [fake_en.text() for _ in range(nq)]
        
        # Prepare reranker parameters (functionScore format)
        reranker_params = {
            "functions": [{
                "name": "tei_reranker",
                "description": "",
                "type": "Rerank",
                "inputFieldNames": ["document"],
                "params": {
                    "reranker": "model",
                    "provider": "tei",
                    "queries": query_texts,
                    "endpoint": tei_reranker_endpoint
                }
            }]
        }
        
        # Test different hybrid search combinations
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            logger.info(f"Executing {search_type} hybrid search with model reranker")
            
            if search_type == "dense+sparse":
                hybrid_search_payload = {
                    "collectionName": name,
                    "search": [
                        {
                            "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                            "annsField": "dense",
                            "limit": 5
                        },
                        {
                            "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                            "annsField": "sparse",
                            "limit": 5
                        }
                    ],
                    "functionScore": reranker_params,
                    "limit": 10,
                    "outputFields": ["doc_id", "document"]
                }
                
            elif search_type == "dense+bm25":
                hybrid_search_payload = {
                    "collectionName": name,
                    "search": [
                        {
                            "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                            "annsField": "dense",
                            "limit": 5
                        },
                        {
                            "data": query_texts,
                            "annsField": "bm25",
                            "limit": 5,
                            "params": {"metric_type": "BM25"}
                        }
                    ],
                    "functionScore": reranker_params,
                    "limit": 10,
                    "outputFields": ["doc_id", "document"]
                }
                
            elif search_type == "sparse+bm25":
                hybrid_search_payload = {
                    "collectionName": name,
                    "search": [
                        {
                            "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                            "annsField": "sparse",
                            "limit": 5
                        },
                        {
                            "data": query_texts,
                            "annsField": "bm25", 
                            "limit": 5,
                            "params": {"metric_type": "BM25"}
                        }
                    ],
                    "functionScore": reranker_params,
                    "limit": 10,
                    "outputFields": ["doc_id", "document"]
                }
            
            rsp = self.vector_client.vector_advanced_search(hybrid_search_payload)
            assert rsp['code'] == 0, f"{search_type} hybrid search with model reranker failed: {rsp}"
            assert len(rsp['data']) > 0, f"{search_type} hybrid search returned no results"
            logger.info(f"{search_type} hybrid search with TEI reranker succeeded")


@pytest.mark.L1
class TestDecayRerank(TestBase):
    """
    ******************************************************************
      Test cases for Decay rerank function via RESTful API
    ******************************************************************
    """

    def _create_collection_with_timestamp_field(self, name, tei_endpoint):
        """Helper method to create collection with timestamp field for decay rerank"""
        import time
        
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection for decay rerank",
                "fields": [
                    {"fieldName": "doc_id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "document", 
                        "dataType": "VarChar", 
                        "elementTypeParams": {
                            "max_length": "65535",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True
                        }
                    },
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "768"}},
                    {"fieldName": "sparse", "dataType": "SparseFloatVector"},
                    {"fieldName": "timestamp", "dataType": "Int64"}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    },
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["sparse"],
                        "params": {}
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index", 
                    "metricType": "COSINE",
                    "indexType": "AUTOINDEX",
                    "params": {}
                },
                {
                    "fieldName": "sparse",
                    "indexName": "sparse_index",
                    "metricType": "BM25",
                    "indexType": "SPARSE_INVERTED_INDEX",
                    "params": {"bm25_k1": 1.2, "bm25_b": 0.75}
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Insert sample data with different timestamps
        current_time = int(time.time())
        data = []
        news_documents = [
            "Artificial intelligence helps medical breakthroughs",
            "Analysis of artificial intelligence trends in 2023", 
            "Artificial intelligence ethical disputes continue to ferment",
            "The latest progress in deep learning technology",
            "Machine learning algorithms improve healthcare diagnosis",
            "Neural networks advance computer vision capabilities",
            "Natural language processing enables better chatbots",
            "Robotics automation transforms manufacturing industry",
            "Quantum computing promises revolutionary breakthroughs",
            "Blockchain technology secures digital transactions"
        ]
        
        # Create data with timestamps ranging from 30 days ago to current time
        for i in range(len(news_documents)):
            timestamp_offset = (len(news_documents) - i - 1) * 24 * 60 * 60 * 3  # 3 days apart
            data.append({
                "doc_id": i,
                "document": news_documents[i],
                "timestamp": current_time - timestamp_offset
            })
        
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        
        return name, current_time

    @pytest.mark.parametrize("decay_function", ["gauss", "exp", "linear"])
    def test_single_vector_search_with_decay_rerank(self, tei_endpoint, decay_function):
        """
        target: test single vector search with decay rerank using different decay functions
        method: test dense/sparse search with gauss/exp/linear decay reranker
        expected: search should succeed with decay reranker and time-based ranking
        """
        import random
        
        name = gen_collection_name(prefix)
        collection_name, current_time = self._create_collection_with_timestamp_field(name, tei_endpoint)
        
        # Prepare decay reranker parameters
        decay_params = {
            "functions": [{
                "name": f"{decay_function}_decay_ranker",
                "description": "",
                "type": "Rerank",
                "inputFieldNames": ["timestamp"],
                "params": {
                    "reranker": "decay",
                    "function": decay_function,
                    "origin": current_time,  # Current time as origin
                    "offset": 0,
                    "decay": 0.5,
                    "scale": 7 * 24 * 60 * 60  # 7 days in seconds
                }
            }]
        }
        
        # Test different search types
        for search_type in ["dense", "sparse"]:
            logger.info(f"Executing {search_type} search with {decay_function} decay reranker")
            
            if search_type == "dense":
                # Dense vector search
                search_payload = {
                    "collectionName": collection_name,
                    "data": ["artificial intelligence technology progress"],
                    "annsField": "dense",
                    "limit": 10,
                    "outputFields": ["doc_id", "document", "timestamp"],
                    "functionScore": decay_params
                }
                
            elif search_type == "sparse":
                # Sparse vector search
                search_payload = {
                    "collectionName": collection_name,
                    "data": ["artificial intelligence technology progress"],
                    "annsField": "sparse",
                    "limit": 10,
                    "outputFields": ["doc_id", "document", "timestamp"],
                    "functionScore": decay_params
                }
            
            rsp = self.vector_client.vector_search(search_payload)
            assert rsp['code'] == 0, f"{search_type} search with {decay_function} decay reranker failed: {rsp}"
            assert len(rsp['data']) > 0, f"{search_type} search returned no results"
            
            # Verify time-based ranking: more recent documents should have higher scores
            if len(rsp['data']) > 1:
                # Check that results are ordered by final score (which includes decay)
                scores = [result.get('distance', 0) for result in rsp['data']]
                logger.info(f"{decay_function} decay results scores: {scores}")
                
            logger.info(f"{search_type} search with {decay_function} decay reranker succeeded")

    @pytest.mark.parametrize("decay_function", ["gauss", "exp", "linear"])
    def test_hybrid_search_with_decay_rerank(self, tei_endpoint, decay_function):
        """
        target: test hybrid search with decay rerank using different decay functions
        method: test dense+sparse hybrid search with decay reranker
        expected: hybrid search should succeed with decay reranker
        """
        import random
        
        name = gen_collection_name(prefix)
        collection_name, current_time = self._create_collection_with_timestamp_field(name, tei_endpoint)
        
        # Prepare decay reranker parameters
        decay_params = {
            "functions": [{
                "name": f"{decay_function}_decay_ranker",
                "description": "",
                "type": "Rerank",
                "inputFieldNames": ["timestamp"],
                "params": {
                    "reranker": "decay",
                    "function": decay_function,
                    "origin": current_time,
                    "offset": 2 * 24 * 60 * 60,  # 2 days offset
                    "decay": 0.3,  # More aggressive decay
                    "scale": 5 * 24 * 60 * 60   # 5 days scale
                }
            }]
        }
        
        logger.info(f"Executing hybrid search with {decay_function} decay reranker")
        
        # Hybrid search with decay rerank
        hybrid_search_payload = {
            "collectionName": collection_name,
            "search": [
                {
                    "data": ["artificial intelligence machine learning"],
                    "annsField": "dense",
                    "limit": 5
                },
                {
                    "data": ["artificial intelligence machine learning"],
                    "annsField": "sparse",
                    "limit": 5
                }
            ],
            "functionScore": decay_params,
            "limit": 10,
            "outputFields": ["doc_id", "document", "timestamp"]
        }
        
        rsp = self.vector_client.vector_advanced_search(hybrid_search_payload)
        assert rsp['code'] == 0, f"Hybrid search with {decay_function} decay reranker failed: {rsp}"
        assert len(rsp['data']) > 0, f"Hybrid search returned no results"
        
        # Log results for manual verification
        logger.info(f"Hybrid search with {decay_function} decay reranker results:")
        for i, result in enumerate(rsp['data'][:3]):  # Show top 3 results
            logger.info(f"  {i+1}. Doc: {result.get('document', '')[:50]}... Timestamp: {result.get('timestamp', 0)}")
        
        logger.info(f"Hybrid search with {decay_function} decay reranker succeeded")

    def test_decay_rerank_with_different_parameters(self, tei_endpoint):
        """
        target: test decay rerank with different parameter combinations
        method: test different origin, offset, decay, scale parameters
        expected: search should succeed with different parameter configurations
        """
        name = gen_collection_name(prefix)
        collection_name, current_time = self._create_collection_with_timestamp_field(name, tei_endpoint)
        
        # Test different parameter combinations
        test_configs = [
            {
                "name": "no_offset_high_decay",
                "params": {
                    "origin": current_time,
                    "offset": 0,
                    "decay": 0.8,  # High decay rate
                    "scale": 3 * 24 * 60 * 60
                }
            },
            {
                "name": "with_offset_low_decay", 
                "params": {
                    "origin": current_time,
                    "offset": 5 * 24 * 60 * 60,  # 5 days offset
                    "decay": 0.2,  # Low decay rate
                    "scale": 10 * 24 * 60 * 60
                }
            },
            {
                "name": "past_origin",
                "params": {
                    "origin": current_time - 15 * 24 * 60 * 60,  # 15 days ago
                    "offset": 0,
                    "decay": 0.5,
                    "scale": 7 * 24 * 60 * 60
                }
            }
        ]
        
        for config in test_configs:
            logger.info(f"Testing decay rerank with config: {config['name']}")
            
            decay_params = {
                "functions": [{
                    "name": f"decay_ranker_{config['name']}",
                    "description": "",
                    "type": "Rerank",
                    "inputFieldNames": ["timestamp"],
                    "params": {
                        "reranker": "decay",
                        "function": "gauss",
                        **config["params"]
                    }
                }]
            }
            
            search_payload = {
                "collectionName": collection_name,
                "data": ["technology progress artificial intelligence"],
                "annsField": "dense", 
                "limit": 10,
                "outputFields": ["doc_id", "document", "timestamp"],
                "functionScore": decay_params
            }
            
            rsp = self.vector_client.vector_search(search_payload)
            assert rsp['code'] == 0, f"Search with {config['name']} failed: {rsp}"
            assert len(rsp['data']) > 0, f"Search with {config['name']} returned no results"
            
            logger.info(f"Decay rerank with {config['name']} succeeded, returned {len(rsp['data'])} results")


@pytest.mark.L2
class TestDecayRerankNegative(TestBase):
    """
    ******************************************************************
      Negative test cases for Decay rerank function via RESTful API
    ******************************************************************
    """

    def _create_collection_with_timestamp_field(self, name, tei_endpoint):
        """Helper method to create collection with timestamp field for decay rerank"""
        import time
        
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection for decay rerank",
                "fields": [
                    {"fieldName": "doc_id", "dataType": "Int64", "isPrimary": True},
                    {
                        "fieldName": "document", 
                        "dataType": "VarChar", 
                        "elementTypeParams": {
                            "max_length": "65535",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True
                        }
                    },
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "768"}},
                    {"fieldName": "sparse", "dataType": "SparseFloatVector"},
                    {"fieldName": "timestamp", "dataType": "Int64"}
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    },
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["sparse"],
                        "params": {}
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index", 
                    "metricType": "COSINE",
                    "indexType": "AUTOINDEX",
                    "params": {}
                },
                {
                    "fieldName": "sparse",
                    "indexName": "sparse_index",
                    "metricType": "BM25",
                    "indexType": "SPARSE_INVERTED_INDEX",
                    "params": {"bm25_k1": 1.2, "bm25_b": 0.75}
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Insert sample data with different timestamps
        current_time = int(time.time())
        data = []
        news_documents = [
            "Artificial intelligence helps medical breakthroughs",
            "Analysis of artificial intelligence trends in 2023", 
            "Artificial intelligence ethical disputes continue to ferment",
            "The latest progress in deep learning technology",
            "Machine learning algorithms improve healthcare diagnosis"
        ]
        
        # Create data with timestamps ranging from 15 days ago to current time
        for i in range(len(news_documents)):
            timestamp_offset = (len(news_documents) - i - 1) * 24 * 60 * 60 * 3  # 3 days apart
            data.append({
                "doc_id": i,
                "document": news_documents[i],
                "timestamp": current_time - timestamp_offset
            })
        
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        
        return name, current_time

    def test_decay_rerank_with_invalid_function_type(self, tei_endpoint):
        """
        target: test decay rerank with invalid function type
        method: create decay rerank with invalid function type
        expected: search should fail with appropriate error
        """
        name = gen_collection_name(prefix)
        collection_name, current_time = self._create_collection_with_timestamp_field(name, tei_endpoint)
        
        # Test with invalid function type
        decay_params = {
            "functions": [{
                "name": "invalid_decay_ranker",
                "description": "",
                "type": "Rerank",
                "inputFieldNames": ["timestamp"],
                "params": {
                    "reranker": "decay",
                    "function": "invalid_function",  # Invalid function type
                    "origin": current_time,
                    "offset": 0,
                    "decay": 0.5,
                    "scale": 7 * 24 * 60 * 60
                }
            }]
        }
        
        search_payload = {
            "collectionName": collection_name,
            "data": ["artificial intelligence"],
            "annsField": "dense",
            "limit": 10,
            "outputFields": ["doc_id", "document", "timestamp"],
            "functionScore": decay_params
        }
        
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] != 0, f"Expected search to fail with invalid function type, but got: {rsp}"

    def test_decay_rerank_with_invalid_field_type(self, tei_endpoint):
        """
        target: test decay rerank with non-numeric field
        method: create decay rerank with non-numeric field
        expected: search should fail appropriately
        """
        name = gen_collection_name(prefix)
        
        # Create collection with string field instead of numeric timestamp
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "description": "test collection for decay rerank negative test",
                "fields": [
                    {"fieldName": "doc_id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "document", "dataType": "VarChar", "elementTypeParams": {"max_length": "65535"}},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "768"}},
                    {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "100"}}  # String field
                ],
                "functions": [
                    {
                        "name": "tei",
                        "type": "TextEmbedding",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["dense"],
                        "params": {
                            "provider": "TEI",
                            "endpoint": tei_endpoint
                        }
                    }
                ]
            }
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Insert data
        data = [{"doc_id": i, "document": fake_en.text(), "category": "tech"} for i in range(5)]
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        
        # Test decay rerank with string field
        decay_params = {
            "functions": [{
                "name": "invalid_field_decay_ranker",
                "description": "",
                "type": "Rerank",
                "inputFieldNames": ["category"],  # String field, should fail
                "params": {
                    "reranker": "decay",
                    "function": "gauss",
                    "origin": 100,
                    "offset": 0,
                    "decay": 0.5,
                    "scale": 10
                }
            }]
        }
        
        search_payload = {
            "collectionName": name,
            "data": ["technology"],
            "annsField": "dense",
            "limit": 10,
            "outputFields": ["doc_id", "document", "category"],
            "functionScore": decay_params
        }
        
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] != 0, f"Expected search to fail with non-numeric field, but got: {rsp}"

    def test_decay_rerank_with_invalid_parameters(self, tei_endpoint):
        """
        target: test decay rerank with invalid parameter values
        method: test with invalid decay, scale, offset values
        expected: search should fail with appropriate error
        """
        name = gen_collection_name(prefix)
        collection_name, current_time = self._create_collection_with_timestamp_field(name, tei_endpoint)
        
        # Test invalid parameter combinations
        invalid_configs = [
            {
                "name": "negative_scale",
                "params": {
                    "origin": current_time,
                    "scale": -100,  # Invalid: scale must be > 0
                    "decay": 0.5
                }
            },
            {
                "name": "invalid_decay_range",
                "params": {
                    "origin": current_time,
                    "scale": 100,
                    "decay": 1.5  # Invalid: decay must be between 0 and 1
                }
            },
            {
                "name": "negative_offset",
                "params": {
                    "origin": current_time,
                    "scale": 100,
                    "decay": 0.5,
                    "offset": -10  # Invalid: offset must be >= 0
                }
            }
        ]
        
        for config in invalid_configs:
            logger.info(f"Testing invalid config: {config['name']}")
            
            decay_params = {
                "functions": [{
                    "name": f"invalid_decay_ranker_{config['name']}",
                    "description": "",
                    "type": "Rerank",
                    "inputFieldNames": ["timestamp"],
                    "params": {
                        "reranker": "decay",
                        "function": "gauss",
                        **config["params"]
                    }
                }]
            }
            
            search_payload = {
                "collectionName": collection_name,
                "data": ["artificial intelligence"],
                "annsField": "dense",
                "limit": 10,
                "outputFields": ["doc_id", "document", "timestamp"],
                "functionScore": decay_params
            }
            
            rsp = self.vector_client.vector_search(search_payload)
            assert rsp['code'] != 0, f"Expected search to fail with {config['name']}, but got: {rsp}"
            logger.info(f"Invalid config {config['name']} correctly failed")


@pytest.mark.L1
class TestRRFWeightedRerank(TestBase):
    """
    ******************************************************************
      Test cases for RRF and Weighted rerank function via RESTful API
    ******************************************************************
    """

    def _create_collection_with_bm25_function(self, name):
        """Helper method to create collection with dense, sparse, and bm25 fields"""
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "enableDynamicField": False,
                "description": "test collection for rrf/weighted rerank",
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "doc_id", "dataType": "VarChar", "elementTypeParams": {"max_length": "100"}},
                    {
                        "fieldName": "document", 
                        "dataType": "VarChar", 
                        "elementTypeParams": {
                            "max_length": "10000",
                            "enable_analyzer": True,
                            "analyzer_params": {"tokenizer": "standard"},
                            "enable_match": True
                        }
                    },
                    {"fieldName": "sparse", "dataType": "SparseFloatVector"},
                    {"fieldName": "dense", "dataType": "FloatVector", "elementTypeParams": {"dim": "768"}},
                    {"fieldName": "bm25", "dataType": "SparseFloatVector"}
                ],
                "functions": [
                    {
                        "name": "bm25_fn",
                        "type": "BM25",
                        "inputFieldNames": ["document"],
                        "outputFieldNames": ["bm25"],
                        "params": {}
                    }
                ]
            },
            "indexParams": [
                {
                    "fieldName": "dense",
                    "indexName": "dense_index", 
                    "metricType": "COSINE",
                    "indexType": "FLAT",
                    "params": {}
                },
                {
                    "fieldName": "sparse",
                    "indexName": "sparse_index",
                    "metricType": "IP",
                    "indexType": "SPARSE_INVERTED_INDEX",
                    "params": {}
                },
                {
                    "fieldName": "bm25",
                    "indexName": "bm25_index",
                    "metricType": "BM25",
                    "indexType": "SPARSE_INVERTED_INDEX",
                    "params": {"bm25_k1": 1.2, "bm25_b": 0.75}
                }
            ]
        }
        
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0, f"Collection creation failed: {rsp}"
        
        # Insert sample data
        import random
        data = []
        data_size = 100  # Reduced size for faster testing
        for i in range(data_size):
            data.append({
                "doc_id": str(i),
                "document": fake_en.text(),
                "sparse": {random.randint(1, 10000): random.random() for _ in range(100)},
                "dense": [random.random() for _ in range(768)]
            })
        
        payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0, f"Insert failed: {rsp}"
        
        return name

    @pytest.mark.parametrize("ranker_model", ["rrf", "weighted"])
    def test_hybrid_vector_search_with_rrf_weighted_rerank(self, ranker_model):
        """
        target: test hybrid vector search with RRF/Weighted rerank using RESTful API
        method: test dense+sparse/dense+bm25/sparse+bm25 search with RRF/Weighted reranker
        expected: hybrid search should succeed with RRF/Weighted reranker
        """
        import random
        
        name = gen_collection_name(prefix)
        self._create_collection_with_bm25_function(name)
        
        # Prepare search parameters for reranker
        nq = 2  # Reduced for faster testing
        query_texts = [fake_en.text() for _ in range(nq)]
        
        # Prepare reranker parameters (functionScore format)
        if ranker_model == "rrf":
            reranker_params = {
                "functions": [{
                    "name": "rrf_ranker",
                    "description": "",
                    "type": "Rerank",
                    "inputFieldNames": [],
                    "params": {
                        "reranker": "rrf",
                        "k": 100
                    }
                }]
            }
        else:  # weighted
            reranker_params = {
                "functions": [{
                    "name": "weighted_ranker",
                    "description": "",
                    "type": "Rerank",
                    "inputFieldNames": [],
                    "params": {
                        "reranker": "weighted",
                        "weights": [0.1, 0.9],
                        "norm_score": True
                    }
                }]
            }
        
        # Test different hybrid search combinations
        for search_type in ["dense+sparse", "dense+bm25", "sparse+bm25"]:
            logger.info(f"Executing {search_type} hybrid search with {ranker_model} reranker")
            
            if search_type == "dense+sparse":
                hybrid_search_payload = {
                    "collectionName": name,
                    "search": [
                        {
                            "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                            "annsField": "dense",
                            "limit": 5
                        },
                        {
                            "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                            "annsField": "sparse",
                            "limit": 5
                        }
                    ],
                    "functionScore": reranker_params,
                    "limit": 10,
                    "outputFields": ["doc_id", "document"]
                }
                
            elif search_type == "dense+bm25":
                hybrid_search_payload = {
                    "collectionName": name,
                    "search": [
                        {
                            "data": [[random.random() for _ in range(768)] for _ in range(nq)],
                            "annsField": "dense",
                            "limit": 5
                        },
                        {
                            "data": query_texts,
                            "annsField": "bm25",
                            "limit": 5
                        }
                    ],
                    "functionScore": reranker_params,
                    "limit": 10,
                    "outputFields": ["doc_id", "document"]
                }
                
            elif search_type == "sparse+bm25":
                hybrid_search_payload = {
                    "collectionName": name,
                    "search": [
                        {
                            "data": [{random.randint(1, 10000): random.random() for _ in range(100)} for _ in range(nq)],
                            "annsField": "sparse",
                            "limit": 5
                        },
                        {
                            "data": query_texts,
                            "annsField": "bm25", 
                            "limit": 5
                        }
                    ],
                    "functionScore": reranker_params,
                    "limit": 10,
                    "outputFields": ["doc_id", "document"],
                    "searchParams": {"metric_type": "BM25"}
                }
            
            rsp = self.vector_client.vector_advanced_search(hybrid_search_payload)
            assert rsp['code'] == 0, f"{search_type} hybrid search with {ranker_model} reranker failed: {rsp}"
            assert len(rsp['data']) > 0, f"{search_type} hybrid search returned no results"
            logger.info(f"{search_type} hybrid search with {ranker_model} reranker succeeded")