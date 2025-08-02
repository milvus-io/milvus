import random
from sklearn import preprocessing
import numpy as np
import sys
import json
import time
from utils import constant
from utils.utils import gen_collection_name, get_sorted_distance, patch_faker_text, en_vocabularies_distribution, \
    zh_vocabularies_distribution
from utils.util_log import test_log as logger
import pytest
from base.testbase import TestBase
from utils.utils import (gen_unique_str, get_data_by_payload, get_common_fields_by_data, gen_vector, analyze_documents)
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection, utility
)
from faker import Faker
import re

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

patch_faker_text(fake_en, en_vocabularies_distribution)
patch_faker_text(fake_zh, zh_vocabularies_distribution)


@pytest.mark.L0
class TestPartialUpdate(TestBase):

    @pytest.mark.parametrize("id_type", ["Int64", "VarChar"])
    def test_partial_update_basic(self, id_type):
        """
        Test basic partial update functionality
        1. Create collection
        2. Insert initial data
        3. Partial update with only some fields
        4. Verify only updated fields are changed
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": f"{id_type}", "isPrimary": True,
                     "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert initial data
        nb = 10
        initial_data = []
        for i in range(nb):
            tmp = {
                "book_id": i if id_type == "Int64" else f"{i}",
                "user_id": i,
                "word_count": i * 100,
                "book_describe": f"original_book_{i}",
                "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            }
            initial_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['insertCount'] == nb

        c = Collection(name)
        c.flush()
        time.sleep(3)  # Wait for data to be available

        # Partial update - only update book_describe field
        partial_update_data = []
        for i in range(nb):
            tmp = {
                "book_id": i if id_type == "Int64" else f"{i}",
                "book_describe": f"updated_book_{i}",  # Only update this field
            }
            partial_update_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True  # Enable partial update
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0

        # Verify partial update worked correctly
        if id_type == "Int64":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= 0"})
        else:
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= '0'"})

        assert rsp['code'] == 0
        assert len(rsp['data']) == nb

        for data in rsp['data']:
            book_id = int(data['book_id'])
            # book_describe should be updated
            assert data['book_describe'] == f"updated_book_{book_id}"
            # Other fields should remain unchanged
            assert data['user_id'] == book_id
            assert data['word_count'] == book_id * 100

        logger.info("Partial update basic test passed")

    @pytest.mark.parametrize("id_type", ["Int64", "VarChar"])
    def test_partial_update_multiple_fields(self, id_type):
        """
        Test partial update with multiple fields
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": f"{id_type}", "isPrimary": True,
                     "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "rating", "dataType": "Double", "elementTypeParams": {}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert initial data
        nb = 10
        initial_data = []
        for i in range(nb):
            tmp = {
                "book_id": i if id_type == "Int64" else f"{i}",
                "user_id": i,
                "word_count": i * 100,
                "book_describe": f"original_book_{i}",
                "rating": 3.5,
                "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            }
            initial_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Partial update - update multiple fields
        partial_update_data = []
        for i in range(nb):
            tmp = {
                "book_id": i if id_type == "Int64" else f"{i}",
                "book_describe": f"updated_book_{i}",
                "rating": 4.5,  # Update rating
                "word_count": i * 200,  # Update word count
            }
            partial_update_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0

        # Verify partial update
        if id_type == "Int64":
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= 0"})
        else:
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= '0'"})

        assert rsp['code'] == 0
        for data in rsp['data']:
            book_id = int(data['book_id'])
            # Updated fields
            assert data['book_describe'] == f"updated_book_{book_id}"
            assert data['rating'] == 4.5
            assert data['word_count'] == book_id * 200
            # Unchanged field
            assert data['user_id'] == book_id

        logger.info("Partial update multiple fields test passed")

    def test_partial_update_new_record(self):
        """
        Test partial update behavior with new records (should fail or require all fields)
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Try partial update on non-existent record (should fail gracefully)
        partial_update_data = [{
            "book_id": 999,
            "book_describe": "new_book_description"
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Note: Depending on implementation, this might fail or require special handling
        # The test should be adapted based on actual system behavior
        logger.info(f"Partial update on new record response: {rsp}")

    def test_partial_update_with_vector_field(self):
        """
        Test partial update including vector field
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert initial data
        nb = 5
        initial_data = []
        for i in range(nb):
            tmp = {
                "book_id": i,
                "user_id": i,
                "book_describe": f"original_book_{i}",
                "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            }
            initial_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Partial update with vector field
        partial_update_data = []
        for i in range(nb):
            tmp = {
                "book_id": i,
                "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            }
            partial_update_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0

        # Verify update
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= 0"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == nb

        logger.info("Partial update with vector field test passed")


@pytest.mark.L1
class TestPartialUpdateNegative(TestBase):

    def test_partial_update_without_primary_key(self):
        """
        Test partial update fails when primary key is missing
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Try partial update without primary key (should fail)
        partial_update_data = [{
            "book_describe": "updated_description"
            # Missing book_id (primary key)
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail with appropriate error code
        assert rsp['code'] != 0
        logger.info(f"Expected failure response: {rsp}")

    def test_partial_update_invalid_collection_name(self):
        """
        Test partial update with invalid collection name
        """
        partial_update_data = [{
            "book_id": 1,
            "book_describe": "updated_description"
        }]

        payload = {
            "collectionName": "non_existent_collection",
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] != 0
        logger.info(f"Expected failure response: {rsp}")

    def test_partial_update_invalid_field_type(self):
        """
        Test partial update with invalid field type
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Try partial update with wrong data type
        partial_update_data = [{
            "book_id": 1,
            "user_id": "invalid_string_for_int_field"  # Should be int, not string
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail with appropriate error code
        assert rsp['code'] != 0
        logger.info(f"Expected failure response: {rsp}")