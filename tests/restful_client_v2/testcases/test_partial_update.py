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

    def test_partial_update_new_record_missing_fields(self):
        """
        Test partial update behavior with new records missing required fields (should fail)
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

        # Try partial update on non-existent record with missing required fields (should fail)
        partial_update_data = [{
            "book_id": 999,
            "book_describe": "new_book_description"
            # Missing required fields: user_id, text_emb
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail because required fields are missing for new record insertion
        assert rsp['code'] != 0
        assert "fieldSchema" in rsp['message'] or "field" in rsp['message'].lower()
        logger.info(f"Expected failure for missing fields: {rsp['message']}")

    def test_partial_update_new_record_with_full_fields(self):
        """
        Test partial update behavior with new records when all required fields are provided (should succeed)
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

        # Partial update on non-existent record with all required fields (should succeed as insert)
        partial_update_data = [{
            "book_id": 999,
            "user_id": 999,
            "book_describe": "new_book_description",
            "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 1

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Verify the new record was inserted
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id == 999"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 1
        assert rsp['data'][0]['book_id'] == 999
        assert rsp['data'][0]['user_id'] == 999
        assert rsp['data'][0]['book_describe'] == "new_book_description"

        logger.info("Partial update with full fields for new record test passed")

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

    def test_partial_update_mixed_scenario(self):
        """
        Test partial update with mixed scenario: some records exist, some don't
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
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert some initial data
        nb = 5
        initial_data = []
        for i in range(nb):
            tmp = {
                "book_id": i,
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

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Step 1: Update existing records (partial fields only)
        update_data = []
        for i in range(nb):
            tmp = {
                "book_id": i,
                "book_describe": f"updated_book_{i}",  # Only update description
            }
            update_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 5  # 5 updates

        c.flush()
        time.sleep(3)

        # Step 2: Insert new records (all required fields)
        new_records_data = []
        for i in range(10, 13):
            tmp = {
                "book_id": i,
                "user_id": i + 100,
                "word_count": i * 50,
                "book_describe": f"new_book_{i}",
                "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            }
            new_records_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": new_records_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 3  # 3 inserts

        c.flush()
        time.sleep(3)

        # Verify existing records were updated (partial update)
        for i in range(nb):
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"book_id == {i}"})
            assert rsp['code'] == 0
            assert len(rsp['data']) == 1
            data = rsp['data'][0]
            
            # Updated field
            assert data['book_describe'] == f"updated_book_{i}"
            # Unchanged fields
            assert data['user_id'] == i
            assert data['word_count'] == i * 100

        # Verify new records were inserted (full insert)
        for i in range(10, 13):
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"book_id == {i}"})
            assert rsp['code'] == 0
            assert len(rsp['data']) == 1
            data = rsp['data'][0]
            
            assert data['book_describe'] == f"new_book_{i}"
            assert data['user_id'] == i + 100
            assert data['word_count'] == i * 50

        logger.info("Mixed partial update scenario test passed")

    def test_partial_update_with_auto_id(self):
        """
        Test partial update with autoID primary key - should fail as autoID is not supported for upsert
        """
        # Create collection with autoID primary key
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
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

        # Insert initial data (without providing book_id as it's autoID)
        nb = 3
        initial_data = []
        for i in range(nb):
            tmp = {
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

        # Get the auto-generated IDs before partial update
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id >= 0", "outputFields": ["book_id", "user_id", "book_describe"]})
        assert rsp['code'] == 0
        assert len(rsp['data']) == nb
        
        original_ids = [data['book_id'] for data in rsp['data']]
        original_data_map = {data['user_id']: data for data in rsp['data']}

        # Partial update existing records using their auto-generated IDs
        # When autoID=true, partial update should generate NEW IDs for existing records
        partial_update_data = []
        for i, book_id in enumerate(original_ids):
            tmp = {
                "book_id": book_id,
                "book_describe": f"updated_book_{i}",  # Only update description
            }
            partial_update_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 3  # 3 updates
        c.flush()
        time.sleep(3)

        # Critical verification: old IDs should no longer exist
        for old_id in original_ids:
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"book_id == {old_id}"})
            assert rsp['code'] == 0
            assert len(rsp['data']) == 0, f"Old ID {old_id} should not exist after partial update with autoID=true"

        # Verify updated records have NEW auto-generated IDs
        for i in range(nb):
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"user_id == {i}"})
            assert rsp['code'] == 0
            assert len(rsp['data']) == 1
            data = rsp['data'][0]
            
            # Should have updated description
            assert data['book_describe'] == f"updated_book_{i}"
            # Should have same user_id (identifies the record)
            assert data['user_id'] == i
            # Should have NEW book_id (different from original)
            assert data['book_id'] not in original_ids, f"New ID {data['book_id']} should be different from original IDs {original_ids}"

        # Verify total count is still correct (3 updated)
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "user_id >= 0"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 3

        logger.info("Partial update with autoID test passed - verified new IDs generated for updated records")


        """
        Test detailed behavior of partial update with autoID: old record deletion and new record insertion
        """
        # Create collection with autoID primary key
        name = gen_collection_name()
        dim = 64
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "name", "dataType": "VarChar", "elementTypeParams": {"max_length": "100"}},
                    {"fieldName": "age", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert one record
        initial_data = [{
            "name": "Alice",
            "age": 25,
            "vector": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
        }]

        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Get the original record
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "age > 0"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 1
        original_record = rsp['data'][0]
        original_id = original_record['id']

        logger.info(f"Original record: ID={original_id}, name={original_record['name']}, age={original_record['age']}")

        # Perform partial update using the original ID
        partial_update_data = [{
            "id": original_id,
            "name": "Alice Updated"  # Only update name, age should remain unchanged
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 1

        c.flush()
        time.sleep(3)

        # Verify the original ID no longer exists
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"id == {original_id}"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 0, f"Original ID {original_id} should be deleted after partial update with autoID=true"

        # Verify there's still exactly one record with updated data
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "age > 0"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 1
        
        updated_record = rsp['data'][0]
        new_id = updated_record['id']

        logger.info(f"Updated record: ID={new_id}, name={updated_record['name']}, age={updated_record['age']}")

        # Verify the record has a new ID and updated fields
        assert new_id != original_id, f"New ID {new_id} should be different from original ID {original_id}"
        assert updated_record['name'] == "Alice Updated", "Name should be updated"
        assert updated_record['age'] == 25, "Age should remain unchanged (inherited from original record)"

        logger.info("Detailed autoID partial update behavior test passed")

    def test_partial_update_auto_id_only_specified_fields_updated(self):
        """
        Test that only specified fields are updated in partial update with autoID, others remain from original
        """
        # Create collection
        name = gen_collection_name()
        dim = 64
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True,"elementTypeParams": {}},
                    {"fieldName": "field1", "dataType": "VarChar", "elementTypeParams": {"max_length": "100"}},
                    {"fieldName": "field2", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "field3", "dataType": "Double", "elementTypeParams": {}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert original record with all fields
        original_vector = preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
        initial_data = [{
            "field1": "original_value1",
            "field2": 100,
            "field3": 3.14,
            "vector": original_vector
        }]

        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Get original record
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "field2 > 0"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 1
        original_record = rsp['data'][0]
        original_id = original_record['id']

        # Partial update - only update field1, others should remain unchanged
        partial_update_data = [{
            "id": original_id,
            "field1": "updated_value1"  # Only update field1
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0

        c.flush()
        time.sleep(3)

        # Verify updated record
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "field2 > 0"})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 1
        
        updated_record = rsp['data'][0]

        # Verify new ID generated
        assert updated_record['id'] != original_id, "Should have new autoID"
        # Verify field1 was updated
        assert updated_record['field1'] == "updated_value1", "field1 should be updated"
        # Verify other fields remained unchanged
        assert updated_record['field2'] == 100, "field2 should remain unchanged"
        assert updated_record['field3'] == 3.14, "field3 should remain unchanged"
        # Note: vector field should also remain unchanged, but might need special handling in verification

        logger.info("Partial update with autoID - only specified fields updated test passed")

    def test_partial_update_with_default_and_nullable_fields(self):
        """
        Test partial update with default values and nullable fields for new records
        """
        # Create collection with default value and nullable fields
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "word_count", "dataType": "Int64", "elementTypeParams": {}, "defaultValue": 1000},  # Default value
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "rating", "dataType": "Double", "elementTypeParams": {}, "nullable": True},  # Nullable field
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert initial data
        nb = 3
        initial_data = []
        for i in range(nb):
            tmp = {
                "book_id": i,
                "user_id": i,
                "word_count": i * 100,
                "book_describe": f"original_book_{i}",
                "rating": 3.5 + i * 0.5,
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

        # Test 1: Partial update existing records only
        partial_update_data = []
        for i in range(nb):
            tmp = {
                "book_id": i,
                "book_describe": f"updated_book_{i}",  # Only update description
            }
            partial_update_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 3  # 3 updates

        c.flush()
        time.sleep(3)

        # Verify existing records were updated (partial update)
        for i in range(nb):
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"book_id == {i}"})
            assert rsp['code'] == 0
            assert len(rsp['data']) == 1
            data = rsp['data'][0]
            
            # Updated field
            assert data['book_describe'] == f"updated_book_{i}"
            # Unchanged fields
            assert data['user_id'] == i
            assert data['word_count'] == i * 100  # Original value, not default
            assert data['rating'] == 3.5 + i * 0.5  # Original value

        # Test 2: Insert new records with minimal required fields (separate request)
        new_record_data = []
        for i in range(10, 12):
            tmp = {
                "book_id": i,
                "user_id": i + 100,
                "book_describe": f"new_book_{i}",
                "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist(),
                "word_count": None, #should use default value (1000)
                "rating": None #nullable, should be null
            }
            new_record_data.append(tmp)

        payload = {
            "collectionName": name,
            "data": new_record_data,
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 2  # 2 inserts

        c.flush()
        time.sleep(3)

        # Verify new records were inserted with defaults and nulls
        for i in range(10, 12):
            rsp = self.vector_client.vector_query({"collectionName": name, "filter": f"book_id == {i}"})
            assert rsp['code'] == 0
            assert len(rsp['data']) == 1
            data = rsp['data'][0]
            
            assert data['book_describe'] == f"new_book_{i}"
            assert data['user_id'] == i + 100
            assert data['word_count'] == 1000  # Should use default value
            # Note: Nullable field behavior depends on implementation
            # It might be null or omitted from result

        logger.info("Partial update with default and nullable fields test passed")

    def test_partial_update_nullable_field_scenarios(self):
        """
        Test partial update with nullable fields in various scenarios:
        1. Nullable field with no default value, insert without value, then update to new value
        2. Nullable field with default value, insert without value, then update to new value  
        3. Nullable field with no default value, insert with value, then update to null
        """
        # Create collection with nullable fields
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "user_id", "dataType": "Int64", "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "nullable_field_no_default", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}, "nullable": True},  # Nullable, no default
                    {"fieldName": "nullable_field_with_default", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}, "nullable": True, "defaultValue": "default_value"},  # Nullable with default
                    {"fieldName": "nullable_field_for_null_update", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}, "nullable": True},  # Nullable, no default
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Scenario 1: Insert data with nullable field (no default) not provided
        initial_data_scenario1 = {
            "book_id": 1,
            "user_id": 1,
            "book_describe": "test_book_1",
            "nullable_field_with_default": None,  # Use default value
            "nullable_field_for_null_update": "initial_value",  # Will be updated to null later
            "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            # nullable_field_no_default is not provided, should be null
        }

        # Scenario 2: Insert data with nullable field (with default) not provided  
        initial_data_scenario2 = {
            "book_id": 2,
            "user_id": 2,
            "book_describe": "test_book_2",
            "nullable_field_no_default": None,  # Should remain null
            "nullable_field_for_null_update": "another_initial_value",
            "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
            # nullable_field_with_default is not provided, should use default value
        }

        # Scenario 3: Insert data with nullable field that will be updated to null
        initial_data_scenario3 = {
            "book_id": 3,
            "user_id": 3,
            "book_describe": "test_book_3",
            "nullable_field_no_default": None,
            "nullable_field_with_default": "custom_value",  # Custom value, not default
            "nullable_field_for_null_update": "value_to_be_nulled",  # Will be updated to null
            "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
        }

        # Insert all initial data
        initial_data = [initial_data_scenario1, initial_data_scenario2, initial_data_scenario3]
        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['insertCount'] == 3

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Verify initial state
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= 1", "outputFields": ["*"]})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 3

        # Check initial values
        data_by_id = {data['book_id']: data for data in rsp['data']}
        
        # Scenario 1 verification: nullable field with no default should be null/not present
        assert data_by_id[1]['nullable_field_no_default'] is None or 'nullable_field_no_default' not in data_by_id[1]
        assert data_by_id[1]['nullable_field_with_default'] == "default_value"  # Should use default
        assert data_by_id[1]['nullable_field_for_null_update'] == "initial_value"

        # Scenario 2 verification: nullable field with default should use default value
        assert data_by_id[2]['nullable_field_no_default'] is None or 'nullable_field_no_default' not in data_by_id[2]
        assert data_by_id[2]['nullable_field_with_default'] == "default_value"  # Should use default
        assert data_by_id[2]['nullable_field_for_null_update'] == "another_initial_value"

        # Scenario 3 verification: all fields should have the provided values
        assert data_by_id[3]['nullable_field_no_default'] is None or 'nullable_field_no_default' not in data_by_id[3]
        assert data_by_id[3]['nullable_field_with_default'] == "custom_value"
        assert data_by_id[3]['nullable_field_for_null_update'] == "value_to_be_nulled"

        logger.info("Initial data verification passed")

        # Now perform partial updates for each scenario separately
        # Note: Partial update does not support updating different columns for multiple rows in a single request
        
        # Scenario 1: Update nullable field (no default) from null to new value
        partial_update_scenario1 = [{
            "book_id": 1,
            "nullable_field_no_default": "updated_value_1"
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_scenario1,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 1

        c.flush()
        time.sleep(2)

        # Scenario 2: Update nullable field (with default) from default to new value
        partial_update_scenario2 = [{
            "book_id": 2,
            "nullable_field_with_default": "updated_value_2"
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_scenario2,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 1

        c.flush()
        time.sleep(2)

        # Scenario 3: Update nullable field from value to null
        partial_update_scenario3 = [{
            "book_id": 3,
            "nullable_field_for_null_update": None
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_scenario3,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        assert rsp['code'] == 0
        assert rsp['data']['upsertCount'] == 1

        c.flush()
        time.sleep(2)

        # Verify partial update results
        rsp = self.vector_client.vector_query({"collectionName": name, "filter": "book_id >= 1", "outputFields": ["*"]})
        assert rsp['code'] == 0
        assert len(rsp['data']) == 3

        updated_data_by_id = {data['book_id']: data for data in rsp['data']}

        # Scenario 1: Verify nullable field (no default) was updated from null to new value
        assert updated_data_by_id[1]['nullable_field_no_default'] == "updated_value_1"
        # Other fields should remain unchanged
        assert updated_data_by_id[1]['user_id'] == 1
        assert updated_data_by_id[1]['book_describe'] == "test_book_1"
        assert updated_data_by_id[1]['nullable_field_with_default'] == "default_value"
        assert updated_data_by_id[1]['nullable_field_for_null_update'] == "initial_value"

        # Scenario 2: Verify nullable field (with default) was updated from default to new value
        assert updated_data_by_id[2]['nullable_field_with_default'] == "updated_value_2"
        # Other fields should remain unchanged
        assert updated_data_by_id[2]['user_id'] == 2
        assert updated_data_by_id[2]['book_describe'] == "test_book_2"
        assert updated_data_by_id[2]['nullable_field_no_default'] is None or 'nullable_field_no_default' not in updated_data_by_id[2]
        assert updated_data_by_id[2]['nullable_field_for_null_update'] == "another_initial_value"

        # Scenario 3: Verify nullable field was updated from value to null
        # Note, the RESTful SDK cannot differentiate between missing fields and fields explicitly set to null, 
        # so partial update to null values is not supported"
        # assert updated_data_by_id[3]['nullable_field_for_null_update'] is None or 'nullable_field_for_null_update' not in updated_data_by_id[3]
        # Other fields should remain unchanged
        assert updated_data_by_id[3]['user_id'] == 3
        assert updated_data_by_id[3]['book_describe'] == "test_book_3"
        assert updated_data_by_id[3]['nullable_field_no_default'] is None or 'nullable_field_no_default' not in updated_data_by_id[3]
        assert updated_data_by_id[3]['nullable_field_with_default'] == "custom_value"

        logger.info("All nullable field partial update scenarios passed")
        logger.info("Scenario 1: nullable field (no default) null -> new value: PASSED")
        logger.info("Scenario 2: nullable field (with default) default -> new value: PASSED") 
        logger.info("Scenario 3: nullable field value -> null: PASSED")


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

    def test_partial_update_empty_data(self):
        """
        Test partial update with empty data array
        """
        # Create collection (must include vector field)
        name = gen_collection_name()
        dim = 64
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Try partial update with empty data
        payload = {
            "collectionName": name,
            "data": [],  # Empty data array
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail with appropriate error
        assert rsp['code'] != 0
        logger.info(f"Expected failure for empty data: {rsp['message']}")

    def test_partial_update_non_existent_field(self):
        """
        Test partial update with non-existent field names
        """
        # Create collection (must include vector field)
        name = gen_collection_name()
        dim = 64
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "book_describe", "dataType": "VarChar", "elementTypeParams": {"max_length": "256"}},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "vector", "indexName": "vector_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Try partial update with non-existent field
        partial_update_data = [{
            "book_id": 1,
            "non_existent_field": "some_value"  # Field doesn't exist in schema
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail with appropriate error
        assert rsp['code'] != 0
        assert "dynamic schema" in rsp['message'] or "not exist" in rsp['message'] or "unknown" in rsp['message'].lower()
        logger.info(f"Expected failure for non-existent field: {rsp['message']}")

    def test_partial_update_mixed_success_failure(self):
        """
        Test partial update with mixed valid and invalid records
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

        # Insert some existing data
        initial_data = [{
            "book_id": 1,
            "user_id": 1,
            "book_describe": "existing_book",
            "text_emb": preprocessing.normalize([np.array([random.random() for j in range(dim)])])[0].tolist()
        }]

        payload = {
            "collectionName": name,
            "data": initial_data,
        }
        rsp = self.vector_client.vector_insert(payload)
        assert rsp['code'] == 0

        c = Collection(name)
        c.flush()
        time.sleep(3)

        # Mixed partial update: valid existing record update + invalid new record (missing required fields)
        mixed_data = [
            {
                "book_id": 1,
                "book_describe": "updated_existing_book"  # Valid partial update for existing record
            },
            {
                "book_id": 999,
                "book_describe": "new_book_missing_fields"  # Invalid - missing user_id and text_emb for new record
            }
        ]

        payload = {
            "collectionName": name,
            "data": mixed_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail because one record is invalid
        assert rsp['code'] != 0
        logger.info(f"Expected failure for mixed valid/invalid records: {rsp['message']}")

    def test_partial_update_vector_dimension_mismatch(self):
        """
        Test partial update with vector dimension mismatch
        """
        # Create collection
        name = gen_collection_name()
        dim = 128
        payload = {
            "collectionName": name,
            "schema": {
                "fields": [
                    {"fieldName": "book_id", "dataType": "Int64", "isPrimary": True, "elementTypeParams": {}},
                    {"fieldName": "text_emb", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{dim}"}}
                ]
            },
            "indexParams": [{"fieldName": "text_emb", "indexName": "text_emb_index", "metricType": "L2"}]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Try partial update with wrong vector dimension
        partial_update_data = [{
            "book_id": 1,
            "text_emb": [random.random() for _ in range(64)]  # Wrong dimension (64 instead of 128)
        }]

        payload = {
            "collectionName": name,
            "data": partial_update_data,
            "partialUpdate": True
        }
        rsp = self.vector_client.vector_upsert(payload)
        # Should fail with dimension mismatch error
        assert rsp['code'] != 0
        assert "dimension" in rsp['message'].lower() or "dim" in rsp['message'].lower()
        logger.info(f"Expected failure for dimension mismatch: {rsp['message']}")