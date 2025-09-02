import logging
import time
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base
import pytest
from idx_ngram import NGRAM

index_type = "NGRAM"
success = "success"
pk_field_name = 'id'
vector_field_name = 'vector'
content_field_name = 'content_ngram'
json_field_name = 'json_field'
dim = 32
default_nb = 2000
default_build_params = {"min_gram": 2, "max_gram": 3}


class TestNgramBuildParams(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", NGRAM.build_params)
    def test_ngram_build_params(self, params):
        """
        Test the build params of NGRAM index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(content_field_name, datatype=DataType.VARCHAR, max_length=100)

        # Check if this test case requires JSON field
        build_params = params.get("params", None)
        has_json_params = (build_params is not None and
                           ("json_path" in build_params or "json_cast_type" in build_params))

        target_field_name = content_field_name  # Default to VARCHAR field

        if has_json_params:
            # Add JSON field for JSON-related parameter tests
            schema.add_field(json_field_name, datatype=DataType.JSON)
            target_field_name = json_field_name

        self.create_collection(client, collection_name, schema=schema)

        # Insert test data
        nb = default_nb
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)

        if has_json_params:
            # Generate JSON test data with varied content
            json_keywords = ["stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"]
            for i, row in enumerate(rows):
                keyword_idx = i % len(json_keywords)
                keyword = json_keywords[keyword_idx]
                row[content_field_name] = f"text content {i}"  # Still provide VARCHAR data
                row[json_field_name] = {
                    "body": f"This is a {keyword} building",
                    "title": f"Location {i}",
                    "description": f"Description for {keyword} number {i}"
                }
        else:
            # Generate VARCHAR test data with varied content
            varchar_keywords = ["stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"]
            for i, row in enumerate(rows):
                keyword_idx = i % len(varchar_keywords)
                keyword = varchar_keywords[keyword_idx]
                row[content_field_name] = f"The {keyword} is large and beautiful number {i}"

        # Insert data in batches for better performance
        batch_size = 1000
        for i in range(0, nb, batch_size):
            batch_rows = rows[i:i + batch_size]
            self.insert(client, collection_name, batch_rows)
        self.flush(client, collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_name = cf.gen_str_by_length(10, letters_only=True)
        index_params.add_index(field_name=target_field_name,
                               index_name=index_name,
                               index_type=index_type,
                               params=build_params)

        # Build index
        if params.get("expected", None) != success:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items=params.get("expected"))
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=index_name)

            # Create vector index before loading collection
            vector_index_params = self.prepare_index_params(client)[0]
            vector_index_params.add_index(field_name=vector_field_name,
                                          metric_type=cf.get_default_metric_for_vector_type(
                                              vector_type=DataType.FLOAT_VECTOR),
                                          index_type="IVF_FLAT",
                                          params={"nlist": 128})
            self.create_index(client, collection_name, vector_index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

            # Load collection
            self.load_collection(client, collection_name)

            # Test query based on field type
            if has_json_params:
                filter_expr = f"{json_field_name}['body'] LIKE \"%stadium%\""
            else:
                filter_expr = f'{content_field_name} LIKE "%stadium%"'

            # Calculate expected count: 2000 data points with 8 keywords cycling
            # Each keyword appears 2000/8 = 250 times
            expected_count = default_nb // 8  # 250 matches for "stadium"

            self.query(client, collection_name, filter=filter_expr,
                       output_fields=["count(*)"],
                       check_task=CheckTasks.check_query_results,
                       check_items={"enable_milvus_client_api": True,
                                    "count(*)": expected_count})

            # Verify the index params are persisted
            idx_info = client.describe_index(collection_name, index_name)
            if build_params is not None:
                for key, value in build_params.items():
                    if value is not None and key not in ["json_path", "json_cast_type"]:
                        assert key in idx_info.keys()
                        assert str(value) in idx_info.values()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("scalar_field_type", ct.all_scalar_data_types)
    def test_ngram_on_all_scalar_fields(self, scalar_field_type):
        """
        Test NGRAM index on all scalar field types and verify proper error handling
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Add the scalar field with appropriate parameters
        if scalar_field_type == DataType.VARCHAR:
            schema.add_field("scalar_field", datatype=scalar_field_type, max_length=1000)
        elif scalar_field_type == DataType.ARRAY:
            schema.add_field("scalar_field", datatype=scalar_field_type,
                             element_type=DataType.VARCHAR, max_capacity=10, max_length=100)
        else:
            schema.add_field("scalar_field", datatype=scalar_field_type)

        self.create_collection(client, collection_name, schema=schema)

        # Generate appropriate test data for each field type
        nb = default_nb
        rows = cf.gen_row_data_by_schema(nb=nb, schema=schema, start=0)

        # Update scalar field with appropriate test data
        if scalar_field_type == DataType.VARCHAR:
            # Generate varied VARCHAR data for better testing
            keywords = ["stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"]
            for i, row in enumerate(rows):
                keyword_idx = i % len(keywords)
                keyword = keywords[keyword_idx]
                row["scalar_field"] = f"The {keyword} is a large building number {i}"
        elif scalar_field_type == DataType.JSON:
            # Generate varied JSON data for better testing
            keywords = ["school", "park", "mall", "library", "hospital", "restaurant", "office", "store"]
            for i, row in enumerate(rows):
                keyword_idx = i % len(keywords)
                keyword = keywords[keyword_idx]
                row["scalar_field"] = {
                    "body": f"This is a {keyword}",
                    "title": f"Location {i}",
                    "category": f"Category {keyword_idx}"
                }
        elif scalar_field_type == DataType.ARRAY:
            # Generate varied ARRAY data for better testing
            base_words = ["word", "text", "data", "item", "element"]
            keywords = ["stadium", "park", "school", "library", "hospital"]
            for i, row in enumerate(rows):
                base_idx = i % len(base_words)
                keyword_idx = i % len(keywords)
                row["scalar_field"] = [f"{base_words[base_idx]}1", f"{base_words[base_idx]}2", keywords[keyword_idx]]
        # For other scalar types, keep the auto-generated data

        # Insert data in batches for better performance
        batch_size = 1000
        for i in range(0, nb, batch_size):
            batch_rows = rows[i:i + batch_size]
            self.insert(client, collection_name, batch_rows)
        self.flush(client, collection_name)

        # Create index
        index_name = cf.gen_str_by_length(10, letters_only=True)
        index_params = self.prepare_index_params(client)[0]
        if scalar_field_type == DataType.JSON:
            # JSON field requires json_path and json_cast_type
            index_params.add_index(field_name="scalar_field",
                                   index_name=index_name,
                                   index_type=index_type,
                                   params={
                                       "min_gram": 2,
                                       "max_gram": 3,
                                       "json_path": "scalar_field['body']",
                                       "json_cast_type": "varchar"
                                   })
        else:
            index_params.add_index(field_name="scalar_field",
                                   index_name=index_name,
                                   index_type=index_type,
                                   params=default_build_params)

        # Check if the field type is supported for NGRAM index
        if scalar_field_type not in NGRAM.supported_field_types:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items={"err_code": 999,
                                           "err_msg": "ngram index can only be created on VARCHAR or JSON field"})
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=index_name)

            # Create vector index before loading collection
            vector_index_params = self.prepare_index_params(client)[0]
            vector_index_params.add_index(field_name=vector_field_name,
                                          metric_type=cf.get_default_metric_for_vector_type(
                                              vector_type=DataType.FLOAT_VECTOR),
                                          index_type="IVF_FLAT",
                                          params={"nlist": 128})
            self.create_index(client, collection_name, vector_index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

            self.load_collection(client, collection_name)

            # Test query for supported types
            if scalar_field_type == DataType.VARCHAR:
                # Calculate expected count: 2000 data points with 8 keywords cycling
                # Each keyword appears 2000/8 = 250 times
                expected_count = default_nb // 8  # 250 matches for "stadium"
                filter_expr = 'scalar_field LIKE "%stadium%"'
                self.query(client, collection_name, filter=filter_expr,
                           output_fields=["count(*)"],
                           check_task=CheckTasks.check_query_results,
                           check_items={"enable_milvus_client_api": True,
                                        "count(*)": expected_count})
            elif scalar_field_type == DataType.JSON:
                # Calculate expected count: 2000 data points with 8 keywords cycling
                # Each keyword appears 2000/8 = 250 times
                expected_count = default_nb // 8  # 250 matches for "school"
                filter_expr = "scalar_field['body'] LIKE \"%school%\""
                self.query(client, collection_name, filter=filter_expr,
                           output_fields=["count(*)"],
                           check_task=CheckTasks.check_query_results,
                           check_items={"enable_milvus_client_api": True,
                                        "count(*)": expected_count})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="skip for issue #44164")
    def test_ngram_alter_index_mmap_and_gram_values(self):
        """
        Test the alter index with mmap and gram values
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("content_ngram", datatype=DataType.VARCHAR, max_length=20)
        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        content_keywords = ["stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"]
        rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=0)
        for i, row in enumerate(rows):
            row["content_ngram"] = content_keywords[i % len(content_keywords)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="content_ngram",
                               index_name="content_ngram",
                               index_type=index_type,
                               params={"min_gram": 2, "max_gram": 3})
        index_params.add_index(field_name=vector_field_name,
                               index_type="IVF_FLAT",
                               metric_type="COSINE",
                               params={"nlist": 128})
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name="content_ngram")
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)
        # Query to check if the index is created
        res = self.query(client, collection_name, filter="content_ngram LIKE 'stad_%'",
                         output_fields=["id", "content_ngram"])[0]
        assert len(res) == default_nb // len(content_keywords)

        # Release collection before alter ngram index
        self.release_collection(client, collection_name)
        # Alter index mmap properties
        self.alter_index_properties(client, collection_name, index_name="content_ngram", properties={"mmap.enabled": True})
        res = self.describe_index(client, collection_name, index_name="content_ngram")[0]
        assert res.get('mmap.enabled', None) == 'True'
        # Load the collection and query again
        self.load_collection(client, collection_name)
        res = self.query(client, collection_name, filter="content_ngram LIKE 'stad_%'",
                         output_fields=["id", "content_ngram"])[0]
        assert len(res) == default_nb // len(content_keywords)

        # Alter index gram value properties is not supported
        self.release_collection(client, collection_name)
        error = {ct.err_code: 1, ct.err_msg: "invalid mmap.enabled value: True, expected: true, false"}
        self.alter_index_properties(client, collection_name, index_name="content_ngram", properties={"min_gram": 3, "max_gram": 4},
                                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_ngram_search_with_diff_length_of_filter_value(self):
        """
        Test the search params of NGRAM index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("content_no_index", datatype=DataType.VARCHAR, max_length=10)
        schema.add_field("content_ngram", datatype=DataType.VARCHAR, max_length=10)

        self.create_collection(client, collection_name, schema=schema)

        # Insert test data
        insert_times = 2
        content_keywords = ["stadium", "park", "school", "library", "hospital", "restaurant", "office", "store"]
        for i in range(insert_times):
            rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=i * default_nb)
            for j, row in enumerate(rows):
                row["content_no_index"] = content_keywords[j % len(content_keywords)]
                row["content_ngram"] = content_keywords[j % len(content_keywords)]
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create vector index before loading collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type="COSINE",
                               index_type="IVF_FLAT",
                               params={"nlist": 128})
        min_gram = 2
        max_gram = 4
        index_params.add_index(field_name="content_ngram",
                               index_type=index_type,
                               params={"min_gram": min_gram, "max_gram": max_gram})
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.wait_for_index_ready(client, collection_name, index_name="content_ngram")
        self.load_collection(client, collection_name)

        # Test query 0: filter value length is less than min_gram
        filter_expr = f'content_ngram LIKE "{content_keywords[0][:min_gram - 1]}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "{content_keywords[0][:min_gram - 1]}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query 1: filter value length is equal to min_gram
        filter_expr = f'content_ngram LIKE "{content_keywords[0][:min_gram]}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "{content_keywords[0][:min_gram]}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query 2: filter value length is less than max_gram
        filter_expr = f'content_ngram LIKE "{content_keywords[0][:max_gram - 1]}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "{content_keywords[0][:max_gram - 1]}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query 3: filter value length is equal to max_gram
        filter_expr = f'content_ngram LIKE "{content_keywords[0][:max_gram]}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "{content_keywords[0][:max_gram]}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query 4: filter value length is greater than max_gram
        filter_expr = f'content_ngram LIKE "{content_keywords[0][:max_gram + 1]}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "{content_keywords[0][:max_gram + 1]}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query with suffix match
        filter_expr = f'content_ngram LIKE "%{content_keywords[0][4:]}"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "%{content_keywords[0][4:]}"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query with infix match
        filter_expr = f'content_ngram LIKE "%{content_keywords[0][2:4]}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "%{content_keywords[0][2:4]}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

        # Test query with Mixed Wildcard Match
        filter_expr = f'content_ngram LIKE "%st_d_um%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= insert_times * default_nb // len(content_keywords)
        filter_expr = f'content_no_index LIKE "%st_d_um%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_no_index) >= insert_times * default_nb // len(content_keywords)
        assert res_ngram == res_no_index

    @pytest.mark.tags(CaseLabel.L2)
    def test_ngram_search_with_multilingual_utf8_strings(self):
        """
        Test NGRAM index with multilingual and UTF-8 strings for LIKE filtering
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("content_no_index", datatype=DataType.JSON)
        schema.add_field("content_ngram", datatype=DataType.JSON)

        self.create_collection(client, collection_name, schema=schema)

        # Multilingual test data with various UTF-8 characters
        multilingual_keywords = [
            "北京大学",           # Chinese
            "東京大学",           # Japanese  
            "Московский",        # Russian
            "café",              # French with accent
            "naïve",             # French with diaeresis
            "München",           # German with umlaut
            "🏫学校🎓",           # Chinese with emojis
            "🌟star⭐",          # English with emojis
            "مدرسة",             # Arabic
            "Γειά",              # Greek
            "प्रविष्टि",           # Hindi/Devanagari
            "한국어",             # Korean
            "español",           # Spanish
            "português",         # Portuguese
            "中英mix英文",        # Mixed Chinese-English
            "café☕北京🏙️"        # Mixed with emojis and multiple languages
        ]

        # Insert test data
        insert_times = 2
        total_records = insert_times * default_nb
        for i in range(insert_times):
            rows = cf.gen_row_data_by_schema(nb=default_nb, schema=schema, start=i * default_nb)
            for j, row in enumerate(rows):
                keyword_idx = j % len(multilingual_keywords)
                keyword = multilingual_keywords[keyword_idx]
                row["content_no_index"] = {
                    "body": f"This is a {keyword} building",
                    "title": f"Location {i}",
                    "description": f"Description for {keyword} number {i}"
                }
                row["content_ngram"] = {
                    "body": f"This is a {keyword} building",
                    "title": f"Location {i}",
                    "description": f"Description for {keyword} number {i}"
                }
            self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create vector index before loading collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type="COSINE",
                               index_type="IVF_FLAT",
                               params={"nlist": 128})
        
        # Create NGRAM index with appropriate parameters for multilingual content
        min_gram = 1  # Use 1 for better multilingual support
        max_gram = 3
        index_params.add_index(field_name="content_ngram",
                               index_name="content_ngram",
                               index_type=index_type,
                               params={"min_gram": min_gram, "max_gram": max_gram, 
                                       "json_path": "content_ngram['body']", "json_cast_type": "varchar"})
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.wait_for_index_ready(client, collection_name, index_name="content_ngram")
        self.load_collection(client, collection_name)

        expected_count_per_keyword = total_records // len(multilingual_keywords)

        # Test 1: Chinese character search
        chinese_keyword = "北京"
        filter_expr = f'content_ngram["body"] LIKE "%{chinese_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{chinese_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 2: Japanese character search
        japanese_keyword = "東京"
        filter_expr = f'content_ngram["body"] LIKE "%{japanese_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{japanese_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 3: Russian Cyrillic character search
        russian_keyword = "Моск"
        filter_expr = f'content_ngram["body"] LIKE "%{russian_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{russian_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 4: French accent character search
        french_keyword = "café"
        filter_expr = f'content_ngram["body"] LIKE "%{french_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{french_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 5: Emoji character search
        emoji_keyword = "🏫"
        filter_expr = f'content_ngram["body"] LIKE "%{emoji_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{emoji_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 6: Star emoji search
        star_keyword = "⭐"
        filter_expr = f'content_ngram["body"] LIKE "%{star_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{star_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 7: Arabic character search
        arabic_keyword = "مدرسة"
        filter_expr = f'content_ngram["body"] LIKE "%{arabic_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{arabic_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 8: Korean character search
        korean_keyword = "한국"
        filter_expr = f'content_ngram["body"] LIKE "%{korean_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{korean_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 9: German umlaut character search
        german_keyword = "München"
        filter_expr = f'content_ngram["body"] LIKE "%{german_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{german_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 10: Mixed language search
        mixed_keyword = "mix"
        filter_expr = f'content_ngram["body"] LIKE "%{mixed_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{mixed_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 11: Complex multilingual with emojis prefix search
        complex_keyword = "café☕"
        filter_expr = f'content_ngram["body"] LIKE "%{complex_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{complex_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 12: Hindi/Devanagari character search
        hindi_keyword = "प्रविष्टि"
        filter_expr = f'content_ngram["body"] LIKE "%{hindi_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{hindi_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 13: Greek character search
        greek_keyword = "Γειά"
        filter_expr = f'content_ngram["body"] LIKE "%{greek_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{greek_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 14: Portuguese with tilde character search
        portuguese_keyword = "português"
        filter_expr = f'content_ngram["body"] LIKE "%{portuguese_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{portuguese_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        assert len(res_ngram) >= expected_count_per_keyword
        assert res_ngram == res_no_index

        # Test 15: Test single character search (especially important for CJK)
        single_char_keyword = "学"
        filter_expr = f'content_ngram["body"] LIKE "%{single_char_keyword}%"'
        res_ngram = self.query(client, collection_name, filter=filter_expr,
                               output_fields=["id", "content_ngram"])[0]
        filter_expr = f'content_no_index["body"] LIKE "%{single_char_keyword}%"'
        res_no_index = self.query(client, collection_name, filter=filter_expr,
                                  output_fields=["id", "content_ngram"])[0]
        # Should match both "北京大学" and "🏫学校🎓"
        assert len(res_ngram) >= expected_count_per_keyword * 2
        assert res_ngram == res_no_index
