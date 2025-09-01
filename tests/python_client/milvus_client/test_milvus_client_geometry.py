import logging
import time
import pytest
from utils.util_pymilvus import *
from utils.util_log import test_log as log
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

# Import shapely for correctness validation
from shapely.geometry import Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon
from shapely.wkt import loads as wkt_loads

index_type = "RTREE"
pk_field_name = 'id'
vector_field_name = 'vector'
geometry_field_name = ct.default_geometry_field_name
dim = 16
default_nb = 2000


@pytest.mark.skip(reason="still in debugging @yanliang567")
class TestGeometryPositive(TestMilvusClientV2Base):
    """
    Positive test cases for geometry data type functionality
    All positive tests include: create collection, insert data, build RTREE index, load and search with filtering
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_collection_with_geometry_field(self):
        """
        Test case: Create a collection with a geometry field
        Tests both scenarios:
        1. Geometry queries WITHOUT RTREE index (brute force scan)
        2. Geometry queries WITH RTREE index (optimized performance)
        
        Steps:
        1. Create collection with geometry field
        2. Insert data with various WKT formats
        3. Test all geo operators WITHOUT RTREE index
        4. Build RTREE index on geometry field
        5. Test all geo operators WITH RTREE index for comparison
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Step 1: Create schema with geometry field
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Verify collection creation
        collections, _ = self.list_collections(client)
        assert collection_name in collections

        # Verify schema contains geometry field
        collection_info, _ = self.describe_collection(client, collection_name)
        geometry_field = None
        for field in collection_info['fields']:
            if field['name'] == geometry_field_name:
                geometry_field = field
                break
        assert geometry_field is not None
        assert geometry_field['type'] == DataType.GEOMETRY

        # Step 2: Insert data using gen_row_data_by_schema
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Step 3: Test geometry queries WITHOUT RTREE index (brute force scan)
        # Create only vector index first to enable collection loading
        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        # Load collection for testing without geometry index
        self.load_collection(client, collection_name)

        # Test all geo operators WITHOUT RTREE index
        log.info("Testing all geo operators WITHOUT RTREE index")
        test_point = ct.geometry_wkt_samples["POINT"][0]
        test_polygon = ct.geometry_test_regions["small_square"]
        test_linestring = ct.geometry_wkt_samples["LINESTRING"][0]

        for spatial_func in ct.spatial_functions:
            log.info(f"Testing {spatial_func} without RTREE index")

            # Choose appropriate test geometry based on function
            if spatial_func in ["ST_CONTAINS", "ST_WITHIN"]:
                test_geom = test_polygon
            elif spatial_func in ["ST_INTERSECTS", "ST_OVERLAPS", "ST_CROSSES"]:
                test_geom = test_linestring
            else:  # ST_TOUCHES, ST_EQUALS, ST_DISJOINT
                test_geom = test_point

            expr = f"{spatial_func}({geometry_field_name}, '{test_geom}')"
            res, _ = self.query(client,
                                collection_name=collection_name,
                                filter=expr,
                                output_fields=[pk_field_name, geometry_field_name],
                                limit=50
                                )
            
            # Use Shapely validation: directly pass inserted rows data to calculate ground truth
            shapely_expected_pks = cf.validate_geometry_query_with_shapely(
                rows, spatial_func, test_geom, geometry_field_name, pk_field_name
            )
            
            # Compare Milvus results with Shapely ground truth
            milvus_pks = {row[pk_field_name] for row in res}
            assert milvus_pks == shapely_expected_pks
                
        # Release collection before creating RTREE index
        self.release_collection(client, collection_name)

        # Step 4: Build RTREE index
        index_params = self.prepare_index_params(client)[0]
        geo_index_name = "geo_index"
        index_params.add_index(
            field_name=geometry_field_name,
            index_name=geo_index_name,
            index_type=index_type
        )
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=geo_index_name)

        # Step 5: Load and test all geo operators WITH RTREE index
        self.load_collection(client, collection_name)

        # Test all geo operators WITH RTREE index for comparison
        log.info("Testing all geo operators WITH RTREE index")
        test_point = ct.geometry_wkt_samples["POINT"][0]
        test_polygon = ct.geometry_test_regions["triangle"]  # Use different test data for comparison
        test_linestring = ct.geometry_wkt_samples["LINESTRING"][1]

        for spatial_func in ct.spatial_functions:
            log.info(f"Testing {spatial_func} with RTREE index")

            # Choose appropriate test geometry based on function
            if spatial_func in ["ST_CONTAINS", "ST_WITHIN"]:
                test_geom = test_polygon
            elif spatial_func in ["ST_INTERSECTS", "ST_OVERLAPS", "ST_CROSSES"]:
                test_geom = test_linestring
            else:  # ST_TOUCHES, ST_EQUALS, ST_DISJOINT
                test_geom = test_point

            expr = f"{spatial_func}({geometry_field_name}, '{test_geom}')"
            res, _ = self.query(client,
                                collection_name=collection_name,
                                filter=expr,
                                output_fields=[pk_field_name, geometry_field_name],
                                limit=50
                                )
            
            # Use Shapely validation: directly pass inserted rows data to calculate ground truth
            shapely_expected_pks = cf.validate_geometry_query_with_shapely(
                rows, spatial_func, test_geom, geometry_field_name, pk_field_name
            )
            
            # Compare Milvus results with Shapely ground truth
            milvus_pks = {row[pk_field_name] for row in res}
            assert milvus_pks == shapely_expected_pks

        # Final verification: basic spatial query should work without errors
        test_polygon_final = ct.geometry_test_regions["small_square"]
        expr_final = f"ST_WITHIN({geometry_field_name}, '{test_polygon_final}')"

        res_final, _ = self.query(client,
                                  collection_name=collection_name,
                                  filter=expr_final,
                                  output_fields=[pk_field_name, geometry_field_name],
                                  limit=100
                                  )

        # Final verification with Shapely
        shapely_expected_pks_final = cf.validate_geometry_query_with_shapely(
            rows, "ST_WITHIN", test_polygon_final, geometry_field_name, pk_field_name
        )
        
        milvus_pks_final = {row[pk_field_name] for row in res_final}
        assert milvus_pks_final == shapely_expected_pks_final

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #44102")
    def test_create_collection_with_multiple_geometry_fields(self):
        """
        Test case: Create a collection with multiple geometry fields
        Focus on multi-field specific functionality and cross-field queries
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with multiple geometry fields
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("location", datatype=DataType.GEOMETRY)
        schema.add_field("boundary", datatype=DataType.GEOMETRY)
        schema.add_field("area", datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Verify all geometry fields are created
        collection_info, _ = self.describe_collection(client, collection_name)
        geometry_field_count = 0
        for field in collection_info['fields']:
            if field['type'] == DataType.GEOMETRY:
                geometry_field_count += 1
        assert geometry_field_count == 3

        # Insert data and build indices (2000+ rows to trigger real index building)
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build RTREE index on each geometry field
        for geo_field in ["location", "boundary", "area"]:
            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=geo_field,
                index_name=f"{geo_field}_index",
                index_type=index_type
            )
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=f"{geo_field}_index")

        # Build vector index
        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.load_collection(client, collection_name)

        # Test multi-field geometry queries (unique to multiple geometry fields)
        log.info("Testing multi-field geometry queries")
        test_point = "POINT (5 5)"
        test_polygon = ct.geometry_test_regions["small_square"]

        # Test cross-field queries (combinations of multiple geometry fields)
        expr_multi_a_and_b = f"ST_CONTAINS(location, '{test_point}') AND ST_WITHIN(boundary, '{test_polygon}')"
        res_multi_a_and_b, _ = self.query(client,
                                          collection_name=collection_name,
                                          filter=expr_multi_a_and_b,
                                          output_fields=[pk_field_name, "location", "boundary", "area"],
                                          limit=10
                                          )
        assert len(res_multi_a_and_b) > 0

        expr_multi_b_and_a = f"ST_WITHIN(boundary, '{test_polygon}') AND ST_CONTAINS(location, '{test_point}')"
        res_multi_b_and_a, _ = self.query(client,
                                          collection_name=collection_name,
                                          filter=expr_multi_b_and_a,
                                          output_fields=[pk_field_name, "location", "boundary", "area"],
                                          limit=10
                                          )
        assert len(res_multi_b_and_a) > 0
        assert res_multi_b_and_a == res_multi_a_and_b

        # Test OR queries across different geometry fields
        expr_a_or_b = f"ST_INTERSECTS(location, '{test_polygon}') OR ST_INTERSECTS(area, '{test_polygon}')"
        res_a_or_b, _ = self.query(client,
                                   collection_name=collection_name,
                                   filter=expr_a_or_b,
                                   output_fields=[pk_field_name, "location", "area"],
                                   limit=20
                                   )
        assert len(res_a_or_b) > 0

        expr_b_or_a = f"ST_INTERSECTS(area, '{test_polygon}') OR ST_INTERSECTS(location, '{test_polygon}')"
        res_b_or_a, _ = self.query(client,
                                   collection_name=collection_name,
                                   filter=expr_b_or_a,
                                   output_fields=[pk_field_name, "location", "area"],
                                   limit=20
                                   )
        assert len(res_b_or_a) > 0
        assert res_b_or_a == res_a_or_b

        # Test individual field queries to ensure all fields work independently
        for geo_field in ["location", "boundary", "area"]:
            expr = f"ST_CONTAINS({geo_field}, '{test_point}')"
            res, _ = self.query(client,
                                collection_name=collection_name,
                                filter=expr,
                                output_fields=[pk_field_name, geo_field],
                                limit=10
                                )
            assert len(res) > 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.skip(reason="Skip for no add field support in this branch")
    def test_add_geometry_field_to_existing_collection(self):
        """
        Test case: Add a geometry field to an existing collection
        Note: This test may not be supported if alter add field is not implemented for geometry
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection without geometry field first
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("description", datatype=DataType.VARCHAR, max_length=100)

        self.create_collection(client, collection_name, schema=schema)

        # Insert initial data (2000+ rows to trigger real index building)
        initial_rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, initial_rows)
        self.flush(client, collection_name)

        # Try to add geometry field (this might not be supported yet)
        # This operation might fail if not supported - will be skipped in test

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #44075")
    def test_create_collection_with_nullable_geometry_field(self):
        """
        Test case: Create a collection with a nullable=True geometry field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with nullable geometry field
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY, nullable=True)

        self.create_collection(client, collection_name, schema=schema)

        # Verify nullable property
        collection_info, _ = self.describe_collection(client, collection_name)
        geometry_field = None
        for field in collection_info['fields']:
            if field['name'] == geometry_field_name:
                geometry_field = field
                break
        assert geometry_field is not None
        assert geometry_field.get('nullable', False) == True

        # Insert data with some None values (2000+ rows to trigger real index building)
        # Use framework method which will automatically generate 10% None values for nullable fields
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=geometry_field_name,
            index_name="geo_index",
            index_type=index_type
        )
        self.create_index(client, collection_name, index_params)

        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)

        self.wait_for_index_ready(client, collection_name, index_name="geo_index")
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.load_collection(client, collection_name)

        # Query non-null values
        expr = f"{geometry_field_name} IS NOT NULL"
        res, _ = self.query(client,
                            collection_name=collection_name,
                            filter=expr,
                            output_fields=[pk_field_name, geometry_field_name],
                            limit=100
                            )

        # Should get approximately 67 records (100 - 33 null values)
        assert len(res) > 50
        for row in res:
            assert row[geometry_field_name] is not None

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="issue #44079")
    def test_create_collection_with_geometry_default_value(self):
        """
        Test case: Create a collection with a geometry field and its default_value="POINT(0, 0)"
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        default_wkt = ct.default_geometry_value

        # Create schema with default value geometry field
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY, default_value=default_wkt)

        self.create_collection(client, collection_name, schema=schema)

        # Insert data without specifying geometry field (should use default)
        # Need to manually construct to test default value behavior (2000+ rows to trigger real index building)
        rows = [
            {
                pk_field_name: i,
                vector_field_name: cf.gen_vectors(1, dim)[0]
                # geometry_field_name is omitted to test default value
            } for i in range(ct.default_nb)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=geometry_field_name,
            index_name="geo_index",
            index_type=index_type
        )
        self.create_index(client, collection_name, index_params)

        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)

        self.wait_for_index_ready(client, collection_name, index_name="geo_index")
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.load_collection(client, collection_name)

        # Query to verify default values using geometry filter
        test_point = ct.geometry_wkt_samples["POINT"][0]
        res, _ = self.query(client,
                            collection_name=collection_name,
                            filter=f"ST_CONTAINS({geometry_field_name}, '{test_point}') OR {geometry_field_name} IS NOT NULL",
                            output_fields=[pk_field_name, geometry_field_name],
                            limit=10
                            )

        for row in res:
            # All should have the default geometry value
            assert row[geometry_field_name] == default_wkt

    @pytest.mark.tags(CaseLabel.L2)
    def test_alter_geometry_field_mmap_property(self):
        """
        Test case: Alter geometry field mmap property
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY, nullable=False)

        self.create_collection(client, collection_name, schema=schema)

        # Insert data (2000+ rows to trigger real index building)
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        # rows = [
        #     {
        #         pk_field_name: i,
        #         vector_field_name: cf.gen_vectors(1, dim)[0],
        #         geometry_field_name: cf.gen_random_geometry_wkt() if i % 3 != 0 else None
        #     } for i in range(100)
        # ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build index for vector field
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=vector_field_name,
            index_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        # Test 1: Enable mmap for geometry field
        self.alter_collection_field(client, collection_name, geometry_field_name, field_params={"mmap.enabled": True})
        # Verify mmap enabled for geometry field
        self.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                 check_items={geometry_field_name: {"mmap_enabled": True}})

        # Load collection and verify spatial queries still work with mmap enabled
        self.load_collection(client, collection_name)
        test_geometry = ct.geometry_wkt_samples["POINT"][0]
        spatial_res, _ = self.query(client,
                                    collection_name=collection_name,
                                    filter=f"ST_CONTAINS({geometry_field_name}, '{test_geometry}')",
                                    output_fields=[pk_field_name, geometry_field_name],
                                    limit=10)
        assert len(spatial_res) > 0

        # Test 2: Disable mmap for geometry field
        self.release_collection(client, collection_name)
        self.alter_collection_field(client, collection_name, geometry_field_name, field_params={"mmap.enabled": False})
        # Verify mmap enabled for geometry field
        self.describe_collection(client, collection_name, check_task=CheckTasks.check_collection_fields_properties,
                                 check_items={geometry_field_name: {"mmap_enabled": False}})

        # Load collection and verify spatial queries still work with mmap disabled
        self.load_collection(client, collection_name)
        test_geometry = ct.geometry_wkt_samples["POINT"][0]
        spatial_res, _ = self.query(client,
                                    collection_name=collection_name,
                                    filter=f"ST_CONTAINS({geometry_field_name}, '{test_geometry}')",
                                    output_fields=[pk_field_name, geometry_field_name],
                                    limit=10)
        assert len(spatial_res) > 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_alter_geometry_index_mmap_property(self):
        """
        Test case: Alter geometry index mmap property
        Tests geometry index mmap properties
        Note: RTREE index supports mmap property modification
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert data
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create vector index (required before any search/load operations)
        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        # Test 3: Create RTREE index and test its mmap properties
        # Create RTREE index for geometry field
        geo_index_params = self.prepare_index_params(client)[0]
        geo_index_params.add_index(
            field_name=geometry_field_name,
            index_name="geo_rtree_index",
            index_type="RTREE"
        )
        self.create_index(client, collection_name, geo_index_params)
        self.wait_for_index_ready(client, collection_name, index_name="geo_rtree_index")

        # Verify initial mmap state for RTREE index
        geo_index_info = self.describe_index(client, collection_name, index_name="geo_rtree_index")[0]
        assert geo_index_info.get('mmap.enabled', None) is None

        # Test 4: Enable mmap for RTREE index is not supported
        error = {"err_code": 999, "err_msg": "index type RTREE does not support mmap"}
        self.alter_index_properties(client, collection_name, "geo_rtree_index", properties={"mmap.enabled": True},
                                    check_task=CheckTasks.err_res, check_items=error)

        # Test 5: Disable mmap for RTREE index
        self.release_collection(client, collection_name)
        self.alter_index_properties(client, collection_name, "geo_rtree_index", properties={"mmap.enabled": False})

        # Verify mmap disabled for RTREE index  
        geo_index_info_disabled = self.describe_index(client, collection_name, index_name="geo_rtree_index")[0]
        assert geo_index_info_disabled.get('mmap.enabled') == 'False'

        # Load and verify spatial queries still work with mmap disabled RTREE index
        self.load_collection(client, collection_name)
        test_geometry = ct.geometry_wkt_samples["POINT"][0]
        spatial_res, _ = self.query(client,
                                    collection_name=collection_name,
                                    filter=f"ST_CONTAINS({geometry_field_name}, '{test_geometry}')",
                                    output_fields=[pk_field_name, geometry_field_name],
                                    limit=10
                                    )
        assert len(spatial_res) > 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("geometry_type", list(ct.geometry_wkt_samples.keys()))
    def test_insert_data_with_wkt_formats(self, geometry_type):
        """
        Test case: Insert data with WKT data, covering all styles of WKT
        Covers: Point, Linestring, Polygon, Multipoint, Multilinestring, Multipolygon, Geometrycollection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert data with specific geometry type
        wkt_samples = ct.geometry_wkt_samples[geometry_type]
        rows = []
        for i, wkt in enumerate(wkt_samples):
            rows.append({
                pk_field_name: i,
                vector_field_name: cf.gen_vectors(1, dim)[0],
                geometry_field_name: wkt
            })

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build RTREE index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=geometry_field_name,
            index_name="geo_index",
            index_type=index_type
        )
        self.create_index(client, collection_name, index_params)

        # Build vector index
        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)

        self.wait_for_index_ready(client, collection_name, index_name="geo_index")
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.load_collection(client, collection_name)

        # Verify data insertion by querying with geometry filter
        # Use a filter that should match most inserted geometries
        test_polygon = ct.geometry_test_regions["large_square"]
        res, _ = self.query(client,
                            collection_name=collection_name,
                            filter=f"ST_WITHIN({geometry_field_name}, '{test_polygon}')", #  OR {geometry_field_name} IS NOT NULL",
                            output_fields=[pk_field_name, geometry_field_name],
                            limit=len(wkt_samples)
                            )

        assert len(res) == len(wkt_samples)
        inserted_wkts = [row[geometry_field_name] for row in res]
        for wkt in wkt_samples:
            assert wkt in inserted_wkts

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("spatial_func", ct.spatial_functions)
    def test_search_query_with_spatial_operators(self, spatial_func):
        """
        Test case: Test spatial function case-insensitive behavior
        Verifies that uppercase and lowercase spatial functions return identical results
        Covers: ST_CONTAINS vs st_contains, ST_WITHIN vs st_within, etc.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with geometry field
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Insert random geometry data using standard framework method
        rows = cf.gen_row_data_by_schema(nb=ct.default_nb, schema=schema)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build indices and load collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=geometry_field_name,
            index_name="geo_index",
            index_type=index_type
        )
        self.create_index(client, collection_name, index_params)

        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)

        self.wait_for_index_ready(client, collection_name, index_name="geo_index")
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.load_collection(client, collection_name)

        # Test spatial query with different operators
        test_geometries = {
            "ST_CONTAINS": "POINT (5 5)",  # Query objects that contain this point
            "ST_WITHIN": ct.geometry_test_regions["small_square"],  # Query objects within this polygon
            "ST_INTERSECTS": ct.geometry_test_regions["triangle"],  # Query objects that intersect
            "ST_TOUCHES": "LINESTRING (10 0, 10 10)",  # Query objects that touch this line
            "ST_CROSSES": "LINESTRING (0 5, 10 5)",  # Query objects that cross this line
            "ST_OVERLAPS": "POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))",  # Query objects that overlap
            "ST_EQUALS": "POINT (1 1)"
        }

        test_geometry = test_geometries.get(spatial_func, "POINT (5 5)")

        # Test uppercase function
        expr_upper = f"{spatial_func}({geometry_field_name}, '{test_geometry}')"
        res_upper, _ = self.query(client,
                                  collection_name=collection_name,
                                  filter=expr_upper,
                                  output_fields=[pk_field_name, geometry_field_name],
                                  limit=50
                                  )

        # Test lowercase function
        expr_lower = f"{spatial_func.lower()}({geometry_field_name}, '{test_geometry}')"
        res_lower, _ = self.query(client,
                                  collection_name=collection_name,
                                  filter=expr_lower,
                                  output_fields=[pk_field_name, geometry_field_name],
                                  limit=50
                                  )

        # Both should work and return same results
        assert len(res_upper) > 0
        assert len(res_lower) > 0
        assert len(res_upper) == len(res_lower)

        # Verify results are consistent
        upper_ids = {row[pk_field_name] for row in res_upper}
        lower_ids = {row[pk_field_name] for row in res_lower}
        assert upper_ids == lower_ids
        
        # Use Shapely validation for result correctness: directly pass inserted rows data to calculate ground truth
        shapely_expected_pks = cf.validate_geometry_query_with_shapely(
            rows, spatial_func, test_geometry, geometry_field_name, pk_field_name
        )
        
        # Compare Milvus results with Shapely ground truth
        if shapely_expected_pks is not None:
            assert upper_ids == shapely_expected_pks, \
                f"Milvus results {upper_ids} != Shapely expected {shapely_expected_pks} for {spatial_func}"
            log.info(f"✅ {spatial_func} case-insensitive test validated with Shapely: {len(upper_ids)} results match")
        else:
            log.info(f"⚠️ Shapely validation not available for {spatial_func}, case-insensitive test passed")


@pytest.mark.skip(reason="still in debugging @yanliang567")
class TestGeometryNegative(TestMilvusClientV2Base):
    """
    Negative test cases for geometry data type - expected to return meaningful error messages
    """

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("invalid_wkt", ct.invalid_wkt_samples)
    def test_insert_with_invalid_wkt_data(self, invalid_wkt):
        """
        Test case: Insert with invalid WKT data
        Expected: Should return meaningful error messages
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Try to insert invalid WKT data
        invalid_rows = [{
            pk_field_name: 0,
            vector_field_name: cf.gen_vectors(1, dim)[0],
            geometry_field_name: invalid_wkt
        }]

        # Should raise an exception with meaningful error message
        error = {ct.err_code: 1, ct.err_msg: "invalid WKT"}
        self.insert(client, collection_name, invalid_rows,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_insert_valid_wkt_into_dynamic_collection_without_geometry_field(self):
        """
        Test case: Insert with valid WKT data into a collection with dynamic field enabled and no geometry field defined
        Expected: Should return meaningful error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with dynamic field enabled but no geometry field
        schema, _ = self.create_schema(client, enable_dynamic_field=True)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        # Note: No geometry field defined

        self.create_collection(client, collection_name, schema=schema)

        # Try to insert valid WKT data into dynamic field
        valid_wkt = "POINT (0 0)"
        rows_with_dynamic_geometry = [{
            pk_field_name: 0,
            vector_field_name: cf.gen_vectors(1, dim)[0],
            "dynamic_geometry": valid_wkt  # Try to insert as dynamic field
        }]

        self.insert(client, collection_name, rows_with_dynamic_geometry)
        self.flush(client, collection_name)

        # If insertion succeeds, verify the data but it should be treated as string, not geometry
        res, _ = self.query(client,
                            collection_name=collection_name,
                            filter="",
                            output_fields=[pk_field_name, "dynamic_geometry"],
                            limit=10
                            )

        # The dynamic field should contain the WKT as string, not as geometry type
        assert len(res) == 1
        assert res[0]["dynamic_geometry"] == valid_wkt

        # Try to use spatial function on dynamic field - should fail
        error = {ct.err_code: 1100, ct.err_msg: "function not support"}
        self.query(client, collection_name, filter=f"ST_CONTAINS(dynamic_geometry, 'POINT (0 0)')",
                   output_fields=[pk_field_name], limit=10,
                   check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("unsupported_index_type", ["IVF_FLAT", "HNSW", "FLAT"])
    def test_build_rtree_index_on_non_geometry_field(self, unsupported_index_type):
        """
        Test case: Build RTREE index on a non-geometry field
        Expected: Should return meaningful error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with non-geometry fields
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("varchar_field", datatype=DataType.VARCHAR, max_length=100)
        schema.add_field("int_field", datatype=DataType.INT32)

        self.create_collection(client, collection_name, schema=schema)

        # Insert test data (use sufficient data for consistency)
        rows = cf.gen_row_data_by_schema(nb=100, schema=schema)  # 100 is enough for negative test

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Try to build RTREE index on varchar field - should fail
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="varchar_field",
            index_name="invalid_rtree_index",
            index_type="RTREE"
        )

        error = {ct.err_code: 1100, ct.err_msg: "RTREE index can only be created on geometry field"}
        self.create_index(client, collection_name, index_params,
                          check_task=CheckTasks.err_res, check_items=error)

        # Try to build unsupported index on geometry field if we have one
        # First add geometry field
        schema_with_geo, _ = self.create_schema(client)
        schema_with_geo.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema_with_geo.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema_with_geo.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        collection_name_geo = cf.gen_collection_name_by_testcase_name() + "_geo"
        self.create_collection(client, collection_name_geo, schema=schema_with_geo)

        # Insert data (use sufficient data for consistency)
        geo_rows = cf.gen_row_data_by_schema(nb=100, schema=schema_with_geo)  # 100 is enough for negative test

        self.insert(client, collection_name_geo, geo_rows)
        self.flush(client, collection_name_geo)

        # Try to build unsupported index on geometry field
        unsupported_index_params = self.prepare_index_params(client)[0]

        if unsupported_index_type in ["IVF_FLAT"]:
            params = {"nlist": 128}
        elif unsupported_index_type == "HNSW":
            params = {"M": 16, "efConstruction": 200}
        else:
            params = {}

        unsupported_index_params.add_index(
            field_name=geometry_field_name,
            index_name=f"invalid_{unsupported_index_type.lower()}_index",
            index_type=unsupported_index_type,
            params=params
        )

        error = {ct.err_code: 1100, ct.err_msg: f"{unsupported_index_type} index is not supported on geometry field"}
        self.create_index(client, collection_name_geo, unsupported_index_params,
                          check_task=CheckTasks.err_res, check_items=error)


@pytest.mark.skip(reason="still in debugging @yanliang567")
class TestGeometryCorrectnessValidation(TestMilvusClientV2Base):
    """
    Correctness validation tests using Shapely library for ground truth comparison
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_spatial_query_correctness_with_shapely(self):
        """
        Validate spatial query results against Shapely library ground truth
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(geometry_field_name, datatype=DataType.GEOMETRY)

        self.create_collection(client, collection_name, schema=schema)

        # Test data: points and a polygon for validation
        test_points = [
            "POINT (1 1)",  # inside
            "POINT (5 5)",  # inside
            "POINT (15 15)",  # outside
            "POINT (0 0)",  # on boundary
            "POINT (10 10)",  # on boundary
        ]

        test_polygon = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"

        # Insert test data
        rows = []
        for i, point_wkt in enumerate(test_points):
            rows.append({
                pk_field_name: i,
                vector_field_name: cf.gen_vectors(1, dim)[0],
                geometry_field_name: point_wkt
            })

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Build index and load
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=geometry_field_name,
            index_name="geo_index",
            index_type=index_type
        )
        self.create_index(client, collection_name, index_params)

        vec_index_params = self.prepare_index_params(client)[0]
        vec_index_params.add_index(
            field_name=vector_field_name,
            metric_type="COSINE",
            index_type="IVF_FLAT",
            params={"nlist": 128}
        )
        self.create_index(client, collection_name, vec_index_params)

        self.wait_for_index_ready(client, collection_name, index_name="geo_index")
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

        self.load_collection(client, collection_name)

        # Query using Milvus
        expr = f"ST_WITHIN({geometry_field_name}, '{test_polygon}')"
        milvus_results, _ = self.query(client,
                                       collection_name=collection_name,
                                       filter=expr,
                                       output_fields=[pk_field_name, geometry_field_name],
                                       limit=10
                                       )

        # Validate using Shapely
        polygon = wkt_loads(test_polygon)
        expected_within = []

        for i, point_wkt in enumerate(test_points):
            point = wkt_loads(point_wkt)
            if polygon.contains(point) or point.within(polygon):
                expected_within.append(i)

        milvus_ids = {row[pk_field_name] for row in milvus_results}
        expected_ids = set(expected_within)

        # Results should match (allowing for boundary cases)
        difference = milvus_ids.symmetric_difference(expected_ids)
        # Allow some tolerance for boundary cases where geometric libraries may differ
        assert len(difference) <= 2, f"Too many differences between Milvus and Shapely results: {difference}"

        log.info(f"Milvus results: {milvus_ids}")
        log.info(f"Shapely expected: {expected_ids}")
        log.info(f"Validation completed with {len(difference)} boundary case differences")
