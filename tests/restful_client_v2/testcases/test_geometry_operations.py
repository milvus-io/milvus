import random
import pytest
import numpy as np
from sklearn import preprocessing
from base.testbase import TestBase
from utils.utils import gen_collection_name, generate_wkt_by_type
from utils.util_log import test_log as logger


default_dim = 128


@pytest.mark.L0
class TestGeometryCollection(TestBase):
    """Test geometry collection operations"""

    def test_create_collection_with_geometry_field(self):
        """
        target: test create collection with geometry field
        method: create collection with geometry field using schema
        expected: create collection successfully
        """
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    {"fieldName": "geo", "dataType": "Geometry"}
                ]
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0
        # Verify collection exists
        rsp = self.collection_client.collection_describe(name)
        assert rsp['code'] == 0
        logger.info(f"Collection created: {rsp}")

    @pytest.mark.parametrize("wkt_type", [
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION"
    ])
    def test_insert_wkt_data(self, wkt_type):
        """
        target: test insert various WKT geometry types
        method: generate and insert different WKT geometry data
        expected: insert successfully
        """
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    {"fieldName": "geo", "dataType": "Geometry"}
                ]
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Generate WKT data
        nb = 100
        wkt_data = generate_wkt_by_type(wkt_type, bounds=(0, 100, 0, 100), count=nb)
        data = []
        for i, wkt in enumerate(wkt_data):
            data.append({
                "id": i,
                "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                "geo": wkt
            })

        # Insert data
        insert_payload = {
            "collectionName": name,
            "data": data
        }
        rsp = self.vector_client.vector_insert(insert_payload)
        assert rsp['code'] == 0
        assert rsp['data']['insertCount'] == nb
        logger.info(f"Inserted {nb} {wkt_type} geometries")

    @pytest.mark.parametrize("index_type", ["RTREE", "AUTOINDEX"])
    def test_build_geometry_index(self, index_type):
        """
        target: test build geometry index on geometry field
        method: create geometry index on geometry field
        expected: build index successfully
        """
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    {"fieldName": "geo", "dataType": "Geometry"}
                ]
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"},
                {"fieldName": "geo", "indexName": "geo_idx", "indexType": index_type}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        # Insert some geometry data
        nb = 50
        data = []
        for i in range(nb):
            x = random.uniform(0, 100)
            y = random.uniform(0, 100)
            data.append({
                "id": i,
                "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                "geo": f"POINT ({x:.2f} {y:.2f})"
            })

        insert_payload = {
            "collectionName": name,
            "data": data
        }
        rsp = self.vector_client.vector_insert(insert_payload)
        assert rsp['code'] == 0

        # Load collection
        self.wait_collection_load_completed(name)

        # Verify index
        rsp = self.index_client.index_list(name)
        assert rsp['code'] == 0
        logger.info(f"Indexes: {rsp}")

    @pytest.mark.parametrize("spatial_func", [
        "ST_INTERSECTS",
        "ST_CONTAINS",
        "ST_WITHIN",
        "ST_EQUALS",
        "ST_TOUCHES",
        "ST_OVERLAPS",
        "ST_CROSSES"
    ])
    @pytest.mark.parametrize("data_state", ["sealed", "growing", "sealed_and_growing"])
    @pytest.mark.parametrize("with_geo_index", [True, False])
    @pytest.mark.parametrize("nullable", [True, False])
    def test_spatial_query_and_search(self, spatial_func, data_state, with_geo_index, nullable):
        """
        target: test spatial query and search with geometry filter
        method: query and search geometry data using spatial operators on sealed/growing data
        expected: query and search execute successfully (with or without geo index, nullable or not)
        """
        name = gen_collection_name()
        index_params = [{"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"}]
        if with_geo_index:
            index_params.append({"fieldName": "geo", "indexName": "geo_idx", "indexType": "RTREE"})

        geo_field = {"fieldName": "geo", "dataType": "Geometry"}
        if nullable:
            geo_field["nullable"] = True

        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    geo_field
                ]
            },
            "indexParams": index_params
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        nb = 100

        # Define query geometry and matching data based on spatial function
        # Each spatial function needs specific data patterns to guarantee matches
        if spatial_func == "ST_INTERSECTS":
            # Query: large polygon covering center area
            # Data: points and polygons inside the query area will intersect
            query_geom = "POLYGON ((20 20, 80 20, 80 80, 20 80, 20 20))"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    # Generate points inside query polygon (25-75 range)
                    x = 25 + (i % 10) * 5
                    y = 25 + (i // 10) * 5
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    elif i % 2 == 0:
                        item["geo"] = f"POINT ({x:.2f} {y:.2f})"
                    else:
                        # Small polygon inside query area
                        item["geo"] = f"POLYGON (({x:.2f} {y:.2f}, {x + 3:.2f} {y:.2f}, {x + 3:.2f} {y + 3:.2f}, {x:.2f} {y + 3:.2f}, {x:.2f} {y:.2f}))"
                    data.append(item)
                return data

        elif spatial_func == "ST_CONTAINS":
            # Query: large polygon
            # ST_CONTAINS(query_geom, geo) - query contains data geometry
            # Data: small points/polygons inside query polygon
            query_geom = "POLYGON ((10 10, 90 10, 90 90, 10 90, 10 10))"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    # Points well inside the query polygon
                    x = 20 + (i % 10) * 6
                    y = 20 + (i // 10) * 6
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    else:
                        # Points are easily contained
                        item["geo"] = f"POINT ({x:.2f} {y:.2f})"
                    data.append(item)
                return data

        elif spatial_func == "ST_WITHIN":
            # ST_WITHIN(geo, query_geom) - data geometry is within query geometry
            # Same as ST_CONTAINS but reversed semantics
            query_geom = "POLYGON ((10 10, 90 10, 90 90, 10 90, 10 10))"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    x = 20 + (i % 10) * 6
                    y = 20 + (i // 10) * 6
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    else:
                        item["geo"] = f"POINT ({x:.2f} {y:.2f})"
                    data.append(item)
                return data

        elif spatial_func == "ST_EQUALS":
            # ST_EQUALS requires exact geometry match
            # Insert known points and query with one of them
            query_geom = "POINT (50.00 50.00)"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    elif i % 10 == 0:
                        # Every 10th record has the exact query point
                        item["geo"] = "POINT (50.00 50.00)"
                    else:
                        x = 20 + (i % 10) * 6
                        y = 20 + (i // 10) * 6
                        item["geo"] = f"POINT ({x:.2f} {y:.2f})"
                    data.append(item)
                return data

        elif spatial_func == "ST_TOUCHES":
            # ST_TOUCHES: geometries touch at boundary but don't overlap interiors
            # Query polygon and data polygons that share edges
            query_geom = "POLYGON ((50 50, 60 50, 60 60, 50 60, 50 50))"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    elif i % 4 == 0:
                        # Polygon touching right edge of query (starts at x=60)
                        item["geo"] = "POLYGON ((60 50, 70 50, 70 60, 60 60, 60 50))"
                    elif i % 4 == 1:
                        # Polygon touching top edge of query (starts at y=60)
                        item["geo"] = "POLYGON ((50 60, 60 60, 60 70, 50 70, 50 60))"
                    elif i % 4 == 2:
                        # Point on edge of query polygon
                        item["geo"] = "POINT (55.00 50.00)"
                    else:
                        # Point on corner
                        item["geo"] = "POINT (50.00 50.00)"
                    data.append(item)
                return data

        elif spatial_func == "ST_OVERLAPS":
            # ST_OVERLAPS: geometries overlap but neither contains the other (same dimension)
            # Need polygons that partially overlap
            query_geom = "POLYGON ((40 40, 60 40, 60 60, 40 60, 40 40))"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    else:
                        # Polygons that partially overlap with query
                        # Shifted to overlap but not contain/be contained
                        offset = (i % 4) * 5
                        if i % 2 == 0:
                            # Overlapping from right side
                            item["geo"] = f"POLYGON (({50 + offset} 45, {70 + offset} 45, {70 + offset} 55, {50 + offset} 55, {50 + offset} 45))"
                        else:
                            # Overlapping from bottom
                            item["geo"] = f"POLYGON ((45 {50 + offset}, 55 {50 + offset}, 55 {70 + offset}, 45 {70 + offset}, 45 {50 + offset}))"
                    data.append(item)
                return data

        elif spatial_func == "ST_CROSSES":
            # ST_CROSSES: geometries cross (line crosses polygon interior)
            # Query with a line, data has polygons that the line passes through
            query_geom = "LINESTRING (0 50, 100 50)"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    else:
                        # Polygons that the horizontal line y=50 crosses through
                        x = 10 + (i % 10) * 8
                        # Polygon spanning y=40 to y=60, so line y=50 crosses it
                        item["geo"] = f"POLYGON (({x} 40, {x + 5} 40, {x + 5} 60, {x} 60, {x} 40))"
                    data.append(item)
                return data
        else:
            query_geom = "POLYGON ((20 20, 80 20, 80 80, 20 80, 20 20))"

            def generate_geo_data(start_id, count):
                data = []
                for i in range(count):
                    x = 30 + (i % 10) * 4
                    y = 30 + (i // 10) * 4
                    item = {
                        "id": start_id + i,
                        "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    }
                    if nullable and i % 5 == 0:
                        item["geo"] = None
                    else:
                        item["geo"] = f"POINT ({x:.2f} {y:.2f})"
                    data.append(item)
                return data

        # Insert data based on data_state
        if data_state == "sealed":
            data = generate_geo_data(0, nb)
            insert_payload = {"collectionName": name, "data": data}
            rsp = self.vector_client.vector_insert(insert_payload)
            assert rsp['code'] == 0
            rsp = self.collection_client.flush(name)
            self.wait_collection_load_completed(name)

        elif data_state == "growing":
            self.wait_collection_load_completed(name)
            data = generate_geo_data(0, nb)
            insert_payload = {"collectionName": name, "data": data}
            rsp = self.vector_client.vector_insert(insert_payload)
            assert rsp['code'] == 0

        else:  # sealed_and_growing
            sealed_data = generate_geo_data(0, nb // 2)
            insert_payload = {"collectionName": name, "data": sealed_data}
            rsp = self.vector_client.vector_insert(insert_payload)
            assert rsp['code'] == 0
            rsp = self.collection_client.flush(name)
            self.wait_collection_load_completed(name)
            growing_data = generate_geo_data(nb // 2, nb // 2)
            insert_payload = {"collectionName": name, "data": growing_data}
            rsp = self.vector_client.vector_insert(insert_payload)
            assert rsp['code'] == 0

        filter_expr = f"{spatial_func}(geo, '{query_geom}')"

        # 1. Query with spatial filter
        query_payload = {
            "collectionName": name,
            "filter": filter_expr,
            "outputFields": ["id", "geo"],
            "limit": 100
        }
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0
        query_count = len(rsp.get('data', []))
        logger.info(f"{spatial_func} ({data_state}, geo_index={with_geo_index}, nullable={nullable}) query returned {query_count} results")
        # Verify we got results (except for edge cases)
        if not nullable or spatial_func not in ["ST_EQUALS"]:
            assert query_count > 0, f"{spatial_func} query should return results"

        # 2. Search with geo filter
        query_vector = preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist()
        search_payload = {
            "collectionName": name,
            "data": [query_vector],
            "annsField": "vector",
            "filter": filter_expr,
            "limit": 10,
            "outputFields": ["id", "geo"]
        }
        rsp = self.vector_client.vector_search(search_payload)
        assert rsp['code'] == 0
        search_count = len(rsp.get('data', []))
        logger.info(f"{spatial_func} ({data_state}, geo_index={with_geo_index}, nullable={nullable}) search returned {search_count} results")

    def test_upsert_geometry_data(self):
        """
        target: test upsert geometry data
        method: upsert geometry data
        expected: upsert executes successfully
        """
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    {"fieldName": "geo", "dataType": "Geometry"}
                ]
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"},
                {"fieldName": "geo", "indexName": "geo_idx", "indexType": "RTREE"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        nb = 100

        def generate_geo_data(start_id, count):
            data = []
            for i in range(count):
                x = random.uniform(10, 90)
                y = random.uniform(10, 90)
                data.append({
                    "id": start_id + i,
                    "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    "geo": f"POINT ({x:.2f} {y:.2f})"
                })
            return data

        # Insert initial data
        data = generate_geo_data(0, nb)
        insert_payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(insert_payload)
        assert rsp['code'] == 0
        self.wait_collection_load_completed(name)

        # Upsert data
        upsert_data = generate_geo_data(0, nb // 2)
        upsert_payload = {"collectionName": name, "data": upsert_data}
        rsp = self.vector_client.vector_upsert(upsert_payload)
        assert rsp['code'] == 0
        logger.info("Upsert geometry data completed successfully")

    def test_delete_geometry_data(self):
        """
        target: test delete geometry data
        method: delete geometry data
        expected: delete executes successfully
        """
        name = gen_collection_name()
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    {"fieldName": "geo", "dataType": "Geometry"}
                ]
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"},
                {"fieldName": "geo", "indexName": "geo_idx", "indexType": "RTREE"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        nb = 100

        def generate_geo_data(start_id, count):
            data = []
            for i in range(count):
                x = random.uniform(10, 90)
                y = random.uniform(10, 90)
                data.append({
                    "id": start_id + i,
                    "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
                    "geo": f"POINT ({x:.2f} {y:.2f})"
                })
            return data

        # Insert data
        data = generate_geo_data(0, nb)
        insert_payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(insert_payload)
        assert rsp['code'] == 0
        self.wait_collection_load_completed(name)

        # Delete data
        delete_ids = list(range(0, nb // 2))
        delete_payload = {"collectionName": name, "filter": f"id in {delete_ids}"}
        rsp = self.vector_client.vector_delete(delete_payload)
        assert rsp['code'] == 0

        # Verify deletion by querying
        query_payload = {
            "collectionName": name,
            "filter": "id >= 0",
            "outputFields": ["id", "geo"],
            "limit": 200
        }
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0
        logger.info(f"Delete geometry data completed, remaining: {len(rsp.get('data', []))} records")

    def test_geometry_default_value(self):
        """
        target: test geometry field with default value
        method: create collection with geometry field having default value
        expected: records without geo field use default value
        """
        name = gen_collection_name()
        default_geo = "POINT (0 0)"
        payload = {
            "collectionName": name,
            "schema": {
                "autoId": False,
                "enableDynamicField": True,
                "fields": [
                    {"fieldName": "id", "dataType": "Int64", "isPrimary": True},
                    {"fieldName": "vector", "dataType": "FloatVector", "elementTypeParams": {"dim": f"{default_dim}"}},
                    {"fieldName": "geo", "dataType": "Geometry", "defaultValue": default_geo}
                ]
            },
            "indexParams": [
                {"fieldName": "vector", "indexName": "vector_idx", "metricType": "L2"},
                {"fieldName": "geo", "indexName": "geo_idx", "indexType": "RTREE"}
            ]
        }
        rsp = self.collection_client.collection_create(payload)
        assert rsp['code'] == 0

        nb = 100
        data = []
        for i in range(nb):
            item = {
                "id": i,
                "vector": preprocessing.normalize([np.array([random.random() for _ in range(default_dim)])])[0].tolist(),
            }
            # 30% use default value (omit geo field)
            if i % 3 != 0:
                x = random.uniform(10, 90)
                y = random.uniform(10, 90)
                item["geo"] = f"POINT ({x:.2f} {y:.2f})"
            # else: geo field omitted, will use default value
            data.append(item)

        insert_payload = {"collectionName": name, "data": data}
        rsp = self.vector_client.vector_insert(insert_payload)
        assert rsp['code'] == 0
        self.wait_collection_load_completed(name)

        # Query for records with default geometry value
        query_payload = {
            "collectionName": name,
            "filter": f"ST_EQUALS(geo, '{default_geo}')",
            "outputFields": ["id", "geo"],
            "limit": 100
        }
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0
        default_count = len(rsp.get('data', []))
        logger.info(f"Default geometry: found {default_count} records with default value")

        # Query all records
        query_payload = {
            "collectionName": name,
            "filter": "id >= 0",
            "outputFields": ["id", "geo"],
            "limit": 200
        }
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0
        total_count = len(rsp.get('data', []))
        logger.info(f"Default geometry: total {total_count} records")

        # Spatial query with default value area
        query_payload = {
            "collectionName": name,
            "filter": "ST_WITHIN(geo, 'POLYGON ((-5 -5, 5 -5, 5 5, -5 5, -5 -5))')",
            "outputFields": ["id", "geo"],
            "limit": 100
        }
        rsp = self.vector_client.vector_query(query_payload)
        assert rsp['code'] == 0
        logger.info(f"Default geometry: spatial query near origin returned {len(rsp.get('data', []))} results")
