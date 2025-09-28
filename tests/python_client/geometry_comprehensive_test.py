import random
import numpy as np
import math
import time

from pymilvus import MilvusClient, DataType

COUNT = 10000

def generate_simple_point():
    """Generate simple random point with integer coordinates"""
    x = random.randint(100, 120)
    y = random.randint(30, 50)
    return f"POINT({x} {y})"

def generate_simple_line():
    """Generate simple random line with integer coordinates"""
    x1, y1 = random.randint(100, 120), random.randint(30, 50)
    x2, y2 = random.randint(100, 120), random.randint(30, 50)
    return f"LINESTRING({x1} {y1}, {x2} {y2})"

def generate_simple_polygon():
    """Generate simple random polygon with integer coordinates"""
    # Generate center point
    center_x = random.randint(100, 120)
    center_y = random.randint(30, 50)
    
    # Generate polygon vertices (triangle or rectangle)
    num_vertices = random.choice([3, 4])
    vertices = []
    
    for i in range(num_vertices):
        angle = (2 * math.pi * i) / num_vertices
        radius = random.randint(1, 3)
        x = center_x + int(radius * math.cos(angle))
        y = center_y + int(radius * math.sin(angle))
        vertices.append(f"{x} {y}")
    
    # Close polygon
    vertices.append(vertices[0])
    return f"POLYGON(({', '.join(vertices)}))"

def generate_clustered_data():
    """Generate clustered data with simple integer coordinates"""
    # Define center areas with simple coordinates
    centers = [
        (110, 40),  # Center 1
        (115, 35),  # Center 2
        (105, 45),  # Center 3
    ]
    
    geometries = []
    
    for i in range(COUNT):
        # Choose a center
        center = random.choice(centers)
        center_x, center_y = center
        
        # Generate geometry objects around center
        offset_x = random.randint(-5, 5)
        offset_y = random.randint(-5, 5)
        
        geom_type = random.choice(['point', 'line', 'polygon'])
        
        if geom_type == 'point':
            x = center_x + offset_x
            y = center_y + offset_y
            geom = f"POINT({x} {y})"
        elif geom_type == 'line':
            x1 = center_x + offset_x
            y1 = center_y + offset_y
            x2 = center_x + offset_x + random.randint(-3, 3)
            y2 = center_y + offset_y + random.randint(-3, 3)
            geom = f"LINESTRING({x1} {y1}, {x2} {y2})"
        else:  # polygon
            # Generate small polygon around center
            vertices = []
            for j in range(3):
                angle = (2 * math.pi * j) / 3
                radius = random.randint(1, 3)
                x = center_x + offset_x + int(radius * math.cos(angle))
                y = center_y + offset_y + int(radius * math.sin(angle))
                vertices.append(f"{x} {y}")
            vertices.append(vertices[0])  # Close polygon
            geom = f"POLYGON(({', '.join(vertices)}))"
        
        geometries.append(geom)
    
    return geometries

def generate_test_data(num_records=10000):
    """Generate test data"""
    ids = list(range(1, num_records + 1))
    
    # Use clustered data to generate geometry objects
    geometries = generate_clustered_data()
    
    # Generate random vectors
    vectors = []
    for i in range(num_records):
        vector = [random.random() for _ in range(128)]
        vectors.append(vector)
    
    return ids, geometries, vectors

def main():
    fmt = "\n=== {:30} ===\n"
    
    # Connection configuration
    client = MilvusClient(
        uri="http://localhost:19530",
        token=""
    )
    collection_name = "comprehensive_geo_test"
    dim = 128  # Vector dimension
    
    # Drop existing collection if exists
    if client.has_collection(collection_name):
        client.drop_collection(collection_name)
        print(f"Dropped existing collection: {collection_name}")
    
    print(fmt.format("Creating Collection"))
    try:
        schema = client.create_schema(auto_id=False, description="comprehensive_geo_test")
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("geo", DataType.GEOMETRY)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vector", index_type="IVF_FLAT", metric_type="L2", nlist=128)
        client.create_collection(collection_name, schema=schema, index_params=index_params)
        print(f"Collection created: {collection_name}")
    except Exception as e:
        print(f"Error creating collection: {e}")
        return
    
    # Generate test data
    print(fmt.format("Generating Test Data"))
    num_records = COUNT
    ids, geometries, vectors = generate_test_data(num_records)
    
    # Show data preview
    print(fmt.format("Data Preview"))
    for i in range(5):
        print(f"ID: {ids[i]}")
        print(f"Geometry: {geometries[i]}")
        print(f"Vector: [{', '.join([f'{x:.3f}' for x in vectors[i][:3]])}...]")
        print("---")
    
    # Insert data
    print(fmt.format("Inserting Data"))
    
    # Insert data in batches to avoid memory issues
    batch_size = 1000
    total_inserted = 0
    time_start = time.time()
    for i in range(0, num_records, batch_size):
        end_idx = min(i + batch_size, num_records)
        batch_data = []
        
        for j in range(i, end_idx):
            row = {
                "id": ids[j],
                "geo": geometries[j],
                "vector": vectors[j]
            }
            batch_data.append(row)
        
        try:
            insert_result = client.insert(collection_name, batch_data)
            total_inserted += len(batch_data)
            print(f"Inserted {total_inserted}/{num_records} records")
        except Exception as e:
            print(f"Error inserting data: {e}")
            return
    time_end = time.time()
    print(f"Data Insertion Time: {(time_end - time_start) * 1000:.2f} ms")
    print(fmt.format("Data Insertion Complete"))
    
    # # Flush data to persistent storage
    # print("Flushing data...")
    # try:
    #     client.flush(collection_name)
    #     print("Data flush complete")
    # except Exception as e:
    #     print(f"Error flushing data: {e}")
    #     return
    
    # Load collection
    try:
        client.load_collection(collection_name)
        print(fmt.format("Collection Loaded"))
    except Exception as e:
        print(f"Error loading collection: {e}")
        return
    
    # Test non-spatial function queries
    print(fmt.format("Testing Non-Spatial Queries"))
    time_start = time.time()
    try:
        # Simple query test
        print("\nSimple query test:")
        query_results = client.query(
            collection_name=collection_name,
            filter="id <= 10",
            output_fields=["id", "geo"],
            limit=10
        )
        print(f"Found {len(query_results)} records")
        for result in query_results[:3]:
            print(f"  ID: {result['id']}, geo: {result['geo']}")
    except Exception as e:
        print(f"Simple query test error: {e}")
    time_end = time.time()
    print(f"Simple query test Time: {(time_end - time_start) * 1000:.2f} ms")
    # Test vector search
    print(fmt.format("Testing Vector Search"))
    try:
        search_vector = vectors[0]
        
        search_results = client.search(
            collection_name=collection_name,
            data=[search_vector],
            anns_field="vector",
            search_params={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=5,
            output_fields=["id", "geo"]
        )
        
        print(f"Search results length: {len(search_results)}")
        for i, hits in enumerate(search_results):
            print(f"Query vector {i+1} search results:")
            for j, hit in enumerate(hits):
                print(f"  Result {j+1} - ID: {hit['id']}, Geo: {hit['geo']}, distance: {hit['distance']:.4f}")
                
    except Exception as e:
        print(f"Vector search test error: {e}")
    
    # Test all spatial functions
    print(fmt.format("Testing Spatial Functions"))
    
    # Define test geometry objects with simple integer coordinates
    test_geometries = {
        "point": "POINT(110 40)",  # Center point
        "line": "LINESTRING(105 35, 115 45)",  # Line across centers
        "polygon": "POLYGON((105 35, 115 35, 115 45, 105 45, 105 35))",  # Rectangle covering centers
        "small_polygon": "POLYGON((108 38, 112 38, 112 42, 108 42, 108 38))",  # Small rectangle
        "crossing_line": "LINESTRING(100 30, 120 50)",  # Line crossing the area
        "overlapping_polygon": "POLYGON((110 38, 115 38, 115 42, 110 42, 110 38))"  # Overlapping polygon
    }
    
    # 空间函数列表
    spatial_functions = [
        ("st_equals", "ST_EQUALS"),
        ("st_touches", "ST_TOUCHES"),
        ("st_overlaps", "ST_OVERLAPS"),
        ("st_crosses", "ST_CROSSES"),
        ("st_contains", "ST_CONTAINS"),
        ("st_intersects", "ST_INTERSECTS"),
        ("st_within", "ST_WITHIN")
    ]
    
    for func_name, func_alias in spatial_functions:
        print(f"\nTesting {func_name} / {func_alias}:")
        # Test different geometry objects
        for geom_key, test_geom in test_geometries.items():
            try:
                time_start = time.time()
                expr = f"{func_name}(geo, '{test_geom}')"
                results = client.query(
                    collection_name=collection_name,
                    filter=expr,
                    output_fields=["id","geo"],
                    limit=10
                )
                time_end = time.time()
                print(f"  {func_name} with {geom_key}: Found {len(results)} records, Time: {(time_end - time_start) * 1000:.2f} ms")
                if results:
                    print(f"    Sample IDs: {[r['id'] for r in results[:5]]}")
                    print(f"    Sample geometries: {[r['geo'] for r in results[:5]]}") 
            except Exception as e:
                print(f"  {func_name} with {geom_key} test failed: {e}")
        
        # Test uppercase function name
        try:
            expr = f"{func_alias}(geo, '{test_geometries['point']}')"
            results = client.query(
                collection_name=collection_name,
                filter=expr,
                output_fields=["id","geo"],
                limit=10
            )
            print(f"  {func_alias}: Found {len(results)} records")
            if results:
                print(f"    Sample IDs: {[r['id'] for r in results[:5]]}")
                print(f"    Sample geometries: {[r['geo'] for r in results[:5]]}")
        except Exception as e:
            print(f"  {func_alias} test failed: {e}")
    
    # Test different geometry types
    print(fmt.format("Testing Different Geometry Types"))
    print(fmt.format("Using ST_INTERSECTS"))
    for geom_type, test_geom in test_geometries.items():
        print(f"\nTesting {geom_type} geometry:")
        try:
            time_start = time.time()
            expr = f"st_intersects(geo, '{test_geom}')"
            results = client.query(
                collection_name=collection_name,
                filter=expr,
                output_fields=["id","geo"],
                limit=10
            )
            time_end = time.time()
            print(f"  Found {len(results)} records, Time: {(time_end - time_start) * 1000:.2f} ms")
            if results:
                print(f"  Sample IDs: {[r['id'] for r in results[:5]]}")
                print(f"  Sample geometries: {[r['geo'] for r in results[:5]]}")
        except Exception as e:
            print(f"  Test failed: {e}")
    
    print(fmt.format("Test Complete"))
    print(f"Total records tested: {num_records}")
    print(f"Total spatial functions tested: {len(spatial_functions)}")
    print("All tests completed!")

if __name__ == "__main__":
    main() 