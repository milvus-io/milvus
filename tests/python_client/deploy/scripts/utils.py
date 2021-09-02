import docker

from pymilvus import (
    connections, FieldSchema, CollectionSchema, DataType,
    Collection, list_collections,
)


def list_containers():
    client = docker.from_env()
    containers = client.containers.list()
    for c in containers:
        if "milvus" in c.name:
            print(c.image)


def get_collections():
    print(f"\nList collections...")
    col_list = list_collections()
    print(f"collections_nums: {len(col_list)}")
    # list entities if collections
    for name in col_list:
        c = Collection(name = name)
        print(f"{name}: {c.num_entities}")


def create_collections_and_insert_data(col_name="hello_milvus"):
    import random
    dim = 128
    default_fields = [
        FieldSchema(name="count", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="random_value", dtype=DataType.DOUBLE), 
        FieldSchema(name="float_vector", dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    default_schema = CollectionSchema(fields=default_fields, description="test collection")

    print(f"\nCreate collection...")
    collection = Collection(name=col_name, schema=default_schema)

    print(f"\nList collections...")
    print(list_collections())

    #  insert data
    nb = 3000
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    collection.insert(
        [
            [i for i in range(nb)],
            [float(random.randrange(-20, -10)) for _ in range(nb)],
            vectors
        ]
    )
    print(f"\nGet collection entities...")
    print(collection.num_entities)

def create_index():
    # create index
    default_index = {"index_type": "IVF_FLAT", "params": {"nlist": 128}, "metric_type": "L2"}
    col_list = list_collections()
    print(f"\nCreate index...")
    for name in col_list:
        c = Collection(name = name)

        print(name)
        print(c)
        c.create_index(field_name="float_vector", index_params=default_index)



def load_and_search():
    print("search data starts")
    col_list = list_collections()
    for name in col_list:
        c = Collection(name=name)
        print(f"collection name: {name}")
        c.load()
        topK = 5
        vectors = [[1.0 for _ in range(128)] for _ in range(3000)]
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}


        import time
        start_time = time.time()
        print(f"\nSearch...")
        # define output_fields of search result
        res = c.search(
            vectors[-2:], "float_vector", search_params, topK,
            "count > 500", output_fields=["count", "random_value"]
        )
        end_time = time.time()
        # show result
        for hits in res:
            for hit in hits:
                # Get value of the random value field for search result
                print(hit, hit.entity.get("random_value"))
            print("###########")
        print("search latency = %.4fs" % (end_time - start_time))
        c.release()
    print("search data ends")  
 