import random
from pymilvus import Milvus, DataType

dim = 128
name = "sift_1m_128_l2"

def generate_values(data_type, vectors, ids):
    values = None
    if data_type in [DataType.INT32, DataType.INT64]:
        values = ids
    elif data_type in [DataType.FLOAT, DataType.DOUBLE]:
        values = [(i + 0.0) for i in ids]
    elif data_type in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
        values = vectors
    return values


def generate_entities(info, vectors, ids=None):
    entities = []
    for field in info["fields"]:
        if field["name"] == "_id":
            continue
        field_type = field["type"]
        entities.append(
            {"name": field["name"], "type": field_type, "values": generate_values(field_type, vectors, ids)})
    return entities


m = Milvus(host="127.0.0.1")
info = m.describe_collection(name)
print(info)
ids = [random.randint(1, 10000000)]
X = [[random.random() for _ in range(dim)] for _ in range(1)]
entities = generate_entities(info, X, ids)
print(entities)
m.insert(name, entities, ids=ids)
