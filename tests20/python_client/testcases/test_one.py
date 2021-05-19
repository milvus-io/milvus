import random

from sklearn import preprocessing
from pymilvus_orm.collection import Collection
from pymilvus_orm.schema import FieldSchema, CollectionSchema
from pymilvus_orm.types import DataType
from pymilvus_orm import connections
from utils.util_log import test_log as log


def gen_vectors(nb, dim):
    vectors = [[random.random() for _ in range(dim)] for _ in range(nb)]
    vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
    return vectors.tolist()


class TestOne(object):
    def test_one(self):
        connections.configure(check_res='', default={"host": "172.28.255.155", "port": 19530})
        connections.create_connection(alias="default")
        field = FieldSchema("int64", DataType.INT64, descrition="int64", is_primary=False)
        float_vec_field = FieldSchema(name="float_vec", dtype=DataType.FLOAT_VECTOR, description="float_vec", dim=128)
        float_field = FieldSchema(name="float", dtype=DataType.FLOAT, description="float")
        schema = CollectionSchema(fields=[field, float_field, float_vec_field], description="drop collection")
        import pandas as pd
        float_values = pd.Series(data=[i for i in range(10)], dtype='float32')
        int64_series = [i for i in range(10)]
        data = pd.DataFrame(data={"int64": int64_series, "float": float_values, "float_vec": gen_vectors(10, 128)})
        log.info(data)
        collection = Collection(name="test_collection", schema=schema, data=data)
        log.info(collection.num_entities)
        collection.drop()
