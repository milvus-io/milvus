import numpy as np
from pymilvus import DataType
from common.common_type import CaseLabel
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base
import random
import pytest

prefix = "search_collection"


class TestSearchArrayIndependent(TestMilvusClientV2Base):

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("array_element_data_type", [DataType.INT64])
    def test_search_array_with_inverted_index(self, array_element_data_type):
        # create collection with Client V2 API
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        additional_params = {"max_length": 1000} if array_element_data_type == DataType.VARCHAR else {}
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("contains", DataType.ARRAY, element_type=array_element_data_type,
                         max_capacity=2000, **additional_params)
        schema.add_field("contains_any", DataType.ARRAY, element_type=array_element_data_type,
                         max_capacity=2000, **additional_params)
        schema.add_field("contains_all", DataType.ARRAY, element_type=array_element_data_type,
                         max_capacity=2000, **additional_params)
        schema.add_field("equals", DataType.ARRAY, element_type=array_element_data_type,
                         max_capacity=2000, **additional_params)
        schema.add_field("array_length_field", DataType.ARRAY, element_type=array_element_data_type,
                         max_capacity=2000, **additional_params)
        schema.add_field("array_access", DataType.ARRAY, element_type=array_element_data_type,
                         max_capacity=2000, **additional_params)
        schema.add_field("emb", DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        train_df, query_expr = cf.prepare_array_test_data(3000, hit_rate=0.05)
        train_data = train_df.to_dict(orient='records')
        self.insert(client, collection_name, data=train_data)

        # create indexes
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name="emb", metric_type="L2", index_type="HNSW",
                      params={"M": 48, "efConstruction": 500})
        for f in ["contains", "contains_any", "contains_all", "equals",
                   "array_length_field", "array_access"]:
            idx.add_index(field_name=f, index_type="INVERTED")
        self.create_index(client, collection_name, index_params=idx)

        # load collection
        self.load_collection(client, collection_name)

        # search and verify results
        for item in query_expr:
            expr = item["expr"]
            ground_truth_candidate = item["ground_truth"]
            res, _ = self.search(client, collection_name,
                                 data=[np.array([random.random() for j in range(128)],
                                                dtype=np.dtype("float32"))],
                                 anns_field="emb",
                                 search_params={"metric_type": "L2",
                                                "params": {"M": 32, "efConstruction": 360}},
                                 limit=10,
                                 filter=expr,
                                 output_fields=["*"])
            assert len(res) == 1
            for i in range(len(res)):
                assert len(res[i]) == 10
                for hit in res[i]:
                    assert hit.id in ground_truth_candidate
