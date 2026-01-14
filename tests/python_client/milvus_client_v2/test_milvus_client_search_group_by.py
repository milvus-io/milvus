import json
import numpy as np
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker
from pymilvus import (
    FieldSchema, CollectionSchema, DataType
)
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base
import random
import pytest

epsilon = 0.001

dyna_filed_name1 = "dyna_filed_name1"
dyna_filed_name2 = "dyna_filed_name2"
inverted_string_field_name = "varchar_inverted"
indexed_json_field_name = "indexed_json"


@pytest.mark.xdist_group("TestGroupSearch")
class TestGroupSearch(TestMilvusClientV2Base):
    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestGroupSearch" + cf.gen_unique_str("_")
        self.partition_names = ["partition_1", "partition_2"]
        self.primary_field = "int64_pk"
        self.float_vector_field_name = ct.default_float_vec_field_name
        self.bfloat16_vector_field_name = "bfloat16_vector"
        self.sparse_vector_field_name = "sparse_vector"
        self.binary_vector_field_name = "binary_vector"
        self.vector_fields = [self.float_vector_field_name, self.bfloat16_vector_field_name,
                              self.sparse_vector_field_name, self.binary_vector_field_name]
        self.float_vector_dim = 36
        self.bf16_vector_dim = 35
        self.binary_vector_dim = 32
        self.dims = [self.float_vector_dim, self.bf16_vector_dim,
                     128, self.binary_vector_dim]
        self.float_vector_metric = "COSINE"
        self.bf16_vector_metric = "L2"
        self.sparse_vector_metric = "IP"
        self.binary_vector_metric = "JACCARD"
        self.float_vector_index = "IVF_FLAT"
        self.bf16_vector_index = "DISKANN"
        self.sparse_vector_index = "SPARSE_INVERTED_INDEX"
        self.binary_vector_index = "BIN_IVF_FLAT"
        self.index_types = [self.float_vector_index, self.bf16_vector_index,
                            self.sparse_vector_index, self.binary_vector_index]
        self.inverted_string_field = inverted_string_field_name
        self.indexed_json_field = indexed_json_field_name
        self.enable_dynamic_field = True
        self.dyna_filed_name1 = dyna_filed_name1
        self.dyna_filed_name2 = dyna_filed_name2

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection before test class runs
        """
        # Get client connection
        client = self._client()

        # Create collection
        fields = []
        fields.append(FieldSchema(name=self.primary_field, dtype=DataType.INT64, is_primary=True, auto_id=True))
        fields.append(
            FieldSchema(name=self.float_vector_field_name, dtype=DataType.FLOAT_VECTOR, dim=self.float_vector_dim))

        for data_type in ct.all_scalar_data_types:
            fields.append(cf.gen_scalar_field(data_type, name=data_type.name, nullable=True))

        collection_schema = CollectionSchema(fields, enable_dynamic_field=self.enable_dynamic_field)
        collection_schema.add_field(self.bfloat16_vector_field_name, DataType.BFLOAT16_VECTOR, dim=self.bf16_vector_dim)
        collection_schema.add_field(self.sparse_vector_field_name, DataType.SPARSE_FLOAT_VECTOR)
        collection_schema.add_field(self.binary_vector_field_name, DataType.BINARY_VECTOR, dim=self.binary_vector_dim)

        collection_schema.add_field(self.inverted_string_field, DataType.VARCHAR, max_length=256, nullable=True)
        collection_schema.add_field(self.indexed_json_field, DataType.JSON, nullable=True)
        self.create_collection(client, self.collection_name, schema=collection_schema, force_teardown=False)
        for partition_name in self.partition_names:
            self.create_partition(client, self.collection_name, partition_name=partition_name)

        # Define number of insert iterations
        insert_times = 150
        nb = 100
        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            # Group rows by partition based on primary key mod 3
            default_rows = []
            partition1_rows = []
            partition2_rows = []

            geo_value = f"POINT({random.uniform(-180, 180)} {random.uniform(-90, 90)})"
            timestamptz_value = cf.gen_timestamptz_str()
            int8_value = random.randint(-128, 127)
            int16_value = random.randint(-32768, 32767)
            int32_value = random.randint(-2147483648, 2147483647)
            for i in range(nb):
                row = {
                    self.float_vector_field_name: cf.gen_vectors(1, dim=self.float_vector_dim,
                                                                 vector_data_type=DataType.FLOAT_VECTOR)[0],
                    self.bfloat16_vector_field_name: cf.gen_vectors(1, dim=self.bf16_vector_dim,
                                                                    vector_data_type=DataType.BFLOAT16_VECTOR)[0],
                    self.sparse_vector_field_name: cf.gen_sparse_vectors(1, empty_percentage=2)[0],
                    self.binary_vector_field_name: cf.gen_vectors(1, dim=self.binary_vector_dim,
                                                                  vector_data_type=DataType.BINARY_VECTOR)[0],
                    DataType.BOOL.name: bool(i % 2) if random.random() < 0.8 else None,
                    DataType.INT8.name: int8_value if random.random() < 0.8 else None,
                    DataType.INT16.name: int16_value if random.random() < 0.8 else None,
                    DataType.INT32.name: int32_value if random.random() < 0.8 else None,
                    DataType.INT64.name: i if random.random() < 0.8 else None,
                    DataType.FLOAT.name: i * 1.0 if random.random() < 0.8 else None,
                    DataType.DOUBLE.name: i * 1.0 if random.random() < 0.8 else None,
                    DataType.TIMESTAMPTZ.name: timestamptz_value if random.random() < 0.8 else None,
                    DataType.VARCHAR.name: f"varchar_{i}" if random.random() < 0.8 else None,
                    DataType.ARRAY.name: [i, i + 1] if random.random() < 0.8 else None,
                    DataType.JSON.name: {"number": i, "string": f"string_{i}"} if random.random() < 0.8 else None,
                    DataType.GEOMETRY.name: geo_value if random.random() < 0.8 else None,
                    self.inverted_string_field: f"inverted_string_{i}" if random.random() < 0.8 else None,
                    self.indexed_json_field: {"number": i, "string": f"string_{i}"} if random.random() < 0.8 else None,
                    self.dyna_filed_name1: f"dyna_value_{i}" if random.random() < 0.8 else None,
                    self.dyna_filed_name2: {"number": i, "string": f"string_{i}"} if random.random() < 0.8 else None,
                }

                # Distribute to partitions based on pk mod 3
                if i % 3 == 0:
                    default_rows.append(row)
                elif i % 3 == 1:
                    partition1_rows.append(row)
                else:
                    partition2_rows.append(row)

            # Insert into respective partitions
            if default_rows:
                self.insert(client, self.collection_name, data=default_rows)
            if partition1_rows:
                self.insert(client, self.collection_name, data=partition1_rows, partition_name=self.partition_names[0])
            if partition2_rows:
                self.insert(client, self.collection_name, data=partition2_rows, partition_name=self.partition_names[1])

        self.flush(client, self.collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.float_vector_field_name,
                               metric_type=self.float_vector_metric,
                               index_type=self.float_vector_index,
                               params={"nlist": 128})
        index_params.add_index(field_name=self.bfloat16_vector_field_name,
                               metric_type=self.bf16_vector_metric,
                               index_type=self.bf16_vector_index,
                               params={})
        index_params.add_index(field_name=self.sparse_vector_field_name,
                               metric_type=self.sparse_vector_metric,
                               index_type=self.sparse_vector_index,
                               params={})
        index_params.add_index(field_name=self.binary_vector_field_name,
                               metric_type=self.binary_vector_metric,
                               index_type=self.binary_vector_index,
                               params={"nlist": 128})
        index_params.add_index(field_name=self.inverted_string_field, index_type="INVERTED")
        index_params.add_index(field_name=self.indexed_json_field, index_type="INVERTED", json_cast_type='json')
        self.create_index(client, self.collection_name, index_params=index_params)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.float_vector_field_name)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.bfloat16_vector_field_name)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("group_by_field", [DataType.VARCHAR.name, inverted_string_field_name
                                                # TODO: need to run after #46605 $46614 #46616 fixed
                                                # , DataType.JSON.name, indexed_json_field_name
                                                # , f"{DataType.JSON.name}['number']"
                                                # , dyna_filed_name1, f"{dyna_filed_name2}['string']"
                                                ])
    def test_search_group_size(self, group_by_field):
        """
        target:
            1. search on 4 different float vector fields with group by varchar field with group size
        verify results entity = limit * group_size  and group size is full if strict_group_size is True
        verify results group counts = limit if strict_group_size is False
        """
        nq = 2
        limit = 50
        group_size = 5
        # we can group by json key instead of a real field, so output a real field name
        output_field = group_by_field
        if "number" in group_by_field:
            output_field = DataType.JSON.name
        if "string" in group_by_field:
            output_field = dyna_filed_name2
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        for j in range(len(self.vector_fields)):
            if self.vector_fields[j] == self.binary_vector_field_name:
                pass
            else:
                search_vectors = cf.gen_vectors(nq, dim=self.dims[j],
                                                vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                                  self.vector_fields[
                                                                                                      j]))
                search_params = {"params": cf.get_search_params_params(self.index_types[j])}
                # when strict_group_size=true, it shall return results with entities = limit * group_size
                res1 = self.search(client, self.collection_name, data=search_vectors, anns_field=self.vector_fields[j],
                                   search_params=search_params, limit=limit,
                                   group_by_field=group_by_field, filter=f"{output_field} is not null",
                                   group_size=group_size, strict_group_size=True,
                                   output_fields=[output_field])[0]
                for i in range(nq):
                    assert len(res1[i]) == limit * group_size
                    for l in range(limit):
                        group_values = []
                        for k in range(group_size):
                            group_values.append(res1[i][l * group_size + k].fields.get(output_field))
                        if group_values and isinstance(group_values[0], dict):
                            group_values = [json.dumps(value) for value in group_values]
                            assert len(set(group_values)) == 1
                        elif group_values:
                            assert len(set(group_values)) == 1

                # when strict_group_size=false, it shall return results with group counts = limit
                res1 = self.search(client, self.collection_name,
                                   data=search_vectors,
                                   anns_field=self.vector_fields[j],
                                   search_params=search_params, limit=limit,
                                   group_by_field=group_by_field, filter=f"{output_field} is not null",
                                   group_size=group_size, strict_group_size=False,
                                   output_fields=[output_field])[0]
                for i in range(nq):
                    group_values = []
                    for l in range(len(res1[i])):
                        group_values.append(res1[i][l].fields.get(output_field))
                    if group_values and isinstance(group_values[0], dict):
                        group_values = [json.dumps(value) for value in group_values]
                        assert len(set(group_values)) == limit
                    elif group_values:
                        assert len(set(group_values)) == limit

    @pytest.mark.tags(CaseLabel.L0)
    def test_hybrid_search_group_size(self):
        """
        hybrid search group by on 4 different float vector fields with group by varchar field with group size
        verify results returns with de-dup group values and group distances are in order as rank_group_scorer
        """
        nq = 2
        limit = 50
        group_size = 5
        req_list = []
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        for j in range(len(self.vector_fields)):
            if self.vector_fields[j] == self.binary_vector_field_name:
                pass  # not support group by search on binary vector
            else:
                search_params = {
                    "data": cf.gen_vectors(nq, dim=self.dims[j],
                                           vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                             self.vector_fields[j])),
                    "anns_field": self.vector_fields[j],
                    "param": {"params": cf.get_search_params_params(self.index_types[j])},
                    "limit": limit,
                    "expr": f"{self.primary_field} > 0"}
                req = AnnSearchRequest(**search_params)
                req_list.append(req)
        # 4. hybrid search group by
        rank_scorers = ["max", "avg", "sum"]
        for scorer in rank_scorers:
            res = self.hybrid_search(client, self.collection_name, reqs=req_list, ranker=WeightedRanker(0.1, 0.3, 0.6),
                                     limit=limit, group_by_field=DataType.VARCHAR.name, group_size=group_size,
                                     rank_group_scorer=scorer, output_fields=[DataType.VARCHAR.name])[0]
            for i in range(nq):
                group_values = []
                for l in range(len(res[i])):
                    group_values.append(res[i][l].get(DataType.VARCHAR.name))
                assert len(set(group_values)) == limit

                # group_distances = []
                tmp_distances = [100 for _ in range(group_size)]  # init with a large value
                group_distances = [res[i][0].distance]  # init with the first value
                for l in range(len(res[i]) - 1):
                    curr_group_value = res[i][l].get(DataType.VARCHAR.name)
                    next_group_value = res[i][l + 1].get(DataType.VARCHAR.name)
                    if curr_group_value == next_group_value:
                        group_distances.append(res[i][l + 1].distance)
                    else:
                        if scorer == 'sum':
                            assert np.sum(group_distances) <= np.sum(tmp_distances)
                        elif scorer == 'avg':
                            assert np.mean(group_distances) <= np.mean(tmp_distances)
                        else:  # default max
                            assert np.max(group_distances) <= np.max(tmp_distances)

                        tmp_distances = group_distances
                        group_distances = [res[i][l + 1].distance]

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_group_by(self):
        """
        verify hybrid search group by works with different Rankers
        """
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        # 3. prepare search params
        req_list = []
        for i in range(len(self.vector_fields)):
            if self.vector_fields[i] == self.binary_vector_field_name:
                pass  # not support group by search on binary vector
            else:
                search_param = {
                    "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i],
                                           vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                             self.vector_fields[i])),
                    "anns_field": self.vector_fields[i],
                    "param": {},
                    "limit": ct.default_limit,
                    "expr": f"{self.primary_field} > 0"}
                req = AnnSearchRequest(**search_param)
                req_list.append(req)
        # 4. hybrid search group by
        res = self.hybrid_search(client, self.collection_name, reqs=req_list, ranker=WeightedRanker(0.1, 0.9, 0.3),
                                 limit=ct.default_limit, group_by_field=DataType.VARCHAR.name,
                                 output_fields=[DataType.VARCHAR.name],
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": ct.default_nq, "limit": ct.default_limit})[0]
        for i in range(ct.default_nq):
            group_values = []
            for l in range(ct.default_limit):
                group_values.append(res[i][l].get(DataType.VARCHAR.name))
            assert len(group_values) == len(set(group_values))

        # 5. hybrid search with RRFRanker on one vector field with group by
        req_list = []
        for i in range(1, len(self.vector_fields)):
            if self.vector_fields[i] == self.binary_vector_field_name:
                pass  # not support group by search on binary vector
            else:
                search_param = {
                    "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i],
                                           vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                             self.vector_fields[i])),
                    "anns_field": self.vector_fields[i],
                    "param": {},
                    "limit": ct.default_limit,
                    "expr": f"{self.primary_field} > 0"}
                req = AnnSearchRequest(**search_param)
                req_list.append(req)
                self.hybrid_search(client, self.collection_name, reqs=req_list, ranker=RRFRanker(),
                                   limit=ct.default_limit, group_by_field=self.inverted_string_field,
                                   output_fields=[self.inverted_string_field],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": ct.default_nq, "limit": ct.default_limit})

    @pytest.mark.tags(CaseLabel.L2)
    def test_hybrid_search_group_by_empty_results(self):
        """
        verify hybrid search group by works if group by empty results
        """
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        # 3. prepare search params
        req_list = []
        for i in range(len(self.vector_fields)):
            if self.vector_fields[i] == self.binary_vector_field_name:
                pass  # not support group by search on binary vector
            else:
                search_param = {
                    "data": cf.gen_vectors(ct.default_nq, dim=self.dims[i],
                                           vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                             self.vector_fields[i])),
                    "anns_field": self.vector_fields[i],
                    "param": {},
                    "limit": ct.default_limit,
                    "expr": f"{self.primary_field} < 0"}  # make sure return empty results
                req = AnnSearchRequest(**search_param)
                req_list.append(req)
        # 4. hybrid search group by empty results
        self.hybrid_search(client, self.collection_name, reqs=req_list, ranker=WeightedRanker(0.1, 0.9, 0.3),
                           limit=ct.default_limit, group_by_field=DataType.VARCHAR.name,
                           output_fields=[DataType.VARCHAR.name],
                           check_task=CheckTasks.check_search_results,
                           check_items={"nq": ct.default_nq, "limit": 0})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_group_by_supported_binary_vector(self):
        """
        verify search group by works with supported binary vector
        """
        client = self._client()
        search_vectors = cf.gen_vectors(1, dim=self.binary_vector_dim,
                                        vector_data_type=DataType.BINARY_VECTOR)
        search_params = {}
        limit = 1
        error = {ct.err_code: 999,
                 ct.err_msg: "not support search_group_by operation based on binary vector"}
        self.search(client, self.collection_name, data=search_vectors, anns_field=self.binary_vector_field_name,
                    search_params=search_params, limit=limit, group_by_field=DataType.VARCHAR.name,
                    output_fields=[DataType.VARCHAR.name],
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("support_field", [DataType.INT8.name, DataType.INT64.name,
                                               DataType.BOOL.name, DataType.VARCHAR.name,
                                               DataType.TIMESTAMPTZ.name])
    def test_search_group_by_supported_scalars(self, support_field):
        """
        verify search group by works with supported scalar fields
        """
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        nq = 2
        limit = 15
        for j in range(len(self.vector_fields)):
            if self.vector_fields[j] == self.binary_vector_field_name:
                pass  # not support group by search on binary vector
            else:
                search_vectors = cf.gen_vectors(nq, dim=self.dims[j],
                                                vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                                  self.vector_fields[
                                                                                                      j]))
                search_params = {"params": cf.get_search_params_params(self.index_types[j])}
                res1 = self.search(client, self.collection_name, data=search_vectors, anns_field=self.vector_fields[j],
                                   search_params=search_params, limit=limit,
                                   filter=f"{support_field} is not null",
                                   group_by_field=support_field,
                                   output_fields=[support_field])[0]
                for i in range(nq):
                    grpby_values = []
                    dismatch = 0
                    results_num = 2 if support_field == DataType.BOOL.name else limit
                    for l in range(results_num):
                        top1 = res1[i][l]
                        top1_grpby_pk = top1.id
                        top1_grpby_value = top1.get(support_field)
                        filter_expr = f"{support_field}=={top1_grpby_value}"
                        if support_field == DataType.VARCHAR.name:
                            filter_expr = f"{support_field}=='{top1_grpby_value}'"
                        if support_field == DataType.TIMESTAMPTZ.name:
                            filter_expr = f"{support_field}== ISO '{top1_grpby_value}'"
                        grpby_values.append(top1_grpby_value)
                        res_tmp = self.search(client, self.collection_name, data=[search_vectors[i]],
                                              anns_field=self.vector_fields[j],
                                              search_params=search_params, limit=1, filter=filter_expr,
                                              output_fields=[support_field])[0]
                        top1_expr_pk = res_tmp[0][0].id
                        if top1_grpby_pk != top1_expr_pk:
                            dismatch += 1
                            log.info(
                                f"{support_field} on {self.vector_fields[j]} dismatch_item, top1_grpby_dis: {top1.distance}, top1_expr_dis: {res_tmp[0][0].distance}")
                    log.info(
                        f"{support_field} on {self.vector_fields[j]}  top1_dismatch_num: {dismatch}, results_num: {results_num}, dismatch_rate: {dismatch / results_num}")
                    baseline = 1 if support_field == DataType.BOOL.name else 0.2  # skip baseline check for boolean
                    assert results_num > 0, "results_num should be greater than 0"
                    assert dismatch / results_num <= baseline
                    # verify no dup values of the group_by_field in results
                    assert len(grpby_values) == len(set(grpby_values))

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("grpby_unsupported_field", [DataType.FLOAT.name, DataType.DOUBLE.name,
                                                         DataType.GEOMETRY.name,
                                                         ct.default_float_vec_field_name])
    def test_search_group_by_unsupported_field(self, grpby_unsupported_field):
        """
        target: test search group by with the unsupported field
        method: 1. create a collection with data
                2. create index
                3. search with group by the unsupported fields
                verify: the error code and msg
        """
        client = self._client()
        search_vectors = cf.gen_vectors(1, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        search_params = {}
        limit = 1
        error = {ct.err_code: 999,
                 ct.err_msg: f"unsupported data type {grpby_unsupported_field} for group by operator"}
        if grpby_unsupported_field == ct.default_float_vec_field_name:
            error = {ct.err_code: 999,
                     ct.err_msg: f"unsupported data type VECTOR_FLOAT for group by operator"}
        self.search(client, self.collection_name, data=search_vectors, anns_field=self.float_vector_field_name,
                    search_params=search_params, limit=limit, group_by_field=grpby_unsupported_field,
                    output_fields=[grpby_unsupported_field],
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_pagination_group_by(self):
        """
        verify search group by works with pagination
        """
        limit = 10
        page_rounds = 3
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        search_param = {}
        default_search_exp = f"{self.primary_field} >= 0"
        grpby_field = self.inverted_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1],
                                        vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                          self.vector_fields[1]))
        all_pages_ids = []
        all_pages_grpby_field_values = []
        for r in range(page_rounds):
            page_res = self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                                   search_params=search_param, limit=limit, offset=limit * r,
                                   filter=default_search_exp, group_by_field=grpby_field,
                                   output_fields=[grpby_field],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1, "limit": limit},
                                   )[0]
            for j in range(limit):
                all_pages_grpby_field_values.append(page_res[0][j].get(grpby_field))
            all_pages_ids += page_res[0].ids
        hit_rate = round(len(set(all_pages_grpby_field_values)) / len(all_pages_grpby_field_values), 3)
        assert hit_rate >= 0.8

        total_res = self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                                search_params=search_param, limit=limit * page_rounds,
                                filter=default_search_exp, group_by_field=grpby_field,
                                output_fields=[grpby_field],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 1, "limit": limit * page_rounds}
                                )[0]
        hit_num = len(set(total_res[0].ids).intersection(set(all_pages_ids)))
        hit_rate = round(hit_num / (limit * page_rounds), 3)
        assert hit_rate >= 0.8
        log.info(f"search pagination with groupby hit_rate: {hit_rate}")
        grpby_field_values = []
        for i in range(limit * page_rounds):
            grpby_field_values.append(total_res[0][i].get(grpby_field))
        assert len(grpby_field_values) == len(set(grpby_field_values))

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("search_by_pk", [True, False])
    def test_search_pagination_group_size(self, search_by_pk):
        """
        verify search group by works with pagination and group_size
        """
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        limit = 10
        group_size = 5
        page_rounds = 3
        search_param = {}
        default_search_exp = f"{self.primary_field} >= 0"
        grpby_field = self.inverted_string_field
        default_search_field = self.vector_fields[1]
        ids_to_search = None
        search_vectors = cf.gen_vectors(1, dim=self.dims[1],
                                        vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                          self.vector_fields[1]))
        if search_by_pk is True:
            query_res = self.query(client, self.collection_name, limit=1, output_fields=[self.primary_field])[0]
            ids_to_search = [query_res[0].get(self.primary_field)]
            search_vectors = None
        all_pages_ids = []
        all_pages_grpby_field_values = []
        res_count = limit * group_size
        for r in range(page_rounds):
            page_res = self.search(client, self.collection_name,
                                   data=search_vectors,
                                   ids=ids_to_search,
                                   anns_field=default_search_field,
                                   search_params=search_param, limit=limit, offset=limit * r,
                                   filter=default_search_exp,
                                   group_by_field=grpby_field, group_size=group_size,
                                   strict_group_size=True,
                                   output_fields=[grpby_field],
                                   check_task=CheckTasks.check_search_results,
                                   check_items={"nq": 1, "limit": res_count},
                                   )[0]
            for j in range(res_count):
                all_pages_grpby_field_values.append(page_res[0][j].get(grpby_field))
            all_pages_ids += page_res[0].ids

        hit_rate = round(len(set(all_pages_grpby_field_values)) / len(all_pages_grpby_field_values), 3)
        expect_hit_rate = round(1 / group_size, 3) * 0.7
        log.info(f"expect_hit_rate :{expect_hit_rate}, hit_rate:{hit_rate}, "
                 f"unique_group_by_value_count:{len(set(all_pages_grpby_field_values))},"
                 f"total_group_by_value_count:{len(all_pages_grpby_field_values)}")
        assert hit_rate >= expect_hit_rate

        total_count = limit * group_size * page_rounds
        total_res = self.search(client, self.collection_name,
                                data=search_vectors,
                                ids=ids_to_search,
                                anns_field=default_search_field,
                                search_params=search_param, limit=limit * page_rounds,
                                filter=default_search_exp,
                                group_by_field=grpby_field, group_size=group_size,
                                strict_group_size=True,
                                output_fields=[grpby_field],
                                check_task=CheckTasks.check_search_results,
                                check_items={"nq": 1, "limit": total_count}
                                )[0]
        hit_num = len(set(total_res[0].ids).intersection(set(all_pages_ids)))
        hit_rate = round(hit_num / (limit * page_rounds), 3)
        assert hit_rate >= 0.8
        log.info(f"search pagination with groupby hit_rate: {hit_rate}")
        grpby_field_values = []
        for i in range(total_count):
            grpby_field_values.append(total_res[0][i].get(grpby_field))
        assert len(grpby_field_values) == total_count
        assert len(set(grpby_field_values)) == limit * page_rounds

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_group_size_min_max(self):
        """
        verify search group by works with min and max group size
        """
        client = self._client()
        collection_info = self.describe_collection(client, self.collection_name)[0]
        group_by_field = self.inverted_string_field
        default_search_field = self.vector_fields[1]
        search_vectors = cf.gen_vectors(1, dim=self.dims[1],
                                        vector_data_type=cf.get_field_dtype_by_field_name(collection_info,
                                                                                          self.vector_fields[1]))
        search_params = {}
        limit = 10
        max_group_size = 10
        self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                    search_params=search_params, limit=limit,
                    group_by_field=group_by_field,
                    group_size=max_group_size, strict_group_size=True,
                    output_fields=[group_by_field])
        exceed_max_group_size = max_group_size + 1
        error = {ct.err_code: 999,
                 ct.err_msg: f"input group size:{exceed_max_group_size} exceeds configured max "
                             f"group size:{max_group_size}"}
        self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                    search_params=search_params, limit=limit,
                    group_by_field=group_by_field,
                    group_size=exceed_max_group_size, strict_group_size=True,
                    output_fields=[group_by_field],
                    check_task=CheckTasks.err_res, check_items=error)

        min_group_size = 1
        self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                    search_params=search_params, limit=limit,
                    group_by_field=group_by_field,
                    group_size=max_group_size, strict_group_size=True,
                    output_fields=[group_by_field])
        below_min_group_size = min_group_size - 1
        error = {ct.err_code: 999,
                 ct.err_msg: f"input group size:{below_min_group_size} is negative"}
        self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                    search_params=search_params, limit=limit,
                    group_by_field=group_by_field,
                    group_size=below_min_group_size, strict_group_size=True,
                    output_fields=[group_by_field],
                    check_task=CheckTasks.err_res, check_items=error)

        below_min_group_size = min_group_size - 10
        error = {ct.err_code: 999,
                 ct.err_msg: f"input group size:{below_min_group_size} is negative"}
        self.search(client, self.collection_name, data=search_vectors, anns_field=default_search_field,
                    search_params=search_params, limit=limit,
                    group_by_field=group_by_field,
                    group_size=below_min_group_size, strict_group_size=True,
                    output_fields=[group_by_field],
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_not_support_group_by(self):
        """
        target: test search iterator does not support group by
        method: 1. create a collection with data
                2. create index HNSW
                3. search iterator with group by
                4. search with filtering every value of group_by_field
                verify: error code and msg
        """
        client = self._client()
        search_vectors = cf.gen_vectors(1, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        search_params = {}
        grpby_field = DataType.VARCHAR.name
        error = {ct.err_code: 1100,
                 ct.err_msg: "Not allowed to do groupBy when doing iteration"}
        self.search_iterator(client, self.collection_name, data=search_vectors,
                             anns_field=self.float_vector_field_name,
                             search_params=search_params, batch_size=10, limit=100,
                             group_by_field=grpby_field,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_not_support_group_by(self):
        """
        target: test range search does not support group by
        method: 1. create a collection with data
                2. create index hnsw
                3. range search with group by
                verify: the error code and msg
        """
        client = self._client()
        search_vectors = cf.gen_vectors(1, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        grpby_field = DataType.VARCHAR.name
        error = {ct.err_code: 1100,
                 ct.err_msg: "Not allowed to do range-search when doing search-group-by"}
        range_search_params = {"metric_type": "COSINE", "params": {"radius": 0.1, "range_filter": 0.5}}
        self.search(client, self.collection_name, data=search_vectors,
                    anns_field=self.float_vector_field_name,
                    search_params=range_search_params, limit=5,
                    group_by_field=grpby_field,
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("grpby_nonexist_field", ["nonexist_field", 21])
    def test_search_group_by_non_exit_field_on_dynamic_enabled_collection(self, grpby_nonexist_field):
        """
        target: test search group by with the non existing field against dynamic field enabled collection
        method: 1. create a collection with dynamic field enabled
                2. create index
                3. search with group by the unsupported fields
                verify: success
        """
        client = self._client()
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        search_params = {}
        limit = 100
        self.search(client, self.collection_name, data=search_vectors,
                    anns_field=self.float_vector_field_name,
                    search_params=search_params, limit=limit,
                    group_by_field=grpby_nonexist_field,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq, "limit": limit})


@pytest.mark.xdist_group("TestGroupSearchInvalid")
class TestGroupSearchInvalid(TestMilvusClientV2Base):
    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestGroupSearchInvalid" + cf.gen_unique_str("_")
        self.primary_field = "int64_pk"
        self.float_vector_field_name = ct.default_float_vec_field_name
        self.int8_vector_field_name = "int8_vector"
        self.vector_fields = [self.float_vector_field_name, self.int8_vector_field_name]
        self.float_vector_dim = 128
        self.int8_vector_dim = 64
        self.dims = [self.float_vector_dim, self.int8_vector_dim]
        self.float_vector_metric = "L2"
        self.int8_vector_metric = "COSINE"
        self.float_vector_index = "FLAT"
        self.int8_vector_index = "HNSW"
        self.index_types = [self.float_vector_index, self.int8_vector_index]

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection before test class runs
        """
        # Get client connection
        client = self._client()

        # Create collection
        fields = []
        fields.append(FieldSchema(name=self.primary_field, dtype=DataType.INT64, is_primary=True, auto_id=True))
        fields.append(
            FieldSchema(name=self.float_vector_field_name, dtype=DataType.FLOAT_VECTOR, dim=self.float_vector_dim))

        for data_type in ct.all_scalar_data_types:
            fields.append(cf.gen_scalar_field(data_type, name=data_type.name, nullable=True))

        collection_schema = CollectionSchema(fields, enable_dynamic_field=False)
        collection_schema.add_field(self.int8_vector_field_name, DataType.INT8_VECTOR, dim=self.int8_vector_dim)
        self.create_collection(client, self.collection_name, schema=collection_schema, force_teardown=False)

        # Define number of insert iterations
        insert_times = 2
        nb = 1000
        # Insert data multiple times with non-duplicated primary keys
        for j in range(insert_times):
            rows = cf.gen_row_data_by_schema(nb, schema=collection_schema)
            # Insert into collection
            self.insert(client, self.collection_name, data=rows)

        self.flush(client, self.collection_name)

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.float_vector_field_name,
                               metric_type=self.float_vector_metric,
                               index_type=self.float_vector_index,
                               params={"nlist": 128})
        index_params.add_index(field_name=self.int8_vector_field_name,
                               metric_type=self.int8_vector_metric,
                               index_type=self.int8_vector_index,
                               params={"nlist": 128})
        self.create_index(client, self.collection_name, index_params=index_params)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.float_vector_field_name)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.int8_vector_field_name)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_group_by_multi_fields(self):
        """
        target: test search group by with the multi fields
        method: 1. create a collection with data
                2. create index
                3. search with group by the multi fields
                verify: the error code and msg
        """
        client = self._client()
        search_params = {}
        search_vectors = cf.gen_vectors(1, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        # verify
        error = {ct.err_code: 1700,
                 ct.err_msg: f"groupBy field not found in schema"}
        self.search(client, self.collection_name, data=search_vectors,
                    anns_field=self.float_vector_field_name,
                    search_params=search_params, limit=10,
                    group_by_field=[DataType.VARCHAR.name, DataType.INT32.name],
                    output_fields=[DataType.VARCHAR.name, DataType.INT32.name],
                    check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("grpby_nonexist_field", ["nonexist_field", 21])
    def test_search_group_by_non_exit_field(self, grpby_nonexist_field):
        """
        target: test search group by with the nonexisting field
        method: 1. create a collection with data
                2. create index
                3. search with group by the unsupported fields
                verify: the error code and msg
        """
        client = self._client()
        search_vectors = cf.gen_vectors(1, dim=self.float_vector_dim,
                                        vector_data_type=DataType.FLOAT_VECTOR)
        search_params = {}
        limit = 1
        error = {ct.err_code: 1700,
                 ct.err_msg: f"groupBy field not found in schema: field not found[field={grpby_nonexist_field}]"}
        self.search(client, self.collection_name, data=search_vectors,
                    anns_field=self.float_vector_field_name,
                    search_params=search_params, limit=limit,
                    group_by_field=grpby_nonexist_field,
                    check_task=CheckTasks.err_res, check_items=error)


class TestSearchGroupByIndependent(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_search_group_by_flat_index_correctness(self, metric):
        """
        target: test search group by with FLAT index returns correct results
        method: 1. create a collection with FLAT index
                2. insert data with group_by field having multiple values per group
                3. search with and without group_by
                4. verify group_by search returns the best result (same as normal search top-1)
        expected: The top result from group_by search should match the top result from normal search
                  for the same group
        issue: https://github.com/milvus-io/milvus/issues/46349
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field(field_name=ct.default_primary_field_name, datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name=ct.default_float_vec_field_name, datatype=DataType.FLOAT_VECTOR, dim=ct.default_dim)
        schema.add_field(field_name=ct.default_int32_field_name, datatype=DataType.INT32)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=ct.default_float_vec_field_name, index_type="FLAT", metric_type=metric)
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        self.load_collection(client, collection_name)

        for _ in range(10):
            rows = []
            for i in range(ct.default_nb):
                row = {
                    ct.default_primary_field_name: i,
                    ct.default_float_vec_field_name: cf.gen_vectors(1, dim=ct.default_dim)[0],
                    ct.default_int32_field_name: i,
                }
                rows.append(row)
            self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        nq = 1
        limit = 1
        search_vectors = cf.gen_vectors(nq, dim=ct.default_dim)
        grpby_field = ct.default_int32_field_name

        search_params = {}

        # normal search to get the best result
        normal_res = self.search(client, collection_name, data=search_vectors,
                                 search_params=search_params, limit=limit,
                                 output_fields=[grpby_field],
                                 check_task=CheckTasks.check_search_results,
                                 check_items={"nq": nq, "limit": limit})[0]
        # group_by search
        groupby_res = self.search(client, collection_name, data=search_vectors,
                                  search_params=search_params, limit=limit,
                                  group_by_field=grpby_field,
                                  output_fields=[grpby_field],
                                  check_task=CheckTasks.check_search_results,
                                  check_items={"nq": nq, "limit": limit})[0]
        # verify that the top result from group_by search matches the normal search
        # for the same group value, group_by should return the best (closest) result
        normal_top_distance = normal_res[0][0].distance
        normal_top_group = normal_res[0][0].entity.get(grpby_field)
        groupby_top_distance = groupby_res[0][0].distance
        groupby_top_group = groupby_res[0][0].entity.get(grpby_field)

        log.info(f"Normal search top result: distance={normal_top_distance}, group={normal_top_group}")
        log.info(f"GroupBy search top result: distance={groupby_top_distance}, group={groupby_top_group}")

        # The group_by result should have the same or better distance as normal search
        # because group_by returns the best result per group
        if metric == "L2":
            # For L2, smaller is better
            assert groupby_top_distance <= normal_top_distance + epsilon, \
                f"GroupBy search should return result with distance <= normal search for L2 metric"
        else:
            # For IP/COSINE, larger is better
            assert groupby_top_distance >= normal_top_distance - epsilon, \
                f"GroupBy search should return result with distance >= normal search for {metric} metric"
