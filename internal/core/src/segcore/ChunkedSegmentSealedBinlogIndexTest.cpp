// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <assert.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/config.h"
#include "knowhere/dataset.h"
#include "knowhere/operands.h"
#include "knowhere/sparse_utils.h"
#include "knowhere/version.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/TimestampIndex.h"
#include "segcore/Types.h"
#include "segcore/segcore_init_c.h"
#include "storage/FileManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

std::unique_ptr<float[]>
GenRandomFloatVecData(int rows, int dim, int seed = 42) {
    auto vecs = std::make_unique<float[]>(rows * dim);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<> distrib(0.0, 100.0);
    for (int i = 0; i < rows * dim; ++i) vecs[i] = (float)distrib(rng);
    return vecs;
}

std::unique_ptr<milvus::float16[]>
GenRandomFloat16VecData(int rows, int dim, int seed = 42) {
    auto vecs = std::make_unique<milvus::float16[]>(rows * dim);
    std::mt19937 rng(seed);
    std::normal_distribution<> distrib(0.0, 1.0);
    for (int i = 0; i < rows * dim; ++i)
        vecs[i] = milvus::float16(distrib(rng));
    return vecs;
}

std::unique_ptr<milvus::bfloat16[]>
GenRandomBFloat16VecData(int rows, int dim, int seed = 42) {
    auto vecs = std::make_unique<milvus::bfloat16[]>(rows * dim);
    std::mt19937 rng(seed);
    std::normal_distribution<> distrib(0.0, 1.0);
    for (int i = 0; i < rows * dim; ++i)
        vecs[i] = milvus::bfloat16(distrib(rng));
    return vecs;
}

std::unique_ptr<uint8_t[]>
GenRandomBinaryVecData(int rows, int dim, int seed = 42) {
    assert(dim % 8 == 0);
    auto byte_dim = dim / 8;
    auto vecs = std::make_unique<uint8_t[]>(rows * byte_dim);
    std::mt19937 rng(seed);
    for (int i = 0; i < rows * byte_dim; ++i)
        vecs[i] = static_cast<uint8_t>(rng());
    return vecs;
}

std::unique_ptr<int8_t[]>
GenRandomInt8VecData(int rows, int dim, int seed = 42) {
    auto vecs = std::make_unique<int8_t[]>(rows * dim);
    std::mt19937 rng(seed);
    for (int i = 0; i < rows * dim; ++i)
        vecs[i] = static_cast<int8_t>(rng() % 256 - 128);
    return vecs;
}

milvus::proto::plan::VectorType
DataTypeToVectorType(DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            return milvus::proto::plan::VectorType::FloatVector;
        case DataType::VECTOR_FLOAT16:
            return milvus::proto::plan::VectorType::Float16Vector;
        case DataType::VECTOR_BFLOAT16:
            return milvus::proto::plan::VectorType::BFloat16Vector;
        case DataType::VECTOR_BINARY:
            return milvus::proto::plan::VectorType::BinaryVector;
        case DataType::VECTOR_INT8:
            return milvus::proto::plan::VectorType::Int8Vector;
        case DataType::VECTOR_SPARSE_U32_F32:
            return milvus::proto::plan::VectorType::SparseFloatVector;
        default:
            throw std::runtime_error("unsupported vector type");
    }
}

milvus::proto::common::PlaceholderGroup
CreatePlaceholderGroupForVectorType(DataType data_type,
                                    int64_t num_queries,
                                    int64_t dim,
                                    const void* data) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");

    switch (data_type) {
        case DataType::VECTOR_FLOAT: {
            value->set_type(ser::PlaceholderType::FloatVector);
            auto ptr = static_cast<const float*>(data);
            for (int i = 0; i < num_queries; ++i) {
                value->add_values(ptr + i * dim, dim * sizeof(float));
            }
            break;
        }
        case DataType::VECTOR_FLOAT16: {
            value->set_type(ser::PlaceholderType::Float16Vector);
            auto ptr = static_cast<const milvus::float16*>(data);
            for (int i = 0; i < num_queries; ++i) {
                value->add_values(ptr + i * dim, dim * sizeof(milvus::float16));
            }
            break;
        }
        case DataType::VECTOR_BFLOAT16: {
            value->set_type(ser::PlaceholderType::BFloat16Vector);
            auto ptr = static_cast<const milvus::bfloat16*>(data);
            for (int i = 0; i < num_queries; ++i) {
                value->add_values(ptr + i * dim,
                                  dim * sizeof(milvus::bfloat16));
            }
            break;
        }
        case DataType::VECTOR_BINARY: {
            value->set_type(ser::PlaceholderType::BinaryVector);
            auto byte_dim = dim / 8;
            auto ptr = static_cast<const uint8_t*>(data);
            for (int i = 0; i < num_queries; ++i) {
                value->add_values(ptr + i * byte_dim, byte_dim);
            }
            break;
        }
        case DataType::VECTOR_INT8: {
            value->set_type(ser::PlaceholderType::Int8Vector);
            auto ptr = static_cast<const int8_t*>(data);
            for (int i = 0; i < num_queries; ++i) {
                value->add_values(ptr + i * dim, dim * sizeof(int8_t));
            }
            break;
        }
        case DataType::VECTOR_SPARSE_U32_F32: {
            value->set_type(ser::PlaceholderType::SparseFloatVector);
            auto ptr = static_cast<
                const knowhere::sparse::SparseRow<milvus::SparseValueType>*>(
                data);
            for (int i = 0; i < num_queries; ++i) {
                value->add_values(ptr[i].data(), ptr[i].data_byte_size());
            }
            break;
        }
        default:
            throw std::runtime_error("unsupported vector type for placeholder");
    }
    return raw_group;
}

inline float
GetKnnSearchRecall(
    size_t nq, int64_t* gt_ids, size_t gt_k, int64_t* res_ids, size_t res_k) {
    uint32_t matched_num = 0;
    for (auto i = 0; i < nq; ++i) {
        std::vector<int64_t> ids_0(gt_ids + i * gt_k,
                                   gt_ids + i * gt_k + res_k);
        std::vector<int64_t> ids_1(res_ids + i * res_k,
                                   res_ids + i * res_k + res_k);

        std::sort(ids_0.begin(), ids_0.end());
        std::sort(ids_1.begin(), ids_1.end());

        std::vector<int64_t> v(std::max(ids_0.size(), ids_1.size()));
        std::vector<int64_t>::iterator it;
        it = std::set_intersection(
            ids_0.begin(), ids_0.end(), ids_1.begin(), ids_1.end(), v.begin());
        v.resize(it - v.begin());
        matched_num += v.size();
    }
    return ((float)matched_num) / ((float)nq * res_k);
}

using Param =
    std::tuple<DataType,
               knowhere::MetricType,
               /* IndexType */ std::string,
               /* DenseVectorInterminIndexType*/ std::optional<std::string>,
               /* Nullable */ bool,
               /* NullPercent */ int>;
class BinlogIndexTest : public ::testing::TestWithParam<Param> {
    void
    SetUp() override {
        std::tie(data_type,
                 metric_type,
                 index_type,
                 dense_vec_intermin_index_type,
                 nullable,
                 null_percent) = GetParam();

        schema = std::make_shared<Schema>();

        valid_count = 0;
        vec_field_id = schema->AddDebugField(
            "fakevec", data_type, data_d, metric_type, nullable);
        auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
        schema->set_primary_field_id(i64_fid);
        vec_field_data = storage::CreateFieldData(
            data_type, DataType::NONE, nullable, data_d);

        if (nullable) {
            valid_data.resize((data_n + 7) / 8, 0);
            for (int i = 0; i < data_n; ++i) {
                bool is_valid = (i % 100) >= null_percent;
                if (is_valid) {
                    valid_data[i >> 3] |= (1 << (i & 0x07));
                    valid_count++;
                    row_ids.push_back(i);
                }
            }
        } else {
            valid_count = data_n;
        }

        if (data_type == DataType::VECTOR_FLOAT) {
            auto vec_data = GenRandomFloatVecData(valid_count, data_d);
            if (nullable) {
                auto vec_field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::FloatVector>>(vec_field_data);
                vec_field_data_impl->FillFieldData(
                    vec_data.get(), valid_data.data(), data_n, 0);
            } else {
                vec_field_data->FillFieldData(vec_data.get(), data_n);
            }

            raw_dataset =
                knowhere::GenDataSet(valid_count, data_d, vec_data.get());
            raw_dataset->SetIsOwner(false);
            raw_float_data = std::move(vec_data);
            if (dense_vec_intermin_index_type.has_value() &&
                dense_vec_intermin_index_type.value() ==
                    knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
                intermin_index_has_raw_data = false;
            } else {
                intermin_index_has_raw_data = true;
            }
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            auto vec_data = GenRandomFloat16VecData(valid_count, data_d);
            if (nullable) {
                auto vec_field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::Float16Vector>>(vec_field_data);
                vec_field_data_impl->FillFieldData(
                    vec_data.get(), valid_data.data(), data_n, 0);
            } else {
                vec_field_data->FillFieldData(vec_data.get(), data_n);
            }

            raw_dataset =
                knowhere::GenDataSet(valid_count, data_d, vec_data.get());
            raw_dataset->SetIsOwner(false);
            raw_float16_data = std::move(vec_data);
            intermin_index_has_raw_data = true;
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            auto vec_data = GenRandomBFloat16VecData(valid_count, data_d);
            if (nullable) {
                auto vec_field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::BFloat16Vector>>(vec_field_data);
                vec_field_data_impl->FillFieldData(
                    vec_data.get(), valid_data.data(), data_n, 0);
            } else {
                vec_field_data->FillFieldData(vec_data.get(), data_n);
            }

            raw_dataset =
                knowhere::GenDataSet(valid_count, data_d, vec_data.get());
            raw_dataset->SetIsOwner(false);
            raw_bfloat16_data = std::move(vec_data);
            intermin_index_has_raw_data = true;
        } else if (data_type == DataType::VECTOR_BINARY) {
            auto vec_data = GenRandomBinaryVecData(valid_count, data_d);
            if (nullable) {
                auto vec_field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::BinaryVector>>(vec_field_data);
                vec_field_data_impl->FillFieldData(
                    vec_data.get(), valid_data.data(), data_n, 0);
            } else {
                vec_field_data->FillFieldData(vec_data.get(), data_n);
            }

            raw_dataset =
                knowhere::GenDataSet(valid_count, data_d / 8, vec_data.get());
            raw_dataset->SetIsOwner(false);
            raw_binary_data = std::move(vec_data);
            intermin_index_has_raw_data = true;
        } else if (data_type == DataType::VECTOR_INT8) {
            auto vec_data = GenRandomInt8VecData(valid_count, data_d);
            if (nullable) {
                auto vec_field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::Int8Vector>>(vec_field_data);
                vec_field_data_impl->FillFieldData(
                    vec_data.get(), valid_data.data(), data_n, 0);
            } else {
                vec_field_data->FillFieldData(vec_data.get(), data_n);
            }

            raw_dataset =
                knowhere::GenDataSet(valid_count, data_d, vec_data.get());
            raw_dataset->SetIsOwner(false);
            raw_int8_data = std::move(vec_data);
            intermin_index_has_raw_data = true;
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            auto sparse_vecs = GenerateRandomSparseFloatVector(valid_count);
            if (nullable) {
                auto vec_field_data_impl = std::dynamic_pointer_cast<
                    milvus::FieldData<milvus::SparseFloatVector>>(
                    vec_field_data);
                vec_field_data_impl->FillFieldData(
                    sparse_vecs.get(), valid_data.data(), data_n, 0);
            } else {
                vec_field_data->FillFieldData(sparse_vecs.get(), data_n);
            }
            data_d = std::dynamic_pointer_cast<
                         milvus::FieldData<milvus::SparseFloatVector>>(
                         vec_field_data)
                         ->Dim();
            raw_dataset =
                knowhere::GenDataSet(valid_count, data_d, sparse_vecs.get());
            raw_dataset->SetIsOwner(false);
            raw_dataset->SetIsSparse(true);
            raw_sparse_data = std::move(sparse_vecs);
            intermin_index_has_raw_data = false;
        } else {
            throw std::runtime_error("not implemented");
        }
    }

 public:
    IndexMetaPtr
    GetCollectionIndexMeta(std::string index_type) {
        std::map<std::string, std::string> index_params = {
            {"index_type", index_type},
            {"metric_type", metric_type},
            {"nlist", "64"}};
        std::map<std::string, std::string> type_params = {{"dim", "4"}};
        FieldIndexMeta fieldIndexMeta(
            vec_field_id, std::move(index_params), std::move(type_params));
        auto& config = SegcoreConfig::default_config();
        config.set_chunk_rows(1024);
        config.set_enable_interim_segment_index(true);
        config.set_nlist(16);
        std::map<FieldId, FieldIndexMeta> filedMap = {
            {vec_field_id, fieldIndexMeta}};
        IndexMetaPtr metaPtr =
            std::make_shared<CollectionIndexMeta>(226985, std::move(filedMap));
        return metaPtr;
    }

    void
    LoadOtherFields() {
        auto dataset = DataGen(schema, data_n);
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareInsertBinlog(kCollectionID,
                                             kPartitionID,
                                             kSegmentID,
                                             dataset,
                                             cm,
                                             "",
                                             {vec_field_id.get()});
        segment->LoadFieldData(load_info);
    }

    void
    LoadVectorField(std::string mmap_dir_path = "") {
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                        kPartitionID,
                                                        kSegmentID,
                                                        vec_field_id.get(),
                                                        {vec_field_data},
                                                        cm,
                                                        mmap_dir_path);
        segment->LoadFieldData(load_info);
    }

    const void*
    GetQueryData(int num_queries) {
        // Generate random query vectors for search
        switch (data_type) {
            case DataType::VECTOR_FLOAT:
                query_float_data =
                    GenRandomFloatVecData(num_queries, data_d, 999);
                return query_float_data.get();
            case DataType::VECTOR_FLOAT16:
                query_float16_data =
                    GenRandomFloat16VecData(num_queries, data_d, 999);
                return query_float16_data.get();
            case DataType::VECTOR_BFLOAT16:
                query_bfloat16_data =
                    GenRandomBFloat16VecData(num_queries, data_d, 999);
                return query_bfloat16_data.get();
            case DataType::VECTOR_BINARY:
                query_binary_data =
                    GenRandomBinaryVecData(num_queries, data_d, 999);
                return query_binary_data.get();
            case DataType::VECTOR_INT8:
                query_int8_data =
                    GenRandomInt8VecData(num_queries, data_d, 999);
                return query_int8_data.get();
            case DataType::VECTOR_SPARSE_U32_F32:
                query_sparse_data = GenerateRandomSparseFloatVector(
                    num_queries, kTestSparseDim, kTestSparseVectorDensity, 999);
                return query_sparse_data.get();
            default:
                throw std::runtime_error("unsupported vector type");
        }
    }

    void
    VerifyQueryResults(const std::vector<int64_t>& seg_offsets) {
        if (seg_offsets.empty()) {
            return;
        }

        std::vector<int64_t> valid_offsets;
        for (auto offset : seg_offsets) {
            if (offset >= 0 && offset < static_cast<int64_t>(data_n)) {
                valid_offsets.push_back(offset);
            }
        }
        if (valid_offsets.empty()) {
            return;
        }

        std::sort(valid_offsets.begin(), valid_offsets.end());
        valid_offsets.erase(
            std::unique(valid_offsets.begin(), valid_offsets.end()),
            valid_offsets.end());

        auto i64_fid = schema->get_primary_field_id().value();

        std::vector<proto::plan::GenericValue> values;
        for (auto offset : valid_offsets) {
            proto::plan::GenericValue val;
            val.set_int64_val(offset);
            values.push_back(val);
        }

        auto term_expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                i64_fid, DataType::INT64, std::vector<std::string>()),
            values);

        auto plan = std::make_unique<query::RetrievePlan>(schema);
        plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
        plan->plan_node_->plannodes_ =
            milvus::test::CreateRetrievePlanByExpr(term_expr);
        std::vector<FieldId> target_fields{vec_field_id};
        plan->field_ids_ = target_fields;

        auto retrieve_results = segment->Retrieve(
            nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);

        ASSERT_TRUE(retrieve_results != nullptr);
        EXPECT_EQ(retrieve_results->fields_data_size(), 1);

        auto& field_data = retrieve_results->fields_data(0);
        EXPECT_TRUE(field_data.has_vectors());

        // Verify the number of returned vectors matches the number of valid offsets we queried
        size_t returned_count = 0;
        switch (data_type) {
            case DataType::VECTOR_FLOAT:
                returned_count =
                    field_data.vectors().float_vector().data_size() / data_d;
                break;
            case DataType::VECTOR_FLOAT16:
                returned_count = field_data.vectors().float16_vector().size() /
                                 (data_d * sizeof(milvus::float16));
                break;
            case DataType::VECTOR_BFLOAT16:
                returned_count = field_data.vectors().bfloat16_vector().size() /
                                 (data_d * sizeof(milvus::bfloat16));
                break;
            case DataType::VECTOR_BINARY:
                returned_count =
                    field_data.vectors().binary_vector().size() / (data_d / 8);
                break;
            case DataType::VECTOR_INT8:
                returned_count =
                    field_data.vectors().int8_vector().size() / data_d;
                break;
            case DataType::VECTOR_SPARSE_U32_F32:
                returned_count =
                    field_data.vectors().sparse_float_vector().contents_size();
                break;
            default:
                break;
        }

        if (!nullable) {
            EXPECT_EQ(returned_count, valid_offsets.size())
                << "Query returned " << returned_count << " vectors, expected "
                << valid_offsets.size();
        }

        EXPECT_GT(returned_count, 0)
            << "Query should return at least some vectors";
    }

 protected:
    milvus::SchemaPtr schema;
    knowhere::MetricType metric_type;
    DataType data_type;
    std::optional<std::string> dense_vec_intermin_index_type = std::nullopt;
    std::string index_type;
    bool nullable = false;
    int null_percent = 0;
    size_t data_n = 1000;
    size_t data_d = 8;
    size_t topk = 10;
    size_t valid_count = 0;
    milvus::FieldDataPtr vec_field_data = nullptr;
    milvus::segcore::SegmentSealedUPtr segment = nullptr;
    milvus::FieldId vec_field_id;
    knowhere::DataSetPtr raw_dataset;
    bool intermin_index_has_raw_data;
    std::vector<uint8_t> valid_data;
    std::vector<int64_t> row_ids;

    std::unique_ptr<float[]> raw_float_data;
    std::unique_ptr<milvus::float16[]> raw_float16_data;
    std::unique_ptr<milvus::bfloat16[]> raw_bfloat16_data;
    std::unique_ptr<uint8_t[]> raw_binary_data;
    std::unique_ptr<int8_t[]> raw_int8_data;
    std::unique_ptr<knowhere::sparse::SparseRow<milvus::SparseValueType>[]>
        raw_sparse_data;

    // Query data (generated randomly for each search)
    mutable std::unique_ptr<float[]> query_float_data;
    mutable std::unique_ptr<milvus::float16[]> query_float16_data;
    mutable std::unique_ptr<milvus::bfloat16[]> query_bfloat16_data;
    mutable std::unique_ptr<uint8_t[]> query_binary_data;
    mutable std::unique_ptr<int8_t[]> query_int8_data;
    mutable std::unique_ptr<
        knowhere::sparse::SparseRow<milvus::SparseValueType>[]>
        query_sparse_data;
};

static std::vector<Param>
GenerateTestParams() {
    std::vector<Param> params;

    std::vector<std::tuple<DataType,
                           knowhere::MetricType,
                           std::string,
                           std::optional<std::string>>>
        base_configs = {
            {DataType::VECTOR_FLOAT,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
             knowhere::IndexEnum::
                 INDEX_FAISS_IVFFLAT_CC},  // intermin index has data
            {DataType::VECTOR_FLOAT,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
             knowhere::IndexEnum::
                 INDEX_FAISS_SCANN_DVR},  // intermin index not has data
            {DataType::VECTOR_FLOAT16,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC},
            {DataType::VECTOR_BFLOAT16,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC},
            {DataType::VECTOR_BINARY,
             knowhere::metric::HAMMING,
             knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
             std::nullopt},
            {DataType::VECTOR_INT8,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC},
            {DataType::VECTOR_SPARSE_U32_F32,
             knowhere::metric::IP,
             knowhere::IndexEnum::
                 INDEX_SPARSE_INVERTED_INDEX,  //intermin index not has data
             std::nullopt},
            {DataType::VECTOR_SPARSE_U32_F32,
             knowhere::metric::IP,
             knowhere::IndexEnum::
                 INDEX_SPARSE_WAND,  // intermin index not has data
             std::nullopt},
        };

    std::vector<std::tuple<bool, int>> null_configs = {
        {false, 0},  // non-nullable with 0% null
        {true, 0},   // nullable with 0% null
        {true, 20},  // nullable with 20% null
        {true, 100}  // nullable with 100% null
    };

    for (const auto& [data_type, metric, index_type, interim_index] :
         base_configs) {
        for (const auto& [nullable, null_percent] : null_configs) {
            params.emplace_back(data_type,
                                metric,
                                index_type,
                                interim_index,
                                nullable,
                                null_percent);
        }
    }
    return params;
}

INSTANTIATE_TEST_SUITE_P(MetricTypeParameters,
                         BinlogIndexTest,
                         ::testing::ValuesIn(GenerateTestParams()));

TEST_P(BinlogIndexTest, AccuracyWithLoadFieldData) {
    IndexMetaPtr collection_index_meta = GetCollectionIndexMeta(index_type);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();

    auto& segcore_config = milvus::segcore::SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    if (dense_vec_intermin_index_type.has_value()) {
        segcore_config.set_dense_vector_intermin_index_type(
            dense_vec_intermin_index_type.value());
    }
    segcore_config.set_nprobe(16);
    // 1. load field data, and build binlog index for binlog data
    LoadVectorField();

    //assert segment has been built binlog index
    bool supports_interim_index =
        (data_type == DataType::VECTOR_FLOAT ||
         data_type == DataType::VECTOR_FLOAT16 ||
         data_type == DataType::VECTOR_BFLOAT16 ||
         data_type == DataType::VECTOR_SPARSE_U32_F32);
    int64_t valid_row_count = nullable ? valid_count : data_n;
    int64_t threshold = segcore_config.get_nlist() * 39;
    bool should_have_index =
        supports_interim_index && (valid_row_count >= threshold);
    if (should_have_index) {
        EXPECT_TRUE(segment->HasIndex(vec_field_id));
    } else {
        EXPECT_FALSE(segment->HasIndex(vec_field_id));
    }
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));

    // 2. search binlog index
    auto num_queries = 10;
    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(DataTypeToVectorType(data_type));
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vec_field_id.get());
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(topk);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(R"({"nprobe": 16})");
    auto plan_str = plan_node.SerializeAsString();

    auto ph_group_raw = CreatePlaceholderGroupForVectorType(
        data_type, num_queries, data_d, GetQueryData(num_queries));

    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_str.data(), plan_str.size());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    std::vector<const milvus::query::PlaceholderGroup*> ph_group_arr = {
        ph_group.get()};
    auto nlist = segcore_config.get_nlist();
    auto binlog_index_sr =
        segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    ASSERT_EQ(binlog_index_sr->total_nq_, num_queries);
    EXPECT_EQ(binlog_index_sr->unity_topK_, topk);
    EXPECT_EQ(binlog_index_sr->distances_.size(), num_queries * topk);
    EXPECT_EQ(binlog_index_sr->seg_offsets_.size(), num_queries * topk);

    for (int q = 0; q < num_queries; ++q) {
        for (size_t k = 0; k < topk; ++k) {
            int64_t seg_offset = binlog_index_sr->seg_offsets_[q * topk + k];
            if (seg_offset == -1) {
                continue;  // No result for this position
            }
            ASSERT_GE(seg_offset, 0);
            ASSERT_LT(seg_offset, static_cast<int64_t>(data_n));

            if (nullable) {
                bool is_valid =
                    (valid_data[seg_offset >> 3] >> (seg_offset & 0x07)) & 1;
                EXPECT_TRUE(is_valid)
                    << "Search returned invalid (null) row at seg_offset="
                    << seg_offset;
            }
        }
    }

    VerifyQueryResults(binlog_index_sr->seg_offsets_);

    {
        milvus::proto::plan::PlanNode filtered_plan_node;
        auto filtered_vector_anns = filtered_plan_node.mutable_vector_anns();
        filtered_vector_anns->set_vector_type(DataTypeToVectorType(data_type));
        filtered_vector_anns->set_placeholder_tag("$0");
        filtered_vector_anns->set_field_id(vec_field_id.get());
        auto filtered_query_info = filtered_vector_anns->mutable_query_info();
        filtered_query_info->set_topk(topk);
        filtered_query_info->set_round_decimal(3);
        filtered_query_info->set_metric_type(metric_type);
        filtered_query_info->set_search_params(R"({"nprobe": 16})");

        auto i64_fid = schema->get_primary_field_id().value();
        auto* predicate = filtered_vector_anns->mutable_predicates();
        auto* unary_range = predicate->mutable_unary_range_expr();
        auto* col_info = unary_range->mutable_column_info();
        col_info->set_field_id(i64_fid.get());
        col_info->set_data_type(milvus::proto::schema::DataType::Int64);
        unary_range->set_op(milvus::proto::plan::OpType::GreaterEqual);
        unary_range->mutable_value()->set_int64_val(data_n / 2);

        auto filtered_plan_str = filtered_plan_node.SerializeAsString();
        auto filtered_plan = milvus::query::CreateSearchPlanByExpr(
            schema, filtered_plan_str.data(), filtered_plan_str.size());
        auto filtered_ph_group = ParsePlaceholderGroup(
            filtered_plan.get(), ph_group_raw.SerializeAsString());

        auto filtered_sr = segment->Search(
            filtered_plan.get(), filtered_ph_group.get(), MAX_TIMESTAMP);

        ASSERT_EQ(filtered_sr->total_nq_, num_queries);
        EXPECT_EQ(filtered_sr->unity_topK_, topk);

        for (size_t i = 0; i < filtered_sr->seg_offsets_.size(); ++i) {
            int64_t seg_offset = filtered_sr->seg_offsets_[i];
            if (seg_offset != -1) {
                EXPECT_GE(seg_offset, data_n / 2)
                    << "Filtered search returned row " << seg_offset
                    << " which should have been filtered (pk < " << data_n / 2
                    << ")";

                if (nullable) {
                    bool is_valid =
                        (valid_data[seg_offset >> 3] >> (seg_offset & 0x07)) &
                        1;
                    EXPECT_TRUE(is_valid) << "Filtered search returned invalid "
                                             "(null) row at seg_offset="
                                          << seg_offset;
                }
            }
        }
    }

    if (null_percent != 100 && supports_interim_index) {
        {
            milvus::index::CreateIndexInfo create_index_info;
            create_index_info.field_type = data_type;
            create_index_info.metric_type = metric_type;
            create_index_info.index_type = index_type;
            create_index_info.index_engine_version =
                knowhere::Version::GetCurrentVersion().VersionNumber();
            auto indexing =
                milvus::index::IndexFactory::GetInstance().CreateIndex(
                    create_index_info, milvus::storage::FileManagerContext());

            auto build_conf =
                knowhere::Json{{knowhere::meta::METRIC_TYPE, metric_type},
                               {knowhere::meta::DIM, std::to_string(data_d)},
                               {knowhere::indexparam::NLIST, "64"}};

            indexing->BuildWithDataset(raw_dataset, build_conf);

            if (nullable) {
                auto vec_indexing =
                    dynamic_cast<milvus::index::VectorIndex*>(indexing.get());
                ASSERT_NE(vec_indexing, nullptr);
                std::unique_ptr<bool[]> valid_data_bool(new bool[data_n]);
                for (int64_t i = 0; i < data_n; ++i) {
                    valid_data_bool[i] = (valid_data[i >> 3] >> (i & 0x07)) & 1;
                }
                vec_indexing->UpdateValidData(valid_data_bool.get(), data_n);
            }

            LoadIndexInfo load_info;
            load_info.field_id = vec_field_id.get();
            load_info.index_params = GenIndexParams(indexing.get());
            load_info.cache_index =
                CreateTestCacheIndex("test", std::move(indexing));
            load_info.index_params["metric_type"] = metric_type;

            ASSERT_NO_THROW(segment->LoadIndex(load_info));

            EXPECT_TRUE(segment->HasIndex(vec_field_id));
            EXPECT_EQ(segment->get_row_count(), data_n);

            std::unique_ptr<SearchResult> ivf_sr;
            try {
                ivf_sr =
                    segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
            } catch (const std::exception& e) {
                throw;
            }

            ASSERT_EQ(ivf_sr->total_nq_, num_queries);
            EXPECT_EQ(ivf_sr->unity_topK_, topk);
            EXPECT_EQ(ivf_sr->distances_.size(), num_queries * topk);
            EXPECT_EQ(ivf_sr->seg_offsets_.size(), num_queries * topk);

            auto similary =
                GetKnnSearchRecall(num_queries,
                                   binlog_index_sr->seg_offsets_.data(),
                                   topk,
                                   ivf_sr->seg_offsets_.data(),
                                   topk);
            ASSERT_GT(similary, 0.45);

            VerifyQueryResults(ivf_sr->seg_offsets_);

            {
                milvus::proto::plan::PlanNode ivf_filtered_plan_node;
                auto ivf_filtered_anns =
                    ivf_filtered_plan_node.mutable_vector_anns();
                ivf_filtered_anns->set_vector_type(
                    DataTypeToVectorType(data_type));
                ivf_filtered_anns->set_placeholder_tag("$0");
                ivf_filtered_anns->set_field_id(vec_field_id.get());
                auto ivf_filtered_info =
                    ivf_filtered_anns->mutable_query_info();
                ivf_filtered_info->set_topk(topk);
                ivf_filtered_info->set_round_decimal(3);
                ivf_filtered_info->set_metric_type(metric_type);
                ivf_filtered_info->set_search_params(R"({"nprobe": 16})");

                // Add filter: pk >= data_n/2
                auto i64_fid = schema->get_primary_field_id().value();
                auto* predicate = ivf_filtered_anns->mutable_predicates();
                auto* unary_range = predicate->mutable_unary_range_expr();
                auto* col_info = unary_range->mutable_column_info();
                col_info->set_field_id(i64_fid.get());
                col_info->set_data_type(milvus::proto::schema::DataType::Int64);
                unary_range->set_op(milvus::proto::plan::OpType::GreaterEqual);
                unary_range->mutable_value()->set_int64_val(data_n / 2);

                auto ivf_filtered_str =
                    ivf_filtered_plan_node.SerializeAsString();
                auto ivf_filtered_plan = milvus::query::CreateSearchPlanByExpr(
                    schema, ivf_filtered_str.data(), ivf_filtered_str.size());
                auto ivf_filtered_ph = ParsePlaceholderGroup(
                    ivf_filtered_plan.get(), ph_group_raw.SerializeAsString());

                auto ivf_filtered_sr = segment->Search(ivf_filtered_plan.get(),
                                                       ivf_filtered_ph.get(),
                                                       MAX_TIMESTAMP);

                ASSERT_EQ(ivf_filtered_sr->total_nq_, num_queries);
                EXPECT_EQ(ivf_filtered_sr->unity_topK_, topk);

                // Verify all returned offsets are >= data_n/2
                for (size_t i = 0; i < ivf_filtered_sr->seg_offsets_.size();
                     ++i) {
                    int64_t seg_offset = ivf_filtered_sr->seg_offsets_[i];
                    if (seg_offset != -1) {
                        EXPECT_GE(seg_offset, data_n / 2)
                            << "IVF filtered search returned row " << seg_offset
                            << " which should have been filtered";

                        if (nullable) {
                            bool is_valid = (valid_data[seg_offset >> 3] >>
                                             (seg_offset & 0x07)) &
                                            1;
                            EXPECT_TRUE(is_valid)
                                << "IVF filtered search returned invalid row "
                                   "at seg_offset="
                                << seg_offset;
                        }
                    }
                }
            }
        }
    }
}

TEST_P(BinlogIndexTest, AccuracyWithMapFieldData) {
    IndexMetaPtr collection_index_meta = GetCollectionIndexMeta(index_type);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();

    auto& segcore_config = milvus::segcore::SegcoreConfig::default_config();
    segcore_config.set_enable_interim_segment_index(true);
    if (dense_vec_intermin_index_type.has_value()) {
        segcore_config.set_dense_vector_intermin_index_type(
            dense_vec_intermin_index_type.value());
    }
    segcore_config.set_nprobe(16);
    // 1. load field data, and build binlog index for binlog data
    LoadVectorField("./data/mmap-test");

    //assert segment has been built binlog index
    bool supports_interim_index =
        (data_type == DataType::VECTOR_FLOAT ||
         data_type == DataType::VECTOR_FLOAT16 ||
         data_type == DataType::VECTOR_BFLOAT16 ||
         data_type == DataType::VECTOR_SPARSE_U32_F32);
    int64_t valid_row_count = nullable ? valid_count : data_n;
    int64_t threshold = segcore_config.get_nlist() * 39;
    bool should_have_index =
        supports_interim_index && (valid_row_count >= threshold);
    if (should_have_index) {
        EXPECT_TRUE(segment->HasIndex(vec_field_id));
    } else {
        EXPECT_FALSE(segment->HasIndex(vec_field_id));
    }
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));

    // 2. search binlog index
    auto num_queries = std::min(10, (int)valid_count);
    if (num_queries == 0) {
        return;
    }

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(DataTypeToVectorType(data_type));
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vec_field_id.get());

    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(topk);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(R"({"nprobe": 16})");
    auto plan_str = plan_node.SerializeAsString();

    // Use the first num_queries vectors from raw data as queries
    auto ph_group_raw = CreatePlaceholderGroupForVectorType(
        data_type, num_queries, data_d, GetQueryData(num_queries));

    auto plan = milvus::query::CreateSearchPlanByExpr(
        schema, plan_str.data(), plan_str.size());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    std::vector<const milvus::query::PlaceholderGroup*> ph_group_arr = {
        ph_group.get()};
    auto nlist = segcore_config.get_nlist();
    auto binlog_index_sr =
        segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
    ASSERT_EQ(binlog_index_sr->total_nq_, num_queries);
    EXPECT_EQ(binlog_index_sr->unity_topK_, topk);
    EXPECT_EQ(binlog_index_sr->distances_.size(), num_queries * topk);
    EXPECT_EQ(binlog_index_sr->seg_offsets_.size(), num_queries * topk);

    for (int q = 0; q < num_queries; ++q) {
        for (size_t k = 0; k < topk; ++k) {
            int64_t seg_offset = binlog_index_sr->seg_offsets_[q * topk + k];
            if (seg_offset == -1) {
                continue;
            }
            ASSERT_GE(seg_offset, 0);
            ASSERT_LT(seg_offset, static_cast<int64_t>(data_n));

            if (nullable) {
                bool is_valid =
                    (valid_data[seg_offset >> 3] >> (seg_offset & 0x07)) & 1;
                EXPECT_TRUE(is_valid)
                    << "Search returned invalid (null) row at seg_offset="
                    << seg_offset;
            }
        }
    }

    VerifyQueryResults(binlog_index_sr->seg_offsets_);

    {
        milvus::proto::plan::PlanNode filtered_plan_node;
        auto filtered_vector_anns = filtered_plan_node.mutable_vector_anns();
        filtered_vector_anns->set_vector_type(DataTypeToVectorType(data_type));
        filtered_vector_anns->set_placeholder_tag("$0");
        filtered_vector_anns->set_field_id(vec_field_id.get());
        auto filtered_query_info = filtered_vector_anns->mutable_query_info();
        filtered_query_info->set_topk(topk);
        filtered_query_info->set_round_decimal(3);
        filtered_query_info->set_metric_type(metric_type);
        filtered_query_info->set_search_params(R"({"nprobe": 16})");

        auto i64_fid = schema->get_primary_field_id().value();
        auto* predicate = filtered_vector_anns->mutable_predicates();
        auto* unary_range = predicate->mutable_unary_range_expr();
        auto* col_info = unary_range->mutable_column_info();
        col_info->set_field_id(i64_fid.get());
        col_info->set_data_type(milvus::proto::schema::DataType::Int64);
        unary_range->set_op(milvus::proto::plan::OpType::GreaterEqual);
        unary_range->mutable_value()->set_int64_val(data_n / 2);

        auto filtered_plan_str = filtered_plan_node.SerializeAsString();
        auto filtered_plan = milvus::query::CreateSearchPlanByExpr(
            schema, filtered_plan_str.data(), filtered_plan_str.size());
        auto filtered_ph_group = ParsePlaceholderGroup(
            filtered_plan.get(), ph_group_raw.SerializeAsString());

        auto filtered_sr = segment->Search(
            filtered_plan.get(), filtered_ph_group.get(), MAX_TIMESTAMP);

        ASSERT_EQ(filtered_sr->total_nq_, num_queries);
        EXPECT_EQ(filtered_sr->unity_topK_, topk);

        for (size_t i = 0; i < filtered_sr->seg_offsets_.size(); ++i) {
            int64_t seg_offset = filtered_sr->seg_offsets_[i];
            if (seg_offset != -1) {
                EXPECT_GE(seg_offset, data_n / 2)
                    << "Filtered search returned row " << seg_offset
                    << " which should have been filtered (pk < " << data_n / 2
                    << ")";

                if (nullable) {
                    bool is_valid =
                        (valid_data[seg_offset >> 3] >> (seg_offset & 0x07)) &
                        1;
                    EXPECT_TRUE(is_valid) << "Filtered search returned invalid "
                                             "(null) row at seg_offset="
                                          << seg_offset;
                }
            }
        }
    }

    if (null_percent != 100 && supports_interim_index) {
        // 3. update vector index
        {
            milvus::index::CreateIndexInfo create_index_info;
            create_index_info.field_type = data_type;
            create_index_info.metric_type = metric_type;
            create_index_info.index_type = index_type;
            create_index_info.index_engine_version =
                knowhere::Version::GetCurrentVersion().VersionNumber();
            auto indexing =
                milvus::index::IndexFactory::GetInstance().CreateIndex(
                    create_index_info, milvus::storage::FileManagerContext());

            auto build_conf =
                knowhere::Json{{knowhere::meta::METRIC_TYPE, metric_type},
                               {knowhere::meta::DIM, std::to_string(data_d)},
                               {knowhere::indexparam::NLIST, "64"}};

            indexing->BuildWithDataset(raw_dataset, build_conf);

            if (nullable) {
                auto vec_indexing =
                    dynamic_cast<milvus::index::VectorIndex*>(indexing.get());
                ASSERT_NE(vec_indexing, nullptr);
                std::unique_ptr<bool[]> valid_data_bool(new bool[data_n]);
                for (int64_t i = 0; i < data_n; ++i) {
                    valid_data_bool[i] = (valid_data[i >> 3] >> (i & 0x07)) & 1;
                }
                vec_indexing->UpdateValidData(valid_data_bool.get(), data_n);
            }

            LoadIndexInfo load_info;
            load_info.field_id = vec_field_id.get();
            load_info.index_params = GenIndexParams(indexing.get());
            load_info.cache_index =
                CreateTestCacheIndex("test", std::move(indexing));
            load_info.index_params["metric_type"] = metric_type;
            ASSERT_NO_THROW(segment->LoadIndex(load_info));
            EXPECT_TRUE(segment->HasIndex(vec_field_id));
            EXPECT_EQ(segment->get_row_count(), data_n);
            auto ivf_sr =
                segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
            ASSERT_EQ(ivf_sr->total_nq_, num_queries);
            EXPECT_EQ(ivf_sr->unity_topK_, topk);
            EXPECT_EQ(ivf_sr->distances_.size(), num_queries * topk);
            EXPECT_EQ(ivf_sr->seg_offsets_.size(), num_queries * topk);

            auto similary =
                GetKnnSearchRecall(num_queries,
                                   binlog_index_sr->seg_offsets_.data(),
                                   topk,
                                   ivf_sr->seg_offsets_.data(),
                                   topk);
            ASSERT_GT(similary, 0.45);

            VerifyQueryResults(ivf_sr->seg_offsets_);

            {
                milvus::proto::plan::PlanNode ivf_filtered_plan_node;
                auto ivf_filtered_anns =
                    ivf_filtered_plan_node.mutable_vector_anns();
                ivf_filtered_anns->set_vector_type(
                    DataTypeToVectorType(data_type));
                ivf_filtered_anns->set_placeholder_tag("$0");
                ivf_filtered_anns->set_field_id(vec_field_id.get());
                auto ivf_filtered_info =
                    ivf_filtered_anns->mutable_query_info();
                ivf_filtered_info->set_topk(topk);
                ivf_filtered_info->set_round_decimal(3);
                ivf_filtered_info->set_metric_type(metric_type);
                ivf_filtered_info->set_search_params(R"({"nprobe": 16})");

                auto i64_fid = schema->get_primary_field_id().value();
                auto* predicate = ivf_filtered_anns->mutable_predicates();
                auto* unary_range = predicate->mutable_unary_range_expr();
                auto* col_info = unary_range->mutable_column_info();
                col_info->set_field_id(i64_fid.get());
                col_info->set_data_type(milvus::proto::schema::DataType::Int64);
                unary_range->set_op(milvus::proto::plan::OpType::GreaterEqual);
                unary_range->mutable_value()->set_int64_val(data_n / 2);

                auto ivf_filtered_str =
                    ivf_filtered_plan_node.SerializeAsString();
                auto ivf_filtered_plan = milvus::query::CreateSearchPlanByExpr(
                    schema, ivf_filtered_str.data(), ivf_filtered_str.size());
                auto ivf_filtered_ph = ParsePlaceholderGroup(
                    ivf_filtered_plan.get(), ph_group_raw.SerializeAsString());

                auto ivf_filtered_sr = segment->Search(ivf_filtered_plan.get(),
                                                       ivf_filtered_ph.get(),
                                                       MAX_TIMESTAMP);

                ASSERT_EQ(ivf_filtered_sr->total_nq_, num_queries);
                EXPECT_EQ(ivf_filtered_sr->unity_topK_, topk);

                for (size_t i = 0; i < ivf_filtered_sr->seg_offsets_.size();
                     ++i) {
                    int64_t seg_offset = ivf_filtered_sr->seg_offsets_[i];
                    if (seg_offset != -1) {
                        EXPECT_GE(seg_offset, data_n / 2)
                            << "IVF filtered search returned row " << seg_offset
                            << " which should have been filtered";

                        if (nullable) {
                            bool is_valid = (valid_data[seg_offset >> 3] >>
                                             (seg_offset & 0x07)) &
                                            1;
                            EXPECT_TRUE(is_valid)
                                << "IVF filtered search returned invalid row "
                                   "at seg_offset="
                                << seg_offset;
                        }
                    }
                }
            }
        }
    }
}

TEST_P(BinlogIndexTest, DisableInterimIndex) {
    IndexMetaPtr collection_index_meta = GetCollectionIndexMeta(index_type);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();
    SegcoreSetEnableInterminSegmentIndex(false);

    LoadVectorField();

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));

    bool supports_final_index = (data_type == DataType::VECTOR_FLOAT ||
                                 data_type == DataType::VECTOR_FLOAT16 ||
                                 data_type == DataType::VECTOR_BFLOAT16 ||
                                 data_type == DataType::VECTOR_SPARSE_U32_F32);

    if (null_percent != 100 && supports_final_index) {
        // load vector index
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = data_type;
        create_index_info.metric_type = metric_type;
        create_index_info.index_type = index_type;
        create_index_info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info, milvus::storage::FileManagerContext());

        auto build_conf =
            knowhere::Json{{knowhere::meta::METRIC_TYPE, metric_type},
                           {knowhere::meta::DIM, std::to_string(data_d)},
                           {knowhere::indexparam::NLIST, "64"}};

        indexing->BuildWithDataset(raw_dataset, build_conf);

        if (nullable) {
            auto vec_indexing =
                dynamic_cast<milvus::index::VectorIndex*>(indexing.get());
            ASSERT_NE(vec_indexing, nullptr);
            std::unique_ptr<bool[]> valid_data_bool(new bool[data_n]);
            for (int64_t i = 0; i < data_n; ++i) {
                valid_data_bool[i] = (valid_data[i >> 3] >> (i & 0x07)) & 1;
            }
            vec_indexing->UpdateValidData(valid_data_bool.get(), data_n);
        }

        LoadIndexInfo load_info;
        load_info.field_id = vec_field_id.get();
        load_info.index_params = GenIndexParams(indexing.get());
        load_info.cache_index =
            CreateTestCacheIndex("test", std::move(indexing));
        load_info.index_params["metric_type"] = metric_type;

        ASSERT_NO_THROW(segment->LoadIndex(load_info));
        EXPECT_TRUE(segment->HasIndex(vec_field_id));
        EXPECT_EQ(segment->get_row_count(), data_n);

        auto num_queries = std::min(10, (int)valid_count);
        if (num_queries > 0) {
            milvus::proto::plan::PlanNode plan_node;
            auto vector_anns = plan_node.mutable_vector_anns();
            vector_anns->set_vector_type(DataTypeToVectorType(data_type));
            vector_anns->set_placeholder_tag("$0");
            vector_anns->set_field_id(vec_field_id.get());
            auto query_info = vector_anns->mutable_query_info();
            query_info->set_topk(topk);
            query_info->set_round_decimal(3);
            query_info->set_metric_type(metric_type);
            query_info->set_search_params(R"({"nprobe": 16})");
            auto plan_str = plan_node.SerializeAsString();

            auto ph_group_raw = CreatePlaceholderGroupForVectorType(
                data_type, num_queries, data_d, GetQueryData(num_queries));

            auto plan = milvus::query::CreateSearchPlanByExpr(
                schema, plan_str.data(), plan_str.size());
            auto ph_group = ParsePlaceholderGroup(
                plan.get(), ph_group_raw.SerializeAsString());

            auto sr =
                segment->Search(plan.get(), ph_group.get(), MAX_TIMESTAMP);
            ASSERT_EQ(sr->total_nq_, num_queries);
            EXPECT_EQ(sr->unity_topK_, topk);

            for (size_t i = 0; i < sr->seg_offsets_.size(); ++i) {
                int64_t seg_offset = sr->seg_offsets_[i];
                if (seg_offset != -1) {
                    ASSERT_GE(seg_offset, 0);
                    ASSERT_LT(seg_offset, static_cast<int64_t>(data_n));
                    if (nullable) {
                        bool is_valid = (valid_data[seg_offset >> 3] >>
                                         (seg_offset & 0x07)) &
                                        1;
                        EXPECT_TRUE(is_valid)
                            << "Search returned invalid row at seg_offset="
                            << seg_offset;
                    }
                }
            }

            VerifyQueryResults(sr->seg_offsets_);
        }
    }
}

TEST_P(BinlogIndexTest, LoadBingLogWihIDMAP) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    segment = CreateSealedSegment(schema, collection_index_meta);
    LoadOtherFields();
    LoadVectorField();

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
}

TEST_P(BinlogIndexTest, LoadBinlogWithoutIndexMeta) {
    IndexMetaPtr collection_index_meta =
        GetCollectionIndexMeta(knowhere::IndexEnum::INDEX_FAISS_IDMAP);

    segment = CreateSealedSegment(schema, collection_index_meta);
    SegcoreSetEnableInterminSegmentIndex(true);

    LoadVectorField();

    EXPECT_FALSE(segment->HasIndex(vec_field_id));
    EXPECT_EQ(segment->get_row_count(), data_n);
    EXPECT_TRUE(segment->HasFieldData(vec_field_id));
}