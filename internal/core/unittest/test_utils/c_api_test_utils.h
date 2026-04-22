// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <boost/format.hpp>
#include <chrono>
#include <iostream>
#include <unordered_set>

#include "common/Types.h"
#include "common/type_c.h"
#include "common/VectorTrait.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce_c.h"
#include "segcore/segment_c.h"
#include "segcore/Types.h"
#include "futures/Future.h"
#include "futures/future_c.h"
#include "segcore/load_index_c.h"
#include "DataGen.h"
#include "PbHelper.h"
#include "indexbuilder_test_utils.h"
#include "cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::index;

// Test utility function for AppendFieldInfoForTest
inline CStatus
AppendFieldInfoForTest(CLoadIndexInfo c_load_index_info,
                       int64_t collection_id,
                       int64_t partition_id,
                       int64_t segment_id,
                       int64_t field_id,
                       enum CDataType field_type,
                       bool enable_mmap,
                       const char* mmap_dir_path) {
    try {
        auto load_index_info =
            (milvus::segcore::LoadIndexInfo*)c_load_index_info;
        load_index_info->collection_id = collection_id;
        load_index_info->partition_id = partition_id;
        load_index_info->segment_id = segment_id;
        load_index_info->field_id = field_id;
        load_index_info->field_type = milvus::DataType(field_type);
        load_index_info->enable_mmap = enable_mmap;
        load_index_info->mmap_dir_path = std::string(mmap_dir_path);

        auto status = CStatus();
        status.error_code = milvus::Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = milvus::UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

namespace {

[[maybe_unused]] std::string
generate_max_float_query_data(int all_nq, int max_float_nq) {
    assert(max_float_nq <= all_nq);
    namespace ser = milvus::proto::common;
    int dim = DIM;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::FloatVector);
    for (int i = 0; i < all_nq; ++i) {
        std::vector<float> vec;
        if (i < max_float_nq) {
            for (int d = 0; d < dim; ++d) {
                vec.push_back(std::numeric_limits<float>::max());
            }
        } else {
            for (int d = 0; d < dim; ++d) {
                vec.push_back(1);
            }
        }
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    auto blob = raw_group.SerializeAsString();
    return blob;
}

template <class TraitType = milvus::FloatVector>
std::string
generate_query_data(int nq) {
    namespace ser = milvus::proto::common;
    GET_ELEM_TYPE_FOR_VECTOR_TRAIT

    std::default_random_engine e(67);
    int dim = DIM;
    std::uniform_int_distribution<int8_t> dis(-128, 127);
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(TraitType::placeholder_type);
    for (int i = 0; i < nq; ++i) {
        std::vector<elem_type> vec;
        for (int d = 0; d < dim / TraitType::dim_factor; ++d) {
            vec.push_back((elem_type)dis(e));
        }
        value->add_values(vec.data(), vec.size() * sizeof(elem_type));
    }
    auto blob = raw_group.SerializeAsString();
    return blob;
}

[[maybe_unused]] void
CheckSearchResultDuplicate(const std::vector<CSearchResult>& results,
                           int group_size = 1) {
    auto nq = ((SearchResult*)results[0])->total_nq_;
    std::unordered_set<PkType> pk_set;
    std::unordered_map<CompositeGroupKey, int, CompositeGroupKeyHash>
        group_by_map;
    for (int qi = 0; qi < nq; qi++) {
        pk_set.clear();
        group_by_map.clear();
        for (size_t i = 0; i < results.size(); i++) {
            auto search_result = (SearchResult*)results[i];
            ASSERT_EQ(nq, search_result->total_nq_);
            auto topk_beg = search_result->topk_per_nq_prefix_sum_[qi];
            auto topk_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
            for (size_t ki = topk_beg; ki < topk_end; ki++) {
                ASSERT_NE(search_result->seg_offsets_[ki], INVALID_SEG_OFFSET);
                auto ret = pk_set.insert(search_result->primary_keys_[ki]);
                ASSERT_TRUE(ret.second);

                if (search_result->composite_group_by_values_.has_value() &&
                    search_result->composite_group_by_values_.value().size() >
                        ki) {
                    const auto& group_by_val =
                        search_result->composite_group_by_values_.value()[ki];
                    group_by_map[group_by_val] += 1;
                    ASSERT_TRUE(group_by_map[group_by_val] <= group_size);
                }
            }
        }
    }
}

// Extract a scalar field value at a given flat index as a string key.
// Used by proto-based post-reduce checks. Supports all group_by-capable types.
[[maybe_unused]] static std::string
ExtractScalarAsKeyString(const milvus::proto::schema::FieldData& fd,
                         int64_t idx) {
    const auto& scalars = fd.scalars();
    switch (fd.type()) {
        case milvus::proto::schema::DataType::Int8:
        case milvus::proto::schema::DataType::Int16:
        case milvus::proto::schema::DataType::Int32:
            return "i32:" + std::to_string(scalars.int_data().data(idx));
        case milvus::proto::schema::DataType::Int64:
        case milvus::proto::schema::DataType::Timestamptz:
            return "i64:" + std::to_string(scalars.long_data().data(idx));
        case milvus::proto::schema::DataType::Bool:
            return scalars.bool_data().data(idx) ? "b:1" : "b:0";
        case milvus::proto::schema::DataType::VarChar:
        case milvus::proto::schema::DataType::String:
            return "s:" + scalars.string_data().data(idx);
        default:
            return "unk";
    }
}

// Post-reduce check reading from proto blobs rather than per-segment
// SearchResult state (which is moved-from after FillOtherData for perf).
// Validates pk uniqueness per-nq and per-composite-key count <= group_size.
[[maybe_unused]] void
CheckGroupByReducedSearchResult(CSearchResultDataBlobs c_blobs,
                                int group_size) {
    auto blobs =
        reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(c_blobs);
    for (const auto& slice_bytes : blobs->blobs) {
        milvus::proto::schema::SearchResultData data;
        ASSERT_TRUE(data.ParseFromArray(slice_bytes.data(),
                                        static_cast<int>(slice_bytes.size())));

        int64_t slice_nq = data.num_queries();
        int64_t offset = 0;
        for (int64_t qi = 0; qi < slice_nq; qi++) {
            int64_t topk_qi = data.topks(qi);
            std::unordered_set<std::string> pk_set;
            std::unordered_map<std::string, int> group_count;

            for (int64_t k = 0; k < topk_qi; k++) {
                int64_t idx = offset + k;

                // pk uniqueness
                std::string pk_str;
                if (data.ids().has_int_id()) {
                    pk_str =
                        "i:" + std::to_string(data.ids().int_id().data(idx));
                } else if (data.ids().has_str_id()) {
                    pk_str = "s:" + data.ids().str_id().data(idx);
                } else {
                    FAIL() << "unknown pk type";
                }
                ASSERT_TRUE(pk_set.insert(pk_str).second)
                    << "duplicate pk at slice_qi=" << qi << " k=" << k;

                // composite key count (read from group_by_field_values, or
                // singular group_by_field_value for legacy single-field path)
                std::string group_key;
                if (data.group_by_field_values_size() > 0) {
                    for (int fi = 0; fi < data.group_by_field_values_size();
                         fi++) {
                        group_key += ExtractScalarAsKeyString(
                                         data.group_by_field_values(fi), idx) +
                                     ";";
                    }
                } else if (data.has_group_by_field_value()) {
                    group_key = ExtractScalarAsKeyString(
                        data.group_by_field_value(), idx);
                } else {
                    continue;  // no group_by in this result
                }
                group_count[group_key]++;
                ASSERT_LE(group_count[group_key], group_size)
                    << "group_size exceeded at slice_qi=" << qi
                    << " key=" << group_key;
            }
            offset += topk_qi;
        }
    }
}

template <class TraitType = milvus::FloatVector>
const std::string
get_default_schema_config() {
    auto fmt = boost::format(R"(name: "default-collection"
                                fields: <
                                  fieldID: 100
                                  name: "fakevec"
                                  data_type: %1%
                                  type_params: <
                                    key: "dim"
                                    value: "4"
                                  >
                                  index_params: <
                                    key: "metric_type"
                                    value: "L2"
                                  >
                                >
                                fields: <
                                  fieldID: 101
                                  name: "age"
                                  data_type: Int64
                                  is_primary_key: true
                                >)") %
               (int(TraitType::data_type));
    return fmt.str();
}

[[maybe_unused]] const char*
get_default_schema_config_nullable() {
    static std::string conf = R"(name: "default-collection"
                                fields: <
                                  fieldID: 100
                                  name: "fakevec"
                                  data_type: FloatVector
                                  type_params: <
                                    key: "dim"
                                    value: "4"
                                  >
                                  index_params: <
                                    key: "metric_type"
                                    value: "L2"
                                  >
                                >
                                fields: <
                                  fieldID: 101
                                  name: "age"
                                  data_type: Int64
                                  is_primary_key: true
                                >
                                fields: <
                                  fieldID: 102
                                  name: "nullable"
                                  data_type: Int32
                                  nullable:true
                                >)";
    static std::string fake_conf = "";
    return conf.c_str();
}

[[maybe_unused]] CStatus
CSearch(CSegmentInterface c_segment,
        CSearchPlan c_plan,
        CPlaceholderGroup c_placeholder_group,
        uint64_t timestamp,
        CSearchResult* result,
        bool filter_only = false) {
    auto future = AsyncSearch({},
                              c_segment,
                              c_plan,
                              c_placeholder_group,
                              timestamp,
                              0,
                              0,
                              0,
                              filter_only);
    auto futurePtr = static_cast<milvus::futures::IFuture*>(
        static_cast<void*>(static_cast<CFuture*>(future)));

    std::mutex mu;
    mu.lock();
    futurePtr->registerReadyCallback(
        [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
        (CLockedGoMutex*)(&mu));
    mu.lock();

    auto [searchResult, status] = futurePtr->leakyGet();
    future_destroy(future);

    if (status.error_code != 0) {
        return status;
    }
    *result = static_cast<CSearchResult>(searchResult);
    return status;
}

// Filter-only search wrapper for two-stage search testing
[[maybe_unused]] CStatus
CSearchFilterOnly(CSegmentInterface c_segment,
                  CSearchPlan c_plan,
                  uint64_t timestamp,
                  CSearchResult* result) {
    return CSearch(c_segment, c_plan, nullptr, timestamp, result, true);
}

[[maybe_unused]] CStatus
CRetrieve(CSegmentInterface c_segment,
          CRetrievePlan c_plan,
          uint64_t timestamp,
          CRetrieveResult** result) {
    auto future = AsyncRetrieve({},
                                c_segment,
                                c_plan,
                                timestamp,
                                DEFAULT_MAX_OUTPUT_SIZE,
                                false,
                                0,
                                0,
                                0);
    auto futurePtr = static_cast<milvus::futures::IFuture*>(
        static_cast<void*>(static_cast<CFuture*>(future)));

    std::mutex mu;
    mu.lock();
    futurePtr->registerReadyCallback(
        [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
        (CLockedGoMutex*)(&mu));
    mu.lock();

    auto [retrieveResult, status] = futurePtr->leakyGet();
    future_destroy(future);

    if (status.error_code != 0) {
        return status;
    }
    *result = static_cast<CRetrieveResult*>(retrieveResult);
    return status;
}

[[maybe_unused]] CStatus
CRetrieveByOffsets(CSegmentInterface c_segment,
                   CRetrievePlan c_plan,
                   int64_t* offsets,
                   int64_t len,
                   CRetrieveResult** result) {
    auto future = AsyncRetrieveByOffsets({}, c_segment, c_plan, offsets, len);
    auto futurePtr = static_cast<milvus::futures::IFuture*>(
        static_cast<void*>(static_cast<CFuture*>(future)));

    std::mutex mu;
    mu.lock();
    futurePtr->registerReadyCallback(
        [](CLockedGoMutex* mutex) { ((std::mutex*)(mutex))->unlock(); },
        (CLockedGoMutex*)(&mu));
    mu.lock();

    auto [retrieveResult, status] = futurePtr->leakyGet();
    future_destroy(future);

    if (status.error_code != 0) {
        return status;
    }
    *result = static_cast<CRetrieveResult*>(retrieveResult);
    return status;
}

template <class TraitType>
std::string
generate_collection_schema(std::string metric_type, int dim) {
    namespace schema = milvus::proto::schema;
    GET_SCHEMA_DATA_TYPE_FOR_VECTOR_TRAIT

    schema::CollectionSchema collection_schema;
    collection_schema.set_name("collection_test");

    auto vec_field_schema = collection_schema.add_fields();
    vec_field_schema->set_name("fakevec");
    vec_field_schema->set_fieldid(100);
    vec_field_schema->set_data_type(schema_data_type);
    auto metric_type_param = vec_field_schema->add_index_params();
    metric_type_param->set_key("metric_type");
    metric_type_param->set_value(metric_type);
    auto dim_param = vec_field_schema->add_type_params();
    dim_param->set_key("dim");
    dim_param->set_value(std::to_string(dim));

    auto other_field_schema = collection_schema.add_fields();
    other_field_schema->set_name("counter");
    other_field_schema->set_fieldid(101);
    other_field_schema->set_data_type(schema::DataType::Int64);
    other_field_schema->set_is_primary_key(true);

    auto other_field_schema2 = collection_schema.add_fields();
    other_field_schema2->set_name("doubleField");
    other_field_schema2->set_fieldid(102);
    other_field_schema2->set_data_type(schema::DataType::Double);

    auto other_field_schema3 = collection_schema.add_fields();
    other_field_schema3->set_name("timestamptzField");
    other_field_schema3->set_fieldid(103);
    other_field_schema3->set_data_type(schema::DataType::Timestamptz);

    std::string schema_string;
    bool marshal = google::protobuf::TextFormat::PrintToString(
        collection_schema, &schema_string);
    AssertInfo(marshal, "failed to serialize collection schema");
    return schema_string;
}

[[maybe_unused]] const char*
get_default_index_meta() {
    static std::string conf = R"(maxIndexRowCount: 1000
                                index_metas: <
                                  fieldID: 100
                                  collectionID: 1001
                                  index_name: "test-index"
                                  type_params: <
                                    key: "dim"
                                    value: "4"
                                  >
                                  index_params: <
                                    key: "index_type"
                                    value: "IVF_FLAT"
                                  >
                                  index_params: <
                                   key: "metric_type"
                                   value: "L2"
                                  >
                                  index_params: <
                                   key: "nlist"
                                   value: "128"
                                  >
                                >)";
    return conf.c_str();
}

[[maybe_unused]] IndexBasePtr
generate_index(void* raw_data,
               DataType field_type,
               MetricType metric_type,
               IndexType index_type,
               int64_t dim,
               int64_t N) {
    auto engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    CreateIndexInfo create_index_info{
        field_type, index_type, metric_type, engine_version};
    auto indexing = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

    auto database = knowhere::GenDataSet(N, dim, raw_data);
    auto build_config = generate_build_conf(index_type, metric_type);
    indexing->BuildWithDataset(database, build_config);

    auto vec_indexing = dynamic_cast<VectorIndex*>(indexing.get());
    EXPECT_EQ(vec_indexing->Count(), N);
    EXPECT_EQ(vec_indexing->GetDim(), dim);

    return indexing;
}

// Helper function to create LoadIndexInfo for tests using C++ API directly
inline milvus::segcore::LoadIndexInfo
CreateTestLoadIndexInfo(IndexBasePtr indexing,
                        DataType field_type,
                        int64_t field_id = 100) {
    milvus::segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = field_id;
    load_index_info.field_type = field_type;
    load_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    load_index_info.index_params = GenIndexParams(indexing.get());
    if (auto vec_index =
            dynamic_cast<const milvus::index::VectorIndex*>(indexing.get())) {
        load_index_info.index_params["metric_type"] =
            vec_index->GetMetricType();
    }
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    return load_index_info;
}

}  // namespace
