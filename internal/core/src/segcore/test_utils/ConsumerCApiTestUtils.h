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
#include <google/protobuf/text_format.h>
#include <cassert>
#include <cstring>
#include <chrono>
#include <iostream>
#include <unordered_set>
#include <limits>
#include <mutex>
#include <random>
#include "gtest/gtest.h"
#include "common/QueryInfo.h"

#include "common/Types.h"
#include "common/type_c.h"
#include "common/VectorTrait.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/Collection.h"
#include "segcore/segment_c.h"
#include "segcore/Types.h"
#include "futures/Future.h"
#include "futures/future_c.h"
#include "segcore/load_index_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/PbHelper.h"
#include "segcore/test_utils/ConsumerIndexTestUtils.h"
#include "index/contracts/query/IVectorReader.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "test_utils/cachinglayer_test_utils.h"

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

constexpr int64_t DIM = 4;
constexpr int64_t BINARY_DIM = 8;

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

// Flatten composite group_by values to a single-field vector. qn-reduce
// group_by tests predate master's multi-field composite group_by (PR #48971)
// and only exercise single-field group_by, so taking the first field of each
// CompositeGroupKey preserves the original test intent.
[[maybe_unused]] static std::vector<milvus::GroupByValueType>
ExtractFirstFieldGroupByValues(const milvus::SearchResult& sr) {
    std::vector<milvus::GroupByValueType> result;
    const auto& composite = sr.composite_group_by_values_.value();
    result.reserve(composite.size());
    for (const auto& key : composite) {
        result.push_back(key[0]);
    }
    return result;
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
                              filter_only,
                              false);
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

// C API consumers need the same IVF parameters as their former fixture, while
// construction and loading are owned by the current builder/loader contracts.
inline Config
generate_build_conf(const IndexType& index_type, const MetricType& metric) {
    return {
        {knowhere::meta::METRIC_TYPE, metric},
        {knowhere::meta::DIM,
         index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT ? BINARY_DIM
                                                                    : DIM},
        {knowhere::indexparam::NLIST, 16}};
}

inline Config
generate_search_conf(const IndexType&, const MetricType& metric) {
    return {{knowhere::meta::METRIC_TYPE, metric},
            {knowhere::indexparam::NPROBE, 4}};
}

inline VectorSearchParams
MakeVectorSearchParams(const SearchInfo& info) {
    return {
        info.search_params_, info.metric_type_, info.topk_, info.trace_ctx_};
}

inline milvus::test::expr_index::OpenedIndex
generate_index(const void* raw_data,
               DataType field_type,
               MetricType metric_type,
               IndexType index_type,
               int64_t dim,
               int64_t rows,
               Config config = Config::object()) {
    if (config.empty()) {
        config = generate_build_conf(index_type, metric_type);
    }
    auto build = [&]<typename T>() {
        auto opened = milvus::test::consumer::BuildVectorReader<T>(
            field_type,
            index_type,
            metric_type,
            dim,
            rows,
            static_cast<const typename index::VectorBuildInput<T>::value_type*>(
                raw_data),
            config);
        EXPECT_EQ(opened.reader->Count(), rows);
        auto vectors = dynamic_cast<const IVectorReader*>(opened.reader.get());
        EXPECT_NE(vectors, nullptr);
        if (vectors != nullptr) {
            EXPECT_EQ(vectors->Dim(), dim);
        }
        return opened;
    };
    switch (field_type) {
        case DataType::VECTOR_FLOAT:
            return build.template operator()<float>();
        case DataType::VECTOR_FLOAT16:
            return build.template operator()<knowhere::fp16>();
        case DataType::VECTOR_BFLOAT16:
            return build.template operator()<knowhere::bf16>();
        case DataType::VECTOR_BINARY:
            return build.template operator()<uint8_t>();
        case DataType::VECTOR_INT8:
            return build.template operator()<int8_t>();
        case DataType::VECTOR_SPARSE_U32_F32:
            return build.template operator()<sparse_u32_f32>();
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported vector type in C API consumer fixture");
    }
}

}  // namespace
