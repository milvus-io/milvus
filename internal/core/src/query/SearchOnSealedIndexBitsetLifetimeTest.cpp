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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "exec/operator/Utils.h"
#include "index/IndexFactory.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "query/Utils.h"

namespace milvus::query {
namespace {

SearchResult
CreateGroupByIteratorsWithOwnedTransformedBitset(
    const index::VectorIndex& index,
    const DatasetPtr& query_dataset,
    const SearchInfo& search_info,
    const BitsetView& logical_bitset) {
    SearchResult search_result;
    BitsetView transformed_view = KeepBitsetAlive(
        search_result,
        TransformBitset(logical_bitset, index.GetOffsetMapping()));
    auto used_iterator = exec::PrepareVectorIteratorsFromIndex(
        search_info, 1, query_dataset, search_result, transformed_view, index);
    EXPECT_TRUE(used_iterator);
    EXPECT_TRUE(search_result.vector_iterators_.has_value());
    return search_result;
}

}  // namespace

TEST(SearchOnSealedIndexBitsetLifetime,
     GroupByIteratorMustNotKeepDanglingTransformedBitset) {
    constexpr int64_t dim = 36;
    constexpr int64_t total_count = 10000;
    constexpr int64_t topk = 15;

    std::unique_ptr<bool[]> valid_data(new bool[total_count]);
    int64_t valid_count = 0;
    for (int64_t i = 0; i < total_count; ++i) {
        valid_data[i] = i % 10 != 9;
        if (valid_data[i]) {
            ++valid_count;
        }
    }

    std::vector<float> base(static_cast<size_t>(valid_count * dim));
    for (size_t i = 0; i < base.size(); ++i) {
        base[i] = static_cast<float>((i % 97) + 1) / 97.0F;
    }

    index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::COSINE;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto index_base = index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, storage::FileManagerContext());
    auto* vector_index = dynamic_cast<index::VectorIndex*>(index_base.get());
    ASSERT_NE(vector_index, nullptr);

    auto build_dataset = knowhere::GenDataSet(valid_count, dim, base.data());
    auto build_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
        {knowhere::meta::DIM, std::to_string(dim)},
        {knowhere::indexparam::NLIST, "128"},
    };
    index_base->BuildWithDataset(build_dataset, build_conf);
    vector_index->BuildValidData(valid_data.get(), total_count);
    ASSERT_TRUE(vector_index->GetOffsetMapping().IsEnabled());

    std::vector<uint8_t> logical_bitset_bytes((total_count + 7) / 8, 0);
    for (int64_t i = 0; i < total_count; i += 7) {
        logical_bitset_bytes[i >> 3] |= 1U << (i & 0x07);
    }
    BitsetView logical_bitset(logical_bitset_bytes.data(), total_count);

    std::vector<float> query(base.begin(), base.begin() + dim);
    auto query_dataset = knowhere::GenDataSet(1, dim, query.data());

    SearchInfo search_info;
    search_info.topk_ = topk;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::COSINE;
    search_info.search_params_ = knowhere::Json{
        {knowhere::indexparam::NPROBE, "32"},
    };
    search_info.group_by_field_ids_.push_back(FieldId(102));

    auto search_result = CreateGroupByIteratorsWithOwnedTransformedBitset(
        *vector_index, query_dataset, search_info, logical_bitset);

    ASSERT_TRUE(search_result.vector_iterators_.has_value());
    auto iterator = search_result.vector_iterators_->at(0);
    ASSERT_NE(iterator, nullptr);

    int result_count = 0;
    while (iterator->HasNext() && result_count < valid_count) {
        auto result = iterator->Next();
        ASSERT_TRUE(result.has_value());
        ++result_count;
    }
    ASSERT_GT(result_count, 0);
}

}  // namespace milvus::query
