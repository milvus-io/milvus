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

#include "segcore/storagev2translator/AggregateTranslator.h"

#include <algorithm>
#include <cmath>
#include <memory>
#include <unordered_set>

#include <arrow/api.h>
#include <arrow/array.h>
#include <fmt/format.h>

#include "common/Chunk.h"
#include "common/Exception.h"
#include "common/FieldData.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"

namespace milvus::segcore::storagev2translator {

AggregateTranslator::AggregateTranslator(
    std::unique_ptr<ManifestGroupTranslator> underlying_translator,
    size_t aggregation_factor)
    : underlying_translator_(std::move(underlying_translator)),
      aggregation_factor_(aggregation_factor) {
    AssertInfo(aggregation_factor_ > 1,
               "Aggregation factor must be greater than 1, got: {}",
               aggregation_factor_);
    AssertInfo(underlying_translator_ != nullptr,
               "Underlying translator cannot be null");

    key_ = fmt::format(
        "agg_{}_{}", aggregation_factor_, underlying_translator_->key());
}

size_t
AggregateTranslator::num_cells() const {
    size_t underlying_num_cells = underlying_translator_->num_cells();
    return (underlying_num_cells + aggregation_factor_ - 1) /
           aggregation_factor_;
}

milvus::cachinglayer::cid_t
AggregateTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    // First map through the underlying translator
    auto underlying_cid = underlying_translator_->cell_id_of(uid);
    // Then aggregate by dividing by the factor
    return underlying_cid / aggregation_factor_;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
AggregateTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    auto underlying_cids = get_underlying_cell_ids(cid);

    milvus::cachinglayer::ResourceUsage total_loading_usage = {0, 0};
    milvus::cachinglayer::ResourceUsage total_loaded_usage = {0, 0};

    for (auto underlying_cid : underlying_cids) {
        auto [loading_usage, loaded_usage] =
            underlying_translator_->estimated_byte_size_of_cell(underlying_cid);
        total_loading_usage += loading_usage;
        total_loaded_usage += loaded_usage;
    }

    return {total_loading_usage, total_loaded_usage};
}

const std::string&
AggregateTranslator::key() const {
    return key_;
}

std::vector<
    std::pair<milvus::cachinglayer::cid_t, std::unique_ptr<milvus::GroupChunk>>>
AggregateTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    // Collect all underlying cell IDs we need to fetch
    std::vector<milvus::cachinglayer::cid_t> all_underlying_cids;
    std::unordered_map<milvus::cachinglayer::cid_t,
                       std::vector<milvus::cachinglayer::cid_t>>
        aggregate_to_underlying_map;

    for (auto aggregate_cid : cids) {
        auto underlying_cids = get_underlying_cell_ids(aggregate_cid);
        aggregate_to_underlying_map[aggregate_cid] = underlying_cids;
        all_underlying_cids.insert(all_underlying_cids.end(),
                                   underlying_cids.begin(),
                                   underlying_cids.end());
    }

    // Read all RecordBatches in one batch (zero-copy read phase)
    auto record_batches =
        underlying_translator_->read_cells(all_underlying_cids);

    // Create a map for quick lookup of RecordBatches
    std::unordered_map<milvus::cachinglayer::cid_t,
                       std::shared_ptr<arrow::RecordBatch>>
        record_batch_map;
    for (auto& [cid, batch] : record_batches) {
        record_batch_map[cid] = batch;
    }

    // Build the result by loading aggregate cells from RecordBatches
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
        result;
    result.reserve(cids.size());

    for (auto aggregate_cid : cids) {
        const auto& underlying_cids =
            aggregate_to_underlying_map[aggregate_cid];

        // Collect the RecordBatches for this aggregate cell
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches_to_load;
        batches_to_load.reserve(underlying_cids.size());

        for (auto underlying_cid : underlying_cids) {
            auto it = record_batch_map.find(underlying_cid);
            if (it != record_batch_map.end() && it->second != nullptr) {
                batches_to_load.push_back(it->second);
            }
        }

        if (batches_to_load.empty()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "No underlying batches found for aggregate cell {}",
                      aggregate_cid);
        }

        // Load the aggregate cell from multiple batches (zero-copy load phase)
        // The concatenation happens at Arrow array level inside load_group_chunk
        auto merged_chunk = underlying_translator_->load_group_chunk(
            batches_to_load, aggregate_cid);
        result.emplace_back(aggregate_cid, std::move(merged_chunk));
    }

    return result;
}

int64_t
AggregateTranslator::cells_storage_bytes(
    const std::vector<milvus::cachinglayer::cid_t>& cids) const {
    std::vector<milvus::cachinglayer::cid_t> all_underlying_cids;

    for (auto aggregate_cid : cids) {
        auto underlying_cids = get_underlying_cell_ids(aggregate_cid);
        all_underlying_cids.insert(all_underlying_cids.end(),
                                   underlying_cids.begin(),
                                   underlying_cids.end());
    }

    return underlying_translator_->cells_storage_bytes(all_underlying_cids);
}

std::vector<milvus::cachinglayer::cid_t>
AggregateTranslator::get_underlying_cell_ids(
    milvus::cachinglayer::cid_t aggregate_cid) const {
    size_t underlying_num_cells = underlying_translator_->num_cells();
    size_t start_cid = aggregate_cid * aggregation_factor_;
    size_t end_cid =
        std::min(start_cid + aggregation_factor_, underlying_num_cells);

    AssertInfo(start_cid < underlying_num_cells,
               "Aggregate cell ID {} out of range (start_cid={} >= "
               "underlying_num_cells={})",
               aggregate_cid,
               start_cid,
               underlying_num_cells);

    std::vector<milvus::cachinglayer::cid_t> result;
    result.reserve(end_cid - start_cid);

    for (size_t i = start_cid; i < end_cid; ++i) {
        result.push_back(i);
    }

    return result;
}

}  // namespace milvus::segcore::storagev2translator
