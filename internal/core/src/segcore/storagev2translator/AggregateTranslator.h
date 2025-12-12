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

#include <memory>
#include <string>
#include <vector>

#include "cachinglayer/Translator.h"
#include "common/GroupChunk.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"

namespace milvus::segcore::storagev2translator {

/**
 * @brief Aggregating proxy translator that merges consecutive cells into coarse-grain cells
 *
 * This class implements a proxy pattern that wraps a ManifestGroupTranslator and aggregates
 * consecutive cells into larger cells using zero-copy optimization. For example, with an 
 * aggregation factor of 2, every 2 consecutive row groups from the underlying translator 
 * are merged into a single cell in this translator.
 *
 * This is useful for:
 * - Reducing cache slot overhead by having fewer, larger cells
 * - Batching multiple small chunks into larger units for more efficient processing
 * - Adjusting granularity of data access patterns
 *
 * Example:
 *   If the underlying translator has 10 cells (row groups 0-9) and aggregation_factor=2:
 *   - Aggregate cell 0 contains underlying cells 0,1
 *   - Aggregate cell 1 contains underlying cells 2,3
 *   - Aggregate cell 2 contains underlying cells 4,5
 *   - Aggregate cell 3 contains underlying cells 6,7
 *   - Aggregate cell 4 contains underlying cells 8,9
 *   - Total aggregate cells: ceil(10/2) = 5
 *
 * Note: This translator specifically requires ManifestGroupTranslator as the underlying
 * translator to enable zero-copy aggregation through the read_cells/load_group_chunk pattern.
 */
class AggregateTranslator
    : public milvus::cachinglayer::Translator<milvus::GroupChunk> {
 public:
    /**
     * @brief Construct an aggregating translator
     *
     * @param underlying_translator The ManifestGroupTranslator to wrap and aggregate
     * @param aggregation_factor Number of consecutive underlying cells to merge into one aggregate cell.
     *                          Must be > 1. Use the underlying translator directly if no aggregation is needed.
     */
    explicit AggregateTranslator(
        std::unique_ptr<ManifestGroupTranslator> underlying_translator,
        size_t aggregation_factor);

    ~AggregateTranslator() override = default;

    /**
     * @brief Get the total number of aggregate cells
     *
     * Returns ceil(underlying_num_cells / aggregation_factor)
     *
     * @return Number of aggregate cells
     */
    size_t
    num_cells() const override;

    /**
     * @brief Map a unit ID to its corresponding aggregate cell ID
     *
     * The mapping divides the unit ID by the aggregation factor.
     *
     * @param uid Unit ID to map
     * @return Corresponding aggregate cell ID
     */
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override;

    /**
     * @brief Estimate memory and disk usage for an aggregate cell
     *
     * Sums the resource usage of all underlying cells that comprise this aggregate cell.
     *
     * @param cid Aggregate cell ID to estimate
     * @return Pair of (memory_usage, disk_usage) for loading and storage
     */
    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(milvus::cachinglayer::cid_t cid) const override;

    /**
     * @brief Get the cache key for this translator
     *
     * Returns a key that includes the aggregation factor and underlying key
     *
     * @return Cache key in format "agg_{factor}_{underlying_key}"
     */
    const std::string&
    key() const override;

    /**
     * @brief Load specified aggregate cells
     *
     * For each aggregate cell ID, loads the corresponding underlying cells,
     * merges their GroupChunks together, and returns the merged result.
     *
     * @param cids List of aggregate cell IDs to load
     * @return Vector of (aggregate_cell_id, merged_GroupChunk) pairs
     */
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::GroupChunk>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    /**
     * @brief Get the metadata object from the underlying translator
     *
     * @return Pointer to the underlying translator's metadata
     */
    milvus::cachinglayer::Meta*
    meta() override {
        return underlying_translator_->meta();
    }

    /**
     * @brief Calculate total storage bytes needed for loading aggregate cells
     *
     * Sums up the storage size for all underlying cells that comprise the requested aggregate cells.
     *
     * @param cids List of aggregate cell IDs
     * @return Total storage bytes required
     */
    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override;

 private:
    /**
     * @brief Get the underlying cell IDs that comprise an aggregate cell
     *
     * For aggregate cell cid, returns the range of underlying cell IDs:
     * [cid * aggregation_factor, min((cid + 1) * aggregation_factor, underlying_num_cells))
     *
     * @param aggregate_cid The aggregate cell ID
     * @return Vector of underlying cell IDs
     */
    std::vector<milvus::cachinglayer::cid_t>
    get_underlying_cell_ids(milvus::cachinglayer::cid_t aggregate_cid) const;

    std::unique_ptr<ManifestGroupTranslator> underlying_translator_;
    size_t aggregation_factor_;
    std::string key_;
};

}  // namespace milvus::segcore::storagev2translator
