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

#pragma once

#include <cstdint>
#include <memory>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnInterface.h"
#include "parquet/statistics.h"
#include "index/skipindex_stats/SkipIndexStats.h"

namespace milvus {

// Lazily re-readable source of one field's per-chunk skip metrics: cells are
// built on first use and can be rebuilt by asking the source again, exactly
// like the column-backed translator below rebuilds from
// ChunkedColumnInterface::GetChunk. NOTE: rebuildable is not the same as
// evicted -- both skip-metrics translators still report support_eviction=false
// and a {0,0} size estimate, so the cache never reclaims these cells. Enabling
// that (now that rebuilding works) needs a real size estimate too, and is a
// follow-up.
//
// The source owns the INTERPRETATION as well as the read, which is what keeps
// this seam storage-neutral: the parquet implementation (see segcore) reads a
// footer and runs SkipIndexStatsBuilder over it, while a manifest/zone-map
// backed one can construct metrics directly. Whatever it reads from must stay
// alive for the duration of the call -- Arrow's BYTE_ARRAY min/max are
// string_views into the file metadata -- but since the built metrics deep-copy,
// they are safe to hand out afterwards.
class ChunkStatsSource {
 public:
    virtual ~ChunkStatsSource() = default;

    // Number of chunks this source can describe; must equal the column's
    // num_chunks() for the positional cell mapping to be correct.
    virtual int64_t
    num_chunks() const = 0;

    // The skip metrics of `chunk_id`, already interpreted. The source owns the
    // interpretation, so a new backing store (v3 manifest, Vortex zone maps, a
    // future range filter) only implements this -- it does not have to express
    // its statistics as parquet's, and a richer filter is just another
    // FieldChunkMetrics subclass. Must never return nullptr; a chunk with no
    // usable statistics yields NoneFieldChunkMetrics (never skips).
    virtual std::unique_ptr<index::FieldChunkMetrics>
    BuildChunkMetrics(int64_t chunk_id) = 0;
};

class FieldChunkMetricsTranslatorFromStatistics
    : public cachinglayer::Translator<index::FieldChunkMetrics> {
 public:
    FieldChunkMetricsTranslatorFromStatistics(
        int64_t segment_id,
        FieldId field_id,
        std::shared_ptr<ChunkStatsSource> stats_source)
        : key_(fmt::format("skip_seg_{}_f_{}", segment_id, field_id.get())),
          stats_source_(std::move(stats_source)),
          meta_(cachinglayer::StorageType::MEMORY,
                milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
                milvus::cachinglayer::CellDataType::OTHER,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                false) {
        AssertInfo(stats_source_ != nullptr,
                   "skip index stats source must not be null");
    }

    size_t
    num_cells() const override {
        return static_cast<size_t>(stats_source_->num_chunks());
    }

    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override {
        return uid;
    }

    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(
        milvus::cachinglayer::cid_t cid) const override {
        // TODO(tiered storage 1): provide a better estimation.
        return {{0, 0}, {0, 0}};
    }

    const std::string&
    key() const override {
        return key_;
    }

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<index::FieldChunkMetrics>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override {
        std::vector<std::pair<milvus::cachinglayer::cid_t,
                              std::unique_ptr<index::FieldChunkMetrics>>>
            cells;
        cells.reserve(cids.size());
        for (auto cid : cids) {
            // Rebuild this chunk's metrics from the source. Nothing is
            // retained between calls, so an evicted cell costs one (small,
            // usually cached) lookup to restore -- the same shape as the
            // column-backed translator below, which re-reads its chunk.
            cells.emplace_back(cid, stats_source_->BuildChunkMetrics(cid));
        }
        return cells;
    }

    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        return 0;
    }

 private:
    std::string key_;
    std::shared_ptr<ChunkStatsSource> stats_source_;
    cachinglayer::Meta meta_;
};

class FieldChunkMetricsTranslator
    : public cachinglayer::Translator<index::FieldChunkMetrics> {
 public:
    FieldChunkMetricsTranslator(int64_t segment_id,
                                FieldId field_id,
                                milvus::DataType data_type,
                                std::shared_ptr<ChunkedColumnInterface> column)
        : key_(fmt::format("skip_seg_{}_f_{}", segment_id, field_id.get())),
          data_type_(data_type),
          column_(column),
          meta_(cachinglayer::StorageType::MEMORY,
                milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
                milvus::cachinglayer::CellDataType::OTHER,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                false) {
    }

    size_t
    num_cells() const override {
        return column_->num_chunks();
    }
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override {
        return uid;
    }
    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(
        milvus::cachinglayer::cid_t cid) const override {
        // TODO(tiered storage 1): provide a better estimation.
        return {{0, 0}, {0, 0}};
    }
    const std::string&
    key() const override {
        return key_;
    }
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<index::FieldChunkMetrics>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        return 0;
    }

 private:
    std::string key_;
    milvus::DataType data_type_;
    cachinglayer::Meta meta_;
    std::shared_ptr<ChunkedColumnInterface> column_;
    index::SkipIndexStatsBuilder builder_;
};

class SkipIndex {
 private:
    template <typename T>
    struct IsAllowedType {
        static constexpr bool isAllowedType =
            std::is_integral<T>::value || std::is_floating_point<T>::value ||
            std::is_same<T, std::string>::value ||
            std::is_same<T, std::string_view>::value;
        static constexpr bool isDisabledType =
            std::is_same<T, milvus::Json>::value ||
            std::is_same<T, bool>::value;
        static constexpr bool value = isAllowedType && !isDisabledType;
        static constexpr bool arith_value =
            std::is_integral<T>::value && !std::is_same<T, bool>::value;
        static constexpr bool in_value = isAllowedType;
    };

    template <typename T>
    using HighPrecisionType =
        std::conditional_t<std::is_integral_v<T> && !std::is_same_v<bool, T>,
                           int64_t,
                           T>;

 public:
    std::shared_ptr<SkipIndex>
    Clone() const {
        auto cloned = std::make_shared<SkipIndex>();
        std::shared_lock lck(mutex_);
        cloned->fieldChunkMetrics_ = fieldChunkMetrics_;
        return cloned;
    }

    // Drop a field's skip metrics. Callers erase before (re)installing so a
    // replaced column -- e.g. ComputeDiffBinlogs remapping a storage v2 grouped
    // column to a v1 per-field binlog -- cannot keep being pruned by the
    // previous load's slot when the new one installs nothing.
    void
    Erase(FieldId field_id) {
        std::unique_lock lck(mutex_);
        fieldChunkMetrics_.erase(field_id);
    }

    // Whether this field has skip metrics at all. A field with none still
    // answers every CanSkip* query -- GetFieldChunkMetrics hands back a shared
    // NoneFieldChunkMetrics that never skips -- so a caller cannot tell "judged
    // and found nothing to prune" from "there was nothing to judge with". That
    // distinction only matters to the effectiveness metrics: counting the
    // second case would report a 0% prune ratio for every numeric and VARCHAR
    // expression on a default (flag off) or storage v3 deployment, where no
    // metrics are installed at all, and bury the collections that do have them.
    bool
    HasFieldMetrics(FieldId field_id) const {
        std::shared_lock lck(mutex_);
        return fieldChunkMetrics_.find(field_id) != fieldChunkMetrics_.end();
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(milvus::OpContext* op_ctx,
                      FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        auto pw = GetFieldChunkMetrics(op_ctx, field_id, chunk_id);
        auto field_chunk_metrics = pw.get();
        return field_chunk_metrics->CanSkipUnaryRange(op_type,
                                                      index::Metrics{val});
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return CanSkipUnaryRange<T>(nullptr, field_id, chunk_id, op_type, val);
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(milvus::OpContext* op_ctx,
                      FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return CanSkipUnaryRange<T>(nullptr, field_id, chunk_id, op_type, val);
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(milvus::OpContext* op_ctx,
                       FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        auto pw = GetFieldChunkMetrics(op_ctx, field_id, chunk_id);
        auto field_chunk_metrics = pw.get();
        return field_chunk_metrics->CanSkipBinaryRange(
            index::Metrics{lower_val},
            index::Metrics{upper_val},
            lower_inclusive,
            upper_inclusive);
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return CanSkipBinaryRange<T>(nullptr,
                                     field_id,
                                     chunk_id,
                                     lower_val,
                                     upper_val,
                                     lower_inclusive,
                                     upper_inclusive);
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(milvus::OpContext* op_ctx,
                       FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return CanSkipBinaryRange<T>(nullptr,
                                     field_id,
                                     chunk_id,
                                     lower_val,
                                     upper_val,
                                     lower_inclusive,
                                     upper_inclusive);
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::arith_value, bool>
    CanSkipBinaryArithRange(milvus::OpContext* op_ctx,
                            FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const HighPrecisionType<T> value,
                            const HighPrecisionType<T> right_operand) const {
        auto check_and_skip = [&](HighPrecisionType<T> new_value_hp,
                                  OpType new_op_type) {
            if constexpr (std::is_integral_v<T>) {
                if (new_value_hp > std::numeric_limits<T>::max() ||
                    new_value_hp < std::numeric_limits<T>::min()) {
                    // Overflow detected. The transformed value cannot be represented by T.
                    // We cannot make a safe comparison with the chunk's min/max.
                    return false;
                }
            }
            return CanSkipUnaryRange<T>(op_ctx,
                                        field_id,
                                        chunk_id,
                                        new_op_type,
                                        static_cast<T>(new_value_hp));
        };
        switch (arith_type) {
            case ArithOpType::Add: {
                // field + C > V  =>  field > V - C
                return check_and_skip(value - right_operand, op_type);
            }
            case ArithOpType::Sub: {
                // field - C > V  =>  field > V + C
                return check_and_skip(value + right_operand, op_type);
            }
            case ArithOpType::Mul: {
                // field * C <op> V cannot be inverted into a range on `field`
                // by integer division: this overload is only instantiated for
                // integral T, so `value / right_operand` truncates and the
                // rewritten predicate is strictly narrower than the original.
                // e.g. `field * 2 < 3` holds for field == 1 (2 < 3), but
                // rewrites to `field < 3/2 == 1`, which prunes a chunk of
                // [1,1] that actually matches -- a silent dropped row.
                // Inverting this correctly needs per-comparator floor/ceil
                // rules; until then do not prune (never skipping is safe).
                return false;
            }
            case ArithOpType::Div: {
                // field / C > V
                // Same truncation problem as Mul, in the other direction:
                // integer division maps a RANGE of field values onto one
                // result, so multiplying the bound back is not the inverse.
                // e.g. `field / 2 == 1` holds for field in {2,3}, but rewrites
                // to `field == 2`, which prunes a chunk of [3,3] that actually
                // matches. Do not prune until the exact rules are implemented.
                return false;
            }
            default:
                return false;
        }
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::arith_value, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const HighPrecisionType<T> value,
                            const HighPrecisionType<T> right_operand) const {
        return CanSkipBinaryArithRange<T>(nullptr,
                                          field_id,
                                          chunk_id,
                                          op_type,
                                          arith_type,
                                          value,
                                          right_operand);
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::arith_value, bool>
    CanSkipBinaryArithRange(milvus::OpContext* op_ctx,
                            FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const HighPrecisionType<T> value,
                            const HighPrecisionType<T> right_operand) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::arith_value, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const HighPrecisionType<T> value,
                            const HighPrecisionType<T> right_operand) const {
        return CanSkipBinaryArithRange<T>(nullptr,
                                          field_id,
                                          chunk_id,
                                          op_type,
                                          arith_type,
                                          value,
                                          right_operand);
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        auto pw = GetFieldChunkMetrics(op_ctx, field_id, chunk_id);
        auto field_chunk_metrics = pw.get();
        auto vals = std::vector<index::Metrics>{};
        vals.reserve(values.size());
        for (const auto& v : values) {
            vals.emplace_back(v);
        }
        return field_chunk_metrics->CanSkipIn(vals);
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return CanSkipInQuery<T>(nullptr, field_id, chunk_id, values);
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(milvus::OpContext* op_ctx,
                   FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return CanSkipInQuery<T>(nullptr, field_id, chunk_id, values);
    }

    void
    LoadSkip(int64_t segment_id,
             milvus::FieldId field_id,
             milvus::DataType data_type,
             std::shared_ptr<ChunkedColumnInterface> column) {
        auto translator = std::make_unique<FieldChunkMetricsTranslator>(
            segment_id, field_id, data_type, column);
        auto cache_slot = cachinglayer::Manager::GetInstance()
                              .CreateCacheSlot<index::FieldChunkMetrics>(
                                  std::move(translator));

        std::unique_lock lck(mutex_);
        fieldChunkMetrics_[field_id] = std::move(cache_slot);
    }

    // Install a lazily re-readable statistics source (storage v2: the parquet
    // footer). Cells are built on demand and stay evictable/rebuildable, just
    // like the column-backed LoadSkip above -- nothing is retained eagerly, and
    // the source keeps its reader alive across each Build so the BYTE_ARRAY
    // min/max views never dangle.
    //
    // CONTRACT: cells are POSITIONAL -- cell i describes chunk i -- so
    // `stats_source->num_chunks()` MUST equal the installed column's
    // num_chunks(); a mismatch would prune the wrong chunks (dropped rows).
    // Callers verify this before installing (see the num_chunks() check in
    // ChunkedSegmentSealedImpl::load_field_data_common).
    void
    LoadSkipFromStatsSource(int64_t segment_id,
                            milvus::FieldId field_id,
                            std::shared_ptr<ChunkStatsSource> stats_source) {
        auto translator =
            std::make_unique<FieldChunkMetricsTranslatorFromStatistics>(
                segment_id, field_id, std::move(stats_source));
        auto cache_slot = cachinglayer::Manager::GetInstance()
                              .CreateCacheSlot<index::FieldChunkMetrics>(
                                  std::move(translator));

        std::unique_lock lck(mutex_);
        fieldChunkMetrics_[field_id] = std::move(cache_slot);
    }

 private:
    OpType
    FlipComparisonOperator(OpType op) const {
        switch (op) {
            case OpType::GreaterThan:
                return OpType::LessThan;
            case OpType::GreaterEqual:
                return OpType::LessEqual;
            case OpType::LessThan:
                return OpType::GreaterThan;
            case OpType::LessEqual:
                return OpType::GreaterEqual;
            // OpType::Equal and OpType::NotEqual do not flip
            default:
                return op;
        }
    }

    const cachinglayer::PinWrapper<const index::FieldChunkMetrics*>
    GetFieldChunkMetrics(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         int chunk_id) const;

    const cachinglayer::PinWrapper<const index::FieldChunkMetrics*>
    GetFieldChunkMetrics(FieldId field_id, int chunk_id) const {
        return GetFieldChunkMetrics(nullptr, field_id, chunk_id);
    }

    std::unordered_map<
        FieldId,
        std::shared_ptr<cachinglayer::CacheSlot<index::FieldChunkMetrics>>>
        fieldChunkMetrics_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
