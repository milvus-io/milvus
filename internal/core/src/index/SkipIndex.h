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
class FieldChunkMetricsTranslatorFromStatistics
    : public cachinglayer::Translator<index::FieldChunkMetrics> {
 public:
    FieldChunkMetricsTranslatorFromStatistics(
        int64_t segment_id,
        FieldId field_id,
        milvus::DataType data_type,
        std::vector<std::shared_ptr<parquet::Statistics>> statistics)
        : key_(fmt::format("skip_seg_{}_f_{}", segment_id, field_id.get())),
          data_type_(data_type),
          meta_(cachinglayer::StorageType::MEMORY,
                milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
                milvus::cachinglayer::CellDataType::OTHER,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                false) {
        for (auto& statistic : statistics) {
            cells_.emplace_back(
                std::move(builder_.Build(data_type_, statistic)));
        }
    }

    size_t
    num_cells() const override {
        return cells_.size();
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
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override {
        std::vector<std::pair<milvus::cachinglayer::cid_t,
                              std::unique_ptr<index::FieldChunkMetrics>>>
            cells;
        cells.reserve(cids.size());
        for (auto cid : cids) {
            cells.emplace_back(cid, cells_[cid]->Clone());
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
    milvus::DataType data_type_;
    index::SkipIndexStatsBuilder builder_;
    cachinglayer::Meta meta_;
    std::vector<std::unique_ptr<index::FieldChunkMetrics>> cells_;
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
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

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
    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        auto pw = GetFieldChunkMetrics(field_id, chunk_id);
        auto field_chunk_metrics = pw.get();
        return field_chunk_metrics->CanSkipUnaryRange(op_type,
                                                      index::Metrics{val});
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        auto pw = GetFieldChunkMetrics(field_id, chunk_id);
        auto field_chunk_metrics = pw.get();
        return field_chunk_metrics->CanSkipBinaryRange(
            index::Metrics{lower_val},
            index::Metrics{upper_val},
            lower_inclusive,
            upper_inclusive);
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::arith_value, bool>
    CanSkipBinaryArithRange(FieldId field_id,
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
            return CanSkipUnaryRange<T>(
                field_id, chunk_id, new_op_type, static_cast<T>(new_value_hp));
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
                // field * C > V
                if (right_operand == 0) {
                    // field * 0 > V => 0 > V. This doesn't depend on the field's range.
                    return false;
                }

                OpType new_op_type = op_type;
                if (right_operand < 0) {
                    new_op_type = FlipComparisonOperator(op_type);
                }
                return check_and_skip(value / right_operand, new_op_type);
            }
            case ArithOpType::Div: {
                // field / C > V
                if (right_operand == 0) {
                    // Division by zero. Cannot evaluate, so cannot skip.
                    return false;
                }

                OpType new_op_type = op_type;
                if (right_operand < 0) {
                    new_op_type = FlipComparisonOperator(op_type);
                }
                return check_and_skip(value * right_operand, new_op_type);
            }
            default:
                return false;
        }
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::arith_value, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const HighPrecisionType<T> value,
                            const HighPrecisionType<T> right_operand) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        auto pw = GetFieldChunkMetrics(field_id, chunk_id);
        auto field_chunk_metrics = pw.get();
        auto vals = std::vector<index::Metrics>{};
        vals.reserve(values.size());
        for (const auto& v : values) {
            vals.emplace_back(v);
        }
        return field_chunk_metrics->CanSkipIn(vals);
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return false;
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

    void
    LoadSkipFromStatistics(
        int64_t segment_id,
        milvus::FieldId field_id,
        milvus::DataType data_type,
        std::vector<std::shared_ptr<parquet::Statistics>> statistics) {
        auto translator =
            std::make_unique<FieldChunkMetricsTranslatorFromStatistics>(
                segment_id, field_id, data_type, statistics);
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
    GetFieldChunkMetrics(FieldId field_id, int chunk_id) const;

    std::unordered_map<
        FieldId,
        std::shared_ptr<cachinglayer::CacheSlot<index::FieldChunkMetrics>>>
        fieldChunkMetrics_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
