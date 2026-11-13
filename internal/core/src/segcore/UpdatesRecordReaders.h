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

#include <utility>
#include "pb/schema.pb.h"

#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "segcore/UpdatesRecord.h"

namespace milvus::segcore::experimental {

// Query-local wall-time counters for the POC index-correction profile.
// The expr layer measures the original predicate separately from these stages.
struct IndexCorrectionTimings {
    uint64_t original_predicate_ns = 0;
    uint64_t original_validity_ns = 0;
    uint64_t correction_ns = 0;
};
class IndexCorrectionProfile {
 public:
    virtual ~IndexCorrectionProfile() = default;
    virtual IndexCorrectionTimings
    CorrectionTimings() const = 0;
};

// Cross-cast to IScalarPredicateReader<int64_t/string_view> for point/range
// predicates; VARCHAR also exposes IPatternMatchReader (literal prefix only).
// No storage IO or reader registration is performed by these resident readers.
class TempIndexReader : public index::IIndexReaderBase,
                        public index::INullReader {
 public:
    uint64_t
    QueryTimestamp() const {
        return binding_->query_ts;
    }
    int64_t
    Count() const override {
        return binding_->row_count;
    }
    index::Domain
    CoordDomain() const override {
        return index::Domain::Row;
    }
    DataType
    ValueType() const override;
    index::ReaderCaps
    Caps() const override;
    TargetBitmap
    IsNull() const override;
    TargetBitmap
    IsNotNull() const override;

 protected:
    explicit TempIndexReader(
        std::shared_ptr<const detail::UpdateReadBinding> binding)
        : binding_(std::move(binding)) {
    }
    const std::shared_ptr<const detail::UpdateReadBinding> binding_;
};

// Cross-cast to IScalarValueReader<int64_t/string_view> for owning Lookup and
// callback Gather. NULL/absent values map to nullopt or valid=false, matching
// that contract. Scan preserves their distinction for a raw evaluator.
class ValueReader : public index::IIndexReaderBase {
 public:
    uint64_t
    QueryTimestamp() const {
        return binding_->query_ts;
    }
    int64_t
    Count() const override {
        return binding_->row_count;
    }
    index::Domain
    CoordDomain() const override {
        return index::Domain::Row;
    }
    std::unique_ptr<index::IIndexReaderBase>
    CorrectOriginalIndex(const index::IIndexReaderBase* original) const;
    // Mask positions are kept aligned but excluded rows are never looked up.
    std::vector<std::shared_ptr<const UpdateValue>>
    ReadCandidates(const std::vector<int64_t>& offsets,
                   const TargetBitmap* candidates) const;
    DataType
    ValueType() const override;
    index::ReaderCaps
    Caps() const override;
    std::vector<std::shared_ptr<const UpdateValue>>
    Scan(int64_t begin_offset, size_t count) const;
    std::vector<std::shared_ptr<const UpdateValue>>
    ReadOffsets(const std::vector<int64_t>& offsets) const;
    std::unique_ptr<proto::schema::FieldData>
    ReadOutput(const int64_t* offsets, int64_t count, bool nullable) const;

 protected:
    explicit ValueReader(
        std::shared_ptr<const detail::UpdateReadBinding> binding)
        : binding_(std::move(binding)) {
    }
    const std::shared_ptr<const detail::UpdateReadBinding> binding_;
};

// Both readers retain the same immutable binding and version-store ownership.
// Destroying the writer facade does not invalidate either reader. Later ordered
// mutations beyond Q_TS may update the store without changing this read view.
// Reader capabilities remain separate; they are not combined into one index.
struct UpdateReaders {
    std::unique_ptr<TempIndexReader>
        temp_index;  // nullptr when index is disabled
    std::unique_ptr<ValueReader> values;
};

}  // namespace milvus::segcore::experimental
