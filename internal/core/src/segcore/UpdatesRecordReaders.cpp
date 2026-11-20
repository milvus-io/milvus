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

#include "segcore/UpdatesRecordReaders.h"

#include <unordered_set>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"

namespace milvus::segcore {
namespace {
// Exactness belongs to the pinned index family. Only VARCHAR exposes this
// interface (e.g. FM LIKE requires a final raw recheck).
template <typename Derived, typename T>
class CorrectedPatternReader {};

template <typename Derived>
class CorrectedPatternReader<Derived, std::string_view>
    : public index::PatternMatchReaderAdapter<Derived, std::string_view, true> {
 public:
    bool
    PatternMatchIsExact(index::PatternOp op) const override {
        return static_cast<const Derived*>(this)->OriginalPatternIsExact(op);
    }
};

// The original index owns base matches. Only visible updated cells are
// reevaluated here; unchanged offsets and their values are never visited.
template <typename T>
class CorrectedIndexReader final
    : public index::IIndexReaderBase,
      public index::IScalarPredicateReader<T>,
      public index::INullReader,
      public index::INgramReader,
      public CorrectedPatternReader<CorrectedIndexReader<T>, T> {
 public:
    CorrectedIndexReader(const UpdateReadBinding& binding,
                         const index::IIndexReaderBase* original)
        : binding_(binding),
          original_(original),
          predicate_(
              dynamic_cast<const index::IScalarPredicateReader<T>*>(original)),
          nulls_(dynamic_cast<const index::INullReader*>(original)) {
        AssertInfo(nulls_ != nullptr,
                   "Original reader has no validity interface");
    }
    int64_t
    Count() const override {
        return original_->Count();
    }
    DataType
    ValueType() const override {
        return original_->ValueType();
    }
    index::Domain
    CoordDomain() const override {
        return original_->CoordDomain();
    }
    index::ReaderCaps
    Caps() const override {
        auto caps = original_->Caps();
        caps.value_lookup = false;
        caps.cheap_value_lookup = false;
        return caps;
    }
    void
    Prefetch() const override {
        original_->Prefetch();
    }
    template <typename Visit>
    void
    VisitCorrectionUpdates(Visit&& visit) const {
        binding_.records.VisitUpdates(
            binding_.field,
            binding_.timestamp,
            [&](int64_t row, const UpdateValue& value) {
                if (row < Count())
                    visit(row, value);
            });
    }
    template <typename Eval>
    TargetBitmap
    Correct(TargetBitmap bitmap, Eval&& eval) const {
        // Predicate evaluation precedes IsNotNull in the scalar expr path.
        // Produce both bitmaps while selecting each visible version only once.
        const bool build_validity = !validity_;
        auto validity = build_validity ? OriginalValidity() : TargetBitmap{};
        VisitCorrectionUpdates([&](int64_t offset, const auto& value) {
            bitmap[offset] = eval(value);
            if (build_validity) {
                validity[offset] =
                    !std::holds_alternative<std::monostate>(value);
            }
        });
        if (build_validity) {
            // Publish only after the complete traversal succeeds.
            validity_.emplace(std::move(validity));
        }
        return bitmap;
    }
    template <typename Eval>
    TargetBitmap
    CorrectValues(TargetBitmap bitmap, Eval&& eval) const {
        return Correct(std::move(bitmap), [&](const auto& value) {
            if (std::holds_alternative<std::monostate>(value))
                return false;
            if constexpr (std::is_same_v<T, std::string_view>)
                return eval(*std::get<UpdateString>(value));
            else
                return eval(std::get<T>(value));
        });
    }
    TargetBitmap
    CorrectEquality(TargetBitmap bitmap, const T& wanted, bool equal) const {
        return CorrectValues(std::move(bitmap), [&](const T& v) {
            return (v == wanted) == equal;
        });
    }
    TargetBitmap
    In(size_t n, const T* values) const override {
        AssertInfo(n == 0 || values != nullptr, "Missing predicate values");

        if (n == 1) {
            return CorrectEquality(predicate_->In(n, values), values[0], true);
        }
        std::unordered_set<T> wanted;
        for (size_t i = 0; i < n; ++i) wanted.insert(values[i]);
        return CorrectValues(predicate_->In(n, values), [&](const T& value) {
            return wanted.count(value) != 0;
        });
    }
    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        AssertInfo(n == 0 || values != nullptr, "Missing predicate values");

        if (n == 1) {
            return CorrectEquality(
                predicate_->NotIn(n, values), values[0], false);
        }
        std::unordered_set<T> wanted;
        for (size_t i = 0; i < n; ++i) wanted.insert(values[i]);
        return CorrectValues(predicate_->NotIn(n, values), [&](const T& value) {
            return wanted.count(value) == 0;
        });
    }
    TargetBitmap
    Range(const T& value, index::CompareOp op) const override {
        return CorrectValues(predicate_->Range(value, op), [&](const T& v) {
            switch (op) {
                case index::CompareOp::Equal:
                    return v == value;
                case index::CompareOp::NotEqual:
                    return v != value;
                case index::CompareOp::GreaterThan:
                    return v > value;
                case index::CompareOp::GreaterEqual:
                    return v >= value;
                case index::CompareOp::LessThan:
                    return v < value;
                case index::CompareOp::LessEqual:
                    return v <= value;
            }
            return false;
        });
    }
    TargetBitmap
    Range(const T& lo, bool li, const T& hi, bool hi_i) const override {
        return CorrectValues(
            predicate_->Range(lo, li, hi, hi_i), [&](const T& v) {
                return (li ? v >= lo : v > lo) && (hi_i ? v <= hi : v < hi);
            });
    }
    TargetBitmap
    IsNull() const override {
        auto result = IsNotNull();
        result.flip();
        return result;
    }
    TargetBitmap
    IsNotNull() const override {
        if (!validity_) {
            // Standalone null predicates still work without a predicate call.
            auto validity = OriginalValidity();

            VisitCorrectionUpdates([&](int64_t offset, const auto& value) {
                validity[offset] =
                    !std::holds_alternative<std::monostate>(value);
            });
            validity_.emplace(std::move(validity));
        }
        return validity_->clone();
    }
    bool
    CanHandleConditionTree(
        const RegexLiteralCondition& condition) const override {
        auto* reader = dynamic_cast<const index::INgramReader*>(original_);
        return reader && reader->CanHandleConditionTree(condition);
    }
    void
    ConditionTreeCandidates(const RegexLiteralCondition& condition,
                            TargetBitmap& candidates) const override {
        auto parent = candidates.clone();
        dynamic_cast<const index::INgramReader*>(original_)
            ->ConditionTreeCandidates(condition, candidates);
        IncludeUpdatedCandidates(parent, candidates);
    }
    bool
    CanHandle(std::string_view literal, index::PatternOp op) const override {
        auto* reader = dynamic_cast<const index::INgramReader*>(original_);
        return reader && reader->CanHandle(literal, op);
    }
    bool
    CandidatesAreExact(std::string_view, index::PatternOp) const override {
        return false;  // Updated candidates require the existing evaluator.
    }
    void
    Candidates(std::string_view literal,
               index::PatternOp op,
               TargetBitmap& candidates) const override {
        auto parent = candidates.clone();
        dynamic_cast<const index::INgramReader*>(original_)->Candidates(
            literal, op, candidates);
        IncludeUpdatedCandidates(parent, candidates);
    }
    void
    IncludeUpdatedCandidates(const TargetBitmap& parent,
                             TargetBitmap& candidates) const {
        VisitCorrectionUpdates([&](int64_t offset, const auto& value) {
            candidates[offset] = parent[offset] &&
                                 !std::holds_alternative<std::monostate>(value);
        });
    }
    bool
    ShouldUseForOpImpl(index::PatternOp op, std::string_view pattern) const {
        auto* reader =
            dynamic_cast<const index::IPatternMatchReader*>(original_);
        return reader && reader->ShouldUseForOp(op, pattern);
    }
    bool
    OriginalPatternIsExact(index::PatternOp op) const {
        return dynamic_cast<const index::IPatternMatchReader*>(original_)
            ->PatternMatchIsExact(op);
    }
    TargetBitmap
    PatternMatchImpl(std::string_view pattern, index::PatternOp op) const {
        if constexpr (std::is_same_v<T, std::string_view>) {
            auto* reader =
                dynamic_cast<const index::IPatternMatchReader*>(original_);

            std::unique_ptr<LikePatternMatcher> like;
            std::unique_ptr<PartialRegexMatcher> regex;
            if (op == index::PatternOp::Match)
                like =
                    std::make_unique<LikePatternMatcher>(std::string(pattern));
            if (op == index::PatternOp::RegexMatch)
                regex =
                    std::make_unique<PartialRegexMatcher>(std::string(pattern));
            return CorrectValues(reader->PatternMatch(pattern, op),
                                 [&](std::string_view v) {
                                     switch (op) {
                                         case index::PatternOp::PrefixMatch:
                                             return v.starts_with(pattern);
                                         case index::PatternOp::PostfixMatch:
                                             return v.ends_with(pattern);
                                         case index::PatternOp::InnerMatch:
                                             return v.find(pattern) != v.npos;
                                         case index::PatternOp::Match:
                                             return (*like)(v);
                                         case index::PatternOp::RegexMatch:
                                             return (*regex)(v);
                                     }
                                     return false;
                                 });
        } else {
            throw SegcoreError(Unsupported,
                               "Pattern correction requires VARCHAR");
        }
    }

 private:
    TargetBitmap
    OriginalValidity() const {
        return binding_.nullable ? nulls_->IsNotNull()
                                 : TargetBitmap(Count(), true);
    }
    // Owned by one expr reader, bound to its immutable Q_TS; never shared
    // between requests or stored in the expression result cache.
    mutable std::optional<TargetBitmap> validity_;
    const UpdateReadBinding& binding_;
    const index::IIndexReaderBase* original_;
    const index::IScalarPredicateReader<T>* predicate_;
    const index::INullReader* nulls_;
};

}  // namespace

std::unique_ptr<index::IIndexReaderBase>
UpdateReadBinding::CorrectOriginalIndex(
    const index::IIndexReaderBase* original) const {
    switch (type) {
        case DataType::BOOL:
            return std::make_unique<CorrectedIndexReader<bool>>(*this,
                                                                original);
        case DataType::INT8:
            return std::make_unique<CorrectedIndexReader<int8_t>>(*this,
                                                                  original);
        case DataType::INT16:
            return std::make_unique<CorrectedIndexReader<int16_t>>(*this,
                                                                   original);
        case DataType::INT32:
            return std::make_unique<CorrectedIndexReader<int32_t>>(*this,
                                                                   original);
        case DataType::INT64:
            return std::make_unique<CorrectedIndexReader<int64_t>>(*this,
                                                                   original);
        case DataType::TIMESTAMPTZ:
            return std::make_unique<CorrectedIndexReader<int64_t>>(*this,
                                                                   original);
        case DataType::VARCHAR:
            return std::make_unique<CorrectedIndexReader<std::string_view>>(
                *this, original);
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported scalar update index type {}",
                      type);
    }
}
}  // namespace milvus::segcore
