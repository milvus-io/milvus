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
#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "index/contracts/query/INgramReader.h"
#include <unordered_set>

namespace milvus::segcore::experimental {
namespace {
void
RequireReaderInput(bool condition, const char* message) {
    if (!condition) {
        throw SegcoreError(UnexpectedError, message);
    }
}

template <typename T>
UpdateValue
Own(const T& value) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return std::string(value);
    } else {
        return value;
    }
}

// The original index owns base matches. Only visible updated cells are
// reevaluated here; unchanged offsets and their values are never visited.
template <typename T>
class CorrectedIndexReader final
    : public index::IIndexReaderBase,
      public index::IScalarPredicateReader<T>,
      public index::INullReader,
      public index::INgramReader,
      public index::
          PatternMatchReaderAdapter<CorrectedIndexReader<T>, T, true> {
 public:
    CorrectedIndexReader(
        std::shared_ptr<const detail::UpdateReadBinding> binding,
        const index::IIndexReaderBase* original)
        : binding_(std::move(binding)),
          original_(original),
          predicate_(
              dynamic_cast<const index::IScalarPredicateReader<T>*>(original)),
          nulls_(dynamic_cast<const index::INullReader*>(original)) {
        RequireReaderInput(nulls_ != nullptr,
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
        binding_->records->VisitUpdates(
            binding_->field_id, binding_->query_ts, visit);
    }
    template <typename Eval>
    TargetBitmap
    Correct(TargetBitmap bitmap, Eval&& eval) const {
        binding_->records->RecordOldIndexRead();
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
            return !std::holds_alternative<std::monostate>(value) &&
                   eval(T(std::get<T>(value)));
        });
    }
    TargetBitmap
    CorrectEquality(TargetBitmap bitmap, const T& wanted, bool equal) const {
        binding_->records->RecordOldIndexRead();
        const bool build_validity = !validity_;
        auto validity = build_validity ? OriginalValidity() : TargetBitmap{};
        const auto value = Own(wanted);
        binding_->records->CorrectEquality(
            binding_->field_id,
            binding_->query_ts,
            value,
            equal,
            bitmap,
            build_validity ? &validity : nullptr);
        if (build_validity) {
            validity_.emplace(std::move(validity));
        }
        return bitmap;
    }
    TargetBitmap
    In(size_t n, const T* values) const override {
        RequireReaderInput(n == 0 || values != nullptr,
                           "Missing predicate values");

        if (n == 1) {
            return CorrectEquality(
                ReadOriginal([&] { return predicate_->In(n, values); }),
                values[0],
                true);
        }
        std::unordered_set<T> wanted;
        for (size_t i = 0; i < n; ++i) wanted.insert(values[i]);
        return CorrectValues(
            ReadOriginal([&] { return predicate_->In(n, values); }),
            [&](const T& value) { return wanted.count(value) != 0; });
    }
    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        RequireReaderInput(n == 0 || values != nullptr,
                           "Missing predicate values");

        if (n == 1) {
            return CorrectEquality(
                ReadOriginal([&] { return predicate_->NotIn(n, values); }),
                values[0],
                false);
        }
        std::unordered_set<T> wanted;
        for (size_t i = 0; i < n; ++i) wanted.insert(values[i]);
        return CorrectValues(
            ReadOriginal([&] { return predicate_->NotIn(n, values); }),
            [&](const T& value) { return wanted.count(value) == 0; });
    }
    TargetBitmap
    Range(const T& value, index::CompareOp op) const override {
        if (op == index::CompareOp::Equal || op == index::CompareOp::NotEqual) {
            return CorrectEquality(
                ReadOriginal([&] { return predicate_->Range(value, op); }),
                value,
                op == index::CompareOp::Equal);
        }
        return CorrectValues(
            ReadOriginal([&] { return predicate_->Range(value, op); }),
            [&](const T& v) {
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
            ReadOriginal([&] { return predicate_->Range(lo, li, hi, hi_i); }),
            [&](const T& v) {
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
        binding_->records->RecordOldIndexRead();
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
    PatternMatchIsExact(index::PatternOp op) const {
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
            return CorrectValues(
                ReadOriginal([&] { return reader->PatternMatch(pattern, op); }),
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
    template <typename Read>
    TargetBitmap
    ReadOriginal(Read&& read) const {
        return read();
    }
    TargetBitmap
    OriginalValidity() const {
        binding_->records->RecordOldIndexRead();
        return nulls_->IsNotNull();
    }
    // Owned by one expr reader, bound to its immutable Q_TS; never shared
    // between requests or stored in the expression result cache.
    mutable std::optional<TargetBitmap> validity_;
    std::shared_ptr<const detail::UpdateReadBinding> binding_;
    const index::IIndexReaderBase* original_;
    const index::IScalarPredicateReader<T>* predicate_;
    const index::INullReader* nulls_;
};

template <typename T>
class TypedValueReader final : public ValueReader,
                               public index::IScalarValueReader<T> {
 public:
    explicit TypedValueReader(
        std::shared_ptr<const detail::UpdateReadBinding> binding)
        : ValueReader(std::move(binding)) {
    }

    std::optional<index::owned_t<T>>
    Lookup(int64_t offset) const override {
        binding_->records->RecordRead();
        auto value = binding_->records->Lookup(
            binding_->field_id, offset, binding_->query_ts);
        if (!value || std::holds_alternative<std::monostate>(*value)) {
            return std::nullopt;
        }
        return std::get<index::owned_t<T>>(*value);
    }

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t, const T*, bool)>& out)
        const override {
        RequireReaderInput(count >= 0, "Negative gather count");
        RequireReaderInput(count == 0 || (offsets != nullptr && bool(out)),
                           "Missing gather offsets or callback");
        std::vector<int64_t> rows;
        if (count != 0) {
            rows.assign(offsets, offsets + count);
        }
        // Retain immutable values, then release the store lock before invoking
        // consumer callbacks. String views remain valid throughout each call.
        binding_->records->RecordRead();
        auto values = binding_->records->Gather(
            binding_->field_id, rows, binding_->query_ts);
        for (int64_t i = 0; i < count; ++i) {
            const auto& value = values[i];
            if (!value || std::holds_alternative<std::monostate>(*value)) {
                out(i, nullptr, false);
            } else {
                T typed = std::get<index::owned_t<T>>(*value);
                out(i, &typed, true);
            }
        }
    }
};

}  // namespace

std::unique_ptr<index::IIndexReaderBase>
ValueReader::CorrectOriginalIndex(
    const index::IIndexReaderBase* original) const {
    if (binding_->type == UpdateFieldType::Int64)
        return std::make_unique<CorrectedIndexReader<int64_t>>(binding_,
                                                               original);
    return std::make_unique<CorrectedIndexReader<std::string_view>>(binding_,
                                                                    original);
}
std::vector<UpdatesRecord::RangeUpdate>
ValueReader::ReadRangeUpdates(int64_t begin,
                              size_t count,
                              const TargetBitmapView* candidates,
                              size_t candidate_offset) const {
    binding_->records->RecordRead();
    return binding_->records->ReadRangeUpdates(binding_->field_id,
                                               binding_->query_ts,
                                               begin,
                                               count,
                                               candidates,
                                               candidate_offset);
}
UpdatesRecord::Values
ValueReader::ReadCandidates(const std::vector<int64_t>& offsets,
                            const TargetBitmapView* candidates) const {
    if (!candidates || candidates->empty())
        return ReadOffsets(offsets);
    RequireReaderInput(candidates->size() == offsets.size(),
                       "POC candidate size mismatch");
    std::vector<int64_t> selected;
    for (size_t i = 0; i < offsets.size(); ++i)
        if ((*candidates)[i])
            selected.push_back(offsets[i]);
    auto values = ReadOffsets(selected);
    UpdatesRecord::Values result(offsets.size());
    size_t n = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
        if ((*candidates)[i])
            result[i] = std::move(values[n++]);
    return result;
}

DataType
ValueReader::ValueType() const {
    return binding_->type == UpdateFieldType::Int64 ? DataType::INT64
                                                    : DataType::VARCHAR;
}

index::ReaderCaps
ValueReader::Caps() const {
    index::ReaderCaps caps;
    caps.value_lookup = true;
    caps.cheap_value_lookup = true;
    return caps;
}

std::vector<std::shared_ptr<const UpdateValue>>
ValueReader::Scan(int64_t begin_offset, size_t count) const {
    binding_->records->RecordRead();
    return binding_->records->Scan(
        binding_->field_id, begin_offset, count, binding_->query_ts);
}

std::vector<std::shared_ptr<const UpdateValue>>
ValueReader::ReadOffsets(const std::vector<int64_t>& offsets) const {
    binding_->records->RecordRead();
    return binding_->records->Gather(
        binding_->field_id, offsets, binding_->query_ts);
}

std::unique_ptr<proto::schema::FieldData>
ValueReader::ReadOutput(const int64_t* offsets,
                        int64_t count,
                        bool nullable) const {
    RequireReaderInput(count >= 0 && (count == 0 || offsets != nullptr),
                       "Invalid output offsets");
    std::vector<int64_t> rows;
    rows.reserve(count);
    for (int64_t i = 0; i < count; ++i) {
        // Search padding uses INVALID_SEG_OFFSET; preserve its output position.
        if (offsets[i] != -1) {
            rows.push_back(offsets[i]);
        }
    }
    auto values = ReadOffsets(rows);
    auto output = std::make_unique<proto::schema::FieldData>();
    output->set_field_id(binding_->field_id);
    output->set_type(static_cast<proto::schema::DataType>(ValueType()));
    auto* scalars = output->mutable_scalars();
    if (ValueType() == DataType::INT64) {
        scalars->mutable_long_data();
    } else {
        scalars->mutable_string_data();
    }
    size_t next = 0;
    for (int64_t i = 0; i < count; ++i) {
        auto value = offsets[i] == -1 ? nullptr : values[next++];
        const bool valid =
            value && !std::holds_alternative<std::monostate>(*value);
        if (nullable) {
            output->add_valid_data(valid);
        }
        if (ValueType() == DataType::INT64) {
            scalars->mutable_long_data()->add_data(
                valid ? std::get<int64_t>(*value) : 0);
        } else {
            scalars->mutable_string_data()->add_data(
                valid ? std::get<std::string>(*value) : "");
        }
    }
    return output;
}

std::unique_ptr<ValueReader>
UpdatesRecord::CreateReader(int64_t field_id, uint64_t query_ts) const {
    auto binding = std::make_shared<const detail::UpdateReadBinding>(
        CaptureReadBinding(field_id, query_ts));
    if (binding->type == UpdateFieldType::Int64)
        return std::make_unique<TypedValueReader<int64_t>>(std::move(binding));
    return std::make_unique<TypedValueReader<std::string_view>>(
        std::move(binding));
}
}  // namespace milvus::segcore::experimental
