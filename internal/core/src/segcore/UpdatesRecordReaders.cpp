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

#include <type_traits>

#include "common/EasyAssert.h"

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

template <typename T>
class TypedTempIndexReader final
    : public TempIndexReader,
      public index::IScalarPredicateReader<T>,
      public index::
          PatternMatchReaderAdapter<TypedTempIndexReader<T>, T, true> {
 public:
    explicit TypedTempIndexReader(
        std::shared_ptr<const detail::UpdateReadBinding> binding)
        : TempIndexReader(std::move(binding)) {
    }

    TargetBitmap
    In(size_t n, const T* values) const override {
        return ToBitmap(Matches(n, values));
    }

    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        auto matched = Matches(n, values);
        auto result = IsNotNull();
        for (auto offset : matched) {
            result.reset(offset);
        }
        return result;
    }

    TargetBitmap
    Range(const T& value, index::CompareOp op) const override {
        using index::CompareOp;
        switch (op) {
            case CompareOp::Equal:
                return In(1, &value);
            case CompareOp::NotEqual:
                return NotIn(1, &value);
            case CompareOp::GreaterThan:
            case CompareOp::GreaterEqual:
                return QueryRange(
                    UpdateRangeBound{Own(value), op == CompareOp::GreaterEqual},
                    std::nullopt);
            case CompareOp::LessThan:
            case CompareOp::LessEqual:
                return QueryRange(
                    std::nullopt,
                    UpdateRangeBound{Own(value), op == CompareOp::LessEqual});
        }
        throw SegcoreError(Unsupported, "Unsupported temp-index comparison");
    }

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override {
        return QueryRange(UpdateRangeBound{Own(lo), lo_inc},
                          UpdateRangeBound{Own(hi), hi_inc});
    }

    bool
    ShouldUseForOpImpl(index::PatternOp op, std::string_view) const {
        return op == index::PatternOp::PrefixMatch;
    }

    TargetBitmap
    PatternMatchImpl(std::string_view pattern, index::PatternOp op) const {
        if (op != index::PatternOp::PrefixMatch) {
            throw SegcoreError(
                Unsupported,
                "Temp index supports only literal prefix matching");
        }
        return ToBitmap(binding_->records->Prefix(
            binding_->field_id, std::string(pattern), binding_->query_ts));
    }

 private:
    std::vector<int64_t>
    Matches(size_t n, const T* values) const {
        RequireReaderInput(n == 0 || values != nullptr,
                           "Missing predicate values");
        std::vector<UpdateValue> owned;
        owned.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            owned.emplace_back(Own(values[i]));
        }
        return binding_->records->In(
            binding_->field_id, owned, binding_->query_ts);
    }

    TargetBitmap
    QueryRange(const std::optional<UpdateRangeBound>& lower,
               const std::optional<UpdateRangeBound>& upper) const {
        return ToBitmap(binding_->records->Range(
            binding_->field_id, lower, upper, binding_->query_ts));
    }
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

template <typename T>
UpdateReaders
MakeReaders(std::shared_ptr<const detail::UpdateReadBinding> binding) {
    UpdateReaders readers;
    readers.values = std::make_unique<TypedValueReader<T>>(binding);
    if (binding->has_index) {
        readers.temp_index = std::make_unique<TypedTempIndexReader<T>>(binding);
    }
    return readers;
}

}  // namespace

DataType
TempIndexReader::ValueType() const {
    return binding_->type == UpdateFieldType::Int64 ? DataType::INT64
                                                    : DataType::VARCHAR;
}

index::ReaderCaps
TempIndexReader::Caps() const {
    index::ReaderCaps caps;
    caps.predicate = true;
    caps.pattern_match = binding_->type == UpdateFieldType::VarChar;
    return caps;
}

TargetBitmap
TempIndexReader::ToBitmap(const std::vector<int64_t>& offsets) const {
    TargetBitmap result(Count(), false);
    for (auto offset : offsets) {
        result.set(offset);
    }
    return result;
}

TargetBitmap
TempIndexReader::IsNull() const {
    return ToBitmap(
        binding_->records->IsNull(binding_->field_id, binding_->query_ts));
}

TargetBitmap
TempIndexReader::IsNotNull() const {
    return ToBitmap(
        binding_->records->IsNotNull(binding_->field_id, binding_->query_ts));
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
    return binding_->records->Scan(
        binding_->field_id, begin_offset, count, binding_->query_ts);
}

UpdateReaders
UpdatesRecord::CreateReaders(int64_t field_id, uint64_t query_ts) const {
    auto binding = std::make_shared<const detail::UpdateReadBinding>(
        CaptureReadBinding(field_id, query_ts));
    switch (binding->type) {
        case UpdateFieldType::Int64:
            return MakeReaders<int64_t>(std::move(binding));
        case UpdateFieldType::VarChar:
            return MakeReaders<std::string_view>(std::move(binding));
    }
    throw SegcoreError(Unsupported, "Unsupported update field type");
}

}  // namespace milvus::segcore::experimental
