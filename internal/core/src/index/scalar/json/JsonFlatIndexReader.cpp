// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/scalar/json/JsonFlatIndexReader.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/JsonUtils.h"
#include "common/RegexQuery.h"
#include "common/Utils.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "storage/artifact/LocalDirectory.h"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

void
AddBytes(size_t& total, size_t bytes, std::string_view object) {
    if (bytes > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataFormatBroken, "{} memory size overflows", object);
    }
    total += bytes;
}

int64_t
ToUsageBytes(size_t bytes, std::string_view object) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(
            DataFormatBroken, "{} resource size exceeds int64 domain", object);
    }
    return static_cast<int64_t>(bytes);
}

size_t
StringHeapBytes(const std::string& value) {
    const auto inline_capacity = std::string{}.capacity();
    if (value.capacity() <= inline_capacity) {
        return 0;
    }
    if (value.capacity() == std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken, "JSON path memory size overflows");
    }
    return value.capacity() + 1;
}

void
ValidateJsonPointer(std::string_view path, std::string_view object) {
    if (path.find('\0') != std::string_view::npos) {
        ThrowInfo(DataTypeInvalid, "{} contains an embedded NUL", object);
    }
    if (!path.empty() && path.front() != '/') {
        ThrowInfo(
            DataTypeInvalid, "{} must be empty or start with '/'", object);
    }
    try {
        (void)parse_json_pointer(std::string(path));
    } catch (const std::invalid_argument& error) {
        ThrowInfo(DataTypeInvalid, "invalid {}: {}", object, error.what());
    }
}

// A null result means the selected JsonFlat index cannot represent this path
// shape. It is deliberately different from a supported path that happens to
// have no values in any row.
std::optional<std::string>
ResolveTantivyPath(std::string_view root, std::string_view path) {
    ValidateJsonPointer(path, "JSON query path");

    std::string_view relative;
    if (root.empty()) {
        relative = path;
    } else if (path == root) {
        relative = {};
    } else if (path.size() > root.size() && path.starts_with(root) &&
               path[root.size()] == '/') {
        relative = path.substr(root.size());
    } else {
        return std::nullopt;
    }

    auto tokens = parse_json_pointer(std::string(relative));
    if (!relative.empty() && relative.back() == '/') {
        tokens.emplace_back();
    }
    for (const auto& token : tokens) {
        if (!token.empty() && milvus::IsInteger(token)) {
            return std::nullopt;
        }
    }

    std::string tantivy_path;
    tantivy_path.reserve(relative.size());
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (i != 0) {
            tantivy_path.push_back('.');
        }
        for (const char ch : tokens[i]) {
            if (ch == '.' || ch == '\\') {
                tantivy_path.push_back('\\');
            }
            tantivy_path.push_back(ch);
        }
    }
    return tantivy_path;
}

template <typename T>
void
ValidateValues(size_t n, const T* values, std::string_view operation) {
    AssertInfo(n == 0 || values != nullptr,
               "JSON {} received null values with non-zero count",
               operation);
}

std::string
OwnString(std::string_view value) {
    return value.empty() ? std::string{}
                         : std::string(value.data(), value.size());
}

std::vector<std::string>
OwnStrings(size_t n, const std::string_view* values) {
    ValidateValues(n, values, "In");
    std::vector<std::string> result;
    result.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        result.push_back(OwnString(values[i]));
    }
    return result;
}

}  // namespace

JsonFlatIndexReaderState::JsonFlatIndexReaderState(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string field_path_prefix,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    bool mmap,
    size_t engine_bytes,
    size_t engine_path_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      field_path_prefix_(std::move(field_path_prefix)),
      mmap_(mmap),
      engine_bytes_(engine_bytes),
      engine_path_bytes_(engine_path_bytes) {
    AssertInfo(engine_ != nullptr, "JSON flat reader requires an engine");
    AssertInfo(null_offsets != nullptr,
               "JSON flat reader requires null-offset state");
    AssertInfo(!mmap_ || directory_ != nullptr,
               "mmap JSON flat reader requires a directory owner");
    ValidateJsonPointer(field_path_prefix_, "JSON flat root path");
    count_ = engine_->count();

    size_t previous = 0;
    bool first = true;
    for (const auto offset : *null_offsets) {
        if ((!first && offset <= previous) || offset >= count_) {
            ThrowInfo(DataFormatBroken,
                      "invalid JSON flat null offset {} for count {}",
                      offset,
                      count_);
        }
        previous = offset;
        first = false;
    }

    // See the valid_bitmap_ declaration: materialize field validity once here
    // so no query replays the offsets, and keep the all-valid case
    // allocation-free.
    if (null_offsets->empty()) {
        all_valid_ = true;
    } else {
        valid_bitmap_ = TargetBitmap(count_, true);
        for (const auto offset : *null_offsets) {
            valid_bitmap_.reset(offset);
        }
    }

    size_t heap = sizeof(JsonFlatIndexReaderState);
    AddBytes(
        heap, sizeof(milvus::tantivy::TantivyIndexWrapper), "JSON flat reader");
    AddBytes(heap, engine_path_bytes_, "JSON flat reader");
    AddBytes(heap, StringHeapBytes(field_path_prefix_), "JSON flat reader");
    AddBytes(heap, valid_bitmap_.size_in_bytes(), "JSON flat validity");
    if (directory_ != nullptr) {
        AddBytes(heap, directory_->HeapBytes(), "JSON flat reader");
    }
    if (!mmap_) {
        AddBytes(heap, engine_bytes_, "JSON flat reader");
    }
    heap_bytes_ = ToUsageBytes(heap, "JSON flat reader");
    file_bytes_ = mmap_ ? ToUsageBytes(engine_bytes_, "JSON flat reader") : 0;
}

std::shared_ptr<const JsonFlatIndexReaderState>
JsonFlatIndexReaderState::Create(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::string field_path_prefix,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    bool mmap,
    size_t engine_bytes,
    size_t engine_path_bytes) {
    return std::shared_ptr<const JsonFlatIndexReaderState>(
        new JsonFlatIndexReaderState(std::move(directory),
                                     std::move(engine),
                                     std::move(field_path_prefix),
                                     std::move(null_offsets),
                                     mmap,
                                     engine_bytes,
                                     engine_path_bytes));
}

milvus::tantivy::TantivyIndexWrapper&
JsonFlatIndexReaderState::Engine() const {
    return *engine_;
}

const std::string&
JsonFlatIndexReaderState::RootPath() const {
    return field_path_prefix_;
}

uint32_t
JsonFlatIndexReaderState::Count() const {
    return count_;
}

int64_t
JsonFlatIndexReaderState::HeapBytes() const {
    return heap_bytes_;
}

int64_t
JsonFlatIndexReaderState::FileBytes() const {
    return file_bytes_;
}

TargetBitmap
JsonFlatIndexReaderState::FieldIsNull() const {
    if (all_valid_) {
        return TargetBitmap(count_);
    }
    auto result = valid_bitmap_.clone();
    result.flip();
    return result;
}

TargetBitmap
JsonFlatIndexReaderState::FieldIsNotNull() const {
    if (all_valid_) {
        return TargetBitmap(count_, true);
    }
    return valid_bitmap_.clone();
}

namespace {

class JsonPathReaderBase : public IIndexReaderBase, public INullReader {
 public:
    JsonPathReaderBase(const JsonFlatIndexReaderState* state,
                       std::string tantivy_path,
                       ::JsonExistValueType comparable_type)
        : state_(state),
          tantivy_path_(std::move(tantivy_path)),
          comparable_type_(comparable_type) {
        AssertInfo(state_ != nullptr,
                   "JSON path reader requires pinned field state");
    }

    Domain
    CoordDomain() const override {
        return Domain::Row;
    }

    int64_t
    Count() const override {
        return static_cast<int64_t>(state_->Count());
    }

    int64_t
    MemoryUsage() const override {
        // Shared engine/directory bytes belong to the root cache cell. This
        // view still reports its own object and bound-path allocation.
        size_t bytes = ObjectBytes();
        AddBytes(bytes, StringHeapBytes(tantivy_path_), "JSON path reader");
        return ToUsageBytes(bytes, "JSON path reader");
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return {MemoryUsage(), 0};
    }

    TargetBitmap
    IsNull() const override {
        auto result = IsNotNull();
        result.flip();
        return result;
    }

    TargetBitmap
    IsNotNull() const override {
        TargetBitmap result(state_->Count());
        state_->Engine().json_exist_query(
            tantivy_path_, false, comparable_type_, &result);
        return result;
    }

 protected:
    const JsonFlatIndexReaderState*
    State() const {
        return state_;
    }

    milvus::tantivy::TantivyIndexWrapper&
    Engine() const {
        return state_->Engine();
    }

    const std::string&
    Path() const {
        return tantivy_path_;
    }

    virtual size_t
    ObjectBytes() const = 0;

 private:
    const JsonFlatIndexReaderState* state_{nullptr};
    std::string tantivy_path_;
    ::JsonExistValueType comparable_type_{::JsonExistValueType::Any};
};

template <typename Reader, typename T>
TargetBitmap
NotIn(const Reader& reader, size_t n, const T* values) {
    auto result = reader.In(n, values);
    result.flip();
    result &= reader.IsNotNull();
    return result;
}

class JsonBoolPathReader final : public JsonPathReaderBase,
                                 public IScalarPredicateReader<bool> {
 public:
    JsonBoolPathReader(const JsonFlatIndexReaderState* state, std::string path)
        : JsonPathReaderBase(
              state, std::move(path), ::JsonExistValueType::Bool) {
    }

    ReaderCaps
    Caps() const override {
        return ReaderCaps{.predicate = true, .exact = true};
    }

    DataType
    ValueType() const override {
        return DataType::BOOL;
    }

    size_t
    ObjectBytes() const override {
        return sizeof(JsonBoolPathReader);
    }

    TargetBitmap
    In(size_t n, const bool* values) const override {
        ValidateValues(n, values, "bool In");
        TargetBitmap result(Count());
        if (n != 0) {
            Engine().json_terms_query(Path(), values, n, &result);
        }
        return result;
    }

    TargetBitmap
    NotIn(size_t n, const bool* values) const override {
        return milvus::index::NotIn(*this, n, values);
    }

    TargetBitmap
    Range(const bool& value, CompareOp op) const override {
        if (op == CompareOp::Equal) {
            return In(1, &value);
        }
        if (op == CompareOp::NotEqual) {
            return NotIn(1, &value);
        }
        switch (op) {
            case CompareOp::GreaterThan:
                return Range(value, false, true, true);
            case CompareOp::GreaterEqual:
                return Range(value, true, true, true);
            case CompareOp::LessThan:
                return Range(false, true, value, false);
            case CompareOp::LessEqual:
                return Range(false, true, value, true);
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                break;
        }
        return TargetBitmap(Count());
    }

    TargetBitmap
    Range(const bool& lo,
          bool lo_inc,
          const bool& hi,
          bool hi_inc) const override {
        // Tantivy's JSON range query does not support boolean terms.
        bool values[2];
        size_t count = 0;
        for (const bool value : {false, true}) {
            if ((value > lo || (lo_inc && value == lo)) &&
                (value < hi || (hi_inc && value == hi))) {
                values[count++] = value;
            }
        }
        return In(count, values);
    }
};

class NumericRange {
 public:
    using U64Range = std::optional<std::pair<uint64_t, uint64_t>>;
    using I64Range = std::optional<std::pair<int64_t, int64_t>>;

    template <typename Predicate>
    static std::optional<uint64_t>
    FirstU64Where(Predicate pred) {
        constexpr auto min = std::numeric_limits<uint64_t>::min();
        constexpr auto max = std::numeric_limits<uint64_t>::max();
        if (pred(min)) {
            return min;
        }
        if (!pred(max)) {
            return std::nullopt;
        }
        uint64_t low = min;
        uint64_t high = max;
        while (low < high) {
            const auto mid = low + (high - low) / 2;
            if (pred(mid)) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    template <typename Predicate>
    static std::optional<uint64_t>
    LastU64Where(Predicate pred) {
        constexpr auto min = std::numeric_limits<uint64_t>::min();
        constexpr auto max = std::numeric_limits<uint64_t>::max();
        if (!pred(min)) {
            return std::nullopt;
        }
        if (pred(max)) {
            return max;
        }
        uint64_t low = min;
        uint64_t high = max;
        while (low < high) {
            const auto mid = high - (high - low) / 2;
            if (pred(mid)) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }

    static int64_t
    I64FromOrdinal(uint64_t ordinal) {
        constexpr auto sign_offset = uint64_t{1} << 63;
        if (ordinal < sign_offset) {
            return std::numeric_limits<int64_t>::lowest() +
                   static_cast<int64_t>(ordinal);
        }
        return static_cast<int64_t>(ordinal - sign_offset);
    }

    template <typename Predicate>
    static std::optional<int64_t>
    FirstI64Where(Predicate pred) {
        constexpr auto min = std::numeric_limits<uint64_t>::min();
        constexpr auto max = std::numeric_limits<uint64_t>::max();
        if (pred(I64FromOrdinal(min))) {
            return I64FromOrdinal(min);
        }
        if (!pred(I64FromOrdinal(max))) {
            return std::nullopt;
        }
        uint64_t low = min;
        uint64_t high = max;
        while (low < high) {
            const auto mid = low + (high - low) / 2;
            if (pred(I64FromOrdinal(mid))) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return I64FromOrdinal(low);
    }

    template <typename Predicate>
    static std::optional<int64_t>
    LastI64Where(Predicate pred) {
        constexpr auto min = std::numeric_limits<uint64_t>::min();
        constexpr auto max = std::numeric_limits<uint64_t>::max();
        if (!pred(I64FromOrdinal(min))) {
            return std::nullopt;
        }
        if (pred(I64FromOrdinal(max))) {
            return I64FromOrdinal(max);
        }
        uint64_t low = min;
        uint64_t high = max;
        while (low < high) {
            const auto mid = high - (high - low) / 2;
            if (pred(I64FromOrdinal(mid))) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return I64FromOrdinal(low);
    }

    static U64Range
    U64ForValue(double value, CompareOp op) {
        if (std::isnan(value)) {
            return std::nullopt;
        }
        switch (op) {
            case CompareOp::LessThan: {
                auto upper = LastU64Where(
                    [value](uint64_t v) { return double(v) < value; });
                return upper ? U64Range{{0, *upper}} : std::nullopt;
            }
            case CompareOp::LessEqual: {
                auto upper = LastU64Where(
                    [value](uint64_t v) { return double(v) <= value; });
                return upper ? U64Range{{0, *upper}} : std::nullopt;
            }
            case CompareOp::GreaterThan: {
                auto lower = FirstU64Where(
                    [value](uint64_t v) { return double(v) > value; });
                return lower ? U64Range{{*lower,
                                         std::numeric_limits<uint64_t>::max()}}
                             : std::nullopt;
            }
            case CompareOp::GreaterEqual: {
                auto lower = FirstU64Where(
                    [value](uint64_t v) { return double(v) >= value; });
                return lower ? U64Range{{*lower,
                                         std::numeric_limits<uint64_t>::max()}}
                             : std::nullopt;
            }
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                return std::nullopt;
        }
        return std::nullopt;
    }

    static U64Range
    U64ForBounds(double lo, bool lo_inc, double hi, bool hi_inc) {
        if (std::isnan(lo) || std::isnan(hi)) {
            return std::nullopt;
        }
        auto lower = FirstU64Where([lo, lo_inc](uint64_t value) {
            const auto converted = static_cast<double>(value);
            return lo_inc ? converted >= lo : converted > lo;
        });
        auto upper = LastU64Where([hi, hi_inc](uint64_t value) {
            const auto converted = static_cast<double>(value);
            return hi_inc ? converted <= hi : converted < hi;
        });
        if (!lower || !upper || *lower > *upper) {
            return std::nullopt;
        }
        return std::make_pair(*lower, *upper);
    }

    static I64Range
    I64ForValue(double value, CompareOp op) {
        if (std::isnan(value)) {
            return std::nullopt;
        }
        switch (op) {
            case CompareOp::LessThan: {
                auto upper = LastI64Where(
                    [value](int64_t v) { return double(v) < value; });
                return upper ? I64Range{{std::numeric_limits<int64_t>::lowest(),
                                         *upper}}
                             : std::nullopt;
            }
            case CompareOp::LessEqual: {
                auto upper = LastI64Where(
                    [value](int64_t v) { return double(v) <= value; });
                return upper ? I64Range{{std::numeric_limits<int64_t>::lowest(),
                                         *upper}}
                             : std::nullopt;
            }
            case CompareOp::GreaterThan: {
                auto lower = FirstI64Where(
                    [value](int64_t v) { return double(v) > value; });
                return lower ? I64Range{{*lower,
                                         std::numeric_limits<int64_t>::max()}}
                             : std::nullopt;
            }
            case CompareOp::GreaterEqual: {
                auto lower = FirstI64Where(
                    [value](int64_t v) { return double(v) >= value; });
                return lower ? I64Range{{*lower,
                                         std::numeric_limits<int64_t>::max()}}
                             : std::nullopt;
            }
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                return std::nullopt;
        }
        return std::nullopt;
    }

    static I64Range
    I64ForBounds(double lo, bool lo_inc, double hi, bool hi_inc) {
        if (std::isnan(lo) || std::isnan(hi)) {
            return std::nullopt;
        }
        auto lower = FirstI64Where([lo, lo_inc](int64_t value) {
            const auto converted = static_cast<double>(value);
            return lo_inc ? converted >= lo : converted > lo;
        });
        auto upper = LastI64Where([hi, hi_inc](int64_t value) {
            const auto converted = static_cast<double>(value);
            return hi_inc ? converted <= hi : converted < hi;
        });
        if (!lower || !upper || *lower > *upper) {
            return std::nullopt;
        }
        return std::make_pair(*lower, *upper);
    }
};

class JsonNumericPathReader final : public JsonPathReaderBase,
                                    public IScalarPredicateReader<int64_t>,
                                    public IScalarPredicateReader<double> {
 public:
    JsonNumericPathReader(const JsonFlatIndexReaderState* state,
                          std::string path)
        : JsonPathReaderBase(
              state, std::move(path), ::JsonExistValueType::Numeric) {
    }

    ReaderCaps
    Caps() const override {
        return ReaderCaps{.predicate = true, .exact = true};
    }

    DataType
    ValueType() const override {
        // JsonCastType's numeric vocabulary is DOUBLE, while this object also
        // exposes the int64 mixin needed for lossless integer predicates.
        return DataType::DOUBLE;
    }

    size_t
    ObjectBytes() const override {
        return sizeof(JsonNumericPathReader);
    }

    TargetBitmap
    In(size_t n, const int64_t* values) const override {
        return InNumeric(n, values);
    }

    TargetBitmap
    In(size_t n, const double* values) const override {
        return InNumeric(n, values);
    }

    TargetBitmap
    NotIn(size_t n, const int64_t* values) const override {
        return milvus::index::NotIn(*this, n, values);
    }

    TargetBitmap
    NotIn(size_t n, const double* values) const override {
        return milvus::index::NotIn(*this, n, values);
    }

    TargetBitmap
    Range(const int64_t& value, CompareOp op) const override {
        return RangeNumeric(value, op);
    }

    TargetBitmap
    Range(const double& value, CompareOp op) const override {
        return RangeNumeric(value, op);
    }

    TargetBitmap
    Range(const int64_t& lo,
          bool lo_inc,
          const int64_t& hi,
          bool hi_inc) const override {
        return RangeNumeric(lo, lo_inc, hi, hi_inc);
    }

    TargetBitmap
    Range(const double& lo,
          bool lo_inc,
          const double& hi,
          bool hi_inc) const override {
        return RangeNumeric(lo, lo_inc, hi, hi_inc);
    }

 private:
    void
    OrU64(TargetBitmap& result, NumericRange::U64Range range) const {
        if (!range) {
            return;
        }
        Engine().json_range_query(Path(),
                                  range->first,
                                  range->second,
                                  false,
                                  false,
                                  true,
                                  true,
                                  &result);
    }

    void
    OrI64(TargetBitmap& result, NumericRange::I64Range range) const {
        if (!range) {
            return;
        }
        Engine().json_range_query(Path(),
                                  range->first,
                                  range->second,
                                  false,
                                  false,
                                  true,
                                  true,
                                  &result);
    }

    template <typename T>
    TargetBitmap
    InNumeric(size_t n, const T* values) const {
        ValidateValues(n, values, "numeric In");
        TargetBitmap result(Count());
        if (n == 0) {
            return result;
        }
        Engine().json_terms_query(Path(), values, n, &result);
        if constexpr (std::is_floating_point_v<T>) {
            for (size_t i = 0; i < n; ++i) {
                const auto range = NumericRange::U64ForBounds(
                    values[i], true, values[i], true);
                if (range && range->first == range->second &&
                    std::isfinite(values[i]) &&
                    std::floor(values[i]) == values[i] && values[i] >= 0 &&
                    static_cast<long double>(values[i]) <=
                        static_cast<long double>(
                            std::numeric_limits<uint64_t>::max()) &&
                    static_cast<uint64_t>(values[i]) == range->first) {
                    continue;
                }
                OrU64(result, range);
            }
        }
        return result;
    }

    template <typename T>
    TargetBitmap
    RangeNumeric(const T& value, CompareOp op) const {
        if (op == CompareOp::Equal) {
            return InNumeric(1, &value);
        }
        if (op == CompareOp::NotEqual) {
            return milvus::index::NotIn(*this, 1, &value);
        }

        TargetBitmap result(Count());
        switch (op) {
            case CompareOp::GreaterThan:
                Engine().json_range_query(
                    Path(), value, T{}, false, true, false, false, &result);
                break;
            case CompareOp::GreaterEqual:
                Engine().json_range_query(
                    Path(), value, T{}, false, true, true, false, &result);
                break;
            case CompareOp::LessThan:
                Engine().json_range_query(
                    Path(), T{}, value, true, false, false, false, &result);
                break;
            case CompareOp::LessEqual:
                Engine().json_range_query(
                    Path(), T{}, value, true, false, true, false, &result);
                break;
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                break;
        }
        if constexpr (std::is_integral_v<T>) {
            const auto as_double = static_cast<double>(value);
            switch (op) {
                case CompareOp::GreaterThan:
                    Engine().json_range_query(Path(),
                                              as_double,
                                              double{},
                                              false,
                                              true,
                                              false,
                                              false,
                                              &result);
                    break;
                case CompareOp::GreaterEqual:
                    Engine().json_range_query(Path(),
                                              as_double,
                                              double{},
                                              false,
                                              true,
                                              true,
                                              false,
                                              &result);
                    break;
                case CompareOp::LessThan:
                    Engine().json_range_query(Path(),
                                              double{},
                                              as_double,
                                              true,
                                              false,
                                              false,
                                              false,
                                              &result);
                    break;
                case CompareOp::LessEqual:
                    Engine().json_range_query(Path(),
                                              double{},
                                              as_double,
                                              true,
                                              false,
                                              true,
                                              false,
                                              &result);
                    break;
                case CompareOp::Equal:
                case CompareOp::NotEqual:
                    break;
            }
        }
        OrU64(result,
              NumericRange::U64ForValue(static_cast<double>(value), op));
        if constexpr (std::is_floating_point_v<T>) {
            OrI64(result,
                  NumericRange::I64ForValue(static_cast<double>(value), op));
        }
        return result;
    }

    template <typename T>
    TargetBitmap
    RangeNumeric(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const {
        TargetBitmap result(Count());
        Engine().json_range_query(
            Path(), lo, hi, false, false, lo_inc, hi_inc, &result);
        if constexpr (std::is_integral_v<T>) {
            Engine().json_range_query(Path(),
                                      static_cast<double>(lo),
                                      static_cast<double>(hi),
                                      false,
                                      false,
                                      lo_inc,
                                      hi_inc,
                                      &result);
        }
        OrU64(result,
              NumericRange::U64ForBounds(static_cast<double>(lo),
                                         lo_inc,
                                         static_cast<double>(hi),
                                         hi_inc));
        if constexpr (std::is_floating_point_v<T>) {
            OrI64(result,
                  NumericRange::I64ForBounds(static_cast<double>(lo),
                                             lo_inc,
                                             static_cast<double>(hi),
                                             hi_inc));
        }
        return result;
    }
};

class JsonStringPathReader final
    : public JsonPathReaderBase,
      public IScalarPredicateReader<std::string_view>,
      public IPatternMatchReader {
 public:
    JsonStringPathReader(const JsonFlatIndexReaderState* state,
                         std::string path)
        : JsonPathReaderBase(
              state, std::move(path), ::JsonExistValueType::String) {
    }

    ReaderCaps
    Caps() const override {
        return ReaderCaps{
            .predicate = true,
            .pattern_match = true,
            .exact = true,
        };
    }

    DataType
    ValueType() const override {
        return DataType::VARCHAR;
    }

    size_t
    ObjectBytes() const override {
        return sizeof(JsonStringPathReader);
    }

    TargetBitmap
    In(size_t n, const std::string_view* values) const override {
        auto owned = OwnStrings(n, values);
        TargetBitmap result(Count());
        if (!owned.empty()) {
            Engine().json_terms_query(
                Path(), owned.data(), owned.size(), &result);
        }
        return result;
    }

    TargetBitmap
    NotIn(size_t n, const std::string_view* values) const override {
        return milvus::index::NotIn(*this, n, values);
    }

    TargetBitmap
    Range(const std::string_view& value, CompareOp op) const override {
        if (op == CompareOp::Equal) {
            return In(1, &value);
        }
        if (op == CompareOp::NotEqual) {
            return NotIn(1, &value);
        }
        const auto owned = OwnString(value);
        TargetBitmap result(Count());
        switch (op) {
            case CompareOp::GreaterThan:
                Engine().json_range_query(Path(),
                                          owned,
                                          std::string{},
                                          false,
                                          true,
                                          false,
                                          false,
                                          &result);
                break;
            case CompareOp::GreaterEqual:
                Engine().json_range_query(Path(),
                                          owned,
                                          std::string{},
                                          false,
                                          true,
                                          true,
                                          false,
                                          &result);
                break;
            case CompareOp::LessThan:
                Engine().json_range_query(Path(),
                                          std::string{},
                                          owned,
                                          true,
                                          false,
                                          false,
                                          false,
                                          &result);
                break;
            case CompareOp::LessEqual:
                Engine().json_range_query(Path(),
                                          std::string{},
                                          owned,
                                          true,
                                          false,
                                          false,
                                          true,
                                          &result);
                break;
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                break;
        }
        return result;
    }

    TargetBitmap
    Range(const std::string_view& lo,
          bool lo_inc,
          const std::string_view& hi,
          bool hi_inc) const override {
        const auto owned_lo = OwnString(lo);
        const auto owned_hi = OwnString(hi);
        TargetBitmap result(Count());
        Engine().json_range_query(
            Path(), owned_lo, owned_hi, false, false, lo_inc, hi_inc, &result);
        return result;
    }

    bool
    ShouldUseForOp(PatternOp op, std::string_view) const override {
        switch (op) {
            case PatternOp::Match:
            case PatternOp::PrefixMatch:
                return true;
            case PatternOp::PostfixMatch:
            case PatternOp::InnerMatch:
            case PatternOp::RegexMatch:
                return false;
        }
        return false;
    }

    TargetBitmap
    PatternMatch(std::string_view pattern, PatternOp op) const override {
        if (op == PatternOp::RegexMatch) {
            // Raw regular expressions require the raw-column fallback. Tantivy's
            // JSON FST query only accepts the translated LIKE syntax below.
            ThrowInfo(Unsupported,
                      "JSON flat index does not support regex matching");
        }
        const auto owned = OwnString(pattern);
        TargetBitmap result(Count());
        switch (op) {
            case PatternOp::PrefixMatch:
                Engine().json_prefix_query(Path(), owned, &result);
                return result;
            case PatternOp::RegexMatch:
                break;
            case PatternOp::Match:
                return LikePattern(owned);
            case PatternOp::PostfixMatch:
                return LikePattern("%" + EscapeLikePattern(owned));
            case PatternOp::InnerMatch:
                return LikePattern("%" + EscapeLikePattern(owned) + "%");
        }
        ThrowInfo(OpTypeInvalid,
                  "unknown JSON pattern operator {}",
                  static_cast<int>(op));
    }

 private:
    TargetBitmap
    LikePattern(std::string_view pattern) const {
        PatternMatchTranslator translator;
        const auto regex = translator(OwnString(pattern));
        TargetBitmap result(Count());
        Engine().json_regex_query(Path(), regex, &result);
        return result;
    }
};

}  // namespace

JsonFlatIndexReader::JsonFlatIndexReader(
    std::shared_ptr<const JsonFlatIndexReaderState> state)
    : state_(std::move(state)) {
    AssertInfo(state_ != nullptr, "JSON flat reader requires shared state");
}

JsonFlatIndexReader::~JsonFlatIndexReader() = default;

ReaderCaps
JsonFlatIndexReader::Caps() const {
    return ReaderCaps{.json_paths = true, .exact = true};
}

Domain
JsonFlatIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
JsonFlatIndexReader::Count() const {
    return static_cast<int64_t>(state_->Count());
}

DataType
JsonFlatIndexReader::ValueType() const {
    return DataType::JSON;
}

int64_t
JsonFlatIndexReader::MemoryUsage() const {
    const auto own =
        ToUsageBytes(sizeof(JsonFlatIndexReader), "JSON flat root reader");
    if (state_->HeapBytes() > std::numeric_limits<int64_t>::max() - own) {
        ThrowInfo(DataFormatBroken, "JSON flat reader memory size overflows");
    }
    return own + state_->HeapBytes();
}

cachinglayer::ResourceUsage
JsonFlatIndexReader::CellByteSize() const {
    return {MemoryUsage(), state_->FileBytes()};
}

TargetBitmap
JsonFlatIndexReader::IsNull() const {
    return state_->FieldIsNull();
}

TargetBitmap
JsonFlatIndexReader::IsNotNull() const {
    return state_->FieldIsNotNull();
}

JsonResolvedReader
JsonFlatIndexReader::Resolve(std::string_view path,
                             JsonCastType cast_type) const {
    auto tantivy_path = ResolveTantivyPath(state_->RootPath(), path);
    if (!tantivy_path) {
        return {};
    }

    switch (cast_type.element_type()) {
        case JsonCastType::DataType::BOOL:
            return JsonResolvedReader::Owned(
                std::make_unique<JsonBoolPathReader>(state_.get(),
                                                     std::move(*tantivy_path)));
        case JsonCastType::DataType::DOUBLE:
            return JsonResolvedReader::Owned(
                std::make_unique<JsonNumericPathReader>(
                    state_.get(), std::move(*tantivy_path)));
        case JsonCastType::DataType::VARCHAR:
            return JsonResolvedReader::Owned(
                std::make_unique<JsonStringPathReader>(
                    state_.get(), std::move(*tantivy_path)));
        case JsonCastType::DataType::UNKNOWN:
        case JsonCastType::DataType::ARRAY:
        case JsonCastType::DataType::JSON:
            return {};
    }
    return {};
}

TargetBitmap
JsonFlatIndexReader::Exists(std::string_view path) const {
    auto tantivy_path = ResolveTantivyPath(state_->RootPath(), path);
    AssertInfo(tantivy_path.has_value(),
               "JSON Exists requires a path supported by CastTypesOf");
    TargetBitmap result(Count());
    state_->Engine().json_exist_query(
        *tantivy_path, true, ::JsonExistValueType::Any, &result);
    return result;
}

std::vector<JsonCastType>
JsonFlatIndexReader::CastTypesOf(std::string_view path) const {
    if (!ResolveTantivyPath(state_->RootPath(), path)) {
        return {};
    }
    return {JsonCastType::FromString("BOOL"),
            JsonCastType::FromString("DOUBLE"),
            JsonCastType::FromString("VARCHAR"),
            JsonCastType::FromString("ARRAY_BOOL"),
            JsonCastType::FromString("ARRAY_DOUBLE"),
            JsonCastType::FromString("ARRAY_VARCHAR")};
}

}  // namespace milvus::index
