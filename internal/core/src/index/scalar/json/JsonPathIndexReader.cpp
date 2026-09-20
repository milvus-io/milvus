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

#include "index/scalar/json/JsonPathIndexReader.h"

#include <limits>
#include <stdexcept>
#include <utility>

#include "common/EasyAssert.h"
#include "common/JsonUtils.h"

namespace milvus::index {
namespace {

bool
SameCastType(JsonCastType lhs, JsonCastType rhs) {
    return lhs.data_type() == rhs.data_type() &&
           lhs.element_type() == rhs.element_type();
}

bool
IsSupportedElementType(JsonCastType::DataType type) {
    return type == JsonCastType::DataType::BOOL ||
           type == JsonCastType::DataType::DOUBLE ||
           type == JsonCastType::DataType::VARCHAR;
}

void
ValidateCastType(JsonCastType cast_type) {
    const auto type = cast_type.data_type();
    const bool scalar = type == JsonCastType::DataType::BOOL ||
                        type == JsonCastType::DataType::DOUBLE ||
                        type == JsonCastType::DataType::VARCHAR;
    const bool array = type == JsonCastType::DataType::ARRAY &&
                       IsSupportedElementType(cast_type.element_type());
    if (!scalar && !array) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported typed JSON projection cast {}",
                  cast_type);
    }
}

void
ValidateJsonPointer(const std::string& path) {
    if (path.empty()) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON projection requires a non-empty json_path");
    }
    if (path.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON projection json_path contains an embedded NUL");
    }
    if (path.front() != '/') {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON projection json_path must start with '/'");
    }
    for (size_t i = 0; i < path.size(); ++i) {
        if (path[i] != '~') {
            continue;
        }
        if (i + 1 == path.size() ||
            (path[i + 1] != '0' && path[i + 1] != '1')) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON projection json_path has an invalid escape");
        }
        ++i;
    }
    try {
        (void)parse_json_pointer(path);
    } catch (const std::invalid_argument& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid typed JSON projection json_path: {}",
                  error.what());
    }
}

bool
ValueTypeMatchesCast(DataType value_type, JsonCastType cast_type) {
    switch (cast_type.element_type()) {
        case JsonCastType::DataType::BOOL:
            return value_type == DataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return value_type == DataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return IsStringDataType(value_type);
        default:
            return false;
    }
}

size_t
StringHeapBytes(const std::string& value) {
    const auto inline_capacity = std::string{}.capacity();
    if (value.capacity() <= inline_capacity) {
        return 0;
    }
    if (value.capacity() == std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON projection path memory size overflows");
    }
    return value.capacity() + 1;
}

void
AddSize(size_t& total, size_t bytes, std::string_view object) {
    if (bytes > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataFormatBroken, "{} memory size overflows", object);
    }
    total += bytes;
}

int64_t
ToUsageBytes(size_t bytes, std::string_view object) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(
            DataFormatBroken, "{} memory size exceeds int64 domain", object);
    }
    return static_cast<int64_t>(bytes);
}

int64_t
AddUsageBytes(int64_t base, int64_t extra, std::string_view object) {
    if (extra > std::numeric_limits<int64_t>::max() - base) {
        ThrowInfo(DataFormatBroken, "{} memory usage overflows", object);
    }
    return base + extra;
}

}  // namespace

DataType
JsonProjectedValueTypeForCast(JsonCastType cast_type) {
    switch (cast_type.element_type()) {
        case JsonCastType::DataType::BOOL:
            return DataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return DataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return DataType::VARCHAR;
        default:
            ThrowInfo(
                DataTypeInvalid, "unsupported typed JSON cast {}", cast_type);
    }
}

JsonProjectedIndexSpec::JsonProjectedIndexSpec(std::string json_path,
                                               JsonCastType cast_type,
                                               int64_t row_count)
    : json_path_(std::move(json_path)),
      cast_type_(cast_type),
      row_count_(row_count) {
    ValidateJsonPointer(json_path_);
    ValidateCastType(cast_type_);
    AssertInfo(row_count_ >= 0,
               "typed JSON projection row count must be non-negative");
    AssertInfo(static_cast<uint64_t>(row_count_) <=
                   static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
               "typed JSON projection row count exceeds size_t domain");
}

const std::string&
JsonProjectedIndexSpec::JsonPath() const {
    return json_path_;
}

JsonCastType
JsonProjectedIndexSpec::CastType() const {
    return cast_type_;
}

int64_t
JsonProjectedIndexSpec::RowCount() const {
    return row_count_;
}

bool
JsonProjectedIndexSpec::Matches(std::string_view json_path,
                                JsonCastType cast_type) const {
    return json_path == json_path_ && SameCastType(cast_type, cast_type_);
}

void
JsonProjectedIndexSpec::ValidateInner(const IIndexReaderBase& inner) const {
    AssertInfo(inner.CoordDomain() == Domain::Row,
               "typed JSON projection inner reader must use row coordinates");
    AssertInfo(inner.Count() == row_count_,
               "typed JSON projection inner count {} disagrees with row count "
               "{}",
               inner.Count(),
               row_count_);
    AssertInfo(ValueTypeMatchesCast(inner.ValueType(), cast_type_),
               "typed JSON projection inner value type {} disagrees with cast "
               "{}",
               static_cast<int>(inner.ValueType()),
               cast_type_);
    const auto caps = inner.Caps();
    AssertInfo(!caps.json_paths,
               "typed JSON projection cannot wrap another JSON router");
    if (cast_type_.data_type() == JsonCastType::DataType::ARRAY) {
        AssertInfo(
            caps.predicate && !caps.nested,
            "typed JSON ARRAY projection requires an ordinary row-domain "
            "predicate reader");
    }
}

void
JsonProjectedIndexSpec::ValidateNonExistOffsets(
    const std::vector<size_t>& offsets) const {
    const auto count = static_cast<size_t>(row_count_);
    size_t previous = 0;
    bool first = true;
    for (const auto offset : offsets) {
        AssertInfo(offset < count,
                   "typed JSON non-exist offset {} exceeds row count {}",
                   offset,
                   row_count_);
        AssertInfo(first || offset > previous,
                   "typed JSON non-exist offsets must be strictly increasing");
        previous = offset;
        first = false;
    }
}

JsonPathIndexReader::JsonPathIndexReader(
    std::unique_ptr<IIndexReaderBase> inner,
    JsonProjectedIndexSpec spec,
    const std::vector<size_t>& non_exist_offsets)
    : inner_(std::move(inner)), spec_(std::move(spec)) {
    AssertInfo(inner_ != nullptr,
               "typed JSON projection reader requires an inner reader");
    spec_.ValidateInner(*inner_);
    spec_.ValidateNonExistOffsets(non_exist_offsets);
    exists_ = TargetBitmap(static_cast<size_t>(spec_.RowCount()), true);
    for (const auto offset : non_exist_offsets) {
        exists_.reset(offset);
    }

    caps_ = inner_->Caps();
    caps_.json_paths = true;
    value_type_ = inner_->ValueType();

    const auto inner_heap = inner_->MemoryUsage();
    const auto inner_usage = inner_->CellByteSize();
    AssertInfo(inner_heap >= 0 && inner_usage.memory_bytes >= 0 &&
                   inner_usage.file_bytes >= 0,
               "typed JSON projection inner reader reported negative resource "
               "usage");

    size_t own_heap = sizeof(JsonPathIndexReader);
    AddSize(own_heap,
            StringHeapBytes(spec_.JsonPath()),
            "typed JSON projection reader");
    AddSize(own_heap, exists_.size_in_bytes(), "typed JSON projection reader");
    const auto own_usage =
        ToUsageBytes(own_heap, "typed JSON projection reader");
    heap_bytes_ =
        AddUsageBytes(inner_heap, own_usage, "typed JSON projection reader");
    cell_memory_bytes_ = AddUsageBytes(
        inner_usage.memory_bytes, own_usage, "typed JSON projection reader");
    file_bytes_ = inner_usage.file_bytes;
}

JsonPathIndexReader::~JsonPathIndexReader() = default;

ReaderCaps
JsonPathIndexReader::Caps() const {
    return caps_;
}

Domain
JsonPathIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
JsonPathIndexReader::Count() const {
    return spec_.RowCount();
}

DataType
JsonPathIndexReader::ValueType() const {
    return value_type_;
}

int64_t
JsonPathIndexReader::MemoryUsage() const {
    return heap_bytes_;
}

cachinglayer::ResourceUsage
JsonPathIndexReader::CellByteSize() const {
    return {cell_memory_bytes_, file_bytes_};
}

JsonResolvedReader
JsonPathIndexReader::Resolve(std::string_view path,
                             JsonCastType cast_type) const {
    return spec_.Matches(path, cast_type)
               ? JsonResolvedReader::Borrowed(inner_.get())
               : JsonResolvedReader{};
}

TargetBitmap
JsonPathIndexReader::Exists(std::string_view path) const {
    AssertInfo(path == spec_.JsonPath(),
               "typed JSON Exists called for an unregistered path");
    return exists_.clone();
}

std::vector<JsonCastType>
JsonPathIndexReader::CastTypesOf(std::string_view path) const {
    if (path != spec_.JsonPath()) {
        return {};
    }
    return {spec_.CastType()};
}

}  // namespace milvus::index
