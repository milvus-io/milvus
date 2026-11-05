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
    // The empty pointer names the JSON field itself (the whole document).
    if (path.empty()) {
        return;
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

bool
IsJsonSortCast(JsonCastType cast_type) {
    const auto element = cast_type.element_type();
    return element == JsonCastType::DataType::DOUBLE ||
           element == JsonCastType::DataType::VARCHAR ||
           (cast_type.data_type() == JsonCastType::DataType::ARRAY &&
            element == JsonCastType::DataType::BOOL);
}

JsonProjectedIndexSpec::JsonProjectedIndexSpec(std::string json_path,
                                               JsonCastType cast_type,
                                               std::optional<int64_t> row_count)
    : json_path_(std::move(json_path)),
      cast_type_(cast_type),
      row_count_(row_count) {
    ValidateJsonPointer(json_path_);
    ValidateCastType(cast_type_);
    AssertInfo(!row_count_ || *row_count_ >= 0,
               "typed JSON projection row count must be non-negative");
    AssertInfo(!row_count_ || static_cast<uint64_t>(*row_count_) <=
                                  static_cast<uint64_t>(
                                      std::numeric_limits<size_t>::max()),
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
    AssertInfo(row_count_.has_value(), "JSON row count is not loaded");
    return *row_count_;
}

bool
JsonProjectedIndexSpec::Matches(std::string_view json_path,
                                JsonCastType cast_type) const {
    return json_path == json_path_ && SameCastType(cast_type, cast_type_);
}

void
JsonProjectedIndexSpec::ValidateInner(const IndexMetadata& inner) const {
    AssertInfo(inner.domain == Domain::Row,
               "typed JSON projection inner reader must use row coordinates");
    AssertInfo(inner.count == row_count_,
               "typed JSON projection inner count {} disagrees with row count "
               "{}",
               inner.count.value_or(-1),
               row_count_.value_or(-1));
    AssertInfo(ValueTypeMatchesCast(inner.value_type, cast_type_),
               "typed JSON projection inner value type {} disagrees with cast "
               "{}",
               static_cast<int>(inner.value_type),
               cast_type_);
    const auto caps = inner.caps;
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
    const auto count = static_cast<size_t>(RowCount());
    size_t previous = 0;
    bool first = true;
    for (const auto offset : offsets) {
        AssertInfo(offset < count,
                   "typed JSON non-exist offset {} exceeds row count {}",
                   offset,
                   row_count_.value_or(-1));
        AssertInfo(first || offset > previous,
                   "typed JSON non-exist offsets must be strictly increasing");
        previous = offset;
        first = false;
    }
}

JsonPathIndex::JsonPathIndex(IndexPtr inner,
                             JsonProjectedIndexSpec spec,
                             const std::vector<size_t>& non_exist_offsets)
    : JsonPathIndex(
          std::move(inner), spec, spec.MakeExists(non_exist_offsets)) {
}

JsonPathIndex::JsonPathIndex(IndexPtr inner,
                             JsonProjectedIndexSpec spec,
                             std::shared_ptr<const TargetBitmap> exists)
    : inner_(std::move(inner)),
      spec_(std::move(spec)),
      exists_(std::move(exists)) {
    AssertInfo(inner_ != nullptr,
               "typed JSON projection requires an inner index");
    spec_.ValidateInner(inner_->Metadata());
    const auto& bitmap = std::get<std::shared_ptr<const TargetBitmap>>(exists_);
    AssertInfo(bitmap && bitmap->size() == spec_.RowCount(),
               "typed JSON existence bitmap count mismatch");
}

std::shared_ptr<const TargetBitmap>
JsonProjectedIndexSpec::MakeExists(const std::vector<size_t>& offsets) const {
    ValidateNonExistOffsets(offsets);
    auto exists =
        std::make_shared<TargetBitmap>(static_cast<size_t>(RowCount()), true);
    for (const auto offset : offsets) {
        exists->reset(offset);
    }
    return exists;
}

JsonPathIndex::JsonPathIndex(IndexPtr inner,
                             JsonProjectedIndexSpec spec,
                             PinExists pin_exists)
    : inner_(std::move(inner)),
      spec_(std::move(spec)),
      exists_(std::move(pin_exists)) {
    AssertInfo(inner_ != nullptr,
               "typed JSON projection requires an inner index");
    AssertInfo(static_cast<bool>(std::get<PinExists>(exists_)),
               "typed JSON projection requires an existence accessor");
    spec_.ValidateInner(inner_->Metadata());
}

IndexMetadata
JsonPathIndex::Metadata() const {
    auto metadata = inner_->Metadata();
    metadata.caps.json_paths = true;
    return metadata;
}

IIndexReaderBasePtr
JsonPathIndex::CreateReader(milvus::OpContext* op_ctx) const {
    return IIndexReaderBasePtr(
        new JsonPathIndexReader(shared_from_this(), op_ctx));
}

void
JsonPathIndex::CancelWarmup() const {
    inner_->CancelWarmup();
}

JsonPathIndexReader::JsonPathIndexReader(
    std::shared_ptr<const JsonPathIndex> index, milvus::OpContext* op_ctx)
    : index_(std::move(index)),
      inner_(index_->inner_->CreateReader(op_ctx)),
      op_ctx_(op_ctx) {
}

JsonPathIndexReader::~JsonPathIndexReader() = default;

ReaderCaps
JsonPathIndexReader::Caps() const {
    return index_->Metadata().caps;
}

Domain
JsonPathIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
JsonPathIndexReader::Count() const {
    return inner_->Count();
}

DataType
JsonPathIndexReader::ValueType() const {
    return inner_->ValueType();
}

JsonResolvedReader
JsonPathIndexReader::Resolve(std::string_view path,
                             JsonCastType cast_type) const {
    return index_->spec_.Matches(path, cast_type)
               ? JsonResolvedReader::Borrowed(inner_.get())
               : JsonResolvedReader{};
}

TargetBitmap
JsonPathIndexReader::Exists(std::string_view path) const {
    AssertInfo(path == index_->spec_.JsonPath(),
               "typed JSON Exists called for an unregistered path");
    std::call_once(exists_once_, [this] {
        const auto* resident =
            std::get_if<std::shared_ptr<const TargetBitmap>>(&index_->exists_);
        exists_ =
            resident
                ? *resident
                : std::get<JsonPathIndex::PinExists>(index_->exists_)(op_ctx_);
    });
    return exists_->clone();
}

std::vector<JsonCastType>
JsonPathIndexReader::CastTypesOf(std::string_view path) const {
    if (path != index_->spec_.JsonPath()) {
        return {};
    }
    return {index_->spec_.CastType()};
}

}  // namespace milvus::index
