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

#include "folly/coro/BlockingWait.h"

#include "index/scalar/json/JsonProjectedIndexLoad.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <optional>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "storage/artifact/LocalFileUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/scalar/json/JsonPathIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

constexpr std::string_view kCompletenessRuntimeKey =
    "__milvus_json_projection_completeness";
constexpr std::string_view kComplete = "complete";
constexpr std::string_view kLegacyUnknown = "legacy_unknown";
constexpr std::string_view kHasNonExistMeta = "has_non_exist";

/** @brief Validated JSON path and cast used to decorate a scalar reader. */
struct ProjectedParams {
    std::string json_path;
    JsonCastType cast_type;
};

/** @brief Completeness evidence read from inventory/metadata, before sidecar loading. */
struct ObservedCompleteness {
    JsonProjectionCompleteness completeness{
        JsonProjectionCompleteness::LegacyUnknown};
    bool has_non_exist_entry{false};
    std::optional<size_t> declared_non_exist_bytes;
};

// Detect JSON candidates in textual or numeric input before strict normalized
// parsing.
bool
IsJsonFieldType(const Config& params) {
    if (!params.is_object() || !params.contains("field_type")) {
        return false;
    }
    const auto& value = params.at("field_type");
    if (value.is_string()) {
        return value.get_ref<const std::string&>() == "JSON";
    }
    if (value.is_number_unsigned()) {
        const auto encoded = value.get<uint64_t>();
        return encoded == static_cast<uint64_t>(DataType::JSON);
    }
    if (value.is_number_integer()) {
        return value.get<int64_t>() == static_cast<int64_t>(DataType::JSON);
    }
    return false;
}

// Read a normalized numeric type or use the fallback only when the key is
// absent.
DataType
ParseNormalizedDataType(const Config& params,
                        std::string_view key,
                        DataType fallback) {
    if (!params.contains(key)) {
        return fallback;
    }
    const auto& value = params.at(key);
    int64_t encoded = 0;
    try {
        if (value.is_number_unsigned()) {
            const auto number = value.get<uint64_t>();
            if (number >
                static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
                ThrowInfo(DataTypeInvalid,
                          "typed JSON parameter {} is out of range",
                          key);
            }
            encoded = static_cast<int64_t>(number);
        } else if (value.is_number_integer()) {
            encoded = value.get<int64_t>();
            if (encoded < std::numeric_limits<int32_t>::min() ||
                encoded > std::numeric_limits<int32_t>::max()) {
                ThrowInfo(DataTypeInvalid,
                          "typed JSON parameter {} is out of range",
                          key);
            }
        } else {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON parameter {} must be a normalized data type",
                      key);
        }
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid typed JSON parameter {}: {}",
                  key,
                  error.what());
    }
    return static_cast<DataType>(static_cast<int32_t>(encoded));
}

// Require a string parameter and report missing/type errors with the
// parameter name.
std::string
ParseString(const Config& params, std::string_view key) {
    try {
        return params.at(key).get<std::string>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON parameter {} must be a string: {}",
                  key,
                  error.what());
    }
}

// Require an actual boolean; do not coerce numeric or textual values.
bool
ParseNormalizedBool(const Config& params, std::string_view key) {
    try {
        if (!params.at(key).is_boolean()) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON parameter {} must be boolean",
                      key);
        }
        return params.at(key).get<bool>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid typed JSON parameter {}: {}",
                  key,
                  error.what());
    }
}

// Accept only the scalar/array cast types supported by typed JSON projection.
JsonCastType
ParseCastType(const Config& params) {
    if (!params.contains(JSON_CAST_TYPE)) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON load requires parameter {}",
                  JSON_CAST_TYPE);
    }
    const auto text = ParseString(params, JSON_CAST_TYPE);
    if (text != "BOOL" && text != "DOUBLE" && text != "VARCHAR" &&
        text != "ARRAY_BOOL" && text != "ARRAY_DOUBLE" &&
        text != "ARRAY_VARCHAR") {
        ThrowInfo(DataTypeInvalid, "unsupported typed JSON cast type {}", text);
    }
    return JsonCastType::FromString(text);
}

// Check the cast against the selected family, including HYBRID family
// restrictions.
void
ValidateFamilyCast(std::string_view family,
                   const Config& params,
                   JsonCastType cast_type) {
    const auto type = cast_type.data_type();
    const auto element = cast_type.element_type();
    if (!params.contains(INDEX_TYPE)) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON load requires normalized index_type");
    }
    const auto index_type = ParseString(params, INDEX_TYPE);
    if (index_type == HYBRID_INDEX_TYPE) {
        const bool supported =
            type != JsonCastType::DataType::ARRAY &&
            (element == JsonCastType::DataType::BOOL ||
             element == JsonCastType::DataType::DOUBLE ||
             element == JsonCastType::DataType::VARCHAR) &&
            (family == families::kBitmap || family == families::kSort ||
             family == families::kInverted ||
             (family == families::kMarisa &&
              element == JsonCastType::DataType::VARCHAR));
        if (!supported) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON HYBRID cast {} cannot load as family {}",
                      cast_type,
                      family);
        }
        return;
    }
    if (family == families::kInverted) {
        return;
    }
    const bool scalar = type != JsonCastType::DataType::ARRAY;
    const bool supported = (family == families::kSort && scalar &&
                            (element == JsonCastType::DataType::DOUBLE ||
                             element == JsonCastType::DataType::VARCHAR)) ||
                           (family == families::kBitmap && scalar &&
                            (element == JsonCastType::DataType::BOOL ||
                             element == JsonCastType::DataType::VARCHAR)) ||
                           (family == families::kNgram && scalar &&
                            element == JsonCastType::DataType::VARCHAR);
    if (!supported) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON cast {} is not supported by family {}",
                  cast_type,
                  family);
    }
}

// Validate JSON projection parameters; return nullopt for non-JSON inputs.
std::optional<ProjectedParams>
ParseProjectedParams(std::string_view family, const Config& params) {
    if (!IsJsonFieldType(params)) {
        return std::nullopt;
    }
    const auto field_type =
        ParseNormalizedDataType(params, "field_type", DataType::NONE);
    if (field_type != DataType::JSON) {
        return std::nullopt;
    }

    std::optional<bool> nested;
    for (const auto key : {std::string_view("nested"),
                           std::string_view("is_nested"),
                           std::string_view("is_nested_index")}) {
        if (!params.contains(key)) {
            continue;
        }
        const auto value = ParseNormalizedBool(params, key);
        if (nested.has_value() && *nested != value) {
            ThrowInfo(DataTypeInvalid, "typed JSON nested parameters disagree");
        }
        nested = value;
    }
    if (!nested.has_value() || *nested) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON projection requires normalized row domain");
    }

    for (const auto key : {std::string_view("element_type"),
                           std::string_view("array_element_type")}) {
        const auto value = ParseNormalizedDataType(params, key, DataType::NONE);
        if (value != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "typed JSON outer parameter {} must be NONE",
                      key);
        }
    }

    const bool has_json_path = params.contains(JSON_PATH);
    const bool has_nested_path = params.contains("nested_path");
    if (!has_json_path && !has_nested_path) {
        ThrowInfo(DataTypeInvalid, "typed JSON projection requires json_path");
    }
    const auto json_path =
        has_json_path ? ParseString(params, JSON_PATH) : std::string{};
    const auto nested_path =
        has_nested_path ? ParseString(params, "nested_path") : std::string{};
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON json_path and nested_path disagree");
    }

    const auto cast_type = ParseCastType(params);
    if (!family.empty()) {
        ValidateFamilyCast(family, params, cast_type);
    }
    const auto value_type =
        ParseNormalizedDataType(params, "value_type", DataType::NONE);
    if (value_type != JsonProjectedValueTypeForCast(cast_type)) {
        ThrowInfo(DataTypeInvalid,
                  "typed JSON value_type {} disagrees with cast {}",
                  static_cast<int>(value_type),
                  cast_type);
    }

    auto path = has_json_path ? json_path : nested_path;
    // Reuse the reader's exact JSON-pointer and cast validation before any
    // family payload is opened.
    static_cast<void>(JsonProjectedIndexSpec(path, cast_type, 0));
    return ProjectedParams{std::move(path), cast_type};
}

// A legacy non-exist sidecar proves completeness; its absence leaves
// completeness unknown.
ObservedCompleteness
InspectCompleteness(storage::FileSource& source) {
    ObservedCompleteness result;
    result.has_non_exist_entry =
        source.HasEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME);
    result.completeness = result.has_non_exist_entry
                              ? JsonProjectionCompleteness::Complete
                              : JsonProjectionCompleteness::LegacyUnknown;
    return result;
}

// Cross-check packed completeness metadata with sidecar presence and declared
// size.
ObservedCompleteness
InspectPackedCompleteness(const storage::IndexEntryDirectory& directory,
                          const nlohmann::json& metadata) {
    ObservedCompleteness result;
    const auto value = metadata.find(kHasNonExistMeta);
    if (value == metadata.end()) {
        return result;
    }
    if (!value->is_boolean()) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON has_non_exist metadata must be boolean");
    }
    const bool has_entry = directory.HasEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME);
    if (has_entry != value->get<bool>()) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON metadata disagrees with non-exist sidecar");
    }
    result.completeness = JsonProjectionCompleteness::Complete;
    result.has_non_exist_entry = has_entry;
    if (has_entry) {
        const auto bytes =
            directory.At(INDEX_NON_EXIST_OFFSET_FILE_NAME).plaintext_size;
        if (bytes == 0 || bytes % sizeof(size_t) != 0) {
            ThrowInfo(DataFormatBroken,
                      "invalid typed JSON non-exist sidecar size {}",
                      bytes);
        }
        result.declared_non_exist_bytes = bytes;
    }
    return result;
}

// Require a recognized runtime annotation produced during cold-load planning.
JsonProjectionCompleteness
ReadAnnotation(const Config& params) {
    AssertInfo(params.contains(kCompletenessRuntimeKey),
               "typed JSON load parameters lack completeness annotation");
    AssertInfo(params.at(kCompletenessRuntimeKey).is_string(),
               "typed JSON completeness annotation has invalid type");
    const auto state =
        params.at(kCompletenessRuntimeKey).get_ref<const std::string&>();
    if (state == kComplete) {
        return JsonProjectionCompleteness::Complete;
    }
    if (state == kLegacyUnknown) {
        return JsonProjectionCompleteness::LegacyUnknown;
    }
    AssertInfo(false, "typed JSON completeness annotation is invalid");
}

/** @brief Remove temporary JSON sidecars when their per-load owner is released. */
class StagingDirectory final {
 public:
    // Create an owned child under the configured parent or system temp directory.
    static StagingDirectory
    Create(const std::string& parent) {
        std::error_code error;
        auto root = parent.empty() ? std::filesystem::temp_directory_path(error)
                                   : std::filesystem::path(parent);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to locate typed JSON staging directory: {}",
                      error.message());
        }
        std::filesystem::create_directories(root, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create typed JSON staging root {}: {}",
                      root.string(),
                      error.message());
        }
        auto pattern = (root / "json_projected_XXXXXX").string();
        std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
        mutable_pattern.push_back('\0');
        if (::mkdtemp(mutable_pattern.data()) == nullptr) {
            ThrowInfo(FileCreateFailed,
                      "failed to create typed JSON staging directory in {}: "
                      "{}",
                      root.string(),
                      std::strerror(errno));
        }
        try {
            return StagingDirectory(mutable_pattern.data());
        } catch (...) {
            // The unique child is still empty here. Preserve the construction
            // failure and release only the directory created above.
            ::rmdir(mutable_pattern.data());
            throw;
        }
    }

    StagingDirectory(StagingDirectory&& other) noexcept
        : path_(std::exchange(other.path_, {})) {
    }

    ~StagingDirectory() {
        if (!path_.empty()) {
            std::error_code ignored;
            std::filesystem::remove_all(path_, ignored);
        }
    }

    const std::string&
    Path() const {
        return path_;
    }

 private:
    explicit StagingDirectory(std::string path) : path_(std::move(path)) {
    }

    std::string path_;
};

using storage::FileDescriptorGuard;

// Stage and size-check the sidecar against the reader row count before heap
// allocation.
folly::coro::Task<std::vector<size_t>>
ReadNonExistOffsets(bool use_async,
                    const JsonProjectedOpenPlan& plan,
                    storage::FileSource& source,
                    int64_t row_count) {
    AssertInfo(row_count >= 0,
               "typed JSON inner reader reported a negative count");
    AssertInfo(static_cast<uint64_t>(row_count) <=
                   static_cast<uint64_t>(std::numeric_limits<size_t>::max()),
               "typed JSON inner count exceeds size_t domain");
    const auto count = static_cast<size_t>(row_count);

    auto directory = StagingDirectory::Create(plan.staging_parent);
    const auto local_path = (std::filesystem::path(directory.Path()) /
                             INDEX_NON_EXIST_OFFSET_FILE_NAME)
                                .string();
    co_await source.ReadEntryToLocalFileAsync(
        INDEX_NON_EXIST_OFFSET_FILE_NAME, local_path, use_async);
    const auto bytes = storage::LocalFileSize(
        local_path, "failed to determine typed JSON sidecar size for");
    if (plan.declared_non_exist_bytes.has_value() &&
        bytes != *plan.declared_non_exist_bytes) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON non-exist sidecar size changed from {} to {}",
                  *plan.declared_non_exist_bytes,
                  bytes);
    }
    if (bytes % sizeof(size_t) != 0) {
        ThrowInfo(DataFormatBroken,
                  "invalid typed JSON non-exist sidecar size {}",
                  bytes);
    }
    const auto offset_count = bytes / sizeof(size_t);
    if (offset_count > count) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON non-exist offset count {} exceeds row count {}",
                  offset_count,
                  count);
    }
    if (bytes == 0) {
        co_return std::vector<size_t>{};
    }

    std::vector<size_t> result(offset_count);
    const auto fd = ::open(local_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open typed JSON staging file {}: {}",
                  local_path,
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    storage::ReadAll(descriptor.Get(),
                     result.data(),
                     bytes,
                     local_path,
                     "typed JSON staging file");
    descriptor.CloseChecked(local_path, "typed JSON staging file");
    co_return result;
}

}  // namespace

Config
AnnotateJsonProjectionCompleteness(Config params, storage::FileSource& source) {
    if (!params.is_object()) {
        return params;
    }
    params.erase(std::string(kCompletenessRuntimeKey));
    const auto projected = ParseProjectedParams({}, params);
    if (!projected.has_value()) {
        return params;
    }
    const auto observed = InspectCompleteness(source);
    params[std::string(kCompletenessRuntimeKey)] = std::string(
        observed.completeness == JsonProjectionCompleteness::Complete
            ? kComplete
            : kLegacyUnknown);
    return params;
}

ReaderCaps
DeriveJsonProjectedCaps(std::string_view family,
                        const Config& annotated_params,
                        ReaderCaps inner_caps) {
    const auto projected = ParseProjectedParams(family, annotated_params);
    if (!projected.has_value()) {
        return inner_caps;
    }
    if (ReadAnnotation(annotated_params) ==
        JsonProjectionCompleteness::Complete) {
        AssertInfo(!inner_caps.json_paths,
                   "typed JSON projection cannot wrap JSON-path caps");
        inner_caps.json_paths = true;
    }
    return inner_caps;
}

JsonProjectedOpenPlan
PrepareJsonProjectedOpen(std::string_view family,
                         storage::FileSource& source,
                         const storage::LoadOptions& opts) {
    JsonProjectedOpenPlan result;
    const auto projected = ParseProjectedParams(family, opts.params);
    if (!projected.has_value()) {
        return result;
    }

    const auto annotated = ReadAnnotation(opts.params);
    const auto observed = InspectCompleteness(source);
    AssertInfo(annotated == observed.completeness,
               "typed JSON completeness changed between load planning and "
               "open");
    result.completeness = observed.completeness;
    result.json_path = projected->json_path;
    result.cast_type = projected->cast_type;
    result.has_non_exist_entry = observed.has_non_exist_entry;
    result.declared_non_exist_bytes = observed.declared_non_exist_bytes;
    result.staging_parent = opts.mmap_dir_path;
    if (result.staging_parent.empty() && opts.params.contains("local_dir")) {
        result.staging_parent = ParseString(opts.params, "local_dir");
    }
    return result;
}

std::unique_ptr<IIndexReaderBase>
FinishJsonProjectedOpen(JsonProjectedOpenPlan plan,
                        storage::FileSource& source,
                        std::unique_ptr<IIndexReaderBase> inner) {
    return folly::coro::blockingWait(FinishJsonProjectedOpenAsync(
        false, std::move(plan), source, std::move(inner)));
}

folly::coro::Task<std::unique_ptr<IIndexReaderBase>>
FinishJsonProjectedOpenAsync(bool use_async,
                             JsonProjectedOpenPlan plan,
                             storage::FileSource& source,
                             std::unique_ptr<IIndexReaderBase> inner) {
    AssertInfo(inner != nullptr,
               "typed JSON load requires a non-null inner reader");
    if (plan.completeness != JsonProjectionCompleteness::Complete) {
        co_return inner;
    }
    AssertInfo(plan.cast_type.has_value(),
               "complete typed JSON load lacks a cast type");
    std::vector<size_t> offsets;
    if (plan.has_non_exist_entry) {
        offsets = (co_await ReadNonExistOffsets(
            use_async, plan, source, inner->Count()));
    }
    const auto row_count = inner->Count();
    co_return std::make_unique<JsonPathIndexReader>(
        std::move(inner),
        JsonProjectedIndexSpec(
            std::move(plan.json_path), *plan.cast_type, row_count),
        offsets);
}

Config
AnnotateJsonProjectionCompleteness(
    Config params,
    const storage::IndexEntryDirectory& directory,
    const nlohmann::json& metadata) {
    if (!params.is_object()) {
        return params;
    }
    params.erase(std::string(kCompletenessRuntimeKey));
    if (!ParseProjectedParams({}, params)) {
        return params;
    }
    const auto observed = InspectPackedCompleteness(directory, metadata);
    params[std::string(kCompletenessRuntimeKey)] = std::string(
        observed.completeness == JsonProjectionCompleteness::Complete
            ? kComplete
            : kLegacyUnknown);
    return params;
}

JsonProjectedOpenPlan
PreparePackedJsonProjectedOpen(std::string_view family,
                               const storage::IndexEntryDirectory& directory,
                               const nlohmann::json& metadata,
                               const storage::LoadOptions& opts,
                               IndexLoadPlan& targets,
                               std::optional<size_t> row_count) {
    JsonProjectedOpenPlan result;
    const auto projected = ParseProjectedParams(family, opts.params);
    if (!projected) {
        return result;
    }
    const auto observed = InspectPackedCompleteness(directory, metadata);
    AssertInfo(ReadAnnotation(opts.params) == observed.completeness,
               "typed JSON completeness changed between planning and open");
    result.completeness = observed.completeness;
    result.json_path = projected->json_path;
    result.cast_type = projected->cast_type;
    result.has_non_exist_entry = observed.has_non_exist_entry;
    result.declared_non_exist_bytes = observed.declared_non_exist_bytes;
    if (observed.has_non_exist_entry) {
        const auto bytes = *observed.declared_non_exist_bytes;
        if (row_count) {
            if (bytes / sizeof(size_t) > *row_count) {
                ThrowInfo(DataFormatBroken,
                          "typed JSON non-exist offsets exceed row count");
            }
            result.packed_non_exist_offsets =
                std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
            targets.entries.push_back(
                {INDEX_NON_EXIST_OFFSET_FILE_NAME,
                 storage::MemoryEntryTarget{
                     result.packed_non_exist_offsets,
                     reinterpret_cast<uint8_t*>(
                         result.packed_non_exist_offsets->data()),
                     bytes}});
        } else {
            // Without an engine row bound, stage the sidecar instead of trusting
            // its declared length for an unbounded heap allocation.
            const auto parent =
                opts.mmap_dir_path.empty() && opts.params.contains("local_dir")
                    ? ParseString(opts.params, "local_dir")
                    : opts.mmap_dir_path;
            auto staging = std::make_shared<StagingDirectory>(
                StagingDirectory::Create(parent));
            const auto path = (std::filesystem::path(staging->Path()) /
                               INDEX_NON_EXIST_OFFSET_FILE_NAME)
                                  .string();
            result.packed_directory = staging;
            result.packed_non_exist_file =
                std::make_shared<storage::IndexFileTarget>(path, bytes, false);
            targets.entries.push_back(
                {INDEX_NON_EXIST_OFFSET_FILE_NAME,
                 storage::FileEntryTarget{
                     result.packed_non_exist_file, 0, bytes}});
        }
    }
    return result;
}

IIndexReaderBasePtr
FinishPackedJsonProjectedOpen(JsonProjectedOpenPlan plan,
                              IIndexReaderBasePtr inner) {
    AssertInfo(inner != nullptr,
               "typed JSON requires an initialized inner reader");
    if (plan.completeness != JsonProjectionCompleteness::Complete) {
        return inner;
    }
    AssertInfo(plan.cast_type.has_value(),
               "typed JSON projection lacks cast type");
    const auto rows = inner->Count();
    AssertInfo(rows >= 0, "typed JSON inner reader reported negative count");
    std::vector<size_t> staged_offsets;
    if (plan.packed_non_exist_file) {
        const auto bytes = plan.packed_non_exist_file->file_size;
        if (bytes / sizeof(size_t) > static_cast<uint64_t>(rows)) {
            ThrowInfo(DataFormatBroken,
                      "typed JSON non-exist offsets exceed row count");
        }
        staged_offsets.resize(bytes / sizeof(size_t));
        const auto& path = plan.packed_non_exist_file->path;
        const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            ThrowInfo(FileOpenFailed,
                      "failed to open typed JSON sidecar {}: {}",
                      path,
                      std::strerror(errno));
        }
        FileDescriptorGuard descriptor(fd);
        storage::ReadAll(
            fd, staged_offsets.data(), bytes, path, "typed JSON sidecar");
        descriptor.CloseChecked(path, "typed JSON sidecar");
    }
    const auto& offsets = plan.packed_non_exist_offsets
                              ? *plan.packed_non_exist_offsets
                              : staged_offsets;
    if (offsets.size() > static_cast<uint64_t>(rows)) {
        ThrowInfo(DataFormatBroken,
                  "typed JSON non-exist offsets exceed row count");
    }
    return std::make_unique<JsonPathIndexReader>(
        std::move(inner),
        JsonProjectedIndexSpec(
            std::move(plan.json_path), *plan.cast_type, rows),
        offsets);
}

}  // namespace milvus::index
