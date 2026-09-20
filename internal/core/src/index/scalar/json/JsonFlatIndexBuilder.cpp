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

#include "index/scalar/json/JsonFlatIndexBuilder.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

#include "index/ParamUtils.h"
#include "index/scalar/json/JsonFlatIndexParams.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonFlatIndexArtifact.h"
#include "storage/artifact/LocalDirectory.h"
#include "nlohmann/json.hpp"
#include "simdjson.h"
#include "tantivy-wrapper.h"

namespace milvus::index {

namespace {

using json_flat_params::ParseInteger;
using json_flat_params::RequireDataTypeParam;
using json_flat_params::ParseString;
using json_flat_params::ValidateRowDomain;

constexpr std::string_view kJsonPathParam = "json_path";
constexpr std::string_view kJsonCastTypeParam = "json_cast_type";

std::string
ParseJsonPath(const Config& params) {
    const bool has_json_path = params.contains(kJsonPathParam);
    const bool has_nested_path = params.contains("nested_path");
    const auto json_path = ParseString(params, kJsonPathParam);
    const auto nested_path = ParseString(params, "nested_path");
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat json_path and nested_path disagree");
    }
    return has_json_path ? json_path : nested_path;
}

uint32_t
ParseTantivyVersion(const Config& params) {
    const auto scalar_version =
        ParseInteger(params, SCALAR_INDEX_ENGINE_VERSION, 1, false, "builder");
    const auto explicit_version =
        ParseInteger(params, TANTIVY_INDEX_VERSION, 0, false, "builder");
    if (scalar_version < 0 || explicit_version < 0 ||
        explicit_version > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat engine versions scalar={} tantivy={}",
                  scalar_version,
                  explicit_version);
    }
    const auto version = explicit_version != 0
                             ? static_cast<uint32_t>(explicit_version)
                             : scalar_version <= 1
                                   ? TANTIVY_INDEX_MINIMUM_VERSION
                                   : TANTIVY_INDEX_LATEST_VERSION;
    if (version != TANTIVY_INDEX_MINIMUM_VERSION &&
        version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported JSON flat Tantivy version {}",
                  version);
    }
    return version;
}

JsonFlatBuildParams
ParseBuildParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat build parameters must be an object");
    }
    ValidateRowDomain(params, "builder");
    if (RequireDataTypeParam(params, "field_type", "builder") !=
            DataType::JSON ||
        RequireDataTypeParam(params, "value_type", "builder") !=
            DataType::JSON) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat builder requires JSON field_type and value_type");
    }
    if (ParseString(params, kJsonCastTypeParam) != "JSON") {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat builder requires JSON json_cast_type");
    }
    for (const auto key : {std::string_view("element_type"),
                           std::string_view("array_element_type")}) {
        if (params.contains(key) &&
            RequireDataTypeParam(params, key, "builder") != DataType::NONE) {
            ThrowInfo(
                DataTypeInvalid, "JSON flat builder does not accept {}", key);
        }
    }

    const auto field_id = ParseInteger(params, FIELD_ID, 0, true, "builder");
    if (field_id < 0) {
        ThrowInfo(DataTypeInvalid, "JSON flat field_id must be non-negative");
    }
    auto local_dir = ParseString(params, "local_dir");
    if (local_dir.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat local_dir contains an embedded NUL");
    }
    return JsonFlatBuildParams{
        .field_name = std::to_string(field_id),
        .nested_path = ParseJsonPath(params),
        .tantivy_index_version = ParseTantivyVersion(params),
        .local_dir = std::move(local_dir),
    };
}

void
ValidatePath(const std::string& path) {
    if (path.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat root path contains an embedded NUL");
    }
    try {
        (void)parse_json_pointer(path);
    } catch (const std::invalid_argument& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat root path {}: {}",
                  path,
                  error.what());
    }
}

JsonFlatBuildParams
NormalizeParams(JsonFlatBuildParams params) {
    AssertInfo(!params.field_name.empty(),
               "JSON flat builder requires a Tantivy field name");
    AssertInfo(params.field_name.find('\0') == std::string::npos,
               "JSON flat Tantivy field name contains an embedded NUL");
    ValidatePath(params.nested_path);
    if (params.tantivy_index_version == 0) {
        params.tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION;
    }
    if (params.tantivy_index_version != TANTIVY_INDEX_MINIMUM_VERSION &&
        params.tantivy_index_version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported JSON flat Tantivy version {}",
                  params.tantivy_index_version);
    }
    return params;
}

void
CheckAppend(size_t current, size_t count) {
    constexpr auto kMaxDocs =
        static_cast<size_t>(std::numeric_limits<uint32_t>::max());
    if (current > kMaxDocs || count > kMaxDocs - current) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat document count {} + {} exceeds uint32 domain",
                  current,
                  count);
    }
}

void
AddEmptyDocument(milvus::tantivy::TantivyIndexWrapper& engine, size_t offset) {
    const Json* empty = nullptr;
    engine.add_json_array_data(empty, 0, static_cast<int64_t>(offset));
}

void
CopyToPadded(std::string_view value,
             simdjson::padded_string& scratch,
             size_t offset,
             std::string_view object) {
    if (value.size() == std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "{} at JSON flat offset {} is too large",
                  object,
                  offset);
    }
    const auto required = value.size() + 1;
    if (scratch.size() < required) {
        if (required > std::numeric_limits<size_t>::max() / 2) {
            ThrowInfo(DataFormatBroken,
                      "{} at JSON flat offset {} is too large",
                      object,
                      offset);
        }
        scratch = simdjson::padded_string(required * 2);
    }
    std::copy(value.begin(), value.end(), scratch.data());
    scratch.data()[value.size()] = '\0';
}

}  // namespace

JsonFlatIndexBuilder::JsonFlatIndexBuilder(JsonFlatBuildParams params)
    : params_(NormalizeParams(std::move(params))),
      directory_(CreateJsonFlatIndexDirectory(params_.local_dir)),
      engine_(std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
          params_.field_name.c_str(),
          TantivyDataType::JSON,
          directory_->Path().c_str(),
          params_.tantivy_index_version,
          false,
          false)),
      path_tokens_(parse_json_pointer(params_.nested_path)) {
}

JsonFlatIndexBuilder::~JsonFlatIndexBuilder() = default;

void
JsonFlatIndexBuilder::AddBatch(
    const ScalarBuildBatch<std::string_view>& batch) {
    const auto n = batch.values.size();
    CheckAppend(count_, n);

    if (batch.validity) {
        size_t invalid = 0;
        for (size_t i = 0; i < n; ++i) {
            invalid += batch.validity[i] ? 0 : 1;
        }
        AssertInfo(invalid <= null_offsets_.max_size() - null_offsets_.size(),
                   "JSON flat null-offset count exceeds vector capacity");
        null_offsets_.reserve(null_offsets_.size() + invalid);
    }

    simdjson::padded_string document_scratch(256);
    simdjson::padded_string nested_scratch(256);
    for (size_t i = 0; i < n; ++i) {
        const auto offset = count_;
        if (batch.validity && !batch.validity[i]) {
            null_offsets_.push_back(offset);
            AddEmptyDocument(*engine_, offset);
            ++count_;
            continue;
        }

        const auto raw = batch.values[i];
        if (raw.empty()) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat input at offset {} is empty",
                      offset);
        }
        if (raw.find('\0') != std::string_view::npos) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat input at offset {} contains an embedded "
                      "NUL",
                      offset);
        }
        CopyToPadded(raw, document_scratch, offset, "JSON document");
        Json document(document_scratch.data(), raw.size());

        // Parsing happens before the writer sees this row. The reusable
        // scratch owns the padded bytes; Json borrows them only across the
        // synchronous parse and FFI call. No caller view is retained.
        auto dom = document.dom_doc();
        const bool exists = path_exists(dom.value(), path_tokens_) &&
                            document.exist(params_.nested_path);
        if (!exists) {
            AddEmptyDocument(*engine_, offset);
            ++count_;
            continue;
        }

        if (params_.nested_path.empty()) {
            engine_->add_json_data(&document, 1, offset);
            ++count_;
            continue;
        }

        auto nested = document.doc().at_pointer(params_.nested_path);
        if (nested.error() != simdjson::SUCCESS) {
            AddEmptyDocument(*engine_, offset);
            ++count_;
            continue;
        }
        auto encoded = simdjson::to_json_string(nested.value());
        if (encoded.error() != simdjson::SUCCESS) {
            AddEmptyDocument(*engine_, offset);
            ++count_;
            continue;
        }
        const std::string_view value = encoded.value();
        CopyToPadded(value, nested_scratch, offset, "JSON nested value");
        Json subdocument(nested_scratch.data(), value.size());
        engine_->add_json_data(&subdocument, 1, offset);
        ++count_;
    }
}

storage::ArtifactPtr
JsonFlatIndexBuilder::Build(
    const ScalarBuildInput<std::string_view>& input) && {
    for (const auto& batch : input.batches) {
        AddBatch(batch);
    }

    auto directory = std::move(directory_);
    auto engine = std::exchange(engine_, nullptr);
    auto null_offsets = std::move(null_offsets_);
    AssertInfo(engine != nullptr, "JSON flat builder has no writer to finish");
    engine->finish();
    engine.reset();
    return std::make_unique<JsonFlatIndexArtifact>(
        std::move(directory), std::move(null_offsets));
}

namespace {

const bool kJsonFlatBuilderRegistered = [] {
    BuilderRegistry<ScalarBuildInput<std::string_view>>::Instance().Register(
        families::kJsonFlat, [](const BuildParams& params) {
            return std::make_unique<JsonFlatIndexBuilder>(
                ParseBuildParams(params));
        });
    return true;
}();

}  // namespace

}  // namespace milvus::index
