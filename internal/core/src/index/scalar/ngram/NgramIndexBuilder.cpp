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

#include "index/scalar/ngram/NgramIndexBuilder.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

#include "index/ParamUtils.h"
#include "index/scalar/ngram/NgramIndexParams.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/scalar/ngram/NgramIndexArtifact.h"
#include "storage/artifact/LocalDirectory.h"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

using ngram_params::ParseJsonPath;
using ngram_params::ParseString;
using ngram_params::ParseUnsigned;
using ngram_params::Upper;

void
ValidateRowDomain(const Config& params) {
    if (ReadNestedConfigParam(params, "NGRAM").value_or(false)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only the row coordinate domain");
    }
}

void
ValidateExplicitEngineVersion(const Config& params) {
    const auto version = ParseUnsigned(params, TANTIVY_INDEX_VERSION, 0, false);
    // The ngram writer binding has always created Tantivy v7 directly and has
    // no v5 constructor. Reject an explicit different request rather than
    // silently producing a newer engine format.
    if (version != 0 && version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM supports only Tantivy index version {}",
                  TANTIVY_INDEX_LATEST_VERSION);
    }
}

template <typename T>
NgramBuildParams
ParseBuildParams(const Config& params) {
    constexpr bool json_projection =
        std::is_same_v<T, JsonProjectedString>;
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "NGRAM build parameters must be an object");
    }
    ValidateRowDomain(params);
    ValidateExplicitEngineVersion(params);

    NgramBuildParams result;
    const auto field_id = ParseUnsigned(params, FIELD_ID, 0, true);
    if (field_id > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataTypeInvalid, "NGRAM field_id is out of range");
    }
    result.field_name = std::to_string(field_id);
    const auto field_type =
        ReadDataTypeParam(params, "field_type").value_or(DataType::VARCHAR);
    result.value_type =
        ReadDataTypeParam(params, "value_type")
            .value_or(field_type == DataType::JSON ? DataType::VARCHAR
                                                   : field_type);
    if (!IsStringDataType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM value_type must be STRING, VARCHAR, or TEXT");
    }
    const auto nested_path = ParseJsonPath(params);
    if (field_type == DataType::JSON) {
        if (nested_path.empty()) {
            ThrowInfo(DataTypeInvalid, "JSON NGRAM requires json_path");
        }
        const auto cast = Upper(ParseString(params, JSON_CAST_TYPE));
        if (cast != "VARCHAR") {
            ThrowInfo(DataTypeInvalid,
                      "JSON NGRAM requires VARCHAR json_cast_type");
        }
        if (!json_projection) {
            ThrowInfo(UnexpectedError,
                      "JSON NGRAM requires the tri-state projection builder");
        }
    } else if (json_projection) {
        ThrowInfo(DataTypeInvalid,
                  "tri-state NGRAM projection requires a JSON field");
    } else if (!IsStringDataType(field_type)) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM field_type must be STRING, VARCHAR, TEXT, or JSON");
    } else if (!nested_path.empty()) {
        ThrowInfo(DataTypeInvalid, "scalar NGRAM must not carry a JSON path");
    }

    const auto min_gram = ParseUnsigned(params, MIN_GRAM, 0, true);
    const auto max_gram = ParseUnsigned(params, MAX_GRAM, 0, true);
    if (min_gram == 0 || max_gram == 0 || min_gram > max_gram ||
        min_gram > std::numeric_limits<uintptr_t>::max() ||
        max_gram > std::numeric_limits<uintptr_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid NGRAM range min_gram={} max_gram={}",
                  min_gram,
                  max_gram);
    }
    result.min_gram = static_cast<uintptr_t>(min_gram);
    result.max_gram = static_cast<uintptr_t>(max_gram);
    result.local_dir = ParseString(params, "local_dir");
    return result;
}

void
CheckAppend(size_t current, size_t count) {
    constexpr size_t kMaxDocs = std::numeric_limits<uint32_t>::max();
    if (count > std::numeric_limits<size_t>::max() - current ||
        current > kMaxDocs || count > kMaxDocs - current) {
        ThrowInfo(DataTypeInvalid,
                  "NGRAM document count {} + {} exceeds uint32 domain",
                  current,
                  count);
    }
}

NgramBuildParams
ValidateBuildParams(NgramBuildParams params) {
    AssertInfo(!params.field_name.empty(),
               "NGRAM builder requires a field identity");
    AssertInfo(IsStringDataType(params.value_type),
               "NGRAM builder requires a string value type");
    AssertInfo(params.min_gram > 0 && params.min_gram <= params.max_gram,
               "NGRAM builder has invalid gram range {}..{}",
               params.min_gram,
               params.max_gram);
    return params;
}

}  // namespace

class NgramBuilderCore {
 public:
    explicit NgramBuilderCore(NgramBuildParams params)
        : params_(ValidateBuildParams(std::move(params))),
          directory_(CreateNgramIndexDirectory(params_.local_dir)),
          engine_(std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
              params_.field_name.c_str(),
              directory_->Path().c_str(),
              params_.min_gram,
              params_.max_gram)) {
    }

    void
    AddBatch(const ScalarBuildBatch<std::string_view>& batch) {
        AddRows(
            batch.values.size(),
            [&batch](size_t i) {
                return batch.validity && !batch.validity[i]
                           ? JsonProjectedStringState::FieldNull
                           : JsonProjectedStringState::Value;
            },
            [&batch](size_t i) { return batch.values[i]; });
    }

    void
    AddBatch(const ScalarBuildBatch<JsonProjectedString>& batch) {
        AssertInfo(!batch.validity,
                   "JSON NGRAM tri-state input must not carry a separate "
                   "validity view");
        AddRows(
            batch.values.size(),
            [&batch](size_t i) { return batch.values[i].state; },
            [&batch](size_t i) { return batch.values[i].value; });
    }

    storage::ArtifactPtr
    Finish() && {
        // On every failure path the writer must be destroyed before its
        // directory owner removes the files it may still reference.
        auto directory = std::move(directory_);
        auto engine = std::exchange(engine_, nullptr);
        auto null_offsets = std::move(null_offsets_);
        AssertInfo(engine != nullptr, "NGRAM builder has no writer to finish");
        engine->finish();
        engine.reset();

        const auto avg_row_size =
            valid_rows_ == 0 ? 0 : total_bytes_ / valid_rows_;
        return std::make_unique<NgramIndexArtifact>(std::move(directory),
                                                    std::move(null_offsets),
                                                    avg_row_size);
    }

 private:
    template <typename StateAt, typename ValueAt>
    void
    AddRows(size_t n, StateAt state_at, ValueAt value_at) {
        CheckAppend(count_, n);

        size_t added_nulls = 0;
        size_t added_bytes = 0;
        size_t added_rows = 0;
        for (size_t i = 0; i < n; ++i) {
            switch (state_at(i)) {
                case JsonProjectedStringState::FieldNull:
                    ++added_nulls;
                    break;
                case JsonProjectedStringState::NoValue:
                    break;
                case JsonProjectedStringState::Value: {
                    const auto value = value_at(i);
                    if (value.size() >
                        std::numeric_limits<size_t>::max() - added_bytes) {
                        ThrowInfo(DataTypeInvalid,
                                  "NGRAM input byte count overflows");
                    }
                    added_bytes += value.size();
                    ++added_rows;
                    break;
                }
                default:
                    ThrowInfo(UnexpectedError,
                              "JSON NGRAM input has an invalid projection "
                              "state");
            }
        }
        if (added_nulls > null_offsets_.max_size() - null_offsets_.size()) {
            ThrowInfo(DataTypeInvalid,
                      "NGRAM null-offset count exceeds vector capacity");
        }
        if (added_bytes > std::numeric_limits<size_t>::max() - total_bytes_ ||
            added_rows > std::numeric_limits<size_t>::max() - valid_rows_) {
            ThrowInfo(DataTypeInvalid, "NGRAM row-size accounting overflows");
        }
        null_offsets_.reserve(null_offsets_.size() + added_nulls);

        std::string owned;
        for (size_t i = 0; i < n; ++i) {
            const auto offset = count_ + i;
            const auto state = state_at(i);
            if (state != JsonProjectedStringState::Value) {
                if (state == JsonProjectedStringState::FieldNull) {
                    null_offsets_.push_back(offset);
                }
                const std::string* empty = nullptr;
                engine_->add_array_data(empty, 0, static_cast<int64_t>(offset));
                continue;
            }
            AssignString(owned, value_at(i));
            engine_->add_data(&owned, 1, static_cast<int64_t>(offset));
        }
        count_ += n;
        total_bytes_ += added_bytes;
        valid_rows_ += added_rows;
    }

    NgramBuildParams params_;
    // The writer must be destroyed before its directory owner.
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    std::vector<size_t> null_offsets_;
    size_t total_bytes_{0};
    size_t valid_rows_{0};
    size_t count_{0};
};

template <typename T>
NgramIndexBuilder<T>::NgramIndexBuilder(NgramBuildParams params)
    : core_(std::make_unique<NgramBuilderCore>(std::move(params))) {
}

template <typename T>
NgramIndexBuilder<T>::~NgramIndexBuilder() = default;

template <typename T>
storage::ArtifactPtr
NgramIndexBuilder<T>::Build(const ScalarBuildInput<T>& input) && {
    for (const auto& batch : input.batches) {
        core_->AddBatch(batch);
    }
    return std::move(*core_).Finish();
}

namespace {

const bool kNgramBuilderRegistered = [] {
    BuilderRegistry<ScalarBuildInput<std::string_view>>::Instance().Register(
        families::kNgram, [](const BuildParams& params) {
            return std::make_unique<NgramIndexBuilder<std::string_view>>(
                ParseBuildParams<std::string_view>(params));
        });
    BuilderRegistry<ScalarBuildInput<JsonProjectedString>>::Instance().Register(
        families::kNgram, [](const BuildParams& params) {
            return std::make_unique<NgramIndexBuilder<JsonProjectedString>>(
                ParseBuildParams<JsonProjectedString>(params));
        });
    return true;
}();

}  // namespace

template class NgramIndexBuilder<std::string_view>;
template class NgramIndexBuilder<JsonProjectedString>;

}  // namespace milvus::index
