// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "indexbuilder/IndexBuildService.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/Utils.h"
#include "indexbuilder/BuildInputMaterializer.h"
#include "indexbuilder/VectorBuildMaterializer.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "milvus-storage/common/extend_status.h"
#include "storage/DataCodec.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"

namespace milvus::indexbuilder {
namespace {

constexpr int64_t kMissingBatchMaxRows = 64 * 1024;
constexpr size_t kMissingBatchTargetBytes = 4U << 20;

BuildFieldSpec
AdaptBuildField(const storage::FileManagerContext& context) {
    const auto& schema = context.fieldDataMeta.field_schema;
    BuildFieldSpec result;
    result.field_type = static_cast<DataType>(schema.data_type());
    result.element_type = static_cast<DataType>(schema.element_type());
    result.nullable = schema.nullable();
    if (schema.has_default_value()) {
        result.estimated_missing_row_bytes =
            std::max<size_t>(1, schema.default_value().ByteSizeLong());
    }
    return result;
}

storage::FieldDataMeta
AdaptOptionalBuildFieldMeta(const storage::FileManagerContext& context,
                            FieldId field_id,
                            DataType field_type,
                            DataType element_type) {
    proto::schema::FieldSchema schema;
    schema.set_fieldid(field_id.get());
    schema.set_data_type(static_cast<proto::schema::DataType>(field_type));
    schema.set_element_type(static_cast<proto::schema::DataType>(element_type));
    schema.set_nullable(true);
    return {context.fieldDataMeta.collection_id,
            context.fieldDataMeta.partition_id,
            context.fieldDataMeta.segment_id,
            field_id.get(),
            std::move(schema)};
}

FieldDataPtr
CreateMissingFieldData(const storage::FileManagerContext& context,
                       int64_t rows) {
    AssertInfo(rows >= 0 && static_cast<uint64_t>(rows) <=
                                static_cast<uint64_t>(
                                    std::numeric_limits<ssize_t>::max()),
               "missing-row batch size is out of range: {}",
               rows);
    const auto& schema = context.fieldDataMeta.field_schema;
    const auto field_type = static_cast<DataType>(schema.data_type());
    const auto element_type = static_cast<DataType>(schema.element_type());
    if (IsVectorDataType(field_type)) {
        AssertInfo(schema.nullable(),
                   "missing vector rows require a nullable field");
        AssertInfo(!schema.has_default_value(),
                   "vector fields cannot synthesize a default value");
        const auto dim = context.indexMeta.dim;
        AssertInfo(dim > 0 || (dim == 0 &&
                               field_type == DataType::VECTOR_SPARSE_U32_F32),
                   "missing vector rows have invalid dimension {}",
                   dim);
        auto batch = storage::CreateFieldData(field_type,
                                              element_type,
                                              /*nullable=*/true,
                                              dim,
                                              /*total_num_rows=*/0);
        AssertInfo(batch != nullptr,
                   "failed to allocate missing-vector field-data batch");
        if (rows != 0) {
            std::vector<uint8_t> validity((static_cast<size_t>(rows) + 7) / 8,
                                          0);
            batch->FillFieldData(
                nullptr, validity.data(), static_cast<ssize_t>(rows), 0);
        }
        return batch;
    }

    if (field_type == DataType::TEXT) {
        AssertInfo(schema.nullable(),
                   "missing TEXT rows require a nullable field");
        AssertInfo(!schema.has_default_value(),
                   "missing TEXT rows cannot synthesize a default value");
        auto batch = storage::CreateFieldData(field_type,
                                              element_type,
                                              /*nullable=*/true,
                                              /*dim=*/1,
                                              rows);
        AssertInfo(batch != nullptr,
                   "failed to allocate missing-TEXT field-data batch");
        auto nulls = arrow::MakeArrayOfNull(arrow::utf8(), rows);
        if (!nulls.ok()) {
            auto error = milvus_storage::ToSegcoreError(nulls.status());
            ThrowInfo(error.get_error_code(),
                      "failed to allocate missing TEXT rows: {}",
                      error.what());
        }
        batch->FillFieldData(std::move(nulls).ValueUnsafe());
        return batch;
    }

    std::optional<DefaultValueType> default_value;
    if (schema.has_default_value()) {
        default_value = schema.default_value();
    }

    // #52905 removed FieldDataBase::FillFieldData(default_value, rows); build
    // the batch straight from the default value instead.
    //
    // A nested ARRAY needs its TypeSchema, or the batch comes back as the flat
    // FieldData<Array> shape while the rest of the build feeds
    // FieldData<ArrayValue>. Mirrors FieldMeta::is_nested_array().
    std::optional<proto::schema::TypeSchema> array_type;
    if (field_type == DataType::ARRAY && schema.has_type_schema() &&
        schema.type_schema().has_array_element() &&
        schema.type_schema().array_element().has_array_element()) {
        array_type = schema.type_schema();
    }
    auto batch =
        storage::CreateFieldDataFromDefaultValue(field_type,
                                                 /*nullable=*/true,
                                                 static_cast<int64_t>(rows),
                                                 default_value,
                                                 std::move(array_type));
    AssertInfo(batch != nullptr,
               "failed to allocate missing-row field-data batch");
    return batch;
}

bool
CompatibleType(DataType left, DataType right) {
    return left == right ||
           (IsStringDataType(left) && IsStringDataType(right)) ||
           ((left == DataType::INT64 || left == DataType::TIMESTAMPTZ) &&
            (right == DataType::INT64 || right == DataType::TIMESTAMPTZ));
}

struct JsonProjectionShape {
    DataType value_type{DataType::NONE};
    bool is_array{false};
    bool is_flat{false};
};

JsonProjectionShape
ParseJsonProjectionShape(const index::BuildParams& params) {
    if (!params.contains(JSON_CAST_TYPE) ||
        !params.at(JSON_CAST_TYPE).is_string()) {
        ThrowInfo(DataTypeInvalid,
                  "JSON index build requires string parameter {}",
                  JSON_CAST_TYPE);
    }
    const auto cast = params.at(JSON_CAST_TYPE).get<std::string>();
    if (cast == "JSON") {
        return {.value_type = DataType::JSON, .is_flat = true};
    }
    if (cast == "BOOL") {
        return {.value_type = DataType::BOOL};
    }
    if (cast == "DOUBLE") {
        return {.value_type = DataType::DOUBLE};
    }
    if (cast == "VARCHAR") {
        return {.value_type = DataType::VARCHAR};
    }
    if (cast == "ARRAY_BOOL") {
        return {.value_type = DataType::BOOL, .is_array = true};
    }
    if (cast == "ARRAY_DOUBLE") {
        return {.value_type = DataType::DOUBLE, .is_array = true};
    }
    if (cast == "ARRAY_VARCHAR") {
        return {.value_type = DataType::VARCHAR, .is_array = true};
    }
    ThrowInfo(DataTypeInvalid, "unsupported JSON cast type {}", cast);
}

int64_t
ParseInt64(const nlohmann::json& value, std::string_view key) {
    const auto parsed = index::TryParseInt64Value(value);
    if (parsed.has_value()) {
        return *parsed;
    }
    ThrowInfo(
        UnexpectedError, "normalized build parameter {} is not an int64", key);
}

void
ValidateOrSetInt64(index::BuildParams& params,
                   std::string_view key,
                   int64_t expected) {
    if (params.contains(key) && !params.at(key).is_null()) {
        const auto actual = ParseInt64(params.at(key), key);
        if (actual != expected) {
            ThrowInfo(UnexpectedError,
                      "normalized build parameter {}={} conflicts with {}",
                      key,
                      actual,
                      expected);
        }
    }
    params[std::string(key)] = expected;
}

void
ValidateOrSetBool(index::BuildParams& params,
                  std::string_view key,
                  bool expected) {
    if (params.contains(key) && !params.at(key).is_null()) {
        const auto actual = index::GetValueFromConfigOrFallback<bool>(
            params, std::string(key), false);
        if (actual != expected) {
            ThrowInfo(UnexpectedError,
                      "normalized build parameter {} conflicts with field "
                      "schema",
                      key);
        }
    }
    params[std::string(key)] = expected;
}

void
ValidateOrSetString(index::BuildParams& params,
                    std::string_view key,
                    const std::string& expected) {
    if (params.contains(key) && !params.at(key).is_null()) {
        if (!params.at(key).is_string() ||
            params.at(key).get<std::string>() != expected) {
            ThrowInfo(UnexpectedError,
                      "normalized build parameter {} conflicts with request",
                      key);
        }
    }
    params[std::string(key)] = expected;
}

void
ValidateExistingString(const index::BuildParams& params,
                       std::string_view key,
                       const std::string& expected) {
    if (!params.contains(key)) {
        return;
    }
    if (!params.at(key).is_string() ||
        params.at(key).get<std::string>() != expected) {
        ThrowInfo(UnexpectedError,
                  "normalized build parameter {} conflicts with request",
                  key);
    }
}

void
NormalizeJsonPath(index::BuildParams& params, const std::string& canonical) {
    ValidateExistingString(params, JSON_PATH, canonical);
    ValidateExistingString(params, "nested_path", canonical);
    params.erase("nested_path");
    params[JSON_PATH] = canonical;
}

int64_t
ParseBinlogSequence(const std::string& path) {
    const auto slash = path.find_last_of('/');
    const auto name = std::string_view(path).substr(
        slash == std::string::npos ? 0 : slash + 1);
    int64_t sequence = 0;
    const auto [end, error] =
        std::from_chars(name.data(), name.data() + name.size(), sequence);
    if (name.empty() || error != std::errc{} ||
        end != name.data() + name.size()) {
        ThrowInfo(DataFormatBroken,
                  "insert binlog path has a non-numeric filename: {}",
                  path);
    }
    return sequence;
}

void
SortBinlogPaths(std::vector<std::string>& paths) {
    std::vector<std::pair<int64_t, std::string>> sequenced;
    sequenced.reserve(paths.size());
    for (auto& path : paths) {
        sequenced.emplace_back(ParseBinlogSequence(path), std::move(path));
    }
    std::sort(sequenced.begin(),
              sequenced.end(),
              [](const auto& left, const auto& right) {
                  return left.first < right.first;
              });
    for (size_t i = 0; i < sequenced.size(); ++i) {
        paths[i] = std::move(sequenced[i].second);
    }
}

int64_t
MissingBatchRows(size_t estimated_row_bytes, int64_t remaining) {
    estimated_row_bytes = std::max<size_t>(1, estimated_row_bytes);
    const auto bytes_limited =
        std::max<size_t>(1, kMissingBatchTargetBytes / estimated_row_bytes);
    const auto bounded = std::min<size_t>(
        static_cast<size_t>(kMissingBatchMaxRows), bytes_limited);
    return std::min<int64_t>(remaining, static_cast<int64_t>(bounded));
}

struct VectorSideInputPlan {
    FieldId field_id;
    DataType field_type{DataType::NONE};
    DataType element_type{DataType::NONE};
    std::vector<std::string> v1_files;
    storage::FieldDataMeta field_meta;
};

storage::VisitOutcome
VisitBuildField(const BuildSource& build_source,
                const std::vector<std::string>* v1_files_override,
                bool empty_v1_is_missing,
                FieldId field_id,
                DataType field_type,
                DataType element_type,
                int64_t dim,
                const storage::FieldDataMeta& field_meta,
                const storage::FileManagerContext& context,
                const storage::FieldDataVisitor& visitor,
                int64_t manifest_inflight_bytes) {
    return std::visit(
        [&](const auto& source) -> storage::VisitOutcome {
            using Source = std::decay_t<decltype(source)>;
            if constexpr (std::is_same_v<Source, V1BinlogBuildSource>) {
                auto files = v1_files_override == nullptr ? source.files
                                                          : *v1_files_override;
                if (files.empty()) {
                    return empty_v1_is_missing
                               ? storage::VisitOutcome::FieldMissing
                               : storage::VisitOutcome::Exhausted;
                }
                SortBinlogPaths(files);

                const auto slice_size = FILE_SLICE_SIZE.load();
                AssertInfo(slice_size > 0,
                           "index-build file slice size must be positive");
                const auto parallel_degree = std::max<int64_t>(
                    1, DEFAULT_FIELD_MAX_MEMORY_LIMIT / slice_size);
                for (size_t begin = 0; begin < files.size();) {
                    const auto remaining = files.size() - begin;
                    const auto count = std::min<size_t>(
                        remaining, static_cast<size_t>(parallel_degree));
                    std::vector<std::string> batch_files(
                        files.begin() + static_cast<ptrdiff_t>(begin),
                        files.begin() + static_cast<ptrdiff_t>(begin + count));
                    auto futures = storage::GetObjectData(
                        context.chunkManagerPtr.get(), batch_files);
                    std::exception_ptr first_failure;
                    bool stopped = false;
                    for (auto& future : futures) {
                        try {
                            auto codec = future.get();
                            AssertInfo(codec != nullptr,
                                       "binlog decoder returned null codec");
                            if (!first_failure && !stopped) {
                                stopped = visitor(codec->GetFieldData()) ==
                                          storage::VisitControl::Stop;
                            }
                        } catch (...) {
                            if (!first_failure) {
                                first_failure = std::current_exception();
                            }
                        }
                    }
                    if (first_failure) {
                        std::rethrow_exception(first_failure);
                    }
                    if (stopped) {
                        return storage::VisitOutcome::Stopped;
                    }
                    begin += count;
                }
                return storage::VisitOutcome::Exhausted;
            } else if constexpr (std::is_same_v<Source, StorageV2BuildSource>) {
                if (source.files.empty()) {
                    return storage::VisitOutcome::FieldMissing;
                }
                return storage::VisitFieldDataFromStorageV2(source.files,
                                                            field_id.get(),
                                                            field_type,
                                                            element_type,
                                                            dim,
                                                            context.fs,
                                                            visitor);
            } else if constexpr (std::is_same_v<Source, ManifestBuildSource>) {
                std::optional<storage::StorageColumnMapping> mapping;
                const auto mapping_it =
                    context.storage_column_mappings.find(field_id.get());
                if (mapping_it != context.storage_column_mappings.end()) {
                    mapping = mapping_it->second;
                }
                return storage::VisitFieldDataFromManifest(
                    source.manifest_path,
                    context.loon_ffi_properties,
                    field_meta,
                    field_type,
                    dim,
                    element_type,
                    std::move(mapping),
                    visitor,
                    manifest_inflight_bytes);
            }
        },
        build_source);
}

std::optional<VectorSideInputPlan>
PrepareVectorSideInputPlan(const BuildRequest& request,
                           const BuildFieldSpec& field_spec,
                           const storage::FileManagerContext& context,
                           const index::BuilderInputSpec& input_spec) {
    if (input_spec.side_inputs.empty()) {
        return std::nullopt;
    }
    if (field_spec.field_type == DataType::VECTOR_ARRAY) {
        ThrowInfo(Unsupported,
                  "VECTOR_ARRAY optional scalar input is not migrated");
    }
    AssertInfo(IsVectorDataType(field_spec.field_type),
               "non-vector builder declared vector optional scalar input");
    if (input_spec.side_inputs.size() != 1) {
        ThrowInfo(Unsupported,
                  "vector index build supports exactly one optional scalar "
                  "field");
    }

    const auto configured =
        index::GetValueFromConfig<OptFieldT>(request.params, VEC_OPT_FIELDS);
    AssertInfo(configured.has_value(),
               "vector builder declared side input without optional field "
               "metadata");
    if (configured->size() != 1) {
        ThrowInfo(Unsupported,
                  "vector index build supports exactly one optional scalar "
                  "field");
    }

    const auto field_id = input_spec.side_inputs.front();
    const auto it = configured->find(field_id.get());
    AssertInfo(it != configured->end(),
               "vector builder side-input field {} is absent from metadata",
               field_id.get());
    const auto& [field_name, field_type, element_type, files] = it->second;
    static_cast<void>(field_name);
    if (!IsSupportedVectorScalarInfoType(field_type) ||
        element_type != DataType::NONE) {
        ThrowInfo(Unsupported,
                  "optional scalar field {} has unsupported type {} and "
                  "element type {}",
                  field_id.get(),
                  field_type,
                  element_type);
    }

    return VectorSideInputPlan{
        .field_id = field_id,
        .field_type = field_type,
        .element_type = element_type,
        .v1_files = files,
        .field_meta = AdaptOptionalBuildFieldMeta(
            context, field_id, field_type, element_type)};
}

VectorScalarInfo
MaterializeVectorScalarInfo(const VectorSideInputPlan& plan,
                            const BuildRequest& request,
                            const storage::FileManagerContext& context,
                            const VectorPrimaryLayout& layout) {
    constexpr auto kMaxPhysicalRows =
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
    if (static_cast<uint64_t>(layout.PhysicalRows()) > kMaxPhysicalRows) {
        ThrowInfo(Unsupported,
                  "vector scalar-info physical row count {} exceeds uint32 "
                  "coordinate capacity",
                  layout.PhysicalRows());
    }

    VectorScalarInfoAccumulator accumulator(
        plan.field_id, plan.field_type, layout);
    const auto outcome = VisitBuildField(
        request.source,
        &plan.v1_files,
        /*empty_v1_is_missing=*/true,
        plan.field_id,
        plan.field_type,
        plan.element_type,
        /*dim=*/1,
        plan.field_meta,
        context,
        [&](FieldDataPtr batch) {
            accumulator.Add(batch);
            return storage::VisitControl::Continue;
        },
        storage::kAccumulatingInflightBytes);
    switch (outcome) {
        case storage::VisitOutcome::FieldMissing: {
            // Legacy V1 returned before inserting the field key when no paths
            // existed. V2/V3 inserted the requested key with no categories
            // after their field lookup returned no batches.
            if (std::holds_alternative<V1BinlogBuildSource>(request.source)) {
                return {};
            }
            VectorScalarInfo result;
            result.emplace(plan.field_id.get(),
                           std::vector<std::vector<uint32_t>>{});
            return result;
        }
        case storage::VisitOutcome::Exhausted:
            return std::move(accumulator).Finish();
        case storage::VisitOutcome::Stopped:
            ThrowInfo(UnexpectedError,
                      "always-continue optional scalar visitor stopped early");
    }
}

}  // namespace

BuildProduct
BuildProduct::FromArtifact(storage::ArtifactPtr artifact) {
    AssertInfo(artifact != nullptr, "index build produced a null artifact");
    return BuildProduct(Kind::Artifact, std::move(artifact));
}

BuildProduct
BuildProduct::SkippedEmpty() {
    return BuildProduct(Kind::SkippedEmpty, nullptr);
}

const storage::Artifact&
BuildProduct::GetArtifact() const {
    AssertInfo(kind_ == Kind::Artifact && artifact_ != nullptr,
               "skipped-empty build product has no artifact");
    return *artifact_;
}

IndexBuildService::IndexBuildService(
    BuildRequest request,
    const storage::FileManagerContext& file_manager_context)
    : req_(std::move(request)),
      file_manager_context_(file_manager_context),
      field_spec_(AdaptBuildField(file_manager_context_)) {
    NormalizeAndValidateRequest();
}

const BuildRequest&
IndexBuildService::Request() const noexcept {
    return req_;
}

const storage::FileManagerContext&
IndexBuildService::Context() const noexcept {
    return file_manager_context_;
}

void
IndexBuildService::NormalizeAndValidateRequest() {
    AssertInfo(file_manager_context_.Valid(),
               "index-build service requires a valid FileManagerContext");
    AssertInfo(req_.params.is_object(),
               "normalized index-build parameters must be an object");
    AssertInfo(req_.expected_rows >= 0,
               "index-build expected row count must be non-negative");
    AssertInfo(
        req_.missing_rows >= 0 && req_.missing_rows <= req_.expected_rows,
        "index-build missing row count {} is outside [0, {}]",
        req_.missing_rows,
        req_.expected_rows);
    AssertInfo(
        req_.field_id.get() == file_manager_context_.fieldDataMeta.field_id,
        "index-build field {} conflicts with FileManagerContext field "
        "{}",
        req_.field_id.get(),
        file_manager_context_.fieldDataMeta.field_id);
    AssertInfo(!req_.staging_parent.empty(),
               "index-build staging parent is not configured");

    if (req_.output.generation == storage::Generation::V3) {
        AssertInfo(!req_.output.packed_file_name.empty(),
                   "V3 index-build output requires a packed file name");
    } else {
        AssertInfo(req_.output.generation == storage::Generation::V1V2,
                   "unknown index-build output generation");
        AssertInfo(req_.output.packed_file_name.empty(),
                   "V1/V2 index-build output cannot use a packed file name");
    }

    const auto field_type = field_spec_.field_type;
    const auto element_type = field_spec_.element_type;
    AssertInfo(field_type != DataType::NONE,
               "index-build field schema has no data type");

    std::optional<JsonProjectionShape> json_projection;
    if (field_type == DataType::JSON) {
        NormalizeJsonPath(req_.params, req_.json_path);
        json_projection = ParseJsonProjectionShape(req_.params);
        if (req_.value_type != json_projection->value_type) {
            ThrowInfo(DataTypeInvalid,
                      "JSON build value type {} conflicts with its cast",
                      static_cast<int>(req_.value_type));
        }
        if (json_projection->is_flat) {
            if (req_.family != index::families::kJsonFlat) {
                ThrowInfo(DataTypeInvalid,
                          "JSON cast JSON requires the flat JSON family");
            }
        } else {
            if (req_.family == index::families::kJsonFlat ||
                req_.json_path.empty()) {
                ThrowInfo(DataTypeInvalid,
                          "typed JSON projection requires a non-empty path "
                          "and a predicate family");
            }
        }
    } else {
        AssertInfo(req_.json_path.empty(),
                   "non-JSON index build has an unexpected JSON path");
    }

    if (IsVectorDataType(field_type)) {
        AssertInfo(req_.output.generation == storage::Generation::V1V2,
                   "vector indexes have no V3 persisted format");
        AssertInfo(req_.family == index::families::kVectorMem ||
                       req_.family == index::families::kVectorDisk,
                   "vector build resolved to non-vector family {}",
                   req_.family);
        if (field_type == DataType::VECTOR_ARRAY) {
            AssertInfo(element_type != DataType::NONE &&
                           IsVectorDataType(element_type) &&
                           element_type != DataType::VECTOR_SPARSE_U32_F32 &&
                           element_type != DataType::VECTOR_ARRAY,
                       "VECTOR_ARRAY has invalid element type {}",
                       element_type);
            AssertInfo(req_.value_type == element_type,
                       "VECTOR_ARRAY build value type {} conflicts with "
                       "element type {}",
                       req_.value_type,
                       element_type);
        } else {
            AssertInfo(element_type == DataType::NONE,
                       "ordinary vector field has unexpected element type {}",
                       element_type);
            AssertInfo(req_.value_type == field_type,
                       "vector build value type {} conflicts with field type "
                       "{}",
                       req_.value_type,
                       field_type);
        }
    } else if (field_type == DataType::ARRAY) {
        AssertInfo(element_type != DataType::NONE,
                   "ARRAY index-build field schema has no element type");
        AssertInfo(CompatibleType(req_.value_type, element_type),
                   "ARRAY index-build value type {} conflicts with schema "
                   "element type {}",
                   static_cast<int>(req_.value_type),
                   static_cast<int>(element_type));
    } else if (field_type != DataType::JSON) {
        AssertInfo(CompatibleType(req_.value_type, field_type),
                   "index-build value type {} conflicts with schema field "
                   "type {}",
                   static_cast<int>(req_.value_type),
                   static_cast<int>(field_type));
    }

    ValidateOrSetInt64(req_.params, index::FIELD_ID, req_.field_id.get());
    ValidateOrSetString(req_.params, "local_dir", req_.staging_parent);
    ValidateOrSetInt64(
        req_.params, "field_type", static_cast<int64_t>(field_type));
    ValidateOrSetInt64(
        req_.params, "value_type", static_cast<int64_t>(req_.value_type));
    if (json_projection.has_value()) {
        const bool inner_nullable =
            json_projection->is_flat || json_projection->is_array
                ? field_spec_.nullable
                : true;
        req_.params["nullable"] = inner_nullable;
        ValidateOrSetInt64(
            req_.params, "element_type", static_cast<int64_t>(DataType::NONE));
        ValidateOrSetInt64(req_.params,
                           "array_element_type",
                           static_cast<int64_t>(DataType::NONE));
    } else {
        ValidateOrSetBool(req_.params, "nullable", field_spec_.nullable);
    }
    if (field_type == DataType::ARRAY) {
        ValidateOrSetInt64(req_.params,
                           "array_element_type",
                           static_cast<int64_t>(element_type));
        if (req_.params.contains("element_type") &&
            !req_.params.at("element_type").is_null()) {
            ValidateOrSetInt64(req_.params,
                               "element_type",
                               static_cast<int64_t>(element_type));
        }
    } else if (field_type == DataType::VECTOR_ARRAY) {
        ValidateOrSetInt64(
            req_.params, "element_type", static_cast<int64_t>(element_type));
        ValidateOrSetInt64(req_.params,
                           "array_element_type",
                           static_cast<int64_t>(element_type));
    }

    std::visit(
        [&](const auto& source) {
            using Source = std::decay_t<decltype(source)>;
            if constexpr (std::is_same_v<Source, StorageV2BuildSource>) {
                if (!source.files.empty()) {
                    AssertInfo(file_manager_context_.fs != nullptr,
                               "storage-v2 index build has no Arrow file "
                               "system");
                    for (const auto& group : source.files) {
                        AssertInfo(!group.empty(),
                                   "storage-v2 index build contains an empty "
                                   "column group");
                    }
                }
            } else if constexpr (std::is_same_v<Source, ManifestBuildSource>) {
                AssertInfo(!source.manifest_path.empty(),
                           "manifest index build has an empty manifest path");
                AssertInfo(file_manager_context_.loon_ffi_properties != nullptr,
                           "manifest index build has no storage properties");
            }
        },
        req_.source);
}

void
IndexBuildService::ValidateInputSpec(
    const index::BuilderInputSpec& spec) const {
    if (!spec.side_inputs.empty() &&
        !IsVectorDataType(field_spec_.field_type)) {
        ThrowInfo(UnexpectedError,
                  "non-vector builder declared unsupported side input");
    }
}

BuildProduct
IndexBuildService::RunToArtifact() {
    AssertInfo(!run_started_, "index-build service cannot run more than once");
    run_started_ = true;
    try {
        const auto add_missing_rows = [&](auto& materializer,
                                          int64_t missing_rows) {
            AssertInfo(missing_rows >= 0,
                       "index-build missing row count {} is negative",
                       missing_rows);
            AssertInfo(
                missing_rows == 0 || field_spec_.nullable ||
                    file_manager_context_.fieldDataMeta.field_schema
                        .has_default_value(),
                "index-build source is missing {} rows for non-nullable field "
                "{} without a default value",
                missing_rows,
                req_.field_id.get());
            while (missing_rows > 0) {
                const auto batch_rows = MissingBatchRows(
                    field_spec_.estimated_missing_row_bytes, missing_rows);
                materializer.Add(
                    CreateMissingFieldData(file_manager_context_, batch_rows));
                missing_rows -= batch_rows;
            }
        };

        const auto materialize_primary = [&](auto& materializer,
                                             int64_t manifest_inflight_bytes,
                                             const char* label) {
            const bool columnar_source =
                std::holds_alternative<StorageV2BuildSource>(req_.source) ||
                std::holds_alternative<ManifestBuildSource>(req_.source);
            const bool accumulate_columnar =
                columnar_source && req_.family != index::families::kVectorDisk;

            if (!columnar_source) {
                add_missing_rows(materializer, req_.missing_rows);
            }

            std::vector<FieldDataPtr> columnar_batches;
            int64_t columnar_rows = 0;
            const auto outcome = VisitBuildField(
                req_.source,
                nullptr,
                false,
                req_.field_id,
                field_spec_.field_type,
                field_spec_.element_type,
                file_manager_context_.indexMeta.dim,
                file_manager_context_.fieldDataMeta,
                file_manager_context_,
                [&](FieldDataPtr batch) {
                    if (!accumulate_columnar) {
                        materializer.Add(batch);
                        return storage::VisitControl::Continue;
                    }

                    AssertInfo(batch != nullptr,
                               "columnar source produced a null field-data "
                               "batch");
                    const auto rows = batch->Length();
                    AssertInfo(
                        rows <= static_cast<size_t>(
                                    std::numeric_limits<int64_t>::max()) &&
                            columnar_rows <= req_.expected_rows &&
                            static_cast<int64_t>(rows) <=
                                req_.expected_rows - columnar_rows,
                        "columnar source exceeds expected row count {}",
                        req_.expected_rows);
                    columnar_rows += static_cast<int64_t>(rows);
                    columnar_batches.push_back(std::move(batch));
                    return storage::VisitControl::Continue;
                },
                manifest_inflight_bytes);
            AssertInfo(outcome != storage::VisitOutcome::Stopped,
                       "always-continue {} visitor stopped early",
                       label);

            if (accumulate_columnar) {
                add_missing_rows(materializer,
                                 req_.expected_rows - columnar_rows);
                for (auto& batch : columnar_batches) {
                    materializer.Add(batch);
                    batch.reset();
                }
            }
        };

        if (req_.family == index::families::kVectorDisk) {
            VectorDiskBuildMaterializer materializer(
                req_.staging_parent,
                field_spec_.field_type,
                req_.value_type,
                file_manager_context_.indexMeta.dim,
                field_spec_.nullable,
                req_.expected_rows,
                req_.family,
                req_.params);
            const auto spec = materializer.InputSpec();
            ValidateInputSpec(spec);
            const auto vector_side_input = PrepareVectorSideInputPlan(
                req_, field_spec_, file_manager_context_, spec);

            materialize_primary(
                materializer, storage::kStreamingInflightBytes, "disk vector");
            materializer.FinishPrimary();
            if (materializer.RequiresEngineBuild() &&
                vector_side_input.has_value()) {
                auto scalar_info =
                    MaterializeVectorScalarInfo(*vector_side_input,
                                                req_,
                                                file_manager_context_,
                                                materializer.PrimaryLayout());
                materializer.SetScalarInfo(std::move(scalar_info));
            }
            return BuildProduct::FromArtifact(std::move(materializer).Build());
        }

        if (req_.family == index::families::kVectorMem) {
            VectorBuildMaterializer materializer(
                field_spec_.field_type,
                req_.value_type,
                file_manager_context_.indexMeta.dim,
                req_.expected_rows,
                req_.family,
                req_.params);
            const auto spec = materializer.InputSpec();
            ValidateInputSpec(spec);
            const auto vector_side_input = PrepareVectorSideInputPlan(
                req_, field_spec_, file_manager_context_, spec);
            materialize_primary(materializer,
                                storage::kAccumulatingInflightBytes,
                                "resident vector");
            if (vector_side_input.has_value()) {
                auto scalar_info =
                    MaterializeVectorScalarInfo(*vector_side_input,
                                                req_,
                                                file_manager_context_,
                                                materializer.PrimaryLayout());
                materializer.SetScalarInfo(std::move(scalar_info));
            }
            return BuildProduct::FromArtifact(std::move(materializer).Build());
        }

        auto materializer =
            MakeScalarBuildInputMaterializer(field_spec_.field_type,
                                             req_.value_type,
                                             req_.expected_rows,
                                             req_.family,
                                             req_.params);
        ValidateInputSpec(materializer->InputSpec());
        materialize_primary(
            *materializer, storage::kAccumulatingInflightBytes, "scalar");
        return BuildProduct::FromArtifact(std::move(*materializer).Build());
    } catch (const SegcoreError& error) {
        if (error.get_error_code() == DataIsEmpty) {
            return BuildProduct::SkippedEmpty();
        }
        throw;
    }
}

storage::ArtifactStats
IndexBuildService::Publish(const BuildProduct& product) const {
    if (product.IsSkippedEmpty()) {
        return {};
    }
    if (req_.output.generation == storage::Generation::V3) {
        storage::MemFileManagerImpl manager(file_manager_context_);
        const bool is_index =
            req_.output.storage_path == storage::ArtifactStoragePath::Index;
        auto writer = manager.CreateIndexEntryWriterUnified(
            req_.output.packed_file_name, is_index);
        if (writer == nullptr) {
            ThrowInfo(FileCreateFailed,
                      "failed to create V3 artifact writer for {}",
                      req_.output.packed_file_name);
        }
        product.GetArtifact().Serialize(*writer);
        writer->Finish();
        const auto bytes = writer->GetTotalBytesWritten();
        AssertInfo(
            bytes <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
            "V3 artifact serialized size overflow");
        const auto size = static_cast<int64_t>(bytes);
        const auto& name = req_.output.packed_file_name;
        const auto basename = std::filesystem::path(name).filename().string();
        const auto location =
            is_index ? manager.GetRemoteIndexObjectPrefix() + "/" + basename
                     : basename;
        return storage::ArtifactStats(
            size, {storage::SerializedFileInfo(location, size)});
    }
    auto sink = std::make_unique<storage::V1DiskSink>(file_manager_context_,
                                                      req_.output.storage_path);
    product.GetArtifact().Serialize(*sink);
    auto stats = sink->Finish();
    sink->ReleaseLocalStaging();
    return stats;
}

}  // namespace milvus::indexbuilder
