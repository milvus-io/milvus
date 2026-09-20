#include "folly/coro/BlockingWait.h"
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

#include "index/IndexTypeAdapter.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/JsonCastType.h"
#include "fmt/format.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/vector/VectorLoadResource.h"
#include "log/Log.h"
#include "storage/artifact/FileSource.h"

namespace milvus::index {

namespace {

JsonCastType
JsonCast(const Config& params) {
    AssertInfo(params.contains(JSON_CAST_TYPE),
               "json_cast_type is required for a JSON index");
    return JsonCastType::FromString(
        params.at(JSON_CAST_TYPE).get<std::string>());
}

bool
IsStringType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR ||
           type == DataType::TEXT;
}

bool
IsPrimitiveScalar(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::FLOAT ||
           type == DataType::DOUBLE || type == DataType::TIMESTAMPTZ ||
           IsStringType(type);
}

ScalarIndexType
ScalarArtifactType(const IndexFamily& family) {
    if (family == families::kBitmap) {
        return ScalarIndexType::BITMAP;
    }
    if (family == families::kSort) {
        return ScalarIndexType::STLSORT;
    }
    if (family == families::kMarisa) {
        return ScalarIndexType::MARISA;
    }
    if (family == families::kHybrid) {
        return ScalarIndexType::HYBRID;
    }
    if (family == families::kRTree) {
        return ScalarIndexType::RTREE;
    }
    if (family == families::kNgram) {
        return ScalarIndexType::NGRAM;
    }
    if (family == families::kFmIndex) {
        return ScalarIndexType::FMINDEX;
    }
    if (family == families::kInverted || family == families::kText ||
        family == families::kJsonFlat) {
        return ScalarIndexType::INVERTED;
    }
    return ScalarIndexType::NONE;
}

IndexFamily
ScalarFamily(const std::string& index_type,
             DataType value_type,
             bool is_text_match) {
    if (is_text_match) {
        AssertInfo(IsStringType(value_type),
                   "text match index requires a string field");
        AssertInfo(index_type == INVERTED_INDEX_TYPE,
                   "text match requires INVERTED index type");
        return families::kText;
    }
    if (index_type == INVERTED_INDEX_TYPE) {
        return families::kInverted;
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return families::kBitmap;
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return families::kHybrid;
    }
    if (index_type == NGRAM_INDEX_TYPE) {
        AssertInfo(IsStringType(value_type),
                   "NGRAM index requires a string value type");
        return families::kNgram;
    }
    if (index_type == FMINDEX_INDEX_TYPE) {
        AssertInfo(IsStringType(value_type),
                   "FMINDEX requires a string value type");
        return families::kFmIndex;
    }
    if (index_type == MARISA_TRIE || index_type == MARISA_TRIE_UPPER) {
        AssertInfo(IsStringType(value_type),
                   "Trie index requires a string value type");
        return families::kMarisa;
    }
    if (index_type == ASCENDING_SORT) {
        return families::kSort;
    }

    // The old primitive factory used sort as the fallback for non-string
    // scalar values. The string specialization rejected unknown spellings.
    AssertInfo(!IsStringType(value_type),
               "unsupported string index type: {}",
               index_type);
    return families::kSort;
}

DataType
JsonCastValueType(const JsonCastType& cast) {
    switch (cast.element_type()) {
        case JsonCastType::DataType::BOOL:
            return DataType::BOOL;
        case JsonCastType::DataType::DOUBLE:
            return DataType::DOUBLE;
        case JsonCastType::DataType::VARCHAR:
            return DataType::VARCHAR;
        case JsonCastType::DataType::JSON:
            return DataType::JSON;
        default:
            ThrowInfo(DataTypeInvalid, "unsupported JSON cast type: {}", cast);
    }
}

uint8_t
ReadHybridSelector(const nlohmann::json& value,
                   std::string_view artifact_generation) {
    uint64_t selector = 0;
    if (value.is_number_unsigned()) {
        selector = value.get<uint64_t>();
    } else if (value.is_number_integer()) {
        const auto signed_selector = value.get<int64_t>();
        if (signed_selector < 0) {
            ThrowInfo(DataFormatBroken,
                      "HYBRID {} index_type selector is negative: {}",
                      artifact_generation,
                      signed_selector);
        }
        selector = static_cast<uint64_t>(signed_selector);
    } else {
        ThrowInfo(DataFormatBroken,
                  "HYBRID {} index_type selector must be an integer",
                  artifact_generation);
    }
    if (selector > std::numeric_limits<uint8_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "HYBRID {} index_type selector is out of range: {}",
                  artifact_generation,
                  selector);
    }
    return static_cast<uint8_t>(selector);
}

// V3 packed file names encode the physical index type as a lowercased
// ScalarIndexType (see PackedScalarIndexFileName), e.g.
// "milvus_packed_stlsort_index.v3". The writer chose the name from the
// physical type, so it is an authoritative discriminator and stays correct as
// new physical types are added -- their meta keys may reuse generic names like
// "version" or "file_names", which would break meta-key-only inference.
// Returns an empty family for the genuine HYBRID name
// ("milvus_packed_hybrid_index.v3") and for anything unrecognized, so callers
// fall back to the file meta.
IndexFamily
FamilyFromPackedFileName(const std::string& path) {
    const auto slash = path.find_last_of('/');
    const auto filename =
        slash == std::string::npos ? path : path.substr(slash + 1);
    for (const auto type : {ScalarIndexType::STLSORT,
                            ScalarIndexType::INVERTED,
                            ScalarIndexType::BITMAP,
                            ScalarIndexType::MARISA}) {
        if (filename == PackedScalarIndexFileName(type)) {
            return FamilyFromScalarIndexType(type);
        }
    }
    return {};
}

// Physical families persist distinct meta keys; use them when neither the
// HYBRID selector nor a recognizable packed file name is available. Weaker than
// the file name (generic key names can collide across future families), so it
// stays a last resort.
IndexFamily
FamilyFromPhysicalMetaKeys(const nlohmann::json& metadata) {
    if (metadata.contains(sort_format::kVersion) ||
        metadata.contains(sort_format::kIndexLength)) {
        return families::kSort;
    }
    if (metadata.contains(FILE_NAMES)) {
        return families::kInverted;
    }
    if (metadata.contains(BITMAP_INDEX_LENGTH)) {
        return families::kBitmap;
    }
    return {};
}

}  // namespace

AdaptedIndexType
AdaptIndexType(const IndexTypeAdapterRequest& request) {
    AssertInfo(!request.index_type.empty(), "index type is empty");

    const auto configured_nested =
        ReadNestedConfigParam(request.params, "index type adapter");
    if (configured_nested.has_value() &&
        *configured_nested != request.is_nested) {
        ThrowInfo(DataTypeInvalid,
                  "configured nested value {} disagrees with the "
                  "schema-derived value {}",
                  *configured_nested,
                  request.is_nested);
    }

    AdaptedIndexType result;
    result.params = request.params;
    result.params[INDEX_TYPE] = request.index_type;
    result.params[INDEX_ENGINE_VERSION] =
        std::to_string(request.index_engine_version);
    result.params["field_type"] = static_cast<int32_t>(request.field_type);
    result.params[ELEMENT_TYPE_KEY] =
        static_cast<int32_t>(request.element_type);
    result.params["array_element_type"] =
        static_cast<int32_t>(request.element_type);
    result.params["nested"] = request.is_nested;
    result.params["is_nested"] = request.is_nested;

    if (IsVectorDataType(request.field_type)) {
        result.value_type = request.field_type == DataType::VECTOR_ARRAY
                                ? request.element_type
                                : request.field_type;
        result.family =
            VectorUsesDiskLoad(request.index_type, request.index_engine_version)
                ? families::kVectorDisk
                : families::kVectorMem;
        result.params["value_type"] = static_cast<int32_t>(result.value_type);
        return result;
    }

    if (request.field_type == DataType::GEOMETRY) {
        if (request.index_type != RTREE_INDEX_TYPE) {
            // A build this node cannot serve, not an unclassified bug: the Go
            // index scheduler matches ErrSegcoreUnsupported and fails the task
            // instead of re-dispatching it forever.
            ThrowInfo(Unsupported,
                      "geometry requires RTREE index, got {}",
                      request.index_type);
        }
        result.family = families::kRTree;
        result.value_type = DataType::GEOMETRY;
    } else if (request.field_type == DataType::JSON) {
        auto cast = JsonCast(request.params);
        result.value_type = JsonCastValueType(cast);
        if (cast.element_type() == JsonCastType::DataType::JSON) {
            if (request.index_type != INVERTED_INDEX_TYPE &&
                request.index_type != NGRAM_INDEX_TYPE) {
                ThrowInfo(
                    Unsupported,
                    "JSON flat index requires INVERTED or NGRAM spelling, "
                    "got {}",
                    request.index_type);
            }
            result.family = families::kJsonFlat;
        } else {
            const auto cast_type = cast.element_type();
            if (request.index_type == ASCENDING_SORT) {
                AssertInfo(cast_type == JsonCastType::DataType::DOUBLE ||
                               cast_type == JsonCastType::DataType::VARCHAR,
                           "JSON sort index requires DOUBLE or VARCHAR cast");
            } else if (request.index_type == BITMAP_INDEX_TYPE) {
                AssertInfo(cast_type == JsonCastType::DataType::BOOL ||
                               cast_type == JsonCastType::DataType::VARCHAR,
                           "JSON bitmap index requires BOOL or VARCHAR cast");
            } else if (request.index_type == NGRAM_INDEX_TYPE) {
                AssertInfo(cast_type == JsonCastType::DataType::VARCHAR,
                           "JSON NGRAM index requires VARCHAR cast");
            } else {
                AssertInfo(request.index_type == INVERTED_INDEX_TYPE ||
                               request.index_type == HYBRID_INDEX_TYPE,
                           "unsupported JSON index type: {}",
                           request.index_type);
            }
            result.family =
                ScalarFamily(request.index_type, result.value_type, false);
        }
    } else if (request.field_type == DataType::ARRAY) {
        // ARRAY indexes expose predicates over their element type; field_type
        // retains the parent ARRAY identity and coordinate semantics.
        result.value_type = request.element_type;
        if (request.is_nested) {
            result.family =
                request.index_type == HYBRID_INDEX_TYPE   ? families::kHybrid
                : request.index_type == BITMAP_INDEX_TYPE ? families::kBitmap
                : request.index_type == INVERTED_INDEX_TYPE
                    ? families::kInverted
                    : families::kSort;
        } else {
            AssertInfo(request.index_type == HYBRID_INDEX_TYPE ||
                           request.index_type == BITMAP_INDEX_TYPE ||
                           request.index_type == INVERTED_INDEX_TYPE,
                       "unsupported ARRAY index type: {}",
                       request.index_type);
            result.family =
                ScalarFamily(request.index_type, request.element_type, false);
        }
    } else {
        AssertInfo(IsPrimitiveScalar(request.field_type),
                   "invalid data type to build index: {}",
                   request.field_type);
        result.value_type = request.field_type == DataType::TIMESTAMPTZ
                                ? DataType::INT64
                                : request.field_type;
        auto text_match = request.is_text_match ||
                          GetValueFromConfigOrFallback<bool>(
                              request.params, "is_text_match", false);
        result.family =
            ScalarFamily(request.index_type, result.value_type, text_match);
    }

    result.params["value_type"] = static_cast<int32_t>(result.value_type);
    result.artifact_type = ScalarArtifactType(result.family);
    return result;
}

IndexFamily
FamilyFromScalarIndexType(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::BITMAP:
            return families::kBitmap;
        case ScalarIndexType::STLSORT:
            return families::kSort;
        case ScalarIndexType::MARISA:
            return families::kMarisa;
        case ScalarIndexType::INVERTED:
            return families::kInverted;
        default:
            return {};
    }
}

IndexFamily
ResolvePackedLoadFamily(const IndexFamily& requested_family,
                        const nlohmann::json& metadata,
                        const Config& load_params) {
    if (requested_family != families::kHybrid) {
        return requested_family;
    }
    auto value = metadata.find(INDEX_TYPE);
    if (value == metadata.end()) {
        // Legacy 3.0.0 files (issue #52620): a struct-array sub-field
        // HYBRID index was written by the plain sort factory, so the
        // physical file carries no HYBRID selector at all while collection
        // metadata still says HYBRID. Failing here fails segment load for
        // an index that is perfectly readable, and no reindex or
        // object-store rewrite is needed to recover the physical family.
        auto recovered = IndexFamily{};
        if (auto files = GetValueFromConfig<std::vector<std::string>>(
                load_params, INDEX_FILES);
            files.has_value() && !files->empty()) {
            recovered = FamilyFromPackedFileName(files->front());
        }
        if (recovered.empty()) {
            recovered = FamilyFromPhysicalMetaKeys(metadata);
        }
        if (recovered.empty()) {
            ThrowInfo(DataFormatBroken,
                      "HYBRID V3 artifact has no index_type selector, a "
                      "recognizable packed file name, or a recognizable "
                      "physical index meta");
        }
        LOG_WARN(
            "HYBRID V3 artifact has no index_type selector; inferred "
            "physical family: {}",
            recovered);
        return recovered;
    }
    const auto encoded_type = ReadHybridSelector(*value, "V3");
    const auto selector = static_cast<ScalarIndexType>(encoded_type);
    auto family = FamilyFromScalarIndexType(selector);
    if (family.empty()) {
        ThrowInfo(DataFormatBroken,
                  "unsupported HYBRID internal index type: {}",
                  encoded_type);
    }
    return family;
}

folly::coro::Task<IndexFamily>
ResolveLoadFamilyAsync(const IndexFamily& requested_family,
                       storage::FileSource& source,
                       const Config& load_params,
                       bool use_async) {
    if (requested_family != families::kHybrid) {
        co_return requested_family;
    }

    uint8_t encoded_type = 0;
    auto bytes = co_await source.ReadEntryAsync(INDEX_TYPE, use_async);
    if (bytes.size() != sizeof(encoded_type)) {
        ThrowInfo(DataFormatBroken,
                  "invalid HYBRID V1/V2 selector size: {}",
                  bytes.size());
    }
    std::memcpy(&encoded_type, bytes.data(), sizeof(encoded_type));

    const auto selector = static_cast<ScalarIndexType>(encoded_type);
    auto family = FamilyFromScalarIndexType(selector);
    if (family.empty()) {
        ThrowInfo(DataFormatBroken,
                  "unsupported HYBRID internal index type: {}",
                  encoded_type);
    }
    co_return family;
}

IndexFamily
ResolveLoadFamily(const IndexFamily& requested_family,
                  storage::FileSource& source,
                  const Config& load_params) {
    return folly::coro::blockingWait(
        ResolveLoadFamilyAsync(requested_family, source, load_params, false));
}

std::string
PackedScalarIndexFileName(ScalarIndexType type) {
    auto type_name = ToString(type);
    AssertInfo(type != ScalarIndexType::NONE && type_name != "UNKNOWN",
               "invalid scalar artifact type for V3 file name");
    std::transform(type_name.begin(),
                   type_name.end(),
                   type_name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return "milvus_packed_" + type_name + "_index.v3";
}

}  // namespace milvus::index
