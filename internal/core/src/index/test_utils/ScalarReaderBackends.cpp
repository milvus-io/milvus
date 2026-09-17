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

#include "index/test_utils/ScalarReaderFactory.h"

#include <cstddef>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/JsonCastType.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/scalar/json/JsonProjectedIndexArtifact.h"
#include "index/scalar/ngram/JsonProjectedString.h"

namespace milvus::index::test {
namespace {

std::function<void(Config&, const BackendCaseMetadata&)>
LocalBuildParams(
    std::function<void(Config&, const BackendCaseMetadata&)> complete = {}) {
    return [complete = std::move(complete)](
               Config& params, const BackendCaseMetadata& metadata) {
        params["local_dir"] = std::filesystem::temp_directory_path().string();
        if (complete) {
            complete(params, metadata);
        }
    };
}

std::string
JsonPath(const BackendCaseMetadata& metadata, std::string fallback) {
    if (!metadata.values.contains(JSON_PATH)) {
        return fallback;
    }
    if (!metadata.values.at(JSON_PATH).is_string()) {
        throw std::logic_error("test dataset json_path must be a string");
    }
    return metadata.values.at(JSON_PATH).get<std::string>();
}

std::function<void(Config&, const BackendCaseMetadata&)>
CompleteJsonParams(std::string cast,
                   std::string default_path,
                   bool needs_local_dir) {
    return
        [cast = std::move(cast),
         default_path = std::move(default_path),
         needs_local_dir](Config& params, const BackendCaseMetadata& metadata) {
            if (needs_local_dir) {
                params["local_dir"] =
                    std::filesystem::temp_directory_path().string();
            }
            params[JSON_PATH] = JsonPath(metadata, default_path);
            params[JSON_CAST_TYPE] = cast;
        };
}

std::function<storage::ArtifactPtr(storage::ArtifactPtr,
                                   const BackendCaseMetadata&)>
WrapJsonProjection(std::string cast, std::string default_path = "/a") {
    return [cast = JsonCastType::FromString(cast),
            default_path = std::move(default_path)](
               storage::ArtifactPtr inner,
               const BackendCaseMetadata& metadata) mutable {
        std::vector<size_t> non_exist_offsets;
        if (metadata.values.contains("non_exist_offsets")) {
            non_exist_offsets = metadata.values.at("non_exist_offsets")
                                    .get<std::vector<size_t>>();
        }
        return std::make_unique<JsonProjectedIndexArtifact>(
            std::move(inner),
            JsonPath(metadata, default_path),
            cast,
            static_cast<int64_t>(metadata.row_count),
            std::move(non_exist_offsets),
            true);
    };
}

const char*
ProjectedIndexType(const char* family) {
    if (std::string_view(family) == families::kBitmap) {
        return BITMAP_INDEX_TYPE;
    }
    if (std::string_view(family) == families::kSort) {
        return ASCENDING_SORT;
    }
    if (std::string_view(family) == families::kInverted) {
        return INVERTED_INDEX_TYPE;
    }
    if (std::string_view(family) == families::kHybrid) {
        return HYBRID_INDEX_TYPE;
    }
    throw std::logic_error("unsupported projected test backend family");
}

template <typename InputT>
void
AddNullablePair(BackendCatalog& catalog,
                std::string nullable_name,
                std::string nonnull_name,
                BackendSpec spec) {
    spec.name = std::move(nullable_name);
    spec.nullable = true;
    catalog.Add<InputT>(spec);
    spec.name = std::move(nonnull_name);
    spec.nullable = false;
    catalog.Add<InputT>(std::move(spec));
}

template <typename T>
void
AddNestedProfile(BackendCatalog& catalog,
                 std::string name,
                 const char* family,
                 Config build_params = Config::object(),
                 std::vector<std::string> load_families = {},
                 PatternQueryPolicies pattern = {}) {
    BackendSpec spec;
    spec.name = name;
    spec.family = family;
    spec.build_params = build_params;
    spec.load_families = load_families;
    spec.pattern = pattern;
    spec.input_shape = BackendInputShape::NestedElements;
    spec.field_type = DataType::ARRAY;
    spec.value_type = detail::ScalarTestType<T>();
    spec.array_element_type = detail::ScalarTestType<T>();
    spec.expected_domain = Domain::Element;
    catalog.Add<T>(spec);

    spec.name = std::move(name) + "Mmap";
    spec.enable_mmap = true;
    catalog.Add<T>(std::move(spec));
}

template <typename T>
void
AddArrayRowsProfiles(BackendCatalog& catalog,
                     std::string_view type_name,
                     std::string_view family_name,
                     const char* family,
                     Config build_params = Config::object(),
                     std::vector<std::string> load_families = {},
                     PatternQueryPolicies pattern = {}) {
    const auto base =
        std::string(family_name) + std::string(type_name) + "Array";
    for (const auto mmap : {false, true}) {
        for (const auto nullable : {true, false}) {
            BackendSpec spec;
            spec.name = base;
            if (!nullable) {
                spec.name += "NonNull";
            }
            if (mmap) {
                spec.name += "Mmap";
            }
            spec.family = family;
            spec.nullable = nullable;
            spec.build_params = build_params;
            spec.enable_mmap = mmap;
            spec.load_families = load_families;
            spec.pattern = pattern;
            spec.input_shape = BackendInputShape::ArrayRows;
            spec.field_type = DataType::ARRAY;
            spec.build_field_type = DataType::ARRAY;
            spec.value_type = detail::ScalarTestType<T>();
            spec.array_element_type = detail::ScalarTestType<T>();
            catalog.Add<ArrayView>(std::move(spec));
        }
    }
}

template <typename T>
void
AddPrimitiveProfiles(BackendCatalog& catalog, std::string_view type_name) {
    const auto add = [&]<typename U>(
                         std::string_view family_name,
                         const char* family,
                         Config build_params = Config::object(),
                         std::vector<std::string> load_families = {},
                         PatternQueryPolicies pattern = {}) {
        auto name = std::string(family_name) + std::string(type_name);
        catalog.Add<U>({
            .name = name,
            .family = family,
            .nullable = true,
            .build_params = build_params,
            .load_families = load_families,
            .pattern = pattern,
        });
        catalog.Add<U>({
            .name = name + "Mmap",
            .family = family,
            .nullable = true,
            .build_params = build_params,
            .enable_mmap = true,
            .load_families = load_families,
            .pattern = pattern,
        });
        catalog.Add<U>({
            .name = name + "NonNull",
            .family = family,
            .nullable = false,
            .build_params = build_params,
            .load_families = load_families,
            .pattern = pattern,
        });
        catalog.Add<U>({
            .name = name + "NonNullMmap",
            .family = family,
            .nullable = false,
            .build_params = std::move(build_params),
            .enable_mmap = true,
            .load_families = std::move(load_families),
            .pattern = pattern,
        });
    };

    add.template operator()<T>("Bitmap", families::kBitmap);
    add.template operator()<T>("Sorted", families::kSort);

    PatternQueryPolicies inverted;
    inverted.postfix = PatternQueryPolicy::DeclineButRun;
    inverted.inner = PatternQueryPolicy::DeclineButRun;
    inverted.regex = PatternQueryPolicy::DeclineButRun;
    Config inverted_params = {{FIELD_ID, 101}};
    add.template operator()<T>(
        "Inverted", families::kInverted, inverted_params, {}, inverted);

    const auto high_family = std::is_same_v<T, std::string_view>
                                 ? INVERTED_INDEX_TYPE
                                 : ASCENDING_SORT;
    Config hybrid_params = {
        {FIELD_ID, 101},
        {BITMAP_INDEX_CARDINALITY_LIMIT, 16},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
        {HYBRID_LOW_CARDINALITY_INDEX_TYPE, BITMAP_INDEX_TYPE},
        {HYBRID_HIGH_CARDINALITY_INDEX_TYPE, high_family},
    };
    std::vector<std::string> hybrid_load_families = {
        families::kBitmap,
        std::is_same_v<T, std::string_view> ? families::kInverted
                                            : families::kSort,
    };
    PatternQueryPolicies hybrid_pattern;
    if constexpr (std::is_same_v<T, std::string_view>) {
        hybrid_pattern.postfix = PatternQueryPolicy::SelectiveAndRun;
        hybrid_pattern.inner = PatternQueryPolicy::SelectiveAndRun;
        hybrid_pattern.regex = PatternQueryPolicy::SelectiveAndRun;
    }
    add.template operator()<T>("Hybrid",
                               families::kHybrid,
                               hybrid_params,
                               hybrid_load_families,
                               hybrid_pattern);

    const auto nested_name = [&](std::string_view family_name) {
        return std::string(family_name) + std::string(type_name) + "Nested";
    };
    AddNestedProfile<T>(catalog, nested_name("Bitmap"), families::kBitmap);
    AddNestedProfile<T>(catalog, nested_name("Sorted"), families::kSort);
    AddNestedProfile<T>(catalog,
                        nested_name("Inverted"),
                        families::kInverted,
                        inverted_params,
                        {},
                        inverted);
    Config nested_hybrid_params = {
        {FIELD_ID, 101},
        {BITMAP_INDEX_CARDINALITY_LIMIT, 16},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
    };
    AddNestedProfile<T>(catalog,
                        nested_name("Hybrid"),
                        families::kHybrid,
                        std::move(nested_hybrid_params),
                        {families::kBitmap, families::kInverted},
                        hybrid_pattern);

    AddArrayRowsProfiles<T>(catalog, type_name, "Bitmap", families::kBitmap);
    AddArrayRowsProfiles<T>(catalog, type_name, "Sorted", families::kSort);
    AddArrayRowsProfiles<T>(catalog,
                            type_name,
                            "Inverted",
                            families::kInverted,
                            std::move(inverted_params),
                            {},
                            inverted);
    Config array_hybrid_params = {
        {FIELD_ID, 101},
        {BITMAP_INDEX_CARDINALITY_LIMIT, 16},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
    };
    AddArrayRowsProfiles<T>(catalog,
                            type_name,
                            "Hybrid",
                            families::kHybrid,
                            std::move(array_hybrid_params),
                            {families::kBitmap, families::kInverted},
                            hybrid_pattern);
}

void
AddTextProfile(BackendCatalog& catalog,
               std::string name,
               DataType value_type,
               int engine_version,
               bool enable_mmap,
               bool consume,
               std::string analyzer) {
    BackendSpec spec;
    spec.family = families::kText;
    spec.build_params = {
        {FIELD_ID, 101},
        {SCALAR_INDEX_ENGINE_VERSION, engine_version},
        {"analyzer_name", "milvus_tokenizer"},
        {"analyzer_params", std::move(analyzer)},
    };
    if (!consume) {
        spec.complete_build_params = LocalBuildParams();
    }
    spec.enable_mmap = enable_mmap;
    spec.field_type = value_type;
    spec.value_type = value_type;
    spec.open_mode =
        consume ? BackendOpenMode::Consume : BackendOpenMode::Serialize;
    AddNullablePair<std::string_view>(
        catalog, name, name + "NonNull", std::move(spec));
}

void
AddTextProfiles(BackendCatalog& catalog) {
    constexpr auto standard = R"({"tokenizer":"standard"})";
    constexpr auto jieba = R"({"tokenizer":"jieba"})";
    AddTextProfile(catalog,
                   "TextVarcharRamV7",
                   DataType::VARCHAR,
                   3,
                   false,
                   true,
                   standard);
    AddTextProfile(catalog,
                   "TextVarcharV7Heap",
                   DataType::VARCHAR,
                   3,
                   false,
                   false,
                   standard);
    AddTextProfile(catalog,
                   "TextVarcharV7Mmap",
                   DataType::VARCHAR,
                   3,
                   true,
                   false,
                   standard);
    AddTextProfile(catalog,
                   "TextVarcharV5Heap",
                   DataType::VARCHAR,
                   1,
                   false,
                   false,
                   standard);
    AddTextProfile(catalog,
                   "TextVarcharV5Mmap",
                   DataType::VARCHAR,
                   1,
                   true,
                   false,
                   standard);
    AddTextProfile(catalog,
                   "TextStringV7Heap",
                   DataType::STRING,
                   3,
                   false,
                   false,
                   standard);
    AddTextProfile(catalog,
                   "TextStringV7Mmap",
                   DataType::STRING,
                   3,
                   true,
                   false,
                   standard);
    AddTextProfile(
        catalog, "TextTextV7Heap", DataType::TEXT, 3, false, false, standard);
    AddTextProfile(
        catalog, "TextTextV7Mmap", DataType::TEXT, 3, true, false, standard);
    AddTextProfile(catalog,
                   "TextJiebaVarcharRamV7",
                   DataType::VARCHAR,
                   3,
                   false,
                   true,
                   jieba);
    AddTextProfile(catalog,
                   "TextJiebaVarcharV7Heap",
                   DataType::VARCHAR,
                   3,
                   false,
                   false,
                   jieba);
    AddTextProfile(catalog,
                   "TextJiebaVarcharV7Mmap",
                   DataType::VARCHAR,
                   3,
                   true,
                   false,
                   jieba);
}

void
AddNgramScalarProfile(BackendCatalog& catalog,
                      std::string name,
                      DataType value_type,
                      int min_gram,
                      int max_gram,
                      bool mmap) {
    BackendSpec spec;
    spec.family = families::kNgram;
    spec.build_params = {
        {FIELD_ID, 101},
        {MIN_GRAM, min_gram},
        {MAX_GRAM, max_gram},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
    };
    spec.load_params = spec.build_params;
    spec.complete_build_params = LocalBuildParams();
    spec.enable_mmap = mmap;
    spec.field_type = value_type;
    spec.value_type = value_type;
    AddNullablePair<std::string_view>(
        catalog, name, name + "NonNull", std::move(spec));
}

void
AddNgramProfiles(BackendCatalog& catalog) {
    AddNgramScalarProfile(
        catalog, "NgramVarcharMin2Max4Heap", DataType::VARCHAR, 2, 4, false);
    AddNgramScalarProfile(
        catalog, "NgramVarcharMin2Max4Mmap", DataType::VARCHAR, 2, 4, true);
    AddNgramScalarProfile(
        catalog, "NgramVarcharMin3Max3Heap", DataType::VARCHAR, 3, 3, false);
    AddNgramScalarProfile(
        catalog, "NgramVarcharMin3Max3Mmap", DataType::VARCHAR, 3, 3, true);
    AddNgramScalarProfile(
        catalog, "NgramStringMin2Max4Heap", DataType::STRING, 2, 4, false);
    AddNgramScalarProfile(
        catalog, "NgramStringMin2Max4Mmap", DataType::STRING, 2, 4, true);
    AddNgramScalarProfile(
        catalog, "NgramTextMin2Max4Heap", DataType::TEXT, 2, 4, false);
    AddNgramScalarProfile(
        catalog, "NgramTextMin2Max4Mmap", DataType::TEXT, 2, 4, true);

    for (const auto mmap : {false, true}) {
        BackendSpec spec;
        spec.name = mmap ? "NgramJsonVarcharMin2Max4Mmap"
                         : "NgramJsonVarcharMin2Max4Heap";
        spec.family = families::kNgram;
        spec.nullable = true;
        spec.build_params = {
            {FIELD_ID, 101},
            {MIN_GRAM, 2},
            {MAX_GRAM, 4},
            {SCALAR_INDEX_ENGINE_VERSION, 3},
        };
        spec.load_params = spec.build_params;
        spec.load_params[INDEX_TYPE] = NGRAM_INDEX_TYPE;
        spec.enable_mmap = mmap;
        spec.input_shape = BackendInputShape::JsonProjected;
        spec.field_type = DataType::JSON;
        spec.build_field_type = DataType::JSON;
        spec.value_type = DataType::VARCHAR;
        spec.complete_build_params = CompleteJsonParams("VARCHAR", "/a", true);
        spec.complete_load_params = CompleteJsonParams("VARCHAR", "/a", false);
        spec.wrap_artifact = WrapJsonProjection("VARCHAR");
        catalog.Add<JsonProjectedString>(std::move(spec));
    }
}

void
AddSpatialProfiles(BackendCatalog& catalog) {
    for (const auto mmap : {false, true}) {
        BackendSpec spec;
        const auto name = std::string(mmap ? "SpatialRTreeMmapRequested"
                                           : "SpatialRTreeHeap");
        spec.family = families::kRTree;
        spec.enable_mmap = mmap;
        spec.input_shape = BackendInputShape::SpatialWkb;
        spec.field_type = DataType::GEOMETRY;
        spec.value_type = DataType::GEOMETRY;
        spec.complete_build_params = LocalBuildParams();
        spec.complete_load_params = [](Config& params,
                                       const BackendCaseMetadata& metadata) {
            params["num_rows"] = metadata.row_count;
        };
        AddNullablePair<std::string_view>(
            catalog, name, name + "NonNull", std::move(spec));
    }
}

void
AddJsonFlatProfile(BackendCatalog& catalog,
                   std::string name,
                   std::string nonnull_name,
                   int engine_version,
                   bool mmap) {
    BackendSpec spec;
    spec.family = families::kJsonFlat;
    spec.build_params = {
        {FIELD_ID, 101},
        {SCALAR_INDEX_ENGINE_VERSION, engine_version},
    };
    spec.enable_mmap = mmap;
    spec.input_shape = BackendInputShape::JsonDocument;
    spec.field_type = DataType::JSON;
    spec.value_type = DataType::JSON;
    spec.complete_build_params = CompleteJsonParams("JSON", "", true);
    spec.complete_load_params = CompleteJsonParams("JSON", "", false);
    AddNullablePair<std::string_view>(
        catalog, name, std::move(nonnull_name), std::move(spec));
}

Config
ProjectedFamilyParams(const char* family, DataType value_type) {
    if (std::string_view(family) == families::kInverted) {
        return {{FIELD_ID, 101}, {SCALAR_INDEX_ENGINE_VERSION, 3}};
    }
    if (std::string_view(family) != families::kHybrid) {
        return Config::object();
    }
    const auto high_family =
        IsStringDataType(value_type) ? INVERTED_INDEX_TYPE : ASCENDING_SORT;
    return {
        {FIELD_ID, 101},
        {BITMAP_INDEX_CARDINALITY_LIMIT, 16},
        {SCALAR_INDEX_ENGINE_VERSION, 3},
        {HYBRID_LOW_CARDINALITY_INDEX_TYPE, BITMAP_INDEX_TYPE},
        {HYBRID_HIGH_CARDINALITY_INDEX_TYPE, high_family},
    };
}

std::vector<std::string>
ProjectedLoadFamilies(const char* family, DataType value_type) {
    if (std::string_view(family) != families::kHybrid) {
        return {};
    }
    return {
        families::kBitmap,
        IsStringDataType(value_type) ? families::kInverted : families::kSort};
}

template <typename InputT>
void
AddProjectedProfileVariants(BackendCatalog& catalog,
                            std::string base_name,
                            const char* family,
                            DataType value_type,
                            std::string cast,
                            DataType build_field_type,
                            DataType array_element_type = DataType::NONE) {
    for (const auto mmap : {false, true}) {
        for (const auto nullable : {true, false}) {
            BackendSpec spec;
            spec.name = base_name;
            if (!nullable) {
                spec.name += "NonNull";
            }
            if (mmap) {
                spec.name += "Mmap";
            }
            spec.family = family;
            spec.nullable = nullable;
            spec.build_params = ProjectedFamilyParams(family, value_type);
            spec.load_params = {{INDEX_TYPE, ProjectedIndexType(family)}};
            spec.enable_mmap = mmap;
            spec.load_families = ProjectedLoadFamilies(family, value_type);
            spec.input_shape = BackendInputShape::JsonProjected;
            spec.field_type = DataType::JSON;
            spec.build_field_type = build_field_type;
            spec.value_type = value_type;
            spec.array_element_type = array_element_type;
            spec.complete_load_params = CompleteJsonParams(cast, "/a", false);
            spec.wrap_artifact = WrapJsonProjection(cast);
            catalog.Add<InputT>(std::move(spec));
        }
    }
}

void
AddProjectedProfiles(BackendCatalog& catalog) {
    AddProjectedProfileVariants<double>(catalog,
                                        "JsonProjectedSortedDouble",
                                        families::kSort,
                                        DataType::DOUBLE,
                                        "DOUBLE",
                                        DataType::DOUBLE);
    AddProjectedProfileVariants<double>(catalog,
                                        "JsonProjectedInvertedDouble",
                                        families::kInverted,
                                        DataType::DOUBLE,
                                        "DOUBLE",
                                        DataType::DOUBLE);
    AddProjectedProfileVariants<double>(catalog,
                                        "JsonProjectedHybridDouble",
                                        families::kHybrid,
                                        DataType::DOUBLE,
                                        "DOUBLE",
                                        DataType::DOUBLE);

    AddProjectedProfileVariants<std::string_view>(catalog,
                                                  "JsonProjectedSortedVarchar",
                                                  families::kSort,
                                                  DataType::VARCHAR,
                                                  "VARCHAR",
                                                  DataType::VARCHAR);
    AddProjectedProfileVariants<std::string_view>(catalog,
                                                  "JsonProjectedBitmapVarchar",
                                                  families::kBitmap,
                                                  DataType::VARCHAR,
                                                  "VARCHAR",
                                                  DataType::VARCHAR);
    AddProjectedProfileVariants<std::string_view>(
        catalog,
        "JsonProjectedInvertedVarchar",
        families::kInverted,
        DataType::VARCHAR,
        "VARCHAR",
        DataType::VARCHAR);
    AddProjectedProfileVariants<std::string_view>(catalog,
                                                  "JsonProjectedHybridVarchar",
                                                  families::kHybrid,
                                                  DataType::VARCHAR,
                                                  "VARCHAR",
                                                  DataType::VARCHAR);

    AddProjectedProfileVariants<bool>(catalog,
                                      "JsonProjectedBitmapBool",
                                      families::kBitmap,
                                      DataType::BOOL,
                                      "BOOL",
                                      DataType::BOOL);
    AddProjectedProfileVariants<bool>(catalog,
                                      "JsonProjectedInvertedBool",
                                      families::kInverted,
                                      DataType::BOOL,
                                      "BOOL",
                                      DataType::BOOL);
    AddProjectedProfileVariants<bool>(catalog,
                                      "JsonProjectedHybridBool",
                                      families::kHybrid,
                                      DataType::BOOL,
                                      "BOOL",
                                      DataType::BOOL);

    AddProjectedProfileVariants<ArrayView>(catalog,
                                           "JsonProjectedInvertedArrayBool",
                                           families::kInverted,
                                           DataType::BOOL,
                                           "ARRAY_BOOL",
                                           DataType::ARRAY,
                                           DataType::BOOL);
    AddProjectedProfileVariants<ArrayView>(catalog,
                                           "JsonProjectedInvertedArrayDouble",
                                           families::kInverted,
                                           DataType::DOUBLE,
                                           "ARRAY_DOUBLE",
                                           DataType::ARRAY,
                                           DataType::DOUBLE);
    AddProjectedProfileVariants<ArrayView>(catalog,
                                           "JsonProjectedInvertedArrayVarchar",
                                           families::kInverted,
                                           DataType::VARCHAR,
                                           "ARRAY_VARCHAR",
                                           DataType::ARRAY,
                                           DataType::VARCHAR);
}

void
AddJsonProfiles(BackendCatalog& catalog) {
    AddJsonFlatProfile(catalog, "JsonFlatV7", "JsonFlatV7NonNull", 3, false);
    AddJsonFlatProfile(
        catalog, "JsonFlatV7Mmap", "JsonFlatV7NonNullMmap", 3, true);
    AddProjectedProfiles(catalog);
}

}  // namespace

const BackendCatalog&
ScalarReaderBackends() {
    static const auto backends = [] {
        BackendCatalog catalog;

        AddPrimitiveProfiles<bool>(catalog, "Bool");
        AddPrimitiveProfiles<int8_t>(catalog, "Int8");
        AddPrimitiveProfiles<int16_t>(catalog, "Int16");
        AddPrimitiveProfiles<int32_t>(catalog, "Int32");
        AddPrimitiveProfiles<int64_t>(catalog, "Int64");
        AddPrimitiveProfiles<float>(catalog, "Float");
        AddPrimitiveProfiles<double>(catalog, "Double");
        AddPrimitiveProfiles<std::string_view>(catalog, "Varchar");

        catalog.Add<std::string_view>({
            .name = "MarisaVarchar",
            .family = families::kMarisa,
            .nullable = true,
        });
        catalog.Add<std::string_view>({
            .name = "MarisaVarcharMmap",
            .family = families::kMarisa,
            .nullable = true,
            .enable_mmap = true,
        });
        catalog.Add<std::string_view>({
            .name = "MarisaVarcharNonNull",
            .family = families::kMarisa,
            .nullable = false,
        });
        catalog.Add<std::string_view>({
            .name = "MarisaVarcharNonNullMmap",
            .family = families::kMarisa,
            .nullable = false,
            .enable_mmap = true,
        });
        catalog.Add<std::string_view>({
            .name = "MarisaStringNonNull",
            .family = families::kMarisa,
            .nullable = false,
            .field_type = DataType::STRING,
            .value_type = DataType::STRING,
        });

        PatternQueryPolicies fm_pattern;
        // General LIKE is served as a candidate superset the executor
        // rechecks on the raw column (FmIndexReader::PatternMatchIsExact).
        fm_pattern.match = PatternQueryPolicy::CandidatesAndRun;
        fm_pattern.prefix = PatternQueryPolicy::SelectiveAndRun;
        fm_pattern.postfix = PatternQueryPolicy::SelectiveAndRun;
        fm_pattern.inner = PatternQueryPolicy::SelectiveAndRun;
        fm_pattern.regex = PatternQueryPolicy::Unsupported;
        catalog.Add<std::string_view>({
            .name = "FmIndexVarchar",
            .family = families::kFmIndex,
            .nullable = true,
            .build_params = {{FM_SA_SAMPLE_RATE, 32}},
            .pattern = fm_pattern,
        });
        catalog.Add<std::string_view>({
            .name = "FmIndexVarcharMmap",
            .family = families::kFmIndex,
            .nullable = true,
            .build_params = {{FM_SA_SAMPLE_RATE, 32}},
            .enable_mmap = true,
            .pattern = fm_pattern,
        });
        catalog.Add<std::string_view>({
            .name = "FmIndexVarcharNonNull",
            .family = families::kFmIndex,
            .nullable = false,
            .build_params = {{FM_SA_SAMPLE_RATE, 32}},
            .pattern = fm_pattern,
        });
        catalog.Add<std::string_view>({
            .name = "FmIndexVarcharNonNullMmap",
            .family = families::kFmIndex,
            .nullable = false,
            .build_params = {{FM_SA_SAMPLE_RATE, 32}},
            .enable_mmap = true,
            .pattern = fm_pattern,
        });

        AddTextProfiles(catalog);
        AddNgramProfiles(catalog);
        AddSpatialProfiles(catalog);
        AddJsonProfiles(catalog);
        return catalog;
    }();
    return backends;
}

}  // namespace milvus::index::test
