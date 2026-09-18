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

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>

#include "index/IndexTypeAdapter.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/test_utils/TestArtifactIO.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index::test {
namespace {

bool
CapsEqual(const ReaderCaps& left, const ReaderCaps& right) {
    return left.predicate == right.predicate &&
           left.pattern_match == right.pattern_match &&
           left.text_match == right.text_match &&
           left.ngram_candidates == right.ngram_candidates &&
           left.spatial == right.spatial && left.nested == right.nested &&
           left.value_lookup == right.value_lookup &&
           left.cheap_value_lookup == right.cheap_value_lookup &&
           left.json_paths == right.json_paths && left.exact == right.exact;
}

Config
ConfiguredMetadata(DataType field_type,
                   DataType value_type,
                   DataType array_element_type,
                   bool nullable,
                   bool nested) {
    Config metadata = {
        {"field_type", static_cast<int32_t>(field_type)},
        {"value_type", static_cast<int32_t>(value_type)},
        {"nullable", nullable},
        {"nested", nested},
    };
    if (field_type == DataType::ARRAY) {
        metadata["array_element_type"] =
            static_cast<int32_t>(array_element_type);
    }
    return metadata;
}

void
AddConfiguredMetadata(std::string_view name,
                      Config& params,
                      const Config& metadata) {
    if (!params.is_object()) {
        throw std::logic_error(std::string(name) +
                               ": parameters must be objects");
    }
    for (const auto& [key, value] : metadata.items()) {
        if (params.contains(key)) {
            throw std::logic_error(std::string(name) + ": reserved parameter " +
                                   key + "; use BackendSpec fields");
        }
        params[key] = value;
    }
}

void
ValidateReader(std::string_view name,
               const ReaderBackend& backend,
               const ReaderCaps& expected_caps,
               const IIndexReaderBasePtr& reader) {
    if (!reader) {
        throw std::logic_error(std::string(name) +
                               ": loader returned no reader");
    }
    if (!CapsEqual(expected_caps, reader->Caps())) {
        throw std::logic_error(std::string(name) +
                               ": opened reader caps disagree with loader");
    }
    if (reader->CoordDomain() != backend.ExpectedDomain()) {
        throw std::logic_error(std::string(name) +
                               ": opened reader coordinate domain disagrees");
    }
    if (!ScalarValueTypesMatch(reader->ValueType(),
                               backend.ExpectedValueType())) {
        throw std::logic_error(std::string(name) +
                               ": opened reader value type disagrees");
    }
}

}  // namespace

PatternQueryPolicy
PatternQueryPolicies::Get(PatternOp op) const {
    switch (op) {
        case PatternOp::Match:
            return match;
        case PatternOp::PrefixMatch:
            return prefix;
        case PatternOp::PostfixMatch:
            return postfix;
        case PatternOp::InnerMatch:
            return inner;
        case PatternOp::RegexMatch:
            return regex;
    }
    throw std::logic_error("invalid pattern operation");
}

Config
ReaderBackend::CompletedBuildParams(const BackendCaseMetadata& metadata) const {
    auto params = spec_.build_params;
    if (spec_.complete_build_params) {
        spec_.complete_build_params(params, metadata);
    }
    if (!params.is_object()) {
        throw std::logic_error(Name() +
                               ": completed build parameters must be object");
    }
    return params;
}

Config
ReaderBackend::LoadParams(const BackendCaseMetadata& metadata) const {
    auto params = spec_.load_params;
    if (spec_.complete_load_params) {
        spec_.complete_load_params(params, metadata);
    }
    if (!params.is_object()) {
        throw std::logic_error(Name() +
                               ": completed load parameters must be object");
    }
    return params;
}

LoaderEntry
ReaderBackend::Loader(std::string_view family) const {
    const auto loader = LoaderRegistry::Instance().Lookup(std::string(family));
    if (!loader) {
        throw std::logic_error(Name() + ": no loader for family " +
                               std::string(family));
    }
    return loader;
}

ReaderCaps
ReaderBackend::DeriveCaps(const BackendCaseMetadata& metadata) const {
    auto params = LoadParams(metadata);
    if (spec_.wrap_artifact) {
        TestArtifactData marker;
        marker.metadata.emplace("has_non_exist", false);
        TestArtifactSource source(marker);
        params = AnnotateJsonProjectionCompleteness(std::move(params), source);
    }
    return Loader(spec_.load_families.front()).derive_caps(params);
}

bool
ReaderBackend::Supports(bool ReaderCaps::*capability) const {
    return std::all_of(spec_.load_families.begin(),
                       spec_.load_families.end(),
                       [&](const auto& family) {
                           auto params = spec_.load_params;
                           if (spec_.complete_load_params) {
                               spec_.complete_load_params(params, {});
                           }
                           if (spec_.wrap_artifact) {
                               TestArtifactData marker;
                               marker.metadata.emplace("has_non_exist", false);
                               TestArtifactSource source(marker);
                               params = AnnotateJsonProjectionCompleteness(
                                   std::move(params), source);
                           }
                           return Loader(family).derive_caps(params).*
                                  capability;
                       });
}

bool
ReaderBackend::BuilderRegistered(const BackendCaseMetadata& metadata) const {
    return builder_input_spec_(CompletedBuildParams(metadata)).has_value();
}

BuilderInputSpec
ReaderBackend::InputSpec(const BackendCaseMetadata& metadata) const {
    auto result = builder_input_spec_(CompletedBuildParams(metadata));
    if (!result.has_value()) {
        throw std::logic_error(Name() + ": no builder for family/input");
    }
    return std::move(*result);
}

bool
ReaderBackend::LoadersRegistered() const {
    return std::all_of(
        spec_.load_families.begin(),
        spec_.load_families.end(),
        [&](const auto& family) {
            return static_cast<bool>(LoaderRegistry::Instance().Lookup(family));
        });
}

IIndexReaderBasePtr
ReaderBackend::Open(storage::ArtifactPtr artifact,
                    const BackendCaseMetadata& metadata) const {
    if (!artifact) {
        throw std::logic_error(Name() + ": cannot open a null artifact");
    }

    auto params = LoadParams(metadata);
    if (spec_.open_mode == BackendOpenMode::Consume) {
        if (spec_.load_families.size() != 1) {
            throw std::logic_error(
                Name() + ": consumed artifact needs one loader family");
        }
        const auto expected_caps =
            Loader(spec_.load_families.front()).derive_caps(params);
        auto reader = IReaderConvertible::FromArtifact(std::move(artifact));
        ValidateReader(Name(), *this, expected_caps, reader);
        return reader;
    }

    TestArtifactData persisted;
    TestArtifactSink sink(persisted);
    artifact->Serialize(sink);
    static_cast<void>(sink.Finish());

    TestArtifactSource source(persisted);
    const auto resolved_family = ResolveLoadFamily(spec_.family, source);
    if (std::find(spec_.load_families.begin(),
                  spec_.load_families.end(),
                  resolved_family) == spec_.load_families.end()) {
        throw std::logic_error(Name() + ": unexpected resolved family " +
                               resolved_family);
    }

    if (spec_.wrap_artifact) {
        params = AnnotateJsonProjectionCompleteness(std::move(params), source);
    }
    const auto loader = Loader(resolved_family);
    const auto expected_caps = loader.derive_caps(params);
    storage::LoadOptions options;
    options.enable_mmap = spec_.enable_mmap;
    options.mmap_dir_path = spec_.mmap_dir_path;
    if (options.enable_mmap && options.mmap_dir_path.empty()) {
        options.mmap_dir_path = std::filesystem::temp_directory_path().string();
    }
    options.params = std::move(params);
    auto reader = loader.open(source, options);
    ValidateReader(Name(), *this, expected_caps, reader);
    return reader;
}

void
BackendCatalog::Add(
    std::type_index input_type,
    DataType inferred_value_type,
    BackendSpec spec,
    std::function<std::optional<BuilderInputSpec>(const Config&)>
        builder_input_spec) {
    for (const auto& backend : backends_) {
        if (backend.Name() == spec.name) {
            throw std::logic_error(spec.name + ": duplicate backend name");
        }
    }
    if (spec.name.empty() || spec.family.empty()) {
        throw std::logic_error("backend requires a name and family");
    }
    if (spec.value_type == DataType::NONE) {
        spec.value_type = inferred_value_type;
    }
    if (spec.field_type == DataType::NONE) {
        spec.field_type = spec.input_shape == BackendInputShape::NestedElements
                              ? DataType::ARRAY
                              : inferred_value_type;
    }
    if (spec.build_field_type == DataType::NONE) {
        spec.build_field_type = spec.field_type;
    }
    if (spec.value_type == DataType::NONE ||
        spec.field_type == DataType::NONE ||
        spec.build_field_type == DataType::NONE) {
        throw std::logic_error(spec.name +
                               ": logical field/value types are required");
    }
    if ((spec.field_type == DataType::ARRAY ||
         spec.build_field_type == DataType::ARRAY) &&
        spec.array_element_type == DataType::NONE) {
        spec.array_element_type = spec.value_type;
    }
    if (spec.expected_domain == Domain::Element &&
        spec.input_shape != BackendInputShape::NestedElements) {
        throw std::logic_error(spec.name +
                               ": element domain requires nested input shape");
    }
    if (spec.input_shape == BackendInputShape::NestedElements &&
        spec.expected_domain != Domain::Element) {
        throw std::logic_error(spec.name +
                               ": nested input shape requires element domain");
    }
    if (spec.load_families.empty()) {
        spec.load_families.push_back(spec.family);
    }
    for (size_t i = 0; i < spec.load_families.size(); ++i) {
        if (spec.load_families[i].empty() ||
            std::find(spec.load_families.begin(),
                      spec.load_families.begin() + i,
                      spec.load_families[i]) !=
                spec.load_families.begin() + i) {
            throw std::logic_error(spec.name +
                                   ": invalid resolved load family list");
        }
    }

    AddConfiguredMetadata(
        spec.name,
        spec.build_params,
        ConfiguredMetadata(spec.build_field_type,
                           spec.value_type,
                           spec.array_element_type,
                           spec.nullable,
                           spec.expected_domain == Domain::Element));
    AddConfiguredMetadata(
        spec.name,
        spec.load_params,
        ConfiguredMetadata(spec.field_type,
                           spec.value_type,
                           spec.array_element_type,
                           spec.nullable,
                           spec.expected_domain == Domain::Element));
    backends_.push_back(ReaderBackend(
        input_type, std::move(spec), std::move(builder_input_spec)));
}

std::vector<ReaderBackend>
BackendCatalog::Select(std::type_index input_type,
                       BackendInputShape input_shape,
                       Domain domain,
                       bool ReaderCaps::*capability,
                       bool requires_nullable,
                       std::optional<DataType> logical_value_type) const {
    std::vector<ReaderBackend> selected;
    selected.reserve(backends_.size());
    for (const auto& backend : backends_) {
        if (backend.input_type_ != input_type ||
            backend.InputShape() != input_shape ||
            backend.ExpectedDomain() != domain ||
            (requires_nullable && !backend.Nullable()) ||
            (logical_value_type.has_value() &&
             !ScalarValueTypesMatch(backend.ExpectedValueType(),
                                    *logical_value_type)) ||
            (capability != nullptr && !backend.Supports(capability))) {
            continue;
        }
        selected.push_back(backend);
    }
    return selected;
}

const ReaderBackend&
BackendCatalog::Get(std::type_index input_type, std::string_view name) const {
    for (const auto& backend : backends_) {
        if (backend.input_type_ == input_type && backend.Name() == name) {
            return backend;
        }
    }
    throw std::logic_error(std::string(name) +
                           ": no backend for name/input type");
}

}  // namespace milvus::index::test
