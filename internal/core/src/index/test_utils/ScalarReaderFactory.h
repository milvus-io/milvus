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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

#include "index/contracts/Registry.h"
#include "index/contracts/build/IReaderConvertible.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/ReaderCaps.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/test_utils/ScalarTestData.h"
#include "storage/artifact/Artifact.h"

namespace milvus::index::test {

enum class PatternQueryPolicy {
    UseAndRun,
    DeclineButRun,
    Unsupported,
    SelectiveAndRun,
    // The reader answers this operation with a CANDIDATE SUPERSET and declares
    // it through IPatternMatchReader::PatternMatchIsExact(op) == false. Routing
    // is selective (the family's own cost guard decides), and the result is
    // asserted to CONTAIN the expected offsets rather than equal them: the
    // consumer is required to recheck candidates against the raw column.
    // FM-index's general LIKE (PatternOp::Match) is the only current case.
    CandidatesAndRun,
};

struct PatternQueryPolicies {
    PatternQueryPolicy match{PatternQueryPolicy::UseAndRun};
    PatternQueryPolicy prefix{PatternQueryPolicy::UseAndRun};
    PatternQueryPolicy postfix{PatternQueryPolicy::UseAndRun};
    PatternQueryPolicy inner{PatternQueryPolicy::UseAndRun};
    PatternQueryPolicy regex{PatternQueryPolicy::UseAndRun};

    PatternQueryPolicy
    Get(PatternOp op) const;
};

enum class BackendOpenMode {
    Serialize,
    Consume,
};

struct BackendCaseMetadata {
    size_t row_count{0};
    Config values = Config::object();
};

// One creation profile in the central test catalog. Add<InputT> binds the C++
// input representation. Logical types and coordinate domain remain explicit
// because Array, JSON projection, spatial, text, and nested inputs differ from
// ordinary scalar rows.
struct BackendSpec {
    std::string name;
    std::string family;
    bool nullable{false};
    Config build_params = Config::object();
    Config load_params = Config::object();
    bool enable_mmap{false};
    std::string mmap_dir_path;
    // Empty defaults to {family}. Selector builders list every family their
    // persisted artifact may resolve to.
    std::vector<std::string> load_families;
    PatternQueryPolicies pattern;

    BackendInputShape input_shape{BackendInputShape::Scalar};
    DataType field_type{DataType::NONE};
    // Defaults to field_type. Projected JSON profiles build the projected
    // scalar/array and load the wrapped artifact as JSON.
    DataType build_field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    DataType array_element_type{DataType::NONE};
    Domain expected_domain{Domain::Row};
    BackendOpenMode open_mode{BackendOpenMode::Serialize};
    std::function<void(Config&, const BackendCaseMetadata&)>
        complete_build_params;
    std::function<void(Config&, const BackendCaseMetadata&)>
        complete_load_params;
    std::function<storage::ArtifactPtr(storage::ArtifactPtr,
                                       const BackendCaseMetadata&)>
        wrap_artifact;
};

namespace detail {

template <typename T>
constexpr DataType
ScalarTestTypeOrNone() {
    return CppDataType<T>(std::is_same_v<T, std::string_view>
                              ? DataType::VARCHAR
                              : DataType::NONE);
}

template <typename T>
constexpr DataType
ScalarTestType() {
    constexpr auto type = ScalarTestTypeOrNone<T>();
    static_assert(type != DataType::NONE,
                  "scalar test backends require a primitive or string_view");
    return type;
}

template <typename T>
size_t
InputRowCount(const ScalarBuildInput<T>& input) {
    size_t count = 0;
    for (const auto& batch : input.batches) {
        if (batch.values.size() > std::numeric_limits<size_t>::max() - count) {
            throw std::logic_error("test input row count overflows");
        }
        count += batch.values.size();
    }
    return count;
}

}  // namespace detail

// Prepared configuration shared by all test input shapes. Production
// registries still provide builder selection, loader selection, and caps.
class ReaderBackend {
 public:
    const std::string&
    Name() const {
        return spec_.name;
    }

    const std::string&
    Family() const {
        return spec_.family;
    }

    bool
    Nullable() const {
        return spec_.nullable;
    }

    BackendInputShape
    InputShape() const {
        return spec_.input_shape;
    }

    Domain
    ExpectedDomain() const {
        return spec_.expected_domain;
    }

    DataType
    ExpectedValueType() const {
        return spec_.value_type;
    }

    BackendOpenMode
    OpenMode() const {
        return spec_.open_mode;
    }

    bool
    MmapRequested() const {
        return spec_.enable_mmap;
    }

    // Stable configured parameters before dataset-specific completion.
    const Config&
    BuildParams() const {
        return spec_.build_params;
    }

    Config
    LoadParams(const BackendCaseMetadata& metadata = {}) const;

    PatternQueryPolicy
    PatternPolicy(PatternOp op) const {
        return spec_.pattern.Get(op);
    }

    ReaderCaps
    DeriveCaps(const BackendCaseMetadata& metadata = {}) const;

    bool
    Supports(bool ReaderCaps::*capability) const;

    bool
    BuilderRegistered(const BackendCaseMetadata& metadata = {}) const;

    BuilderInputSpec
    InputSpec(const BackendCaseMetadata& metadata = {}) const;

    bool
    LoadersRegistered() const;

    // Prepare registry lookup and dataset-specific builder configuration
    // separately from the one-shot Build call. Negative builder-contract tests
    // use this to keep fixture/setup failures outside their expected-error
    // boundary.
    template <typename T>
    std::unique_ptr<IArtifactBuilder<ScalarBuildInput<T>>>
    CreateBuilder(BackendCaseMetadata metadata = {}) const {
        if (input_type_ != std::type_index(typeid(T))) {
            throw std::logic_error(Name() + ": wrong test input type");
        }
        auto builder = BuilderRegistry<ScalarBuildInput<T>>::Instance().Create(
            spec_.family, CompletedBuildParams(metadata));
        if (!builder) {
            throw std::logic_error(Name() + ": no builder for family/input");
        }
        return builder;
    }

    template <typename T>
    storage::ArtifactPtr
    Build(const ScalarBuildInput<T>& input,
          BackendCaseMetadata metadata = {}) const {
        if (input_type_ != std::type_index(typeid(T))) {
            throw std::logic_error(Name() + ": wrong test input type");
        }
        if (metadata.row_count == 0) {
            metadata.row_count = detail::InputRowCount(input);
        }
        auto builder = CreateBuilder<T>(metadata);
        auto artifact = std::move(*builder).Build(input);
        if (!artifact) {
            throw std::logic_error(Name() + ": builder returned no artifact");
        }
        if (spec_.wrap_artifact) {
            artifact = spec_.wrap_artifact(std::move(artifact), metadata);
            if (!artifact) {
                throw std::logic_error(Name() +
                                       ": artifact wrapper returned null");
            }
        }
        return artifact;
    }

    IIndexReaderBasePtr
    Open(storage::ArtifactPtr artifact,
         const BackendCaseMetadata& metadata = {}) const;

    template <typename T>
    IIndexReaderBasePtr
    Create(const ScalarBuildInput<T>& input,
           BackendCaseMetadata metadata = {}) const {
        if (metadata.row_count == 0) {
            metadata.row_count = detail::InputRowCount(input);
        }
        return Open(Build(input, metadata), metadata);
    }

 private:
    friend class BackendCatalog;

    ReaderBackend(std::type_index input_type,
                  BackendSpec spec,
                  std::function<std::optional<BuilderInputSpec>(const Config&)>
                      builder_input_spec)
        : input_type_(input_type),
          spec_(std::move(spec)),
          builder_input_spec_(std::move(builder_input_spec)) {
    }

    Config
    CompletedBuildParams(const BackendCaseMetadata& metadata) const;

    LoaderEntry
    Loader(std::string_view family) const;

    std::type_index input_type_;
    BackendSpec spec_;
    std::function<std::optional<BuilderInputSpec>(const Config&)>
        builder_input_spec_;
};

class BackendCatalog {
 public:
    template <typename InputT>
    void
    Add(BackendSpec spec) {
        auto builder_input_spec =
            [family = spec.family](
                const Config& params) -> std::optional<BuilderInputSpec> {
            auto builder =
                BuilderRegistry<ScalarBuildInput<InputT>>::Instance().Create(
                    family, params);
            if (!builder) {
                return std::nullopt;
            }
            return builder->InputSpec();
        };
        Add(std::type_index(typeid(InputT)),
            detail::ScalarTestTypeOrNone<InputT>(),
            std::move(spec),
            std::move(builder_input_spec));
    }

    // Existing predicate/pattern cases remain scalar row-domain by default.
    template <typename T>
    std::vector<ReaderBackend>
    For(bool ReaderCaps::*capability) const {
        return For<T>(capability, false);
    }

    template <typename T>
    std::vector<ReaderBackend>
    For(bool ReaderCaps::*capability, bool requires_nullable) const {
        return ForInput<T>(BackendInputShape::Scalar,
                           Domain::Row,
                           capability,
                           requires_nullable,
                           detail::ScalarTestType<T>());
    }

    template <typename InputT>
    std::vector<ReaderBackend>
    ForInput(BackendInputShape input_shape,
             Domain domain,
             bool ReaderCaps::*capability,
             bool requires_nullable,
             std::optional<DataType> logical_value_type = std::nullopt) const {
        return Select(std::type_index(typeid(InputT)),
                      input_shape,
                      domain,
                      capability,
                      requires_nullable,
                      logical_value_type);
    }

    template <typename T>
    std::vector<ReaderBackend>
    All(bool requires_nullable) const {
        return AllInput<T>(BackendInputShape::Scalar,
                           Domain::Row,
                           requires_nullable,
                           detail::ScalarTestType<T>());
    }

    template <typename InputT>
    std::vector<ReaderBackend>
    AllInput(BackendInputShape input_shape,
             Domain domain,
             bool requires_nullable,
             std::optional<DataType> logical_value_type = std::nullopt) const {
        return Select(std::type_index(typeid(InputT)),
                      input_shape,
                      domain,
                      nullptr,
                      requires_nullable,
                      logical_value_type);
    }

    // Focused family/capability guards use an unfiltered named configuration.
    template <typename InputT>
    const ReaderBackend&
    Get(std::string_view name) const {
        return Get(std::type_index(typeid(InputT)), name);
    }

    const std::vector<ReaderBackend>&
    Profiles() const {
        return backends_;
    }

 private:
    void
    Add(std::type_index input_type,
        DataType inferred_value_type,
        BackendSpec spec,
        std::function<std::optional<BuilderInputSpec>(const Config&)>
            builder_input_spec);

    std::vector<ReaderBackend>
    Select(std::type_index input_type,
           BackendInputShape input_shape,
           Domain domain,
           bool ReaderCaps::*capability,
           bool requires_nullable,
           std::optional<DataType> logical_value_type) const;

    const ReaderBackend&
    Get(std::type_index input_type, std::string_view name) const;

    std::vector<ReaderBackend> backends_;
};

const BackendCatalog&
ScalarReaderBackends();

}  // namespace milvus::index::test
