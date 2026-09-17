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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {

// GTest sees one uniform parameter type. Its callback owns only descriptors,
// operation arguments, and one backend profile; data stays lazy per test.
struct FilterParam {
    std::string name;
    std::function<void()> run;
};

inline std::string
FilterParamName(const ::testing::TestParamInfo<FilterParam>& info) {
    return info.param.name;
}

// For small, manually specified answers. The result bitmap is allocated only
// during execution. Large/custom answers can supply an expected callback.
inline auto
ManualHits(std::vector<size_t> offsets) {
    return [offsets = std::move(offsets)](const auto& data, const auto&) {
        return Hits(data.values.size(), offsets);
    };
}

template <typename Op>
struct Query {
    using ValueType = typename Op::ValueType;
    using Args = typename Op::Args;
    using Expected = std::function<TargetBitmap(
        const ScalarTestData<ValueType>&, const Args&)>;

    Args args;
    // Used only by SelectiveAndRun routing cases. Ordinary result cases leave
    // this empty and still execute the direct query.
    std::optional<bool> expected_should_use;
    // Query-only failure expectation. Reader preparation and routing checks
    // remain outside the exception boundary.
    std::optional<ErrorCode> expected_error;
    // Empty selects Op::Oracle. ManualHits({}) is a nonempty callback that
    // produces an all-zero bitmap, so it never falls back to the oracle.
    Expected expected;
};

template <typename T>
using ObserveFn = std::function<void(
    const ReaderBackend&, const ScalarTestData<T>&, IIndexReaderBasePtr&)>;

template <typename T>
struct Observe {
    bool ReaderCaps::*capability{nullptr};
    ObserveFn<T> run;
};

struct BuildFails {
    ErrorCode expected_error;
    // Opt in only for a negative builder contract that deliberately feeds a
    // dataset containing null rows to profiles selected as non-nullable.
    // Registration rejects any selector result that still contains a nullable
    // profile.
    bool allow_nullability_mismatch{false};
};

enum class CasePhase {
    Query,
    Observe,
    Build,
};

enum class InputLifetime {
    // The generated data and its input views remain alive through the query or
    // observation callback.
    KeepUntilBodyCompletes,
    // Build/Open use a second data owner that is destroyed before the callback;
    // expectations retain an independently generated owner.
    ReleaseBeforeBody,
};

inline void
ExpectReaderBase(const ReaderBackend& backend,
                 const IIndexReaderBase& reader,
                 size_t expected_count) {
    ASSERT_EQ(reader.Count(), expected_count);
    EXPECT_EQ(reader.CoordDomain(), backend.ExpectedDomain());
    EXPECT_EQ(reader.ValueType(), backend.ExpectedValueType());

    const auto caps = reader.Caps();
    EXPECT_EQ(caps.nested, reader.CoordDomain() == Domain::Element);
    EXPECT_FALSE(caps.cheap_value_lookup && !caps.value_lookup);

    const auto memory = reader.MemoryUsage();
    const auto usage = reader.CellByteSize();
    EXPECT_GE(memory, 0);
    EXPECT_GE(usage.memory_bytes, 0);
    EXPECT_GE(usage.file_bytes, 0);
    EXPECT_EQ(reader.MemoryUsage(), memory);
    const auto repeated_usage = reader.CellByteSize();
    EXPECT_EQ(repeated_usage.memory_bytes, usage.memory_bytes);
    EXPECT_EQ(repeated_usage.file_bytes, usage.file_bytes);
}

inline void
ExpectQueryReaderBase(const ReaderBackend& backend,
                      const IIndexReaderBase& reader,
                      Domain expected_domain,
                      size_t expected_count) {
    ASSERT_EQ(reader.Count(), expected_count);
    ASSERT_EQ(reader.CoordDomain(), expected_domain);
    ASSERT_EQ(reader.CoordDomain(), backend.ExpectedDomain());
}

namespace detail {

template <typename T>
void
ValidateCaseData(const ReaderBackend& backend,
                 const ScalarDataSet<T>& dataset,
                 const ScalarTestData<T>& data,
                 bool allow_nullability_mismatch) {
    ASSERT_EQ(dataset.requires_nullable, ScalarTestHasNulls(data))
        << "dataset nullability descriptor disagrees with generated rows";
    ASSERT_EQ(dataset.domain, data.domain);
    if (!allow_nullability_mismatch && !backend.Nullable()) {
        ASSERT_FALSE(ScalarTestHasNulls(data))
            << "non-nullable backend received null test rows";
    }
}

template <typename T>
void
PrepareReader(const ReaderBackend& backend,
              const ScalarDataSet<T>& dataset,
              const ScalarTestData<T>& expected,
              IIndexReaderBasePtr& reader) {
    auto input_data = dataset.make_data();
    ASSERT_NO_FATAL_FAILURE(
        ValidateCaseData(backend, dataset, input_data, false));
    ASSERT_EQ(input_data.values.size(), expected.values.size());
    ASSERT_EQ(input_data.domain, expected.domain);
    ASSERT_EQ(input_data.metadata, expected.metadata);
    const ScalarTestInput<T> input(input_data);
    reader = backend.Create(
        input.View(),
        {.row_count = input_data.values.size(), .values = input_data.metadata});
}

template <typename Op>
TargetBitmap
QueryGroundTruth(const Query<Op>& query,
                 const ScalarTestData<typename Op::ValueType>& data) {
    if (query.expected) {
        return query.expected(data, query.args);
    }
    if constexpr (requires { Op::Oracle(data, query.args); }) {
        return Op::Oracle(data, query.args);
    } else {
        throw std::logic_error("query expected result is required");
    }
}

template <typename Op>
void
RunQueryBody(const ReaderBackend& backend,
             const ScalarTestData<typename Op::ValueType>& data,
             IIndexReaderBasePtr& reader,
             const Query<Op>& query) {
    EXPECT_TRUE(reader->Caps().*Op::kCapability);
    EXPECT_TRUE(backend.Supports(Op::kCapability));
    const auto* contract =
        dynamic_cast<const typename Op::Reader*>(reader.get());
    ASSERT_NE(contract, nullptr);

    auto expected_error = query.expected_error;
    if constexpr (requires {
                      Op::QueryPolicy(backend, query.args);
                      Op::ShouldUse(*contract, query.args);
                  }) {
        const auto policy = Op::QueryPolicy(backend, query.args);
        const auto should_use = Op::ShouldUse(*contract, query.args);
        switch (policy) {
            case PatternQueryPolicy::UseAndRun:
                EXPECT_TRUE(should_use);
                break;
            case PatternQueryPolicy::DeclineButRun:
                EXPECT_FALSE(should_use);
                break;
            case PatternQueryPolicy::SelectiveAndRun:
                if (query.expected_should_use.has_value()) {
                    EXPECT_EQ(should_use, *query.expected_should_use);
                }
                break;
            case PatternQueryPolicy::Unsupported:
                EXPECT_FALSE(should_use);
                expected_error = ErrorCode::Unsupported;
                break;
            case PatternQueryPolicy::CandidatesAndRun:
                if (query.expected_should_use.has_value()) {
                    EXPECT_EQ(should_use, *query.expected_should_use);
                }
                break;
        }
    }

    if (expected_error.has_value()) {
        ExpectSegcoreError(*expected_error, [&] {
            static_cast<void>(Op::Run(*contract, query.args));
        });
        return;
    }

    const auto expected = QueryGroundTruth<Op>(query, data);
    ASSERT_EQ(expected.size(), data.values.size());
    auto actual = Op::Run(*contract, query.args);
    if constexpr (requires { Op::QueryPolicy(backend, query.args); }) {
        if (Op::QueryPolicy(backend, query.args) ==
            PatternQueryPolicy::CandidatesAndRun) {
            // Candidates-only answer: every expected row must be present, but
            // extra rows are allowed (the consumer rechecks them). Asserting
            // equality here would encode a guarantee the contract does not
            // make.
            ExpectBitmapSuperset(actual, expected);
            return;
        }
    }
    ExpectBitmap(actual, expected);
}

template <typename T>
using ReaderBody = ObserveFn<T>;

template <typename T>
void
RunReaderCase(const ReaderBackend& backend,
              const ScalarDataSet<T>& dataset,
              InputLifetime input_lifetime,
              CasePhase phase,
              const ReaderBody<T>& body) {
    if (input_lifetime == InputLifetime::KeepUntilBodyCompletes) {
        auto data = dataset.make_data();
        ASSERT_NO_FATAL_FAILURE(
            ValidateCaseData(backend, dataset, data, false));
        const ScalarTestInput<T> input(data);
        auto reader = backend.Create(
            input.View(),
            {.row_count = data.values.size(), .values = data.metadata});
        ASSERT_NE(reader, nullptr);
        if (phase == CasePhase::Query) {
            ASSERT_NO_FATAL_FAILURE(ExpectQueryReaderBase(
                backend, *reader, dataset.domain, data.values.size()));
        } else {
            ASSERT_NO_FATAL_FAILURE(
                ExpectReaderBase(backend, *reader, data.values.size()));
        }
        body(backend, data, reader);
        return;
    }

    auto expected = dataset.make_data();
    ASSERT_NO_FATAL_FAILURE(
        ValidateCaseData(backend, dataset, expected, false));
    IIndexReaderBasePtr reader;
    ASSERT_NO_FATAL_FAILURE(PrepareReader(backend, dataset, expected, reader));
    ASSERT_NE(reader, nullptr);
    if (phase == CasePhase::Query) {
        ASSERT_NO_FATAL_FAILURE(ExpectQueryReaderBase(
            backend, *reader, dataset.domain, expected.values.size()));
    } else {
        ASSERT_NO_FATAL_FAILURE(
            ExpectReaderBase(backend, *reader, expected.values.size()));
    }
    body(backend, expected, reader);
}

template <typename T>
void
RunBuildFailure(const ReaderBackend& backend,
                const ScalarDataSet<T>& dataset,
                ErrorCode expected_error,
                bool allow_nullability_mismatch) {
    auto data = dataset.make_data();
    ASSERT_NO_FATAL_FAILURE(
        ValidateCaseData(backend, dataset, data, allow_nullability_mismatch));
    const ScalarTestInput<T> input(data);
    const auto build_input = input.View();
    const BackendCaseMetadata metadata{.row_count = data.values.size(),
                                       .values = data.metadata};
    auto builder = backend.CreateBuilder<T>(metadata);
    ASSERT_NE(builder, nullptr);

    storage::ArtifactPtr artifact;
    ExpectSegcoreError(expected_error, [&] {
        artifact = std::move(*builder).Build(build_input);
    });
}

template <typename F, typename T>
concept ObservationCallback = requires(std::decay_t<F>& callback,
                                       const ReaderBackend& backend,
                                       const ScalarTestData<T>& data,
                                       IIndexReaderBasePtr& reader) {
    callback(backend, data, reader);
};

}  // namespace detail

template <typename T>
class QueryBatch {
 public:
    class Entry {
     public:
        template <typename Op>
        Entry(std::string name, Query<Op> query)
            : name_(std::move(name)),
              capability_(Op::kCapability),
              run_([query = std::move(query)](const ReaderBackend& backend,
                                              const ScalarTestData<T>& data,
                                              IIndexReaderBasePtr& reader) {
                  static_assert(std::is_same_v<T, typename Op::ValueType>);
                  detail::RunQueryBody<Op>(backend, data, reader, query);
              }) {
            static_assert(std::is_same_v<T, typename Op::ValueType>);
        }

     private:
        friend class QueryBatch;

        std::string name_;
        bool ReaderCaps::*capability_;
        ObserveFn<T> run_;
    };

    QueryBatch(std::initializer_list<Entry> entries) : entries_(entries) {
        if (entries_.empty()) {
            throw std::logic_error("query batch requires at least one query");
        }
        for (auto current = entries_.begin(); current != entries_.end();
             ++current) {
            if (current->name_.empty()) {
                throw std::logic_error("query batch requires query names");
            }
            if (std::find_if(std::next(current),
                             entries_.end(),
                             [&](const auto& candidate) {
                                 return candidate.name_ == current->name_;
                             }) != entries_.end()) {
                throw std::logic_error(current->name_ +
                                       ": duplicate query name in batch");
            }
        }
    }

    void
    Run(const ReaderBackend& backend,
        const ScalarTestData<T>& data,
        IIndexReaderBasePtr& reader) const {
        for (const auto& entry : entries_) {
            SCOPED_TRACE("query: " + entry.name_);
            ASSERT_NO_FATAL_FAILURE(entry.run_(backend, data, reader));
        }
    }

    std::vector<bool ReaderCaps::*>
    RequiredCapabilities() const {
        std::vector<bool ReaderCaps::*> result;
        result.reserve(entries_.size());
        for (const auto& entry : entries_) {
            if (std::find(result.begin(), result.end(), entry.capability_) ==
                result.end()) {
                result.push_back(entry.capability_);
            }
        }
        return result;
    }

 private:
    std::vector<Entry> entries_;
};

template <typename T>
class CaseBody {
 public:
    template <typename Op>
    CaseBody(Query<Op> query)
        : phase_(CasePhase::Query),
          capability_(Op::kCapability),
          reader_body_([query = std::move(query)](const ReaderBackend& backend,
                                                  const ScalarTestData<T>& data,
                                                  IIndexReaderBasePtr& reader) {
              static_assert(std::is_same_v<T, typename Op::ValueType>);
              detail::RunQueryBody<Op>(backend, data, reader, query);
          }) {
        static_assert(std::is_same_v<T, typename Op::ValueType>);
    }

    CaseBody(QueryBatch<T> batch)
        : phase_(CasePhase::Query),
          batch_capabilities_(batch.RequiredCapabilities()),
          reader_body_([batch = std::move(batch)](const ReaderBackend& backend,
                                                  const ScalarTestData<T>& data,
                                                  IIndexReaderBasePtr& reader) {
              batch.Run(backend, data, reader);
          }) {
    }

    CaseBody(Observe<T> observe)
        : phase_(CasePhase::Observe),
          capability_(observe.capability),
          reader_body_(std::move(observe.run)) {
    }

    template <typename F>
    requires detail::ObservationCallback<F, T>
    CaseBody(F&& observe)
        : CaseBody(Observe<T>{.run = std::forward<F>(observe)}) {
    }

    CaseBody(BuildFails failure)
        : phase_(CasePhase::Build),
          allow_nullability_mismatch_(failure.allow_nullability_mismatch),
          build_error_(failure.expected_error) {
    }

 private:
    friend class IndexTestCases;

    CasePhase phase_;
    bool ReaderCaps::*capability_{nullptr};
    std::vector<bool ReaderCaps::*> batch_capabilities_;
    bool allow_nullability_mismatch_{false};
    ObserveFn<T> reader_body_;
    std::optional<ErrorCode> build_error_;
};

template <typename T>
struct IndexTestCase {
    std::string name;
    std::string dataset;
    BackendInputShape input_shape{BackendInputShape::Scalar};
    Domain domain{Domain::Row};
    std::optional<DataType> logical_value_type;
    InputLifetime input_lifetime{InputLifetime::KeepUntilBodyCompletes};
    // All three selectors are intersected after type, shape, domain,
    // nullability, and capability eligibility.
    std::vector<std::string> families;
    std::vector<std::string> backends;
    std::function<bool(const ReaderBackend&)> select_backend;
    CaseBody<T> body;
};

class IndexTestCases {
 public:
    template <typename T>
    void
    Add(IndexTestCase<T> test_case) {
        if (test_case.name.empty()) {
            throw std::logic_error("case requires a name");
        }
        if (test_case.body.phase_ != CasePhase::Build &&
            !test_case.body.reader_body_) {
            throw std::logic_error(test_case.name +
                                   ": reader case requires a callback");
        }
        if (test_case.body.phase_ == CasePhase::Build &&
            test_case.input_lifetime != InputLifetime::KeepUntilBodyCompletes) {
            throw std::logic_error(
                test_case.name +
                ": builder case must keep input alive through Build");
        }

        // Resolve only the descriptor here; never invoke its generator during
        // registration.
        const auto& dataset = ScalarDataSets().Get<T>(test_case.dataset);
        if (dataset.input_shape != test_case.input_shape ||
            dataset.domain != test_case.domain) {
            throw std::logic_error(test_case.name +
                                   ": dataset shape/domain mismatch");
        }
        if (dataset.logical_value_type.has_value() &&
            test_case.logical_value_type.has_value() &&
            !ScalarValueTypesMatch(*dataset.logical_value_type,
                                   *test_case.logical_value_type)) {
            throw std::logic_error(test_case.name +
                                   ": dataset logical type mismatch");
        }
        if (test_case.body.allow_nullability_mismatch_ &&
            !dataset.requires_nullable) {
            throw std::logic_error(
                test_case.name +
                ": nullability mismatch requires a dataset containing null "
                "rows");
        }
        const auto logical_type = test_case.logical_value_type.has_value()
                                      ? test_case.logical_value_type
                                      : dataset.logical_value_type;
        const auto requires_nullable =
            test_case.body.allow_nullability_mismatch_
                ? false
                : dataset.requires_nullable;
        auto backends =
            test_case.body.capability_ == nullptr
                ? ScalarReaderBackends().AllInput<T>(test_case.input_shape,
                                                     test_case.domain,
                                                     requires_nullable,
                                                     logical_type)
                : ScalarReaderBackends().ForInput<T>(test_case.input_shape,
                                                     test_case.domain,
                                                     test_case.body.capability_,
                                                     requires_nullable,
                                                     logical_type);
        if (!test_case.body.batch_capabilities_.empty()) {
            std::erase_if(backends, [&](const auto& backend) {
                return !std::all_of(test_case.body.batch_capabilities_.begin(),
                                    test_case.body.batch_capabilities_.end(),
                                    [&](const auto capability) {
                                        return backend.Supports(capability);
                                    });
            });
        }
        if (!test_case.families.empty()) {
            std::erase_if(backends, [&](const auto& backend) {
                return std::find(test_case.families.begin(),
                                 test_case.families.end(),
                                 backend.Family()) == test_case.families.end();
            });
        }
        if (!test_case.backends.empty()) {
            std::erase_if(backends, [&](const auto& backend) {
                return std::find(test_case.backends.begin(),
                                 test_case.backends.end(),
                                 backend.Name()) == test_case.backends.end();
            });
        }
        if (test_case.select_backend) {
            std::erase_if(backends, [&](const auto& backend) {
                return !test_case.select_backend(backend);
            });
        }
        if (test_case.body.allow_nullability_mismatch_ &&
            std::any_of(
                backends.begin(), backends.end(), [](const auto& backend) {
                    return backend.Nullable();
                })) {
            throw std::logic_error(test_case.name +
                                   ": nullability mismatch selected a nullable "
                                   "backend");
        }
        if (backends.empty()) {
            throw std::logic_error(test_case.name +
                                   ": no matching reader backend");
        }

        for (auto& backend : backends) {
            auto name =
                backend.Name() + "_" + test_case.dataset + "_" + test_case.name;
            for (const auto& existing : cases_) {
                if (existing.name == name) {
                    throw std::logic_error(name + ": duplicate case");
                }
            }
            const auto* dataset_ptr = &dataset;
            cases_.push_back({
                .name = std::move(name),
                .run =
                    [backend = std::move(backend), test_case, dataset_ptr] {
                        if (test_case.body.phase_ == CasePhase::Build) {
                            detail::RunBuildFailure(
                                backend,
                                *dataset_ptr,
                                *test_case.body.build_error_,
                                test_case.body.allow_nullability_mismatch_);
                            return;
                        }
                        detail::RunReaderCase(backend,
                                              *dataset_ptr,
                                              test_case.input_lifetime,
                                              test_case.body.phase_,
                                              test_case.body.reader_body_);
                    },
            });
        }
    }

    const std::vector<FilterParam>&
    All() const {
        return cases_;
    }

 private:
    std::vector<FilterParam> cases_;
};

}  // namespace milvus::index::test
