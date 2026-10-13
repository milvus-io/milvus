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

#include "index/test_utils/CaseTestDriver.h"

#include <gtest/gtest-spi.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"

namespace milvus::index::test {
namespace {

struct BatchExecutionLog {
    std::vector<std::string> events;
    std::vector<const IScalarPredicateReader<int64_t>*> readers;
    const ScalarTestData<int64_t>* expected_data{nullptr};
};

struct LoggedIn {
    using ValueType = int64_t;
    using Reader = IScalarPredicateReader<int64_t>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        int64_t key;
        std::shared_ptr<BatchExecutionLog> log;
        std::string event;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        args.log->events.push_back(args.event);
        args.log->readers.push_back(&reader);
        return reader.In(1, &args.key);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<int64_t>& data, const Args& args) {
        TargetBitmap expected(data.values.size(), false);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.validity[i] && data.values[i] == args.key) {
                expected.set(i);
            }
        }
        return expected;
    }
};

struct DistinctOperationTag {};

template <typename T, typename Tag>
struct LoggedNotIn {
    using ValueType = T;
    using Reader = IScalarPredicateReader<T>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        T key;
        std::shared_ptr<BatchExecutionLog> log;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        args.log->events.emplace_back("not-in-run");
        args.log->readers.push_back(&reader);
        return reader.NotIn(1, &args.key);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<T>& data, const Args& args) {
        static_assert(std::is_same_v<T, int64_t>);
        static_cast<void>(sizeof(Tag));
        args.log->events.emplace_back("not-in-oracle");
        args.log->expected_data = &data;
        TargetBitmap expected(data.values.size(), false);
        for (size_t i = 0; i < data.values.size(); ++i) {
            if (data.validity[i] && data.values[i] != args.key) {
                expected.set(i);
            }
        }
        return expected;
    }
};

struct LoggedError {
    using ValueType = int64_t;
    using Reader = IScalarPredicateReader<int64_t>;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {
        std::shared_ptr<BatchExecutionLog> log;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        args.log->events.emplace_back("error-run");
        args.log->readers.push_back(&reader);
        ThrowInfo(DataTypeInvalid, "query batch expected error");
    }
};

struct LookupQuery {
    using ValueType = int64_t;
    using Reader = IScalarValueReader<int64_t>;
    static constexpr auto kCapability = &ReaderCaps::value_lookup;

    struct Args {
        int64_t offset;
        size_t count;
    };

    static TargetBitmap
    Run(const Reader& reader, const Args& args) {
        TargetBitmap result(args.count, false);
        if (args.offset >= 0 && static_cast<size_t>(args.offset) < args.count &&
            reader.Lookup(args.offset).has_value()) {
            result.set(static_cast<size_t>(args.offset));
        }
        return result;
    }

    static TargetBitmap
    Oracle(const ScalarTestData<int64_t>& data, const Args& args) {
        TargetBitmap expected(data.values.size(), false);
        if (args.offset >= 0 &&
            static_cast<size_t>(args.offset) < data.values.size() &&
            data.validity[static_cast<size_t>(args.offset)]) {
            expected.set(static_cast<size_t>(args.offset));
        }
        return expected;
    }
};

struct SpatialQuery {
    using ValueType = int64_t;
    using Reader = IIndexReaderBase;
    static constexpr auto kCapability = &ReaderCaps::spatial;

    struct Args {};

    static TargetBitmap
    Run(const Reader& reader, const Args&) {
        return TargetBitmap(reader.Count(), false);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<int64_t>& data, const Args&) {
        return TargetBitmap(data.values.size(), false);
    }
};

class MissingReader {
 public:
    virtual ~MissingReader() = default;
};

struct MissingReaderQuery {
    using ValueType = int64_t;
    using Reader = MissingReader;
    static constexpr auto kCapability = &ReaderCaps::predicate;

    struct Args {};

    static TargetBitmap
    Run(const Reader&, const Args&) {
        return TargetBitmap(0, false);
    }

    static TargetBitmap
    Oracle(const ScalarTestData<int64_t>& data, const Args&) {
        return TargetBitmap(data.values.size(), false);
    }
};

QueryBatch<int64_t>
ExecutionBatch(const std::shared_ptr<BatchExecutionLog>& log) {
    return QueryBatch<int64_t>{
        {"ManualIn",
         Query<LoggedIn>{
             .args = {.key = 10, .log = log, .event = "in-run"},
             .expected = ManualHits({0, 3}),
         }},
        {"OracleNotIn",
         Query<LoggedNotIn<int64_t, DistinctOperationTag>>{
             .args = {.key = 10, .log = log},
         }},
        {"ExpectedError",
         Query<LoggedError>{
             .args = {.log = log},
             .expected_error = ErrorCode::DataTypeInvalid,
         }},
    };
}

ScalarDataSet<int64_t>
CountingDataSet(const std::shared_ptr<size_t>& generations,
                const std::shared_ptr<std::vector<std::uintptr_t>>& buffers) {
    return {
        .name = "CountingData",
        .make_data =
            [generations, buffers] {
                ++*generations;
                ScalarTestData<int64_t> data({10, 20, 30, 10});
                buffers->push_back(
                    reinterpret_cast<std::uintptr_t>(data.values.data()));
                return data;
            },
    };
}

TEST(QueryBatchTest, RunsHeterogeneousQueriesWithOneReaderAndDataOwner) {
    const auto generations = std::make_shared<size_t>(0);
    const auto buffers = std::make_shared<std::vector<std::uintptr_t>>();
    const auto log = std::make_shared<BatchExecutionLog>();
    const auto dataset = CountingDataSet(generations, buffers);
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const auto batch = ExecutionBatch(log);

    detail::RunReaderCase<int64_t>(
        backend,
        dataset,
        InputLifetime::KeepUntilBodyCompletes,
        CasePhase::Query,
        [&](const ReaderBackend& selected,
            const ScalarTestData<int64_t>& data,
            IIndexReaderBasePtr& reader) {
            ASSERT_EQ(*generations, 1);
            ASSERT_EQ(buffers->size(), 1);
            EXPECT_EQ(buffers->front(),
                      reinterpret_cast<std::uintptr_t>(data.values.data()));
            const auto* const owned_reader = reader.get();

            ASSERT_NO_FATAL_FAILURE(batch.Run(selected, data, reader));

            EXPECT_EQ(reader.get(), owned_reader);
            EXPECT_EQ(log->expected_data, &data);
            ASSERT_EQ(log->readers.size(), 3);
            EXPECT_TRUE(std::all_of(log->readers.begin(),
                                    log->readers.end(),
                                    [&](const auto* observed) {
                                        return observed == log->readers.front();
                                    }));
        });

    EXPECT_EQ(*generations, 1);
    EXPECT_EQ(log->events,
              (std::vector<std::string>{
                  "in-run", "not-in-oracle", "not-in-run", "error-run"}));
}

TEST(QueryBatchTest, ReleaseBeforeBodyUsesTwoIndependentDataOwners) {
    const auto generations = std::make_shared<size_t>(0);
    const auto buffers = std::make_shared<std::vector<std::uintptr_t>>();
    const auto log = std::make_shared<BatchExecutionLog>();
    const auto dataset = CountingDataSet(generations, buffers);
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const auto batch = ExecutionBatch(log);

    detail::RunReaderCase<int64_t>(
        backend,
        dataset,
        InputLifetime::ReleaseBeforeBody,
        CasePhase::Query,
        [&](const ReaderBackend& selected,
            const ScalarTestData<int64_t>& data,
            IIndexReaderBasePtr& reader) {
            ASSERT_EQ(*generations, 2);
            ASSERT_EQ(buffers->size(), 2);
            EXPECT_EQ(buffers->front(),
                      reinterpret_cast<std::uintptr_t>(data.values.data()));
            EXPECT_NE((*buffers)[0], (*buffers)[1]);
            const auto* const owned_reader = reader.get();

            ASSERT_NO_FATAL_FAILURE(batch.Run(selected, data, reader));

            EXPECT_EQ(reader.get(), owned_reader);
            EXPECT_EQ(log->expected_data, &data);
        });

    EXPECT_EQ(*generations, 2);
}

TEST(QueryBatchTest, ContinuesAfterNonFatalFailureAndNamesItsTrace) {
    const auto log = std::make_shared<BatchExecutionLog>();
    const auto dataset =
        CountingDataSet(std::make_shared<size_t>(),
                        std::make_shared<std::vector<std::uintptr_t>>());
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const QueryBatch<int64_t> batch{
        {"FirstMismatch",
         Query<LoggedIn>{
             .args = {.key = 10, .log = log, .event = "first-run"},
             .expected = ManualHits({1}),
         }},
        {"SecondStillRuns",
         Query<LoggedIn>{
             .args = {.key = 10, .log = log, .event = "second-run"},
             .expected = ManualHits({0, 3}),
         }},
    };

    detail::RunReaderCase<int64_t>(backend,
                                   dataset,
                                   InputLifetime::KeepUntilBodyCompletes,
                                   CasePhase::Query,
                                   [&](const ReaderBackend& selected,
                                       const ScalarTestData<int64_t>& data,
                                       IIndexReaderBasePtr& reader) {
                                       EXPECT_NONFATAL_FAILURE(
                                           batch.Run(selected, data, reader),
                                           "FirstMismatch");
                                   });

    EXPECT_EQ(log->events,
              (std::vector<std::string>{"first-run", "second-run"}));
}

TEST(QueryBatchTest, StopsAfterFatalFailureAndNamesItsTrace) {
    const auto log = std::make_shared<BatchExecutionLog>();
    const auto dataset =
        CountingDataSet(std::make_shared<size_t>(),
                        std::make_shared<std::vector<std::uintptr_t>>());
    const auto& backend = ScalarReaderBackends().Get<int64_t>("BitmapInt64");
    const QueryBatch<int64_t> batch{
        {"MissingContract", Query<MissingReaderQuery>{}},
        {"MustNotRun",
         Query<LoggedIn>{
             .args = {.key = 10, .log = log, .event = "must-not-run"},
         }},
    };

    detail::RunReaderCase<int64_t>(
        backend,
        dataset,
        InputLifetime::KeepUntilBodyCompletes,
        CasePhase::Query,
        [&](const ReaderBackend& selected,
            const ScalarTestData<int64_t>& data,
            IIndexReaderBasePtr& reader) {
            const auto* const owned_reader = reader.get();
            ::testing::TestPartResultArray failures;
            {
                ::testing::ScopedFakeTestPartResultReporter reporter(
                    ::testing::ScopedFakeTestPartResultReporter::
                        INTERCEPT_ONLY_CURRENT_THREAD,
                    &failures);
                batch.Run(selected, data, reader);
            }

            EXPECT_EQ(reader.get(), owned_reader);
            EXPECT_TRUE(log->events.empty());
            ASSERT_GT(failures.size(), 0);
            bool found_named_fatal = false;
            for (int i = 0; i < failures.size(); ++i) {
                const auto& failure = failures.GetTestPartResult(i);
                found_named_fatal |=
                    failure.fatally_failed() &&
                    std::string(failure.message()).find("MissingContract") !=
                        std::string::npos;
            }
            EXPECT_TRUE(found_named_fatal);
        });
}

TEST(QueryBatchTest, RejectsEmptyAndDuplicateQueryNames) {
    const auto query = Query<LoggedIn>{
        .args = {.key = 10,
                 .log = std::make_shared<BatchExecutionLog>(),
                 .event = "unused"},
    };

    EXPECT_THROW(QueryBatch<int64_t>{}, std::logic_error);
    EXPECT_THROW((QueryBatch<int64_t>{{"", query}}), std::logic_error);
    EXPECT_THROW((QueryBatch<int64_t>{{"Same", query}, {"Same", query}}),
                 std::logic_error);
}

TEST(QueryBatchTest, SelectsTheIntersectionOfEveryRequiredCapability) {
    auto expected = ScalarReaderBackends().AllInput<int64_t>(
        BackendInputShape::Scalar, Domain::Row, false, DataType::INT64);
    std::erase_if(expected, [](const auto& backend) {
        return !backend.Supports(&ReaderCaps::predicate) ||
               !backend.Supports(&ReaderCaps::value_lookup);
    });
    ASSERT_FALSE(expected.empty());

    IndexTestCases cases;
    cases.Add(IndexTestCase<int64_t>{
        .name = "CapabilityIntersection",
        .dataset = "HundredThousandRows",
        .body =
            QueryBatch<int64_t>{
                {"Predicate",
                 Query<LoggedIn>{
                     .args = {.key = 10,
                              .log = std::make_shared<BatchExecutionLog>(),
                              .event = "unused"},
                 }},
                {"Lookup",
                 Query<LookupQuery>{.args = {.offset = 0, .count = 100'000}}},
            },
    });

    EXPECT_EQ(cases.All().size(), expected.size());
}

TEST(QueryBatchTest, RejectsACombinationWithNoCapableBackend) {
    const auto log = std::make_shared<BatchExecutionLog>();
    IndexTestCases cases;

    EXPECT_THROW(cases.Add(IndexTestCase<int64_t>{
                     .name = "NoCapabilityIntersection",
                     .dataset = "HundredThousandRows",
                     .body =
                         QueryBatch<int64_t>{
                             {"Predicate",
                              Query<LoggedIn>{
                                  .args = {.key = 10,
                                           .log = log,
                                           .event = "must-not-run"},
                              }},
                             {"Spatial", Query<SpatialQuery>{}},
                         },
                 }),
                 std::logic_error);
    EXPECT_TRUE(log->events.empty());
}

TEST(QueryBatchTest, LeavesSingleObserveAndBuildSelectionRulesUnchanged) {
    const auto expected_predicate =
        ScalarReaderBackends().ForInput<int64_t>(BackendInputShape::Scalar,
                                                 Domain::Row,
                                                 &ReaderCaps::predicate,
                                                 false,
                                                 DataType::INT64);
    const auto expected_lookup =
        ScalarReaderBackends().ForInput<int64_t>(BackendInputShape::Scalar,
                                                 Domain::Row,
                                                 &ReaderCaps::value_lookup,
                                                 false,
                                                 DataType::INT64);
    const auto expected_build = ScalarReaderBackends().AllInput<int64_t>(
        BackendInputShape::Scalar, Domain::Row, false, DataType::INT64);

    IndexTestCases single;
    single.Add(IndexTestCase<int64_t>{
        .name = "SingleQuery",
        .dataset = "HundredThousandRows",
        .body =
            Query<LoggedIn>{
                .args = {.key = 10,
                         .log = std::make_shared<BatchExecutionLog>(),
                         .event = "unused"},
            },
    });
    EXPECT_EQ(single.All().size(), expected_predicate.size());

    IndexTestCases observe;
    observe.Add(IndexTestCase<int64_t>{
        .name = "Observe",
        .dataset = "HundredThousandRows",
        .body =
            Observe<int64_t>{
                .capability = &ReaderCaps::value_lookup,
                .run = [](const ReaderBackend&,
                          const ScalarTestData<int64_t>&,
                          IIndexReaderBasePtr&) {},
            },
    });
    EXPECT_EQ(observe.All().size(), expected_lookup.size());

    IndexTestCases build;
    build.Add(IndexTestCase<int64_t>{
        .name = "Build",
        .dataset = "HundredThousandRows",
        .body = BuildFails{ErrorCode::UnexpectedError},
    });
    EXPECT_EQ(build.All().size(), expected_build.size());

    EXPECT_THROW(build.Add(IndexTestCase<int64_t>{
                     .name = "BuildRelease",
                     .dataset = "HundredThousandRows",
                     .input_lifetime = InputLifetime::ReleaseBeforeBody,
                     .body = BuildFails{ErrorCode::UnexpectedError},
                 }),
                 std::logic_error);
}

TEST(CaseTestDriverTest, ProjectedProfilesRetainJsonRoutingCapability) {
    const auto& profiles = ScalarReaderBackends();
    for (const auto* name : {"JsonProjectedSortedDouble",
                             "JsonProjectedInvertedDouble",
                             "JsonProjectedHybridDouble"}) {
        SCOPED_TRACE(name);
        const auto& backend = profiles.Get<double>(name);
        EXPECT_TRUE(backend.Supports(&ReaderCaps::json_paths));
        EXPECT_TRUE(backend.DeriveCaps().json_paths);
    }
}

}  // namespace
}  // namespace milvus::index::test
