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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/IndexLoaderFactory.h"
#include "index/LegacyIndexLoad.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/CaseTestDriver.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/TestArtifactIO.h"

namespace milvus::index::test {
namespace {

class EmptyArtifact final : public storage::Artifact {
 public:
    explicit EmptyArtifact(int marker = 0) : marker(marker) {
    }

    void
    Serialize(storage::FileSink&) const override {
    }

    int marker;
};

template <typename T>
class EmptyBuilder final : public IArtifactBuilder<ScalarBuildInput<T>> {
 public:
    explicit EmptyBuilder(int marker = 0) : marker_(marker) {
    }

    storage::ArtifactPtr
        Build(const ScalarBuildInput<T>&) &&
        override {
        return std::make_unique<EmptyArtifact>(marker_);
    }

 private:
    int marker_;
};

class EmptyReader final : public IIndexReaderBase {
 public:
    explicit EmptyReader(ReaderCaps caps = {}) : caps_(caps) {
    }

    ReaderCaps
    Caps() const override {
        return caps_;
    }

    Domain
    CoordDomain() const override {
        return Domain::Row;
    }

    int64_t
    Count() const override {
        return 0;
    }

    DataType
    ValueType() const override {
        return DataType::INT64;
    }

    int64_t
    MemoryUsage() const override {
        return 0;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return {0, 0};
    }

 private:
    ReaderCaps caps_;
};

struct LoaderProbeA : IndexLoader {
    storage::LoadOptions options_;
    static inline std::thread::id open_thread;
    static inline std::thread::id load_thread;
    static constexpr std::string_view kFamily = "test.registry.loader.a";

    static ReaderCaps
    DeriveCaps(const Config& params) {
        return {.predicate = params.value("predicate", false)};
    }

    static folly::coro::Task<std::unique_ptr<IndexLoader>>
    Open(IndexOpenRequest request) {
        return OpenIndexLoader(
            std::move(request),
            [](OpenedIndexInput, storage::LoadOptions options)
                -> folly::coro::Task<std::unique_ptr<IndexLoader>> {
                open_thread = std::this_thread::get_id();
                auto result = std::make_unique<LoaderProbeA>();
                result->options_ = std::move(options);
                co_return result;
            });
    }
    folly::coro::Task<IIndexReaderBasePtr>
    Load(milvus::OpContext*) override {
        load_thread = std::this_thread::get_id();
        co_return std::make_unique<EmptyReader>(DeriveCaps(options_.params));
    }
};

std::string
UniqueFamily(std::string_view prefix) {
    static std::atomic<uint64_t> next{0};
    return std::string(prefix) + "." +
           std::to_string(next.fetch_add(1, std::memory_order_relaxed));
}

void
ExpectNoThreadException(const std::exception_ptr& error,
                        std::string_view thread_name) {
    if (!error) {
        return;
    }
    try {
        std::rethrow_exception(error);
    } catch (const std::exception& exception) {
        ADD_FAILURE() << thread_name << " thread threw: " << exception.what();
    } catch (...) {
        ADD_FAILURE() << thread_name << " thread threw a non-std exception";
    }
}

void
ExpectProjectedCapsRejected(std::string_view family, Config params) {
    const auto loader = LoaderRegistry::Instance().Lookup(std::string(family));
    ASSERT_TRUE(loader);
    TestArtifactData marker;
    marker.metadata["has_non_exist"] = false;
    params = AnnotateJsonProjectionCompleteness(
        std::move(params), storage::IndexEntryDirectory{}, marker.metadata);
    try {
        static_cast<void>(loader.derive_caps(params));
        ADD_FAILURE() << "unsupported projected family/cast was accepted";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataTypeInvalid);
    } catch (const std::exception& error) {
        ADD_FAILURE() << "projected caps threw a non-SegcoreError: "
                      << error.what();
    } catch (...) {
        ADD_FAILURE() << "projected caps threw a non-SegcoreError";
    }
}

TEST(RegistryTest, UnknownEntriesAreEmpty) {
    EXPECT_FALSE(LoaderRegistry::Instance().Lookup("test.registry.unknown"));
    EXPECT_EQ(BuilderRegistry<ScalarBuildInput<int64_t>>::Instance().Create(
                  "test.registry.unknown", {}),
              nullptr);
}

TEST(RegistryTest, BuilderTablesAreTypedAndReceiveParametersUnchanged) {
    const auto family = UniqueFamily("test.registry.builder.int64");
    Config observed;
    BuilderRegistry<ScalarBuildInput<int64_t>>::Instance().Register(
        family, [&](const Config& params) {
            observed = params;
            return std::make_unique<EmptyBuilder<int64_t>>(17);
        });

    const Config expected = {{"number", 7}, {"text", "kept"}};
    auto builder =
        BuilderRegistry<ScalarBuildInput<int64_t>>::Instance().Create(family,
                                                                      expected);
    ASSERT_NE(builder, nullptr);
    EXPECT_EQ(observed, expected);
    EXPECT_EQ(BuilderRegistry<ScalarBuildInput<float>>::Instance().Create(
                  family, expected),
              nullptr);

    auto artifact = std::move(*builder).Build({});
    auto* typed = dynamic_cast<EmptyArtifact*>(artifact.get());
    ASSERT_NE(typed, nullptr);
    EXPECT_EQ(typed->marker, 17);
}

TEST(RegistryTest, FactoryExceptionIsPreserved) {
    const auto family = UniqueFamily("test.registry.builder.throw");
    BuilderRegistry<ScalarBuildInput<int64_t>>::Instance().Register(
        family,
        [](const Config&)
            -> std::unique_ptr<IArtifactBuilder<ScalarBuildInput<int64_t>>> {
            ThrowInfo(DataTypeInvalid, "registry factory marker");
        });

    try {
        static_cast<void>(
            BuilderRegistry<ScalarBuildInput<int64_t>>::Instance().Create(
                family, {}));
        FAIL() << "factory exception was not propagated";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataTypeInvalid);
    }
}

TEST(RegistryTest, InvalidAndDuplicateBuilderRegistrationKeepOriginalEntry) {
    const auto family = UniqueFamily("test.registry.builder.duplicate");
    auto& registry = BuilderRegistry<ScalarBuildInput<int64_t>>::Instance();
    registry.Register(family, [](const Config&) {
        return std::make_unique<EmptyBuilder<int64_t>>(1);
    });

    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { registry.Register(family, {}); });
    ExpectSegcoreError(ErrorCode::UnexpectedError, [&] {
        registry.Register(family, [](const Config&) {
            return std::make_unique<EmptyBuilder<int64_t>>(2);
        });
    });

    auto builder = registry.Create(family, {});
    ASSERT_NE(builder, nullptr);
    auto artifact = std::move(*builder).Build({});
    auto* typed = dynamic_cast<EmptyArtifact*>(artifact.get());
    ASSERT_NE(typed, nullptr);
    EXPECT_EQ(typed->marker, 1);
}

TEST(RegistryTest, LoaderDeriveAndOpenDispatchIndependently) {
    static std::once_flag registered;
    std::call_once(registered,
                   [] { LoaderRegistry::Instance().Register<LoaderProbeA>(); });
    const auto entry =
        LoaderRegistry::Instance().Lookup(std::string(LoaderProbeA::kFamily));
    ASSERT_TRUE(entry);
    EXPECT_TRUE(entry.derive_caps({{"predicate", true}}).predicate);
    EXPECT_FALSE(entry.derive_caps({{"predicate", false}}).predicate);

    TestArtifactData artifact;
    TestArtifactSource source(artifact);
    storage::LoadOptions options;
    options.params = {{"predicate", true}};
    auto reader = LoadIndex(
        entry,
        {OpenedIndexInput{LegacyIndexSource{
             std::shared_ptr<storage::FileSource>(&source, [](auto*) {}),
             false}},
         options});
    ASSERT_NE(reader, nullptr);
    EXPECT_TRUE(reader->Caps().predicate);
    EXPECT_EQ(LoaderProbeA::open_thread, std::this_thread::get_id());
    EXPECT_EQ(LoaderProbeA::load_thread, std::this_thread::get_id());
}

TEST(RegistryTest, IndependentConcurrentRegistrationAndLookupAreSafe) {
    constexpr size_t kRegistrations = 64;
    auto& registry = BuilderRegistry<ScalarBuildInput<float>>::Instance();
    const auto anchor = UniqueFamily("test.registry.builder.anchor");
    registry.Register(anchor, [](const Config&) {
        return std::make_unique<EmptyBuilder<float>>();
    });
    std::vector<std::string> families;
    families.reserve(kRegistrations);
    for (size_t i = 0; i < kRegistrations; ++i) {
        families.push_back(UniqueFamily("test.registry.builder.concurrent"));
    }

    constexpr auto timeout = std::chrono::seconds(5);
    std::mutex gate;
    std::condition_variable changed;
    bool first_lookup{false};
    bool writer_midpoint{false};
    bool second_lookup{false};
    bool writer_done{false};
    bool aborted{false};
    bool timed_out{false};
    bool missing_anchor{false};
    std::exception_ptr writer_error;
    std::exception_ptr reader_error;
    std::thread writer([&] {
        try {
            {
                std::unique_lock lock(gate);
                if (!changed.wait_for(lock, timeout, [&] {
                        return first_lookup || aborted;
                    })) {
                    timed_out = true;
                    aborted = true;
                    changed.notify_all();
                    return;
                }
                if (aborted) {
                    return;
                }
            }
            for (size_t i = 0; i < families.size() / 2; ++i) {
                registry.Register(families[i], [](const Config&) {
                    return std::make_unique<EmptyBuilder<float>>();
                });
            }
            {
                std::unique_lock lock(gate);
                writer_midpoint = true;
                changed.notify_all();
                if (!changed.wait_for(lock, timeout, [&] {
                        return second_lookup || aborted;
                    })) {
                    timed_out = true;
                    aborted = true;
                    changed.notify_all();
                    return;
                }
                if (aborted) {
                    return;
                }
            }
            for (size_t i = families.size() / 2; i < families.size(); ++i) {
                registry.Register(families[i], [](const Config&) {
                    return std::make_unique<EmptyBuilder<float>>();
                });
            }
            {
                std::lock_guard lock(gate);
                writer_done = true;
                changed.notify_all();
            }
        } catch (...) {
            std::lock_guard lock(gate);
            writer_error = std::current_exception();
            aborted = true;
            changed.notify_all();
        }
    });
    std::thread reader([&] {
        try {
            if (registry.Create(anchor, {}) == nullptr) {
                missing_anchor = true;
            }
            {
                std::unique_lock lock(gate);
                first_lookup = true;
                changed.notify_all();
                if (!changed.wait_for(lock, timeout, [&] {
                        return writer_midpoint || aborted;
                    })) {
                    timed_out = true;
                    aborted = true;
                    changed.notify_all();
                    return;
                }
                if (aborted) {
                    return;
                }
            }
            if (registry.Create(anchor, {}) == nullptr) {
                missing_anchor = true;
            }
            {
                std::unique_lock lock(gate);
                second_lookup = true;
                changed.notify_all();
                if (!changed.wait_for(lock, timeout, [&] {
                        return writer_done || aborted;
                    })) {
                    timed_out = true;
                    aborted = true;
                    changed.notify_all();
                }
            }
        } catch (...) {
            std::lock_guard lock(gate);
            reader_error = std::current_exception();
            aborted = true;
            changed.notify_all();
        }
    });
    writer.join();
    reader.join();

    ExpectNoThreadException(writer_error, "writer");
    ExpectNoThreadException(reader_error, "reader");
    ASSERT_FALSE(timed_out) << "registry concurrency handshake timed out";
    ASSERT_FALSE(aborted) << "registry concurrency handshake aborted";
    EXPECT_FALSE(missing_anchor);
    for (const auto& family : families) {
        EXPECT_NE(registry.Create(family, {}), nullptr);
    }
}

TEST(RegistryTest, JsonBitmapRejectsDoubleProjection) {
    ExpectProjectedCapsRejected(
        families::kBitmap,
        {{"field_type", static_cast<int32_t>(DataType::JSON)},
         {"value_type", static_cast<int32_t>(DataType::DOUBLE)},
         {"nullable", true},
         {"nested", false},
         {INDEX_TYPE, BITMAP_INDEX_TYPE},
         {JSON_PATH, "/a"},
         {JSON_CAST_TYPE, "DOUBLE"}});
}

TEST(RegistryTest, JsonSortedRejectsBoolProjection) {
    ExpectProjectedCapsRejected(
        families::kSort,
        {{"field_type", static_cast<int32_t>(DataType::JSON)},
         {"value_type", static_cast<int32_t>(DataType::BOOL)},
         {"nullable", true},
         {"nested", false},
         {INDEX_TYPE, ASCENDING_SORT},
         {JSON_PATH, "/a"},
         {JSON_CAST_TYPE, "BOOL"}});
}

const std::vector<FilterParam>&
ProductionProfiles() {
    static const auto params = [] {
        std::vector<FilterParam> result;
        for (const auto& backend : ScalarReaderBackends().Profiles()) {
            result.push_back({
                .name = backend.Name(),
                .run =
                    [backend] {
                        EXPECT_TRUE(backend.BuilderRegistered());
                        EXPECT_TRUE(backend.LoadersRegistered());
                        const auto requested =
                            LoaderRegistry::Instance().Lookup(backend.Family());
                        if (backend.Family() == families::kHybrid) {
                            EXPECT_FALSE(requested);
                        } else {
                            EXPECT_TRUE(requested);
                        }
                    },
            });
        }
        return result;
    }();
    return params;
}

class ProductionRegistryTest : public ::testing::TestWithParam<FilterParam> {};

TEST_P(ProductionRegistryTest, DeclaredProfileUsesRegisteredBuilderAndLoader) {
    GetParam().run();
}

INSTANTIATE_TEST_SUITE_P(ScalarProfiles,
                         ProductionRegistryTest,
                         ::testing::ValuesIn(ProductionProfiles()),
                         FilterParamName);

}  // namespace
}  // namespace milvus::index::test
