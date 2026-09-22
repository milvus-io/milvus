// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the
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

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/fmindex/FmIndexArtifact.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"

namespace milvus::index::test {
namespace {

constexpr std::string_view kBlobEntry = "fm_index.bin";
constexpr std::string_view kNullBitmapEntry = "fm_index_null_bitmap";
constexpr std::string_view kTotalRowsMeta = "total_rows";
constexpr std::string_view kNullableMeta = "nullable";

storage::ArtifactPtr
BuildFm(const ReaderBackend& backend) {
    ScalarTestData<std::string_view> data({"alpha",
                                           "beta",
                                           "alpine",
                                           "gamma",
                                           "delta",
                                           "alphabet",
                                           "omega",
                                           "zeta",
                                           "theta"});
    if (backend.Nullable()) {
        data.validity.reset(1);
        data.validity.reset(8);
    } else {
        data.validity_present = false;
    }
    const ScalarTestInput<std::string_view> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

TestArtifactData
SerializedFm(std::string_view backend_name) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>(backend_name);
    auto artifact = BuildFm(backend);
    return SerializeV3(*artifact);
}

void
ExpectFmQueries(const IIndexReaderBase& reader, size_t expected_nulls) {
    const auto* nulls = dynamic_cast<const INullReader*>(&reader);
    ASSERT_NE(nulls, nullptr);
    const auto null_result = nulls->IsNull();
    ASSERT_EQ(null_result.size(), 9);
    EXPECT_EQ(null_result.count(), expected_nulls);

    const auto* pattern = dynamic_cast<const IPatternMatchReader*>(&reader);
    ASSERT_NE(pattern, nullptr);
    const auto hits = pattern->PatternMatch("alp", PatternOp::PrefixMatch);
    ASSERT_EQ(hits.size(), 9);
    EXPECT_TRUE(hits[0]);
    EXPECT_TRUE(hits[2]);
    EXPECT_TRUE(hits[5]);
    EXPECT_EQ(hits.count(), 3);
}

TEST(FmIndexArtifactTest, LegacySerializationIsRejectedExactly) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto artifact = BuildFm(backend);

    ExpectSegcoreError(ErrorCode::UnexpectedError,
                       [&] { static_cast<void>(SerializeV1V2(*artifact)); });
}

class FmIndexV3OwnershipTest : public ::testing::TestWithParam<const char*> {};

TEST_P(FmIndexV3OwnershipTest, ReaderOwnsPersistedState) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>(GetParam());
    auto artifact = BuildFm(backend);
    auto persisted = SerializeV3(*artifact);

    EXPECT_TRUE(persisted.entries.contains(std::string(kBlobEntry)));
    EXPECT_TRUE(persisted.entries.contains(std::string(kNullBitmapEntry)));
    EXPECT_EQ(persisted.metadata.at(std::string(kTotalRowsMeta)), 9);
    EXPECT_EQ(persisted.metadata.at(std::string(kNullableMeta)), true);

    auto reader = OpenV3(
        backend, persisted, {.row_count = 9, .values = Config::object()});
    artifact.reset();
    persisted.entries.clear();
    persisted.metadata.clear();
    ASSERT_NE(reader, nullptr);
    ExpectFmQueries(*reader, 2);
    EXPECT_GE(reader->MemoryUsage(), 0);
    if (backend.MmapRequested()) {
        EXPECT_GT(reader->CellByteSize().file_bytes, 0);
    }
}

INSTANTIATE_TEST_SUITE_P(HeapAndMmap,
                         FmIndexV3OwnershipTest,
                         ::testing::Values("FmIndexVarchar",
                                           "FmIndexVarcharMmap"),
                         [](const auto& info) {
                             return std::string(info.param).find("Mmap") ==
                                            std::string::npos
                                        ? "Heap"
                                        : "Mmap";
                         });

TEST(FmIndexArtifactTest, NonNullableV3OmitsNullBitmapAndReopens) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto artifact = BuildFm(backend);
    auto persisted = SerializeV3(*artifact);

    EXPECT_TRUE(persisted.entries.contains(std::string(kBlobEntry)));
    EXPECT_FALSE(persisted.entries.contains(std::string(kNullBitmapEntry)));
    EXPECT_EQ(persisted.metadata.at(std::string(kTotalRowsMeta)), 9);
    EXPECT_EQ(persisted.metadata.at(std::string(kNullableMeta)), false);

    auto reader = OpenV3(backend, persisted, {.row_count = 9});
    artifact.reset();
    persisted.entries.clear();
    persisted.metadata.clear();
    ASSERT_NE(reader, nullptr);
    ExpectFmQueries(*reader, 0);
}

struct MissingFmPartCase {
    const char* name;
    bool metadata;
    const char* key;
};

class FmIndexMissingPartTest
    : public ::testing::TestWithParam<MissingFmPartCase> {};

TEST_P(FmIndexMissingPartTest, RejectsMissingRequiredPart) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    if (GetParam().metadata) {
        persisted.metadata.erase(GetParam().key);
    } else {
        persisted.entries.erase(GetParam().key);
    }

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

INSTANTIATE_TEST_SUITE_P(
    RequiredParts,
    FmIndexMissingPartTest,
    ::testing::Values(
        MissingFmPartCase{"MissingBlob", false, kBlobEntry.data()},
        MissingFmPartCase{"MissingRowCount", true, kTotalRowsMeta.data()},
        MissingFmPartCase{"MissingNullable", true, kNullableMeta.data()}),
    [](const auto& info) { return info.param.name; });

TEST(FmIndexArtifactTest, RejectsNegativeRowCount) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    persisted.metadata[std::string(kTotalRowsMeta)] = -1;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsRowCountThatDisagreesWithBlob) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    persisted.metadata[std::string(kTotalRowsMeta)] = 8;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsNonBooleanNullableMetadata) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    persisted.metadata[std::string(kNullableMeta)] = "false";

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsRuntimeNullableDisagreement) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    persisted.metadata[std::string(kNullableMeta)] = true;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsCorruptBlob) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    persisted.entries[std::string(kBlobEntry)] = {0x00, 0x01, 0x02};

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsMissingNullableBitmap) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarchar");
    auto persisted = SerializedFm(backend.Name());
    persisted.entries.erase(std::string(kNullBitmapEntry));

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsBitmapOnNonNullableArtifact) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarcharNonNull");
    auto persisted = SerializedFm(backend.Name());
    persisted.entries[std::string(kNullBitmapEntry)] = {0x00, 0x00};

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsWrongBitmapLength) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarchar");
    auto persisted = SerializedFm(backend.Name());
    auto& bitmap = persisted.entries.at(std::string(kNullBitmapEntry));
    ASSERT_FALSE(bitmap.empty());
    bitmap.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

TEST(FmIndexArtifactTest, RejectsSetBitmapPaddingBits) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("FmIndexVarchar");
    auto persisted = SerializedFm(backend.Name());
    auto& bitmap = persisted.entries.at(std::string(kNullBitmapEntry));
    ASSERT_EQ(bitmap.size(), 2);
    bitmap.back() |= 0x80;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 9}));
    });
}

// GuardFmIndexLibrary is the single classifying boundary every call into the
// vendored fm-index-lite library goes through. What it must not do is let an
// exception past untyped: FailureCStatus() reports anything that is not a
// SegcoreError as UnexpectedError(2001), which merr classifies as a permanent
// system error.
//
// The bad_alloc case is the one with teeth. FMIndex::parseView self-classifies
// every other std::exception into a false return value (which the caller turns
// into DataFormatBroken via !valid()) and deliberately RETHROWS bad_alloc, so
// an out-of-memory during index load is the exception that actually escapes the
// library. MemAllocateFailed(2034) is retriable in merr's classForCode while
// 2001 is not, so folding it into the phase fallback would make a transient OOM
// look permanent and stop the load from being retried.
TEST(FmIndexArtifactTest, LibraryGuardClassifiesEscapingExceptions) {
    auto code_of = [](auto&& fn, ErrorCode fallback) {
        try {
            GuardFmIndexLibrary(std::forward<decltype(fn)>(fn),
                                fallback,
                                "load");
        } catch (const SegcoreError& error) {
            return error.get_error_code();
        }
        return ErrorCode::Success;
    };

    // OOM stays retriable and is never folded into the phase fallback.
    EXPECT_EQ(
        code_of([] { throw std::bad_alloc(); }, ErrorCode::DataFormatBroken),
        ErrorCode::MemAllocateFailed);
    EXPECT_EQ(
        code_of([] { throw std::bad_alloc(); }, ErrorCode::IndexBuildError),
        ErrorCode::MemAllocateFailed);

    // Any other std:: exception takes the phase's fallback, which differs
    // between build and load: a library failure while building is a build
    // error, while one during load means the persisted blob is unusable.
    EXPECT_EQ(code_of([] { throw std::runtime_error("truncated"); },
                      ErrorCode::DataFormatBroken),
              ErrorCode::DataFormatBroken);
    EXPECT_EQ(code_of([] { throw std::length_error("corpus too large"); },
                      ErrorCode::IndexBuildError),
              ErrorCode::IndexBuildError);

    // A non-std exception must not escape the boundary untyped either.
    EXPECT_EQ(code_of([] { throw 42; }, ErrorCode::DataFormatBroken),
              ErrorCode::DataFormatBroken);

    // An already-classified error keeps its own code instead of being
    // relabelled by the boundary.
    EXPECT_EQ(
        code_of(
            [] { ThrowInfo(ErrorCode::FileReadFailed, "staged file gone"); },
            ErrorCode::DataFormatBroken),
        ErrorCode::FileReadFailed);

    // The success path returns normally and throws nothing.
    EXPECT_EQ(code_of([] {}, ErrorCode::DataFormatBroken), ErrorCode::Success);
}

}  // namespace
}  // namespace milvus::index::test
