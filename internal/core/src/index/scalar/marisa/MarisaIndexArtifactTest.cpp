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
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include "common/OpContext.h"
#include "index/Meta.h"
#include "storage/artifact/LocalDirectory.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/test_utils/ArtifactTestUtils.h"
#include "index/test_utils/ScalarTestData.h"
#include <filesystem>
#include <variant>
#include <memory>
#include <utility>
#include "index/scalar/marisa/MarisaIndexLoader.h"
#include "index/test_utils/LoaderTestAccess.h"
#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <sys/stat.h>
#include "index/Families.h"

namespace milvus::index::test {
namespace {

constexpr std::string_view kCsrFormatVersionMeta = "marisa_csr_format_version";
constexpr std::string_view kCsrNumKeysMeta = "csr_num_keys";

storage::ArtifactPtr
BuildMarisa(const ReaderBackend& backend) {
    ScalarTestData<std::string_view> data(
        {"alpha", "beta", "beta", "gamma", "delta"});
    if (backend.Nullable()) {
        data.validity.reset(3);
    } else {
        data.validity_present = false;
    }
    const ScalarTestInput<std::string_view> input(data);
    return backend.Build(input.View(), {.row_count = data.values.size()});
}

TestArtifactData
SerializedMarisa(std::string_view backend_name) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>(backend_name);
    auto artifact = BuildMarisa(backend);
    return SerializeV3(*artifact);
}

void
ExpectBetaRows(const IIndexReaderBase& reader) {
    const auto* predicate =
        dynamic_cast<const IScalarPredicateReader<std::string_view>*>(&reader);
    ASSERT_NE(predicate, nullptr);
    const std::string_view key = "beta";
    const auto hits = predicate->In(1, &key);
    ASSERT_EQ(hits.size(), 5);
    EXPECT_TRUE(hits[1]);
    EXPECT_TRUE(hits[2]);
    EXPECT_EQ(hits.count(), 2);
}

TEST(MarisaIndexArtifactTest, EmbeddedNulSurvivesBothFormats) {
    for (const auto* name :
         {"MarisaVarcharNonNull", "MarisaVarcharNonNullMmap"}) {
        SCOPED_TRACE(name);
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        ScalarTestData<std::string_view> data({"a",
                                               std::string("a\0b", 3),
                                               std::string("a\0c", 3),
                                               "",
                                               std::string("\0", 1)});
        data.validity_present = false;
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = backend.Build(input.View(), {.row_count = 5});
        for (const bool packed : {false, true}) {
            SCOPED_TRACE(packed);
            auto reader = packed ? OpenV3(backend, SerializeV3(*artifact))
                                 : OpenV1V2(backend, SerializeV1V2(*artifact));
            const auto* predicate =
                dynamic_cast<const IScalarPredicateReader<std::string_view>*>(
                    reader.get());
            const auto* values =
                dynamic_cast<const IScalarValueReader<std::string_view>*>(
                    reader.get());
            const auto* pattern =
                dynamic_cast<const IPatternMatchReader*>(reader.get());
            ASSERT_NE(predicate, nullptr);
            ASSERT_NE(values, nullptr);
            ASSERT_NE(pattern, nullptr);
            for (size_t row = 0; row < data.values.size(); ++row) {
                const std::string_view key = data.values[row];
                const auto hits = predicate->In(1, &key);
                EXPECT_EQ(hits.count(), 1);
                EXPECT_TRUE(hits[row]);
                ASSERT_TRUE(values->Lookup(row).has_value());
                EXPECT_EQ(*values->Lookup(row), data.values[row]);
            }
            const auto prefix = pattern->PatternMatch(
                std::string_view("a\0", 2), PatternOp::PrefixMatch);
            EXPECT_EQ(prefix.count(), 2);
            EXPECT_TRUE(prefix[1]);
            EXPECT_TRUE(prefix[2]);
        }
    }
}

TEST(MarisaIndexArtifactTest, LegacyRoundTripRebuildsCsr) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    auto artifact = BuildMarisa(backend);
    auto buffers = SerializeV1V2(*artifact);

    EXPECT_TRUE(buffers.entries.contains(MARISA_TRIE_INDEX));
    EXPECT_TRUE(buffers.entries.contains(MARISA_STR_IDS));
    EXPECT_FALSE(buffers.entries.contains(MARISA_CSR_INDEX));
    EXPECT_FALSE(buffers.entries.contains(MARISA_CSR_OFFSETS));

    auto reader = OpenV1V2(
        backend, buffers, {.row_count = 5, .values = Config::object()});
    artifact.reset();
    buffers = {};
    ASSERT_NE(reader, nullptr);
    ExpectBetaRows(*reader);
    const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(nulls->IsNull().count(), 1);
}

TEST(MarisaIndexArtifactTest, V3MmapReaderOwnsPersistedState) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    auto artifact = BuildMarisa(backend);
    auto persisted = SerializeV3(*artifact);

    EXPECT_TRUE(persisted.entries.contains(MARISA_TRIE_INDEX));
    EXPECT_TRUE(persisted.entries.contains(MARISA_STR_IDS));
    EXPECT_TRUE(persisted.entries.contains(MARISA_CSR_INDEX));
    EXPECT_TRUE(persisted.entries.contains(MARISA_CSR_OFFSETS));
    EXPECT_EQ(persisted.metadata.at(std::string(kCsrFormatVersionMeta)), 1);
    EXPECT_GT(
        persisted.metadata.at(std::string(kCsrNumKeysMeta)).get<uint64_t>(), 0);

    auto reader = OpenV3(
        backend, persisted, {.row_count = 5, .values = Config::object()});
    artifact.reset();
    persisted.entries.clear();
    persisted.metadata.clear();
    ASSERT_NE(reader, nullptr);
    ExpectBetaRows(*reader);
    EXPECT_GE(reader->MemoryUsage(), 0);
    EXPECT_GT(reader->CellByteSize().file_bytes, 0);
}

TEST(MarisaIndexArtifactTest, AsyncHeapAndMmapRoundTrip) {
    for (const auto name : {"MarisaVarchar", "MarisaVarcharMmap"}) {
        const auto& backend =
            ScalarReaderBackends().Get<std::string_view>(name);
        const auto persisted = SerializedMarisa(name);
        auto reader = OpenV3(backend, persisted, {.row_count = 5}, true);
        ASSERT_NE(reader, nullptr);
        ExpectBetaRows(*reader);
        const auto* nulls = dynamic_cast<const INullReader*>(reader.get());
        ASSERT_NE(nulls, nullptr);
        EXPECT_EQ(nulls->IsNull().count(), 1);
    }
}

// Cancel only payload reads, after planning has created its staging directory.
class CancelMarisaPayloadStream final : public storage::RemoteInputStream {
 public:
    CancelMarisaPayloadStream(std::shared_ptr<arrow::Buffer> bytes,
                              folly::CancellationSource& cancellation)
        : storage::RemoteInputStream(
              std::make_shared<arrow::io::BufferReader>(std::move(bytes))),
          cancellation_(cancellation) {
    }
    bool armed{false};
    folly::SemiFuture<size_t>
    ReadAtAsync(void* data, size_t offset, size_t size) override {
        if (armed) {
            cancellation_.requestCancellation();
        }
        return storage::RemoteInputStream::ReadAtAsync(data, offset, size);
    }

 private:
    folly::CancellationSource& cancellation_;
};

TEST(MarisaIndexArtifactTest, AsyncCancellationRemovesPlannedFiles) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharMmap");
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "marisa-cancel-XXXXXX",
        "Marisa cancellation test");
    folly::CancellationSource cancellation;
    auto input = std::make_shared<CancelMarisaPayloadStream>(
        MakePackedArtifactBuffer(SerializedMarisa(backend.Name())),
        cancellation);
    const auto priority = proto::common::LoadPriority::HIGH;
    auto source = folly::coro::blockingWait(folly::coro::co_withExecutor(
        storage::ResolveAsyncLoadExecutor({}, priority),
        storage::AsyncIndexEntryReader::Open(input, 0, priority)));
    storage::LoadOptions options;
    options.enable_mmap = true;
    options.mmap_dir_path = staging->Path();
    options.params =
        AnnotateJsonProjectionCompleteness(backend.LoadParams({.row_count = 5}),
                                           source->Directory(),
                                           source->IndexMeta());
    OpContext context(cancellation.getToken());
    options.op_ctx = &context;
    const auto loader_entry =
        LoaderRegistry::Instance().Lookup(backend.Family());
    input->armed = true;
    ExpectSegcoreError(ErrorCode::FollyCancel, [&] {
        static_cast<void>(loader_entry.Load(
            {OpenedIndexSource{PackedIndexSource{
                 std::shared_ptr<storage::AsyncIndexEntryReader>(
                     std::move(source))}},
             options}));
    });
    EXPECT_TRUE(cancellation.isCancellationRequested());
    EXPECT_TRUE(std::filesystem::is_empty(staging->Path()));
}

struct MissingEntryCase {
    const char* name;
    const char* entry;
};

class MarisaMissingEntryTest
    : public ::testing::TestWithParam<MissingEntryCase> {};

TEST_P(MarisaMissingEntryTest, RejectsMissingRequiredEntry) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    persisted.entries.erase(GetParam().entry);

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

INSTANTIATE_TEST_SUITE_P(
    RequiredEntries,
    MarisaMissingEntryTest,
    ::testing::Values(MissingEntryCase{"MissingTrie", MARISA_TRIE_INDEX},
                      MissingEntryCase{"MissingRowIds", MARISA_STR_IDS}),
    [](const auto& info) { return info.param.name; });

TEST(MarisaIndexArtifactTest, RejectsTruncatedRowIds) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& ids = persisted.entries.at(MARISA_STR_IDS);
    ASSERT_FALSE(ids.empty());
    ids.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsOutOfRangeRowId) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& ids = persisted.entries.at(MARISA_STR_IDS);
    ASSERT_GE(ids.size(), sizeof(int64_t));
    const auto invalid = std::numeric_limits<int64_t>::max();
    std::memcpy(ids.data(), &invalid, sizeof(invalid));

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsCorruptTriePayload) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& trie = persisted.entries.at(MARISA_TRIE_INDEX);
    ASSERT_FALSE(trie.empty());
    // A short file raises MARISA_IO_ERROR; corrupt a complete header to test
    // the library's distinct MARISA_FORMAT_ERROR classification.
    trie.front() ^= uint8_t{0xff};

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

struct MissingCsrPartCase {
    const char* name;
    bool metadata;
    const char* key;
};

class MarisaIncompleteCsrTest
    : public ::testing::TestWithParam<MissingCsrPartCase> {};

TEST_P(MarisaIncompleteCsrTest, RejectsIncompleteCsrSideData) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    if (GetParam().metadata) {
        persisted.metadata.erase(GetParam().key);
    } else {
        persisted.entries.erase(GetParam().key);
    }

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

INSTANTIATE_TEST_SUITE_P(
    CsrParts,
    MarisaIncompleteCsrTest,
    ::testing::Values(
        MissingCsrPartCase{"MissingIndex", false, MARISA_CSR_INDEX},
        MissingCsrPartCase{"MissingOffsets", false, MARISA_CSR_OFFSETS},
        MissingCsrPartCase{
            "MissingVersion", true, kCsrFormatVersionMeta.data()},
        MissingCsrPartCase{"MissingKeyCount", true, kCsrNumKeysMeta.data()}),
    [](const auto& info) { return info.param.name; });

TEST(MarisaIndexArtifactTest, RejectsUnknownCsrVersion) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    persisted.metadata[std::string(kCsrFormatVersionMeta)] = 2;

    ExpectSegcoreError(ErrorCode::Unsupported, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsCsrKeyCountDisagreement) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& key_count = persisted.metadata[std::string(kCsrNumKeysMeta)];
    key_count = key_count.get<uint64_t>() + 1;

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsTruncatedCsrIndex) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& index = persisted.entries.at(MARISA_CSR_INDEX);
    ASSERT_FALSE(index.empty());
    index.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsTruncatedCsrOffsets) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& offsets = persisted.entries.at(MARISA_CSR_OFFSETS);
    ASSERT_FALSE(offsets.empty());
    offsets.pop_back();

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, RejectsCsrOffsetForWrongRow) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto persisted = SerializedMarisa(backend.Name());
    auto& offsets = persisted.entries.at(MARISA_CSR_OFFSETS);
    ASSERT_GE(offsets.size(), sizeof(uint32_t));
    const uint32_t invalid = 100;
    std::memcpy(offsets.data(), &invalid, sizeof(invalid));

    ExpectSegcoreError(ErrorCode::DataFormatBroken, [&] {
        static_cast<void>(OpenV3(backend, persisted, {.row_count = 5}));
    });
}

TEST(MarisaIndexArtifactTest, TrieReadFailureReportsFileReadFailed) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(SerializedMarisa(backend.Name()))));
    auto source = storage::IndexEntryReader::Open(input, input->Size());
    auto staging = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "marisa-read-error-XXXXXX",
        "marisa read error test");
    storage::LoadOptions options;
    options.mmap_dir_path = staging->Path();
    options.params = backend.LoadParams({.row_count = 5});
    auto plan = LoaderTestAccess::Plan<MarisaIndexLoader>(
        source->Directory(), source->IndexMeta(), options);
    const auto& trie =
        std::get<storage::FileEntryTarget>(plan.At(MARISA_TRIE_INDEX).target);
    // Opening succeeds; MARISA read then fails on the directory with EISDIR.
    ASSERT_TRUE(std::filesystem::create_directory(trie.staging->path));
    ExpectSegcoreError(ErrorCode::FileReadFailed, [&] {
        static_cast<void>(
            LoaderTestAccess::Finish<MarisaIndexLoader>(plan, options));
    });
}

class ObserveMarisaTrieWriter final : public storage::IndexEntryWriter {
 public:
    ObserveMarisaTrieWriter(TestArtifactData& output,
                            std::string directory,
                            bool fail)
        : inner_(output), directory_(std::move(directory)), fail_(fail) {
    }

    void
    WriteEntry(const std::string& name,
               const void* data,
               size_t size) override {
        inner_.WriteEntry(name, data, size);
    }

    void
    WriteEntry(const std::string& name, int fd, size_t size) override {
        if (name == MARISA_TRIE_INDEX) {
            trie_fd = fd;
            struct stat status {};
            ASSERT_EQ(::fstat(fd, &status), 0);
            EXPECT_EQ(status.st_nlink, 0);
            const auto flags = ::fcntl(fd, F_GETFD);
            ASSERT_NE(flags, -1);
            EXPECT_NE(flags & FD_CLOEXEC, 0);
            const auto path = std::filesystem::read_symlink("/proc/self/fd/" +
                                                            std::to_string(fd))
                                  .string();
            EXPECT_EQ(path.find(directory_ + "/marisa_trie_"), 0);
            const std::string suffix = " (deleted)";
            ASSERT_TRUE(path.ends_with(suffix));
            replacement = path.substr(0, path.size() - suffix.size());
            std::ofstream file(replacement);
            file << "replacement";
            file.close();
            ASSERT_FALSE(file.fail());
            if (fail_) {
                ThrowInfo(FileWriteFailed, "injected MARISA sink failure");
            }
        }
        inner_.WriteEntry(name, fd, size);
    }

    void
    Finish() override {
        for (const auto& [key, value] : meta_json_.items()) {
            inner_.PutMeta(key, value);
        }
        inner_.Finish();
    }

    size_t
    GetTotalBytesWritten() const override {
        return inner_.GetTotalBytesWritten();
    }

    int trie_fd{-1};
    std::string replacement;

 private:
    TestArtifactWriter inner_;
    std::string directory_;
    bool fail_;
};

TEST(MarisaIndexArtifactTest, PackedTrieStagesAndReleasesItsUnlinkedName) {
    const auto& backend =
        ScalarReaderBackends().Get<std::string_view>("MarisaVarcharNonNull");
    for (const bool fail : {false, true}) {
        SCOPED_TRACE(fail);
        auto root = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "marisa-staging-XXXXXX",
            "marisa staging test");
        const auto directory = root->Path() + "/configured";
        auto params = backend.BuildParams();
        params["local_dir"] = directory;
        auto builder =
            BuilderRegistry<ScalarBuildInput<std::string_view>>::Instance()
                .Create(families::kMarisa, params);
        ASSERT_NE(builder, nullptr);
        ScalarTestData<std::string_view> data(
            {"alpha", "beta", "beta", "gamma", "delta"});
        data.validity_present = false;
        const ScalarTestInput<std::string_view> input(data);
        auto artifact = std::move(*builder).Build(input.View());
        TestArtifactData persisted;
        ObserveMarisaTrieWriter sink(persisted, directory, fail);
        if (fail) {
            ExpectSegcoreError(ErrorCode::FileWriteFailed,
                               [&] { artifact->Serialize(sink); });
        } else {
            artifact->Serialize(sink);
            sink.Finish();
        }
        ASSERT_GE(sink.trie_fd, 0);
        errno = 0;
        EXPECT_EQ(::fcntl(sink.trie_fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
        if (!fail) {
            auto reader = OpenV3(backend, persisted, {.row_count = 5});
            ExpectBetaRows(*reader);
        }
        ASSERT_FALSE(sink.replacement.empty());
        std::ifstream replacement(sink.replacement);
        std::string contents;
        std::getline(replacement, contents);
        EXPECT_EQ(contents, "replacement");
    }
}

}  // namespace
}  // namespace milvus::index::test
