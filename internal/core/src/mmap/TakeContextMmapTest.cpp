// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "mmap/ChunkedColumn.h"
#include "test_utils/Constants.h"
#include "test_utils/OriginalNqTextFixture.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {
namespace {

class OriginalMmapDirectory final {
 public:
    OriginalMmapDirectory() {
        AssertInfo(!TestMmapPath.empty(), "test mmap root is not initialized");
        std::filesystem::create_directories(TestMmapPath);
        auto pattern =
            (std::filesystem::path(TestMmapPath) / "take-original-nq-XXXXXX")
                .string();
        auto* created = ::mkdtemp(pattern.data());
        if (created == nullptr) {
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        }
        path = created;
    }

    ~OriginalMmapDirectory() {
        // ChunkMmapGuard removes its own files. Never recursively remove the
        // shared test root or unrelated test data if a test fails.
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }

    std::filesystem::path path;
};

// The standard test translator owns actual file-mapped chunks in this test.
// Override its MEMORY-only metadata and byte estimates to match the real DISK
// backing; this is not an in-memory buffer labeled as a disk experiment.
class OriginalMmapTranslator final : public TestChunkTranslator {
 public:
    OriginalMmapTranslator(std::vector<int64_t> rows,
                           std::string key,
                           std::vector<std::unique_ptr<Chunk>> chunks,
                           std::vector<int64_t> bytes)
        : TestChunkTranslator(
              std::move(rows), std::move(key), std::move(chunks)),
          bytes_(std::move(bytes)),
          meta_(StorageType::DISK,
                CellIdMappingMode::IDENTICAL,
                CellDataType::SCALAR_FIELD,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                true) {
        const auto* base =
            static_cast<const segcore::storagev1translator::CTMeta*>(
                TestChunkTranslator::meta());
        meta_.num_rows_until_chunk_ = base->num_rows_until_chunk_;
        meta_.virt_chunk_order_ = base->virt_chunk_order_;
        meta_.vcid_to_cid_arr_ = base->vcid_to_cid_arr_;
    }

    Meta*
    meta() override {
        return &meta_;
    }

    std::pair<ResourceUsage, ResourceUsage>
    estimated_byte_size_of_cell(cid_t id) const override {
        return {{0, bytes_.at(id)}, {bytes_.at(id) * 2, bytes_.at(id)}};
    }

    int64_t
    cells_storage_bytes(const std::vector<cid_t>& ids) const override {
        int64_t bytes = 0;
        for (auto id : ids) {
            bytes += bytes_.at(id);
        }
        return bytes;
    }

 private:
    std::vector<int64_t> bytes_;
    segcore::storagev1translator::CTMeta meta_;
};

std::unique_ptr<Chunk>
MapOriginalText(const std::vector<OriginalNqTextRow>& original,
                size_t first,
                size_t count,
                const std::filesystem::path& path,
                std::weak_ptr<ChunkMmapGuard>& mapped_owner) {
    const size_t header = (count + 1) * sizeof(uint32_t);
    size_t size = header;
    for (size_t row = first; row < first + count; ++row) {
        size += original[row].text.size();
    }
    std::vector<char> bytes(size);
    uint32_t position = header;
    std::memcpy(bytes.data(), &position, sizeof(position));
    for (size_t lane = 0; lane < count; ++lane) {
        const auto& text = original[first + lane].text;
        std::memcpy(bytes.data() + position, text.data(), text.size());
        position += text.size();
        std::memcpy(bytes.data() + (lane + 1) * sizeof(position),
                    &position,
                    sizeof(position));
    }
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.exceptions(std::ios::badbit | std::ios::failbit);
    file.write(bytes.data(), bytes.size());
    file.close();
    const auto fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        throw std::system_error(
            errno, std::generic_category(), "open mmap file");
    }
    auto* data = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    const auto saved_errno = errno;
    ::close(fd);
    if (data == MAP_FAILED) {
        throw std::system_error(
            saved_errno, std::generic_category(), "mmap original text");
    }
    auto owner = std::make_shared<ChunkMmapGuard>(
        static_cast<char*>(data), size, path.string());
    mapped_owner = owner;
    return std::make_unique<StringChunk>(
        count, static_cast<char*>(data), size, false, std::move(owner));
}

TEST(TakeContextMmapFoundation, OriginalFileMappedViewsReuseAndReleasePins) {
    // Functional/lifetime coverage of the already-supported raw mmap backend.
    // This does not establish cold pages, actual physical I/O, or disk latency.
    OriginalMmapDirectory directory;
    const auto original = LoadOriginalNqTextRows();
    std::vector<int64_t> row_counts, cell_bytes;
    std::vector<std::unique_ptr<Chunk>> chunks;
    std::vector<std::weak_ptr<ChunkMmapGuard>> mapped_owners;
    std::vector<std::filesystem::path> paths;
    for (size_t first = 0; first < original.size(); first += 32) {
        const auto count = std::min<size_t>(32, original.size() - first);
        const auto path = directory.path / std::to_string(first);
        std::weak_ptr<ChunkMmapGuard> owner;
        auto chunk = MapOriginalText(original, first, count, path, owner);
        EXPECT_EQ(chunk->CellByteSize().memory_bytes, 0);
        EXPECT_EQ(chunk->CellByteSize().file_bytes, chunk->Size());
        row_counts.push_back(count);
        cell_bytes.push_back(chunk->Size());
        chunks.push_back(std::move(chunk));
        mapped_owners.push_back(owner);
        paths.push_back(path);
    }
    const auto budget = *std::max_element(cell_bytes.begin(), cell_bytes.end());
    auto translator =
        std::make_unique<OriginalMmapTranslator>(row_counts,
                                                 "take-original-nq-file-mapped",
                                                 std::move(chunks),
                                                 cell_bytes);
    auto slot = Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    auto column =
        std::make_shared<ChunkedColumn>(slot,
                                        FieldMeta(FieldName("original_text"),
                                                  FieldId(102),
                                                  DataType::VARCHAR,
                                                  false,
                                                  std::nullopt));
    auto context = std::make_shared<TakeContext>(
        TakeContextOptions{static_cast<size_t>(budget)});
    std::vector<int64_t> offsets;
    for (size_t lane = 0; lane < original.size(); ++lane) {
        offsets.push_back((lane * 73 + 11) % original.size());
    }
    offsets.insert(offsets.end(), {0, 73, 128});
    std::vector<std::string> copies;
    for (int repetition = 0; repetition < 2; ++repetition) {
        copies.clear();
        copies.reserve(offsets.size());
        auto take =
            column->Take(nullptr,
                         ChunkedColumnInterface::TakeOptions{
                             OffsetView::From(offsets.data(), offsets.size()),
                             TargetType::StringView,
                             nullptr,
                             context});
        ASSERT_NE(take, nullptr);
        take->VisitViews<std::string_view>(
            offsets.size(),
            [&](const std::string_view* values,
                ValidityView valid,
                const bool* skipped,
                int64_t first,
                int64_t count) {
                ASSERT_EQ(first, copies.size());
                ASSERT_GT(count, 0);
                ASSERT_LE(first + count, offsets.size());
                if (repetition == 1 && first + count == offsets.size()) {
                    context->Clear();
                    slot->ManualEvictAll();
                }
                // A short prefix borrows only its current mapped owners.
                // Previous prefixes need not stay pinned after their consumer.
                for (int64_t lane = 0; lane < count; ++lane) {
                    const auto id = offsets[first + lane] / 32;
                    auto owner = mapped_owners[id].lock();
                    ASSERT_NE(owner, nullptr);
                    EXPECT_TRUE(owner->is_file_backed());
                    EXPECT_TRUE(std::filesystem::is_regular_file(paths[id]));
                }
                for (int64_t lane = 0; lane < count; ++lane) {
                    EXPECT_TRUE(valid[lane]);
                    EXPECT_FALSE(skipped[lane]);
                    EXPECT_EQ(values[lane],
                              original[offsets[first + lane]].text);
                    copies.emplace_back(values[lane]);
                }
            });
        EXPECT_EQ(copies.size(), offsets.size());
        EXPECT_LE(context->Stats().retained_bytes, budget);
    }
    EXPECT_GT(context->Stats().evicted_cohorts, 0);
    EXPECT_EQ(context->Stats().retained_bytes, 0);
    slot->ManualEvictAll();
    for (size_t id = 0; id < mapped_owners.size(); ++id) {
        EXPECT_TRUE(mapped_owners[id].expired());
        EXPECT_FALSE(std::filesystem::exists(paths[id]));
    }
    ASSERT_EQ(copies.size(), offsets.size());
    for (size_t lane = 0; lane < offsets.size(); ++lane) {
        EXPECT_EQ(copies[lane], original[offsets[lane]].text);
    }
}
}  // namespace
}  // namespace milvus
