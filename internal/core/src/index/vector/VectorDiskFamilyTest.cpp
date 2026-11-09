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

#include "index/test_utils/IndexCacheTestUtils.h"

#include <gtest/gtest.h>

#ifdef BUILD_DISK_ANN

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <map>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "cachinglayer/Metrics.h"
#include "folly/ScopeGuard.h"
#include "knowhere/index/index_factory.h"
#include "common/Utils.h"
#include "common/OpContext.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/vector/KnowhereEngine.h"
#include "index/vector/VectorDiskBuildFileManager.h"
#include "index/vector/VectorDiskArtifact.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index::test {
namespace {

// Exercise the native Serialize boundary without building a real ANN index.
class SerializationTestNode final : public knowhere::IndexNode {
 public:
    explicit SerializationTestNode(std::function<knowhere::Status()> serialize)
        : serialize_(std::move(serialize)) {
    }
    knowhere::Status
    Serialize(knowhere::BinarySet&) const override {
        return serialize_();
    }
    knowhere::Status
    Train(const knowhere::DataSetPtr,
          std::shared_ptr<knowhere::Config>,
          bool) override {
        throw std::logic_error("unexpected Train");
    }
    knowhere::Status
    Add(const knowhere::DataSetPtr,
        std::shared_ptr<knowhere::Config>,
        bool) override {
        throw std::logic_error("unexpected Add");
    }
    knowhere::expected<knowhere::DataSetPtr>
    Search(const knowhere::DataSetPtr,
           std::unique_ptr<knowhere::Config>,
           const knowhere::BitsetView&,
           milvus::OpContext*) const override {
        throw std::logic_error("unexpected Search");
    }
    knowhere::expected<knowhere::DataSetPtr>
    GetVectorByIds(const knowhere::DataSetPtr,
                   milvus::OpContext*) const override {
        throw std::logic_error("unexpected GetVectorByIds");
    }
    bool
    HasRawData(const std::string&) const override {
        return false;
    }
    knowhere::expected<knowhere::DataSetPtr>
    GetIndexMeta(std::unique_ptr<knowhere::Config>) const override {
        throw std::logic_error("unexpected GetIndexMeta");
    }
    knowhere::Status
    Deserialize(const knowhere::BinarySet&,
                std::shared_ptr<knowhere::Config>) override {
        throw std::logic_error("unexpected Deserialize");
    }
    knowhere::Status
    DeserializeFromFile(const std::string&,
                        std::shared_ptr<knowhere::Config>) override {
        throw std::logic_error("unexpected DeserializeFromFile");
    }
    std::unique_ptr<knowhere::BaseConfig>
    CreateConfig() const override {
        return std::make_unique<knowhere::BaseConfig>();
    }
    int64_t
    Dim() const override {
        return 4;
    }
    int64_t
    Size() const override {
        return 0;
    }
    int64_t
    Count() const override {
        return 1;
    }
    std::string
    Type() const override {
        return "serialization-test";
    }

 private:
    std::function<knowhere::Status()> serialize_;
};

class SerializationTestSink final : public storage::FileSink {
 public:
    storage::Generation
    Gen() const override {
        return storage::Generation::V1V2;
    }
    void
    WriteEntry(std::string_view, const void*, size_t) override {
        throw std::logic_error("unexpected sliced entry");
    }
    void
    WriteEntryFromLocalFile(std::string_view, const std::string&) override {
        throw std::logic_error("unexpected sliced file");
    }
    void
    WriteRawEntryFromLocalFile(std::string_view,
                               const std::string& path) override {
        ++files;
        EXPECT_EQ(std::filesystem::file_size(path), 7);
    }
    void
    WriteRawEntry(std::string_view,
                  std::shared_ptr<arrow::Buffer> data) override {
        if (fail_write) {
            throw std::runtime_error("sink write failed");
        }
        ++buffers;
        EXPECT_EQ(data->ToString(), "payload");
    }
    storage::ArtifactStats
    Finish() override {
        return {};
    }
    void
    ReleaseLocalStaging() override {
    }
    int files{0};
    int buffers{0};
    bool fail_write{false};
};

constexpr int64_t kRows = 1000;
constexpr int64_t kDim = 4;

template <typename T>
void
WriteRaw(const std::string& path,
         const std::vector<T>& values,
         uint32_t rows,
         uint32_t dim) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(stream.good());
    stream.write(reinterpret_cast<const char*>(&rows), sizeof(rows));
    stream.write(reinterpret_cast<const char*>(&dim), sizeof(dim));
    stream.write(reinterpret_cast<const char*>(values.data()),
                 static_cast<std::streamsize>(values.size() * sizeof(T)));
    ASSERT_TRUE(stream.good());
}

void
WriteOffsets(const std::string& path, const std::vector<size_t>& offsets) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(stream.good());
    const size_t count = offsets.size();
    stream.write(reinterpret_cast<const char*>(&count), sizeof(count));
    stream.write(reinterpret_cast<const char*>(offsets.data()),
                 static_cast<std::streamsize>(count * sizeof(size_t)));
    ASSERT_TRUE(stream.good());
}

template <typename T>
DataType
PhysicalType() {
    if constexpr (std::is_same_v<T, float>) {
        return DataType::VECTOR_FLOAT;
    } else if constexpr (std::is_same_v<T, float16>) {
        return DataType::VECTOR_FLOAT16;
    } else {
        return DataType::VECTOR_BFLOAT16;
    }
}

template <typename T>
std::vector<T>
MakeRows() {
    std::vector<T> values(kRows * kDim);
    for (int64_t row = 0; row < kRows; ++row) {
        for (int64_t col = 0; col < kDim; ++col) {
            const float value = static_cast<float>(
                ((row * (col * 73 + 41) + col * 11) % 997) / 997.0);
            values[row * kDim + col] = static_cast<T>(value);
        }
    }
    return values;
}

template <typename T>
struct OpenedDiskIndex {
    IIndexReaderBasePtr base;
    std::vector<uint8_t> persisted_validity;
};

struct DiskBuildCase {
    std::string metric = "L2";
    int64_t dim = kDim;
    int64_t logical_rows = kRows;
    // A VECTOR_ARRAY field; it has embedding offsets only under an
    // embedding-list metric.
    bool vector_array = false;
    std::string offsets_path;
    bool mmap = false;
    std::string validity_path;
    bool element_nullable = false;
    bool mmap_id_map = false;
    // Published next to the engine files as the artifact's validity entry, as
    // a pre-element-level build published the row-domain validity of its
    // input.
    std::vector<uint8_t> legacy_validity;
    // A resident payload; the raw file then holds only its header.
    const void* tensor = nullptr;
    bool defer_load = false;
    OpContext* reader_context = nullptr;
    std::optional<IndexCacheConfig> cache_config;
};

template <typename T>
void
BuildDisk(const std::string& raw_path,
          const std::string& staging_dir,
          OpenedDiskIndex<T>& opened,
          const DiskBuildCase& build = {}) {
    const bool vector_array = build.vector_array || !build.offsets_path.empty();
    const bool nullable =
        !build.validity_path.empty() || !build.legacy_validity.empty();
    auto adapted = AdaptIndexType({
        .index_type = knowhere::IndexEnum::INDEX_DISKANN,
        .field_type = vector_array ? DataType::VECTOR_ARRAY : PhysicalType<T>(),
        .element_type = vector_array ? PhysicalType<T>() : DataType::NONE,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE, build.metric},
                   {DIM_KEY, build.dim},
                   {DISK_ANN_MAX_DEGREE, "24"},
                   {DISK_ANN_SEARCH_LIST_SIZE, "56"},
                   {DISK_ANN_PQ_CODE_BUDGET, "0.001"},
                   {DISK_ANN_BUILD_DRAM_BUDGET, "2"},
                   {DISK_ANN_BUILD_THREAD_NUM, "2"}},
    });
    adapted.params["local_dir"] = staging_dir;
    auto builder =
        BuilderRegistry<PreparedVectorBuildFiles<T>>::Instance().Create(
            adapted.family, adapted.params);
    ASSERT_NE(builder, nullptr);
    PreparedVectorBuildFiles<T> input{
        .raw_path = raw_path,
        .validity_path = build.validity_path.empty()
                             ? std::nullopt
                             : std::make_optional(build.validity_path),
        .embedding_offsets_path = build.offsets_path.empty()
                                      ? std::nullopt
                                      : std::make_optional(build.offsets_path),
        .tensor = build.tensor,
        .raw_file_complete = build.tensor == nullptr,
    };
    auto artifact = std::move(*builder).Build(input);
    ASSERT_NE(artifact, nullptr);

    // DiskANN needs the V1/V2 directory source's native file-manager handle.
    // A buffer-backed test source can read the entries but cannot open that
    // handle, so persist through the same sink/source pair as production.
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = staging_dir + "/objects/";
    std::filesystem::create_directories(config.root_path);
    const auto field_type =
        vector_array ? DataType::VECTOR_ARRAY : PhysicalType<T>();
    storage::FileManagerContext context({1, 2, 3, 100},
                                        {3,
                                         100,
                                         1000,
                                         1,
                                         "vector_disk_family",
                                         "field",
                                         field_type,
                                         build.dim,
                                         false},
                                        storage::CreateChunkManager(config),
                                        storage::InitArrowFileSystem(config));
    context.use_async_load = false;
    storage::V1DiskSink sink(context);
    artifact->Serialize(sink);
    if (!build.legacy_validity.empty()) {
        // Before element-level builds dropped embedding offsets, the disk
        // builder staged the validity input as a local file and published it
        // through the same sink entry as the engine files.
        const auto local = staging_dir + "/legacy_" + VALID_DATA_KEY;
        {
            std::ofstream stream(local, std::ios::binary | std::ios::trunc);
            stream.write(
                reinterpret_cast<const char*>(build.legacy_validity.data()),
                static_cast<std::streamsize>(build.legacy_validity.size()));
            ASSERT_TRUE(stream.good());
        }
        sink.WriteEntryFromLocalFile(VALID_DATA_KEY, local);
    }
    const auto stats = sink.Finish();
    ASSERT_FALSE(stats.Files().empty());
    std::vector<std::string> paths;
    paths.reserve(stats.Files().size());
    for (const auto& file : stats.Files()) {
        paths.push_back(file.file_name);
    }
    if (nullable) {
        storage::V1RemoteSource source(context,
                                       paths,
                                       {},
                                       storage::ArtifactStorageNamespace::Index,
                                       storage::V1SourceLayout::DiskFiles);
        opened.persisted_validity = source.ReadEntry(VALID_DATA_KEY);
    }
    sink.ReleaseLocalStaging();
    storage::LoadOptions options;
    options.params = adapted.params;
    options.params[INDEX_NUM_ROWS_KEY] = build.logical_rows;
    if (vector_array) {
        options.params[ELEMENT_NULLABLE] = build.element_nullable;
    }
    if (nullable) {
        options.params["nullable"] = true;
        options.params[ENABLE_MMAP_I2O_MAP] = build.mmap_id_map;
        options.params[ENABLE_MMAP_O2I_MAP] = build.mmap_id_map;
    }
    options.params[DISK_ANN_LOAD_THREAD_NUM] = 2;
    options.enable_mmap = build.mmap;
    options.mmap_dir_path = staging_dir;
    auto cache = milvus::index::test::TestIndexCacheConfig();
    if (build.defer_load) {
        cache = IndexCacheConfig{
            staging_dir + "/engine_cache",
            cachinglayer::Meta{cachinglayer::StorageType::DISK,
                               cachinglayer::CellIdMappingMode::IDENTICAL,
                               cachinglayer::CellDataType::OTHER,
                               CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                               true},
            {1024 * 1024, 1024 * 1024},
            {},
            4096};
    }
    if (build.cache_config) {
        cache = *build.cache_config;
    }
    auto index = LoaderRegistry::Instance()
                     .Lookup(adapted.family)
                     .Load({IndexFiles{context,
                                       std::move(paths),
                                       LegacyIndexStorageConfig{
                                           storage::V1SourceLayout::DiskFiles}},
                            options,
                            cache});
    if (build.defer_load)
        EXPECT_FALSE(index->Metadata().count.has_value());
    opened.base = index->CreateReader(build.reader_context);
}

template <typename T>
void
RunOrdinaryDiskSearch(bool mmap,
                      bool resident = false,
                      bool defer_load = false) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_family_XXXXXX", "vector disk family test");
    const auto raw_path = files->Path() + "/raw";
    auto values = MakeRows<T>();
    WriteRaw(raw_path, resident ? std::vector<T>{} : values, kRows, kDim);
    OpenedDiskIndex<T> opened;
    BuildDisk(raw_path,
              files->Path(),
              opened,
              {.mmap = mmap,
               .tensor = resident ? values.data() : nullptr,
               .defer_load = defer_load});
    ASSERT_NE(opened.base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(opened.base->Count(), kRows);
    EXPECT_EQ(reader->Dim(), kDim);
    EXPECT_EQ(reader->KnowhereIndexType(), knowhere::IndexEnum::INDEX_DISKANN);
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 8}},
        .metric_type_ = "L2",
        .topk_ = 4,
    };
    auto query = GenDataset(2, kDim, values.data() + 100 * kDim);
    SearchResult result;
    reader->Search(query, params, BitsetView{}, result);
    EXPECT_EQ(result.total_nq_, 2);
    ASSERT_EQ(result.seg_offsets_.size(), 8);
    EXPECT_EQ(result.unity_topK_, 4);
    for (int64_t nq = 0; nq < 2; ++nq) {
        EXPECT_GE(result.seg_offsets_[nq * 4], 0);
        EXPECT_LT(result.seg_offsets_[nq * 4], kRows);
    }
    params.search_params_[knowhere::meta::RADIUS] = 0.2;
    params.search_params_[knowhere::meta::RANGE_FILTER] = 0.1;
    SearchResult range_result;
    reader->Search(query, params, BitsetView{}, range_result);
    EXPECT_EQ(range_result.total_nq_, 2);
    EXPECT_EQ(range_result.seg_offsets_.size(), 8);
    if constexpr (std::is_same_v<T, float>) {
        const std::array<int64_t, 2> ids{511, 100};
        if (reader->HasRawData()) {
            auto got = reader->GetVector(GenIdsDataset(ids.size(), ids.data()));
            ASSERT_EQ(got.size(), ids.size() * kDim * sizeof(T));
            for (size_t i = 0; i < ids.size(); ++i) {
                EXPECT_EQ(std::memcmp(got.data() + i * kDim * sizeof(T),
                                      values.data() + ids[i] * kDim,
                                      kDim * sizeof(T)),
                          0);
            }
        } else {
            EXPECT_ANY_THROW(static_cast<void>(
                reader->GetVector(GenIdsDataset(ids.size(), ids.data()))));
        }
    }
}

TEST(VectorDiskFileManagerTest, PreparedInputsAreBorrowedReadOnly) {
    auto inputs = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_inputs_XXXXXX", "prepared input test");
    const auto raw = inputs->Path() + "/raw";
    WriteRaw<float>(raw, {1.0f, 2.0f}, 1, 2);
    auto output = storage::LocalDirectory::CreateOwned(
        inputs->Path(), "outputs_XXXXXX", "prepared input test");
    const auto output_dir = output->Path();
    auto manager =
        std::make_shared<VectorDiskBuildFileManager>(std::move(output));
    manager->SetInputFiles({raw});
    auto stream = manager->OpenInputStream(raw);
    ASSERT_NE(stream, nullptr);
    EXPECT_EQ(stream->Size(), 2 * sizeof(uint32_t) + 2 * sizeof(float));
    std::array<uint32_t, 2> header;
    EXPECT_EQ(stream->Read(header.data(), sizeof(header)), sizeof(header));
    EXPECT_EQ(header, (std::array<uint32_t, 2>{1, 2}));
    std::array<float, 2> values;
    EXPECT_EQ(stream->Read(values.data(), sizeof(values)), sizeof(values));
    EXPECT_EQ(values, (std::array<float, 2>{1.0f, 2.0f}));
    EXPECT_TRUE(manager->LocalFiles().empty());
    EXPECT_THROW(manager->OpenOutputStream(raw), SegcoreError);
    EXPECT_FALSE(manager->AddFile(raw));
    EXPECT_FALSE(manager->RemoveFile(raw));
    EXPECT_TRUE(std::filesystem::exists(raw));
    manager.reset();
    EXPECT_TRUE(std::filesystem::exists(output_dir));
    stream.reset();
    EXPECT_FALSE(std::filesystem::exists(output_dir));
    EXPECT_TRUE(std::filesystem::exists(raw));
}

TEST(VectorDiskFileManagerTest, InputFailuresKeepTheirOriginalCode) {
    auto inputs = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_inputs_XXXXXX", "prepared input failure test");
    const auto raw = inputs->Path() + "/raw";
    WriteRaw<float>(raw, {1.0f}, 1, 1);
    auto make_manager = [&] {
        return std::make_shared<VectorDiskBuildFileManager>(
            storage::LocalDirectory::CreateOwned(
                inputs->Path(),
                "outputs_XXXXXX",
                "prepared input failure test"));
    };
    auto expect_code = [](auto&& operation, ErrorCode code) {
        try {
            operation();
            FAIL() << "expected input failure";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), code);
        }
    };
    auto undeclared = make_manager();
    expect_code([&] { undeclared->OpenInputStream(raw); }, FileReadFailed);
    auto missing = make_manager();
    missing->SetInputFiles({raw + ".missing"});
    expect_code([&] { missing->OpenInputStream(raw + ".missing"); },
                FileOpenFailed);
    expect_code([&] { missing->RethrowFirstFailure(); }, FileOpenFailed);
    auto truncated = make_manager();
    truncated->SetInputFiles({raw});
    auto stream = truncated->OpenInputStream(raw);
    std::filesystem::resize_file(raw, 0);
    uint32_t value;
    expect_code([&] { stream->Read(&value, sizeof(value)); }, FileReadFailed);
    expect_code([&] { truncated->RethrowFirstFailure(); }, FileReadFailed);
}

TEST(VectorDiskFileManagerTest, MetadataBeforeOrAfterClosePublishesRawBytes) {
    for (bool before_close : {true, false}) {
        SCOPED_TRACE(before_close);
        auto owner = storage::LocalDirectory::CreateOwned(
            "/tmp", "vector_stream_XXXXXX", "stream metadata test");
        auto manager = std::make_shared<VectorDiskBuildFileManager>(owner);
        const auto path = owner->Path() + "/engine";
        const std::array<uint8_t, 3> bytes{4, 5, 6};
        milvus::FileManager& base = *manager;
        auto output = base.OpenOutputStream(
            path, {.expected_size = 1, .use_memory = false});
        ASSERT_EQ(output->Write(bytes.data(), bytes.size()), bytes.size());
        const milvus::FileMeta meta{path, bytes.size()};
        if (before_close) {
            ASSERT_TRUE(manager->AddFileMeta(meta));
            EXPECT_THROW(manager->LocalFiles(), SegcoreError);
        }
        output->Close();
        ASSERT_TRUE(manager->AddFileMeta(meta));
        EXPECT_TRUE(manager->AddFileMeta(meta));
        EXPECT_NO_THROW(manager->RethrowFirstFailure());
        auto files = manager->LocalFiles();
        ASSERT_EQ(files.size(), 1);
        EXPECT_EQ(files[0].transport, VectorDiskFileTransport::RawUnsliced);
        EXPECT_EQ(files[0].size, bytes.size());
        VectorDiskArtifact artifact(nullptr, manager);
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = owner->Path() + "/objects/";
        storage::FileManagerContext context(
            {1, 2, 3, 100},
            {3, 100, 6108, 1},
            storage::CreateChunkManager(config),
            storage::InitArrowFileSystem(config));
        storage::V1DiskSink sink(context);
        artifact.Serialize(sink);
        const auto stats = sink.Finish();
        ASSERT_EQ(stats.Files().size(), 1);
        const auto& remote = stats.Files().front().file_name;
        EXPECT_EQ(context.chunkManagerPtr->Size(remote), bytes.size());
        std::array<uint8_t, 3> loaded;
        context.chunkManagerPtr->Read(remote, loaded.data(), loaded.size());
        EXPECT_EQ(loaded, bytes);
        sink.ReleaseLocalStaging();
    }
}

TEST(VectorDiskFileManagerTest, MetadataConflictKeepsOutputFailed) {
    for (bool before_close : {true, false}) {
        SCOPED_TRACE(before_close);
        auto owner = storage::LocalDirectory::CreateOwned(
            "/tmp", "vector_stream_XXXXXX", "stream metadata failure test");
        auto manager = std::make_shared<VectorDiskBuildFileManager>(owner);
        const auto path = owner->Path() + "/engine";
        auto output = manager->OpenOutputStream(path);
        const uint8_t byte = 4;
        output->Write(&byte, 1);
        if (before_close) {
            ASSERT_TRUE(manager->AddFileMeta({path, 1}));
            output->Write(&byte, 1);
            EXPECT_THROW(output->Close(), SegcoreError);
            EXPECT_THROW(manager->LocalFiles(), SegcoreError);
        } else {
            output->Close();
            EXPECT_FALSE(manager->AddFileMeta({path, 2}));
            EXPECT_TRUE(manager->AddFileMeta({path, 1}));
        }
        try {
            manager->RethrowFirstFailure();
            FAIL() << "expected metadata failure";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), FileWriteFailed);
        }
    }
}

TEST(VectorDiskFileManagerTest, MetadataCannotConvertLegacyFilesToRaw) {
    auto owner = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_stream_XXXXXX", "stream metadata transport test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(owner);
    const auto path = owner->Path() + "/engine";
    WriteRaw<float>(path, {1.0f}, 1, 1);
    ASSERT_TRUE(manager->AddFile(path));
    EXPECT_FALSE(
        manager->AddFileMeta({path, std::filesystem::file_size(path)}));
    EXPECT_EQ(manager->LocalFiles().front().transport,
              VectorDiskFileTransport::LegacySliced);
}

TEST(VectorDiskFamilyTest, SerializationRequiresMemoryOrFileOutput) {
    for (int outputs = 0; outputs < 4; ++outputs) {
        SCOPED_TRACE(outputs);
        auto owner = storage::LocalDirectory::CreateOwned(
            "/tmp", "vector_serialize_XXXXXX", "serialization output test");
        auto manager = std::make_shared<VectorDiskBuildFileManager>(owner);
        auto engine = std::make_unique<KnowhereEngine>(
            DataType::VECTOR_FLOAT,
            DataType::NONE,
            knowhere::IndexEnum::INDEX_FAISS_IDMAP,
            "L2",
            knowhere::Version::GetCurrentVersion().VersionNumber());
        int calls = 0;
        engine->native_index =
            knowhere::Index<SerializationTestNode>::Create([&] {
                ++calls;
                for (bool memory : {false, true}) {
                    if ((outputs & (memory ? 1 : 2)) == 0) {
                        continue;
                    }
                    auto stream = manager->OpenOutputStream(
                        owner->Path() + (memory ? "/memory" : "/disk"),
                        {.use_memory = memory});
                    stream->Write("payload", 7);
                    stream->Close();
                }
                return knowhere::Status::success;
            });
        VectorDiskArtifact artifact(std::move(engine), manager);
        SerializationTestSink sink;
        if (outputs == 0) {
            try {
                artifact.Serialize(sink);
                FAIL() << "empty output was accepted";
            } catch (const SegcoreError& error) {
                EXPECT_NE(std::string(error.what()).find("no completed output"),
                          std::string::npos);
            }
            EXPECT_EQ(sink.buffers + sink.files, 0);
        } else {
            ASSERT_NO_THROW(artifact.Serialize(sink));
            EXPECT_EQ(sink.buffers, outputs & 1 ? 1 : 0);
            EXPECT_EQ(sink.files, outputs & 2 ? 1 : 0);
        }
        EXPECT_EQ(calls, 1);
        EXPECT_TRUE(manager->TakeOutput().empty());
        if (outputs == 1) {
            SerializationTestSink failed;
            failed.fail_write = true;
            EXPECT_THROW(artifact.Serialize(failed), std::runtime_error);
            EXPECT_TRUE(manager->TakeOutput().empty());
            SerializationTestSink retry;
            ASSERT_NO_THROW(artifact.Serialize(retry));
            EXPECT_EQ(retry.buffers, 1);
            EXPECT_EQ(calls, 3);
        }
    }
}

TEST(VectorDiskCacheTest, ColdMetadataAndCancelledCountDoNotOpenEngine) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_cold_XXXXXX", "vector disk cold test");
    const auto raw_path = files->Path() + "/raw";
    const auto values = MakeRows<float>();
    WriteRaw(raw_path, values, kRows, kDim);
    folly::CancellationSource cancellation;
    cancellation.requestCancellation();
    OpContext context(cancellation.getToken());
    OpenedDiskIndex<float> opened;
    BuildDisk(raw_path,
              files->Path(),
              opened,
              {.defer_load = true, .reader_context = &context});
    ASSERT_NE(opened.base, nullptr);
    const auto* vector = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(vector, nullptr);
    EXPECT_EQ(vector->Metric(), "L2");
    EXPECT_EQ(vector->KnowhereIndexType(), knowhere::IndexEnum::INDEX_DISKANN);
    try {
        (void)opened.base->Count();
        FAIL() << "cancelled cold disk load unexpectedly succeeded";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FollyCancel);
    }
}

// Scope the lazy feature bit on real DiskANN to exercise the loader policy
// even when a native lazy backend is not linked. Backend algorithms are unchanged.
TEST(VectorDiskCacheTest, NativeLazyEngineUsesAdmissionAndResidentAccounting) {
    auto& features = const_cast<std::map<std::string, uint64_t>&>(
        knowhere::IndexFactory::GetIndexFeatures());
    auto& disk_features = features.at(knowhere::IndexEnum::INDEX_DISKANN);
    const auto original_features = disk_features;
    auto restore_features =
        folly::makeGuard([&] { disk_features = original_features; });
    disk_features |= knowhere::feature::LAZY_LOAD;
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_native_lazy_XXXXXX", "native lazy budget test");
    const auto raw_path = files->Path() + "/raw";
    WriteRaw(raw_path, MakeRows<float>(), kRows, kDim);
    auto& resident = cachinglayer::monitor::cache_loaded_bytes(
        cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::MEMORY);
    auto& loading = cachinglayer::monitor::cache_loading_bytes(
        cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::MEMORY);
    auto& resident_disk = cachinglayer::monitor::cache_loaded_bytes(
        cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::DISK);
    auto& loading_disk = cachinglayer::monitor::cache_loading_bytes(
        cachinglayer::CellDataType::OTHER, cachinglayer::StorageType::DISK);
    const auto resident_disk_before = resident_disk.Value();
    const auto loading_disk_before = loading_disk.Value();
    const auto resident_before = resident.Value();
    const auto loading_before = loading.Value();
    IndexCacheConfig config{
        files->Path() + "/native_engine",
        cachinglayer::Meta{cachinglayer::StorageType::DISK,
                           cachinglayer::CellIdMappingMode::IDENTICAL,
                           cachinglayer::CellDataType::OTHER,
                           CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                           true},
        {1024 * 1024, 1024 * 1024},
        {4096, 4096},
        4096,
        0,
        {8192, 0}};
    OpContext ctx;
    OpenedDiskIndex<float> opened;
    BuildDisk(
        raw_path,
        files->Path(),
        opened,
        {.defer_load = true, .reader_context = &ctx, .cache_config = config});
    ASSERT_NE(opened.base, nullptr);
    const auto* vector = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(vector, nullptr);
    folly::CancellationSource cancelled;
    cancelled.requestCancellation();
    ctx.cancellation_token = cancelled.getToken();
    EXPECT_THROW(opened.base->Prefetch(), SegcoreError);
    EXPECT_EQ(resident.Value(), resident_before);
    EXPECT_EQ(resident_disk.Value(), resident_disk_before);
    EXPECT_EQ(loading.Value(), loading_before);
    EXPECT_EQ(loading_disk.Value(), loading_disk_before);
    ctx.cancellation_token = {};
    opened.base->Prefetch();
    EXPECT_EQ(opened.base->Count(), kRows);
    EXPECT_EQ(resident.Value(),
              resident_before + config.loaded_usage.memory_bytes);
    EXPECT_EQ(resident_disk.Value(),
              resident_disk_before + config.loaded_usage.file_bytes);
    EXPECT_EQ(loading.Value(), loading_before);
    EXPECT_EQ(loading_disk.Value(), loading_disk_before);
    opened.base->Prefetch();
    opened.base.reset();
    EXPECT_EQ(resident.Value(), resident_before);
    EXPECT_EQ(resident_disk.Value(), resident_disk_before);
    EXPECT_EQ(loading.Value(), loading_before);
    EXPECT_EQ(loading_disk.Value(), loading_disk_before);

    // A request larger than the test cache budget must fail admission.
    config.loaded_usage.memory_bytes = int64_t{1} << 40;
    BuildDisk(raw_path,
              files->Path(),
              opened,
              {.defer_load = true, .cache_config = config});
    vector = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(vector, nullptr);
    try {
        opened.base->Prefetch();
        FAIL() << "native lazy load bypassed resource admission";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), InsufficientResource);
    }
    opened.base.reset();
    EXPECT_EQ(resident.Value(), resident_before);
    EXPECT_EQ(resident_disk.Value(), resident_disk_before);
    EXPECT_EQ(loading.Value(), loading_before);
    EXPECT_EQ(loading_disk.Value(), loading_disk_before);
}

TEST(VectorDiskFamilyTest, CachedFloatBuildLoadSearchAndRawCapability) {
    RunOrdinaryDiskSearch<float>(false, false, true);
}

TEST(VectorDiskFamilyTest, FloatBuildLoadSearchAndRawCapability) {
    RunOrdinaryDiskSearch<float>(false);
}

TEST(VectorDiskFamilyTest, ResidentInputBuildLoadSearch) {
    RunOrdinaryDiskSearch<float>(false, true);
}

TEST(VectorDiskFamilyTest, PreparedInputFollowsBuilderOptions) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_input_XXXXXX", "vector disk input test");
    auto adapted = AdaptIndexType({
        .index_type = knowhere::IndexEnum::INDEX_DISKANN,
        .field_type = DataType::VECTOR_FLOAT,
        .element_type = DataType::NONE,
        .index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber(),
        .params = {{METRIC_TYPE, "L2"},
                   {DIM_KEY, kDim},
                   {DISK_ANN_MAX_DEGREE, "24"},
                   {DISK_ANN_SEARCH_LIST_SIZE, "56"},
                   {DISK_ANN_PQ_CODE_BUDGET, "0.001"},
                   {DISK_ANN_BUILD_DRAM_BUDGET, "2"},
                   {DISK_ANN_BUILD_THREAD_NUM, "2"}},
    });
    adapted.params["local_dir"] = files->Path();
    for (bool direct : {false, true}) {
        indexbuilder::VectorDiskBuildMaterializer materializer(
            files->Path(),
            DataType::VECTOR_FLOAT,
            DataType::VECTOR_FLOAT,
            kDim,
            false,
            false,
            kRows,
            adapted.family,
            adapted.params);
        const auto values = MakeRows<float>();
        const bool use_memory = materializer.InputSpec().use_memory;
        if (direct && use_memory) {
            std::memcpy(materializer.AllocateInput(),
                        values.data(),
                        values.size() * sizeof(float));
            materializer.FinishInput(kRows);
        } else {
            auto batch = storage::CreateFieldData(
                DataType::VECTOR_FLOAT, DataType::NONE, false, kDim);
            batch->FillFieldData(values.data(), kRows);
            materializer.Add(batch);
        }
        materializer.FinishPrimary();
        auto artifact = std::move(materializer).Build();
        ASSERT_NE(artifact, nullptr);
    }
}

TEST(VectorDiskFamilyTest, Float16BuildLoadSearch) {
    RunOrdinaryDiskSearch<float16>(false);
}

TEST(VectorDiskFamilyTest, BFloat16BuildLoadSearch) {
    RunOrdinaryDiskSearch<bfloat16>(false);
}

TEST(VectorDiskFamilyTest, MmapCapabilityAndSearch) {
#ifdef __APPLE__
    GTEST_SKIP() << "faiss mapped I/O is unavailable on macOS";
#endif
    const bool native_mmap =
        KnowhereMmapSupported(knowhere::IndexEnum::INDEX_DISKANN);
    RunOrdinaryDiskSearch<float>(native_mmap);
}

TEST(VectorDiskFamilyTest, RejectsQueryListSmallerThanTopK) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_invalid_XXXXXX", "vector disk test");
    const auto raw_path = files->Path() + "/raw";
    auto values = MakeRows<float>();
    WriteRaw(raw_path, values, kRows, kDim);
    OpenedDiskIndex<float> opened;
    BuildDisk(raw_path, files->Path(), opened);
    ASSERT_NE(opened.base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 2}},
        .metric_type_ = "L2",
        .topk_ = 4,
    };
    SearchResult result;
    EXPECT_ANY_THROW(reader->Search(
        GenDataset(1, kDim, values.data()), params, BitsetView{}, result));
}

TEST(VectorDiskFamilyTest, EmbeddingListsPreserveTrailingEmptyParents) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_array_XXXXXX", "vector disk array test");
    constexpr int64_t parents = 100;
    std::vector<size_t> offsets{0};
    for (int64_t row = 0; row < parents; ++row) {
        offsets.push_back(offsets.back() +
                          (row >= parents - 2 ? 0 : (row % 5) + 1));
    }
    std::vector<float> values(offsets.back() * kDim);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<float>((i * 47) % 997) / 997.0F;
    }
    const auto raw_path = files->Path() + "/raw";
    const auto offsets_path = files->Path() + "/offsets";
    WriteRaw(raw_path, values, offsets.back(), kDim);
    WriteOffsets(offsets_path, offsets);
    OpenedDiskIndex<float> opened;
    BuildDisk(raw_path,
              files->Path(),
              opened,
              {.metric = knowhere::metric::MAX_SIM_L2,
               .logical_rows = parents,
               .offsets_path = offsets_path});
    ASSERT_NE(opened.base, nullptr);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(opened.base->Count(), offsets.back());
    const std::array<int64_t, 2> empty_ids{parents - 2, parents - 1};
    const auto [bytes, got_offsets] = reader->GetEmbListByIds(
        GenIdsDataset(empty_ids.size(), empty_ids.data()), reader->Metric());
    EXPECT_TRUE(bytes.empty());
    EXPECT_EQ(got_offsets, (std::vector<size_t>{0, 0, 0}));

    std::array<size_t, 3> query_offsets{0, 3, 5};
    auto query = GenDataset(5, kDim, values.data());
    query->Set(knowhere::meta::EMB_LIST_OFFSET,
               static_cast<const size_t*>(query_offsets.data()));
    query->Set(knowhere::meta::EMB_LIST_COUNT, int64_t{2});
    query->Set(knowhere::meta::NQ, int64_t{2});
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 8}},
        .metric_type_ = knowhere::metric::MAX_SIM_L2,
        .topk_ = 4,
    };
    SearchResult result;
    reader->Search(query, params, BitsetView{}, result);
    EXPECT_EQ(result.total_nq_, 2);
    EXPECT_EQ(result.unity_topK_, 4);
    EXPECT_EQ(result.seg_offsets_.size(), 8);
    EXPECT_EQ(result.distances_.size(), 8);
}

std::vector<uint8_t>
EncodeValidity(const std::vector<bool>& valid) {
    const uint64_t wire_count = valid.size();
    std::vector<uint8_t> bytes(sizeof(wire_count) + (valid.size() + 7) / 8, 0);
    std::memcpy(bytes.data(), &wire_count, sizeof(wire_count));
    for (size_t i = 0; i < valid.size(); ++i) {
        if (valid[i]) {
            bytes[sizeof(wire_count) + i / 8] |=
                static_cast<uint8_t>(1U << (i % 8));
        }
    }
    return bytes;
}

std::vector<float>
RandomVectors(int64_t count) {
    std::default_random_engine rng(42);
    std::normal_distribution<float> dist(0.0F, 1.0F);
    std::vector<float> values(count * kDim);
    for (auto& value : values) {
        value = dist(rng);
    }
    return values;
}

// An element-level index of a nullable VECTOR_ARRAY built before element-level
// builds dropped embedding offsets persists the per-list validity of its
// input, while the index numbers flattened elements. Loading it restores
// element validity from the vector count, so it needs no rebuild (#54899).
void
RunNullableElementLevelDiskSearch(bool more_rows_than_vectors,
                                  bool mmap_id_map) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_nullable_XXXXXX", "vector disk nullable test");
    // Both shapes need enough vectors for the disk index's PQ training.
    constexpr int64_t parents = kRows * 2;
    std::vector<bool> row_valid(parents, false);
    int64_t element_count = 0;
    for (int64_t row = 0; row < parents; ++row) {
        if (row % 5 == 1) {
            continue;  // Null lists contribute no vectors.
        }
        row_valid[row] = true;
        element_count += more_rows_than_vectors
                             ? (row % 5 == 0 ? 1 : 0)
                             : (row % 5 == 3 ? 0 : row % 4 + 1);
    }
    ASSERT_EQ(more_rows_than_vectors, parents > element_count);
    const auto values = RandomVectors(element_count);
    const auto row_validity = EncodeValidity(row_valid);

    const auto raw_path = files->Path() + "/raw";
    WriteRaw(raw_path, values, element_count, kDim);
    OpenedDiskIndex<float> opened;
    BuildDisk(
        raw_path,
        files->Path(),
        opened,
        {.metric = knowhere::metric::COSINE,
         .logical_rows = parents,
         .vector_array = true,
         .mmap = KnowhereMmapSupported(knowhere::IndexEnum::INDEX_DISKANN),
         .mmap_id_map = mmap_id_map,
         .legacy_validity = row_validity});
    ASSERT_NE(opened.base, nullptr);
    EXPECT_EQ(opened.persisted_validity, row_validity);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(opened.base->Count(), element_count);
    EXPECT_EQ(reader->ValidCount(), element_count);

    TargetBitmap filtered(element_count, false);
    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 128}},
        .metric_type_ = knowhere::metric::COSINE,
        .topk_ = 1,
    };
    SearchResult unfiltered_result;
    reader->Search(GenDataset(1, kDim, values.data()),
                   params,
                   BitsetView(filtered),
                   unfiltered_result);
    ASSERT_EQ(unfiltered_result.seg_offsets_.size(), 1);
    EXPECT_GE(unfiltered_result.seg_offsets_[0], 0);
    EXPECT_LT(unfiltered_result.seg_offsets_[0], element_count);
    // A nonempty filter also has to address the flattened element ids.
    filtered.set();
    const auto allowed = element_count - 1;
    filtered.reset(allowed);
    SearchResult result;
    reader->Search(GenDataset(1, kDim, values.data() + allowed * kDim),
                   params,
                   BitsetView(filtered),
                   result);
    ASSERT_EQ(result.seg_offsets_.size(), 1);
    EXPECT_EQ(result.seg_offsets_[0], allowed);
}

TEST(VectorDiskFamilyTest, NullableElementLevelLoadsRowValidity) {
    RunNullableElementLevelDiskSearch(false, false);
    RunNullableElementLevelDiskSearch(true, false);
}

TEST(VectorDiskFamilyTest, MmapNullableElementLevelLoadsRowValidity) {
    RunNullableElementLevelDiskSearch(false, true);
    RunNullableElementLevelDiskSearch(true, true);
}

// An element-nullable VECTOR_ARRAY persists validity over its logical
// elements. The loader keeps it, so ids and filters are logical element ids.
void
RunElementNullableDiskSearch(bool mmap_id_map) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_disk_element_null_XXXXXX", "vector disk element test");
    constexpr int64_t parents = kRows * 2;
    std::vector<bool> element_valid;
    for (int64_t row = 0; row < parents; ++row) {
        if (row % 5 == 1) {
            continue;  // A null row takes no element id.
        }
        for (int64_t j = 0; j < row % 4 + 1; ++j) {
            element_valid.push_back(element_valid.size() % 7 != 3);
        }
    }
    const auto logical = static_cast<int64_t>(element_valid.size());
    std::vector<int64_t> logical_ids;  // Logical id of each stored vector.
    for (int64_t id = 0; id < logical; ++id) {
        if (element_valid[id]) {
            logical_ids.push_back(id);
        }
    }
    const auto physical = static_cast<int64_t>(logical_ids.size());
    ASSERT_LT(physical, logical);
    const auto values = RandomVectors(physical);
    const auto element_validity = EncodeValidity(element_valid);

    const auto raw_path = files->Path() + "/raw";
    const auto validity_path = files->Path() + "/input_" + VALID_DATA_KEY;
    WriteRaw(raw_path, values, physical, kDim);
    {
        std::ofstream stream(validity_path, std::ios::binary | std::ios::trunc);
        stream.write(reinterpret_cast<const char*>(element_validity.data()),
                     static_cast<std::streamsize>(element_validity.size()));
        ASSERT_TRUE(stream.good());
    }
    OpenedDiskIndex<float> opened;
    BuildDisk(
        raw_path,
        files->Path(),
        opened,
        {.metric = knowhere::metric::COSINE,
         .logical_rows = parents,
         .vector_array = true,
         .mmap = KnowhereMmapSupported(knowhere::IndexEnum::INDEX_DISKANN),
         .validity_path = validity_path,
         .element_nullable = true,
         .mmap_id_map = mmap_id_map});
    ASSERT_NE(opened.base, nullptr);
    EXPECT_EQ(opened.persisted_validity, element_validity);
    const auto* reader = dynamic_cast<const IVectorReader*>(opened.base.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(opened.base->Count(), physical);
    EXPECT_TRUE(reader->HasValidData());
    EXPECT_EQ(reader->ValidCount(), physical);
    for (int64_t id = 0; id < logical; ++id) {
        EXPECT_EQ(reader->IsRowValid(id), element_valid[id]) << id;
    }

    VectorSearchParams params{
        .search_params_ = {{DISK_ANN_QUERY_LIST, 128}},
        .metric_type_ = knowhere::metric::COSINE,
        .topk_ = 1,
    };
    const auto last = physical - 1;
    SearchResult result;
    reader->Search(GenDataset(1, kDim, values.data() + last * kDim),
                   params,
                   BitsetView{},
                   result);
    ASSERT_EQ(result.seg_offsets_.size(), 1);
    EXPECT_EQ(result.seg_offsets_[0], logical_ids[last]);
    // The filter addresses logical element ids as well.
    TargetBitmap filtered(logical, true);
    const auto allowed = logical_ids[last - 1];
    filtered.reset(allowed);
    SearchResult filtered_result;
    reader->Search(GenDataset(1, kDim, values.data() + (last - 1) * kDim),
                   params,
                   BitsetView(filtered),
                   filtered_result);
    ASSERT_EQ(filtered_result.seg_offsets_.size(), 1);
    EXPECT_EQ(filtered_result.seg_offsets_[0], allowed);
}

TEST(VectorDiskFamilyTest, ElementNullableElementLevelKeepsElementValidity) {
    RunElementNullableDiskSearch(false);
    RunElementNullableDiskSearch(true);
}

}  // namespace
}  // namespace milvus::index::test

#endif  // BUILD_DISK_ANN
