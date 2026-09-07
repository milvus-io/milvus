// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <boost/core/enable_if.hpp>
#include <boost/filesystem/directory.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <stddef.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <thread>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "RTreeIndex.h"
#include "Utils.h"
#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Geometry.h"
#include "common/GeometryCache.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "expr/ITypeExpr.h"
#include "geos_c.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Utils.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "storage/ChunkManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/TmpPath.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

// Helper: create simple POINT(x,y) WKB (little-endian)
static std::string
CreatePointWKB(double x, double y) {
    std::vector<uint8_t> wkb;
    // Byte order – little endian (1)
    wkb.push_back(0x01);
    // Geometry type – Point (1) – 32-bit little endian
    uint32_t geom_type = 1;
    uint8_t* type_bytes = reinterpret_cast<uint8_t*>(&geom_type);
    wkb.insert(wkb.end(), type_bytes, type_bytes + sizeof(uint32_t));
    // X coordinate
    uint8_t* x_bytes = reinterpret_cast<uint8_t*>(&x);
    wkb.insert(wkb.end(), x_bytes, x_bytes + sizeof(double));
    // Y coordinate
    uint8_t* y_bytes = reinterpret_cast<uint8_t*>(&y);
    wkb.insert(wkb.end(), y_bytes, y_bytes + sizeof(double));
    return std::string(reinterpret_cast<const char*>(wkb.data()), wkb.size());
}

// Helper: create simple WKB from WKT
static std::string
CreateWkbFromWkt(const std::string& wkt) {
    auto ctx = GEOS_init_r();
    auto wkb = milvus::Geometry(ctx, wkt.c_str()).to_wkb_string();
    GEOS_finish_r(ctx);
    return wkb;
}

// The returned Geometry keeps a pointer to the context it was built on and
// dereferences it on every copy and in its destructor, so that context must
// OUTLIVE the returned value. Building on a local context and finishing it
// here would hand back a Geometry whose ctx_ is already freed -- a
// use-after-free on the first copy or destruction, not at the call itself.
// The thread-local context lives until the thread exits, which is what makes
// returning by value safe.
static milvus::Geometry
CreateGeometryFromWkt(const std::string& wkt) {
    return milvus::Geometry(milvus::GetThreadLocalGEOSContext(), wkt.c_str());
}

struct FileSliceSizeGuard {
    explicit FileSliceSizeGuard(int64_t slice_size)
        : old_slice_size_(milvus::FILE_SLICE_SIZE.load()) {
        milvus::FILE_SLICE_SIZE.store(slice_size);
    }

    ~FileSliceSizeGuard() {
        milvus::FILE_SLICE_SIZE.store(old_slice_size_);
    }

    int64_t old_slice_size_;
};

// Helper: write an InsertData parquet file to "remote" storage managed by chunk_manager_
static std::string
WriteGeometryInsertFile(const milvus::storage::ChunkManagerPtr& cm,
                        const milvus::storage::FieldDataMeta& field_meta,
                        const std::string& remote_path,
                        const std::vector<std::string>& wkbs,
                        bool nullable = false,
                        const uint8_t* valid_bitmap = nullptr) {
    auto field_data =
        milvus::storage::CreateFieldData(milvus::storage::DataType::GEOMETRY,
                                         milvus::storage::DataType::NONE,
                                         nullable);
    if (nullable && valid_bitmap != nullptr) {
        field_data->FillFieldData(wkbs.data(), valid_bitmap, wkbs.size(), 0);
    } else {
        field_data->FillFieldData(wkbs.data(), wkbs.size());
    }
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    milvus::storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto bytes = insert_data.Serialize(milvus::storage::StorageType::Remote);
    std::vector<uint8_t> buf(bytes.begin(), bytes.end());
    cm->Write(remote_path, buf.data(), buf.size());
    return remote_path;
}

class RTreeIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        temp_path_ = milvus::test::TmpPath{};
        // create storage config that writes to temp dir
        storage_config_.storage_type = "local";
        storage_config_.root_path = temp_path_.get().string();
        chunk_manager_ = milvus::storage::CreateChunkManager(storage_config_);

        fs_ = milvus::storage::InitArrowFileSystem(storage_config_);

        // prepare field & index meta – minimal info for DiskFileManagerImpl
        field_meta_ = milvus::storage::FieldDataMeta{1, 1, 1, 100};
        // set geometry data type in field schema for index schema checks
        field_meta_.field_schema.set_data_type(
            ::milvus::proto::schema::DataType::Geometry);
        index_meta_ = milvus::storage::IndexMeta{1, 100, 1, 1};
    }

    void
    TearDown() override {
        // Clean up chunk manager files and index directories
        try {
            // Remove all files in the storage root path
            if (chunk_manager_) {
                auto root_path = storage_config_.root_path;
                if (boost::filesystem::exists(root_path)) {
                    for (auto& entry :
                         boost::filesystem::directory_iterator(root_path)) {
                        if (boost::filesystem::is_regular_file(entry)) {
                            boost::filesystem::remove(entry);
                        } else if (boost::filesystem::is_directory(entry)) {
                            boost::filesystem::remove_all(entry);
                        }
                    }
                }
            }
            // TmpPath cleanup handles the test directory
        } catch (const std::exception& e) {
            // Log error but don't fail the test
            std::cout << "Warning: Failed to clean up test files: " << e.what()
                      << std::endl;
        }
        // TmpPath destructor will also remove the temp directory
    }

    // Helper method to clean up index files
    void
    CleanupIndexFiles(const std::vector<std::string>& index_files,
                      const std::string& test_name = "") {
        try {
            for (const auto& file : index_files) {
                if (chunk_manager_->Exist(file)) {
                    chunk_manager_->Remove(file);
                }
            }
        } catch (const std::exception& e) {
            std::cout << "Warning: Failed to clean up " << test_name
                      << " index files: " << e.what() << std::endl;
        }
    }

    // A sealed segment carrying a LEGACY SHORT R-Tree index. Layout of the N
    // rows: every row is POINT(0 0) except row N-1, which is a genuine NULL.
    // The persisted index has the exact legacy shape: valid row 1 is absent
    // from the tree (as if GEOSWKBReader_read_r transiently returned nullptr),
    // later valid rows retain absolute offsets 2..N-2, and row N-1 is NULL.
    // AddGeometry records the absolute null offset N-1, but after reload
    // Count() is reconstructed as (N-2) tree entries + one NULL = N-1. The
    // missing valid row is therefore an INTERIOR hole below Count(), not the
    // suffix bit that a tail-only resize() would add.
    struct LegacyShortIndexSegment {
        std::unique_ptr<milvus::segcore::SegmentSealed> sealed;
        milvus::FieldId pk_id;
        milvus::FieldId geo_id;
        std::vector<std::string> index_files;
    };

    LegacyShortIndexSegment
    MakeLegacyShortIndexSealed(int N,
                               int64_t field_id_for_meta,
                               const std::string& cache_key) {
        using namespace milvus;
        using namespace milvus::segcore;
        LegacyShortIndexSegment out;

        auto schema = std::make_shared<Schema>();
        out.pk_id = schema->AddDebugField("id", DataType::INT64);
        schema->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        out.geo_id = schema->AddDebugField(
            "geo", DataType::GEOMETRY, true /* nullable */);
        schema->set_primary_field_id(out.pk_id);

        auto full_ds = DataGen(schema, N);
        out.sealed = CreateSealedWithFieldDataLoaded(
            schema, full_ds, false, {out.geo_id.get()});

        std::vector<std::string> segment_wkbs(N,
                                              CreateWkbFromWkt("POINT(0 0)"));
        segment_wkbs[N - 1].clear();  // payload for the genuine NULL tail row
        std::vector<uint8_t> valid_bitmap((N + 7) / 8, 0);
        for (int i = 0; i < N - 1; ++i) {
            valid_bitmap[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
        }
        auto geo_field_data = milvus::storage::CreateFieldData(
            milvus::storage::DataType::GEOMETRY,
            milvus::storage::DataType::NONE,
            true);
        geo_field_data->FillFieldData(
            segment_wkbs.data(), valid_bitmap.data(), segment_wkbs.size(), 0);
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(
            1, 1, 1, out.geo_id.get(), {geo_field_data}, cm);
        out.sealed->LoadFieldData(load_info);

        milvus::storage::FieldDataMeta short_field_meta{
            1, 1, 1, field_id_for_meta};
        short_field_meta.field_schema.set_data_type(
            ::milvus::proto::schema::DataType::Geometry);
        short_field_meta.field_schema.set_nullable(true);
        milvus::storage::IndexMeta short_index_meta{1, field_id_for_meta, 1, 1};
        milvus::storage::FileManagerContext fm_ctx(
            short_field_meta, short_index_meta, chunk_manager_, fs_);
        auto rtree_index =
            std::make_unique<milvus::index::RTreeIndex<std::string>>(fm_ctx);
        rtree_index->InitForBuildIndex(false);
        rtree_index->AddGeometry(segment_wkbs[0], 0, true);
        for (int i = 2; i < N - 1; ++i) {
            rtree_index->AddGeometry(segment_wkbs[i], i, true);
        }
        rtree_index->AddGeometry(segment_wkbs[N - 1], N - 1, false);
        EXPECT_EQ(rtree_index->Count(), N);
        auto stats = rtree_index->UploadUnified({});
        out.index_files = stats->GetIndexFiles();

        milvus::segcore::LoadIndexInfo info{};
        info.collection_id = 1;
        info.partition_id = 1;
        info.segment_id = 1;
        info.field_id = out.geo_id.get();
        info.field_type = DataType::GEOMETRY;
        info.index_id = 1;
        info.index_build_id = 1;
        info.index_version = 1;
        info.schema.set_data_type(proto::schema::DataType::Geometry);
        info.schema.set_nullable(true);
        info.index_params["index_type"] = milvus::index::RTREE_INDEX_TYPE;
        nlohmann::json cfg_load;
        cfg_load["index_files"] = out.index_files;
        rtree_index->LoadUnified(cfg_load);
        // The reload is what makes the index SHORT.
        EXPECT_EQ(rtree_index->Count(), N - 1);
        auto legacy_validity = rtree_index->IsNotNull();
        EXPECT_EQ(legacy_validity.size(), static_cast<size_t>(N - 1));
        EXPECT_TRUE(legacy_validity[N - 2]);
        auto full_validity = rtree_index->IsNotNull(N);
        EXPECT_FALSE(full_validity[N - 1]);
        info.cache_index =
            CreateTestCacheIndex(cache_key, std::move(rtree_index));
        out.sealed->LoadIndex(info);
        return out;
    }

    milvus::storage::StorageConfig storage_config_;
    milvus::storage::ChunkManagerPtr chunk_manager_;
    milvus::storage::FieldDataMeta field_meta_;
    milvus::storage::IndexMeta index_meta_;
    milvus::test::TmpPath temp_path_;
    milvus_storage::ArrowFileSystemPtr fs_;
};

class TestableRTreeIndex : public milvus::index::RTreeIndex<std::string> {
 public:
    using milvus::index::RTreeIndex<std::string>::RTreeIndex;

    void
    ThrowOnNextQueryForTesting() {
        ASSERT_NE(wrapper_, nullptr);
        wrapper_->SetThrowOnQueryForTesting(true);
    }
};

TEST_F(RTreeIndexTest, Build_Upload_Load) {
    // ---------- Build via BuildWithRawDataForUT ----------
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree_build(ctx_build);

    std::vector<std::string> wkbs = {CreatePointWKB(1.0, 1.0),
                                     CreatePointWKB(2.0, 2.0)};
    rtree_build.BuildWithRawDataForUT(wkbs.size(), wkbs.data());

    ASSERT_EQ(rtree_build.Count(), 2);

    // ---------- Upload ----------
    auto stats = rtree_build.UploadUnified({});
    ASSERT_NE(stats, nullptr);
    ASSERT_GT(stats->GetIndexFiles().size(), 0);

    // ---------- Load back ----------
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();

    milvus::tracer::TraceContext trace_ctx;  // empty context
    rtree_load.LoadUnified(cfg);

    ASSERT_EQ(rtree_load.Count(), 2);
}

TEST_F(RTreeIndexTest, Load_WithFileNamesOnly) {
    // Build & upload first
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree_build(ctx_build);

    std::vector<std::string> wkbs2 = {CreatePointWKB(10.0, 10.0),
                                      CreatePointWKB(20.0, 20.0)};
    rtree_build.BuildWithRawDataForUT(wkbs2.size(), wkbs2.data());

    auto stats = rtree_build.UploadUnified({});

    // gather only filenames (strip parent path)
    std::vector<std::string> filenames;
    for (const auto& path : stats->GetIndexFiles()) {
        filenames.emplace_back(
            boost::filesystem::path(path).filename().string());
        // V3 mode: files are stored via ArrowFileSystem (fs_),
        // so chunk_manager won't find them.
    }

    // Load using filename only list
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = filenames;  // no directory info

    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);

    ASSERT_EQ(rtree_load.Count(), 2);
}

TEST_F(RTreeIndexTest, Build_EmptyInput_ShouldThrow) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    std::vector<std::string> empty;
    EXPECT_THROW(rtree.BuildWithRawDataForUT(0, empty.data()),
                 milvus::SegcoreError);
}

TEST_F(RTreeIndexTest, Build_WithInvalidWKB_Upload_Load) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    std::string bad = CreatePointWKB(0.0, 0.0);
    bad.resize(bad.size() / 2);  // truncate to make invalid

    std::vector<std::string> wkbs = {
        CreateWkbFromWkt("POINT(1 1)"), bad, CreateWkbFromWkt("POINT(2 2)")};
    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data());

    // Upload and then load back to let loader compute count from wrapper
    auto stats = rtree.UploadUnified({});

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);

    // All 3 rows must be present: the row whose WKB fails to parse is indexed
    // with a placeholder MBR rather than dropped. Dropping it would leave the
    // index row count permanently short of the segment row count, which then
    // trips the growing coarse-bitmap bounds check on every subsequent
    // geometry query. The R-tree is only a coarse filter -- exact refinement
    // still filters the placeholder row out of any result.
    ASSERT_EQ(rtree_load.Count(), 3);
}

TEST_F(RTreeIndexTest, Build_VariousGeometries) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    std::vector<std::string> wkbs = {
        CreateWkbFromWkt("POINT(-1.5 2.5)"),
        CreateWkbFromWkt("LINESTRING(0 0,1 1,2 3)"),
        CreateWkbFromWkt("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        CreateWkbFromWkt("POINT(1000000 -1000000)"),
        CreateWkbFromWkt("POINT(0 0)")};

    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data());
    ASSERT_EQ(rtree.Count(), wkbs.size());

    auto stats = rtree.UploadUnified({});
    ASSERT_FALSE(stats->GetIndexFiles().empty());

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);
    ASSERT_EQ(rtree_load.Count(), wkbs.size());
}

TEST_F(RTreeIndexTest, Build_ConfigAndMetaJson) {
    // Prepare one insert file via storage pipeline
    std::vector<std::string> wkbs = {CreateWkbFromWkt("POINT(0 0)"),
                                     CreateWkbFromWkt("POINT(1 1)")};
    auto remote_file = (temp_path_.get() / "geom.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);
    auto stats = rtree.UploadUnified({});

    // V3 mode: verify upload produced a single packed file and can be loaded
    auto index_files = stats->GetIndexFiles();
    ASSERT_EQ(index_files.size(), 1);

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = index_files;
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);
    ASSERT_EQ(rtree_load.Count(), 2);
}

TEST_F(RTreeIndexTest, Load_MixedFileNamesAndPaths) {
    // Build and upload
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);
    std::vector<std::string> wkbs = {CreatePointWKB(6.0, 6.0),
                                     CreatePointWKB(7.0, 7.0)};
    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data());
    auto stats = rtree.UploadUnified({});

    // Use full list, but replace one with filename-only
    auto mixed = stats->GetIndexFiles();
    ASSERT_FALSE(mixed.empty());
    mixed[0] = boost::filesystem::path(mixed[0]).filename().string();

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = mixed;
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);
    ASSERT_EQ(rtree_load.Count(), wkbs.size());
}

TEST_F(RTreeIndexTest, Load_NonexistentRemote_ShouldThrow) {
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    // nonexist file
    nlohmann::json cfg;
    cfg["index_files"] = std::vector<std::string>{
        (temp_path_.get() / "does_not_exist.bgi_0").string()};
    milvus::tracer::TraceContext trace_ctx;
    EXPECT_THROW(rtree_load.LoadUnified(cfg), milvus::SegcoreError);
}

TEST_F(RTreeIndexTest, Build_EndToEnd_FromInsertFiles) {
    // prepare remote file via InsertData serialization
    std::vector<std::string> wkbs = {CreateWkbFromWkt("POINT(0 0)"),
                                     CreateWkbFromWkt("POINT(2 2)")};
    auto remote_file = (temp_path_.get() / "geom3.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);

    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);
    ASSERT_EQ(rtree.Count(), wkbs.size());

    auto stats = rtree.UploadUnified({});

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);
    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);
    ASSERT_EQ(rtree_load.Count(), wkbs.size());
}

TEST_F(RTreeIndexTest, Build_Upload_Load_LargeDataset) {
    // Generate ~10k POINT geometries
    const size_t N = 10000;
    std::vector<std::string> wkbs;
    wkbs.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        // POINT(i i)
        wkbs.emplace_back(CreateWkbFromWkt("POINT(" + std::to_string(i) + " " +
                                           std::to_string(i) + ")"));
    }

    // Write one insert file into remote storage
    auto remote_file = (temp_path_.get() / "geom_large.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);

    // Build from insert_files (not using BuildWithRawDataForUT)
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);

    ASSERT_EQ(rtree.Count(), static_cast<int64_t>(N));

    // Upload index
    auto stats = rtree.UploadUnified({});
    ASSERT_GT(stats->GetIndexFiles().size(), 0);

    // Load index back and verify
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg_load;
    cfg_load["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg_load);

    ASSERT_EQ(rtree_load.Count(), static_cast<int64_t>(N));

    // Clean up large dataset index files to avoid conflicts
    CleanupIndexFiles(stats->GetIndexFiles(), "large dataset");
}

TEST_F(RTreeIndexTest, Build_BulkLoad_Nulls_And_BadWKB) {
    // five geometries:
    // 1. valid
    // 2. valid but will be marked null
    // 3. valid
    // 4. will be truncated to make invalid
    // 5. valid
    std::vector<std::string> wkbs = {
        CreateWkbFromWkt("POINT(0 0)"),  // valid
        CreateWkbFromWkt("POINT(1 1)"),  // valid
        CreateWkbFromWkt("POINT(2 2)"),  // valid
        CreatePointWKB(3.0, 3.0),        // will be truncated to make invalid
        CreateWkbFromWkt("POINT(4 4)")   // valid
    };
    // make bad WKB: truncate the 4th geometry
    wkbs[3].resize(wkbs[3].size() / 2);

    // write to remote storage file (chunk manager's root directory)
    auto remote_file = (temp_path_.get() / "geom_bulk.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);

    // build (default to bulk load)
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);

    // All 5 rows are indexed. The bad-WKB row (index 3) is NOT dropped: like the
    // growing add_geometry path, bulk_load now indexes it with a placeholder MBR
    // so the index row count stays in lockstep with the segment rows, and exact
    // refinement tolerates the placeholder (Geometry::TryParseFromWkb -> skip) in
    // every configuration. See PR #50951 review.
    ASSERT_EQ(rtree.Count(), 5);

    // upload -> load back and verify consistency
    auto stats = rtree.UploadUnified({});
    ASSERT_GT(stats->GetIndexFiles().size(), 0);

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();

    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);
    ASSERT_EQ(rtree_load.Count(), 5);
}

TEST_F(RTreeIndexTest, LoadSlicedNullOffsets) {
    FileSliceSizeGuard slice_size_guard(64);
    field_meta_.field_schema.set_nullable(true);

    constexpr size_t kRows = 24;
    std::vector<std::string> geometries;
    geometries.reserve(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        if (i % 2 == 0) {
            geometries.emplace_back();
            continue;
        }
        geometries.emplace_back(CreateWkbFromWkt(
            "POINT(" + std::to_string(i) + " " + std::to_string(i) + ")"));
    }
    constexpr size_t kNullCount = kRows / 2;

    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    rtree.BuildWithStrings(geometries);
    ASSERT_EQ(rtree.Count(), static_cast<int64_t>(kRows));

    auto stats = rtree.Upload({});
    auto index_files = stats->GetIndexFiles();
    ASSERT_TRUE(std::any_of(
        index_files.begin(), index_files.end(), [](const std::string& file) {
            return boost::filesystem::path(file).filename().string() ==
                   milvus::INDEX_FILE_SLICE_META;
        }));

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = index_files;
    rtree_load.Load(milvus::tracer::TraceContext{}, cfg);

    ASSERT_EQ(rtree_load.Count(), static_cast<int64_t>(kRows));
    EXPECT_EQ(rtree_load.IsNull().count(), kNullCount);
    EXPECT_EQ(rtree_load.IsNotNull().count(), kRows - kNullCount);
}

// The following two tests only test the coarse query (R-Tree) and not the exact query (GDAL)

TEST_F(RTreeIndexTest, Query_CoarseAndExact_Equals_Intersects_Within) {
    // Build a small index in-memory (via UT API)
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    // Prepare simple geometries: two points and a square polygon
    std::vector<std::string> wkbs;
    wkbs.emplace_back(CreateWkbFromWkt("POINT(0 0)"));  // id 0
    wkbs.emplace_back(CreateWkbFromWkt("POINT(2 2)"));  // id 1
    wkbs.emplace_back(
        CreateWkbFromWkt("POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))"));  // id 2 square

    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data(), {});
    ASSERT_EQ(rtree.Count(), 3);

    // Upload and then load into a new index instance for querying
    auto stats = rtree.UploadUnified({});
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);
    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);

    // Helper to run Query
    auto run_query = [&](::milvus::proto::plan::GISFunctionFilterExpr_GISOp op,
                         const std::string& wkt) {
        auto ds = std::make_shared<milvus::Dataset>();
        ds->Set(milvus::index::OPERATOR_TYPE, op);
        ds->Set(milvus::index::MATCH_VALUE, CreateGeometryFromWkt(wkt));
        return rtree_load.Query(ds);
    };

    // Equals with same point should match id 0 only
    {
        auto bm =
            run_query(::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Equals,
                      "POINT(0 0)");
        EXPECT_TRUE(bm[0]);
        EXPECT_FALSE(bm[1]);
        EXPECT_TRUE(
            bm[2]);  //This is true because POINT(0 0) is within the square (0 0, 0 3, 3 3, 3 0, 0 0) and we have not done exact spatial query yet
    }

    // Intersects: square intersects point (on boundary considered intersect)
    {
        auto bm = run_query(
            ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))");
        // square(0..1) intersects POINT(0,0) and POLYGON(0..3)
        // but not POINT(2,2)
        EXPECT_TRUE(bm[0]);   // point (0,0)
        EXPECT_FALSE(bm[1]);  // point (2,2)
        EXPECT_TRUE(bm[2]);   // big polygon
    }

    // Within: point within the big square
    {
        auto bm =
            run_query(::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Within,
                      "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))");
        EXPECT_TRUE(
            bm[0]);  // (0,0) is within or on boundary considered within by GDAL Within?
        // GDAL Within returns true only if strictly inside (no boundary). If boundary excluded, (0,0) may be false.
        // To make assertion robust across GEOS versions, simply check big polygon within itself should be true.
        auto bm_poly =
            run_query(::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Within,
                      "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))");
        EXPECT_TRUE(bm_poly[2]);
    }
}

TEST_F(RTreeIndexTest, Query_Touches_Contains_Crosses_Overlaps) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    // Two overlapping squares and one disjoint square
    std::vector<std::string> wkbs;
    wkbs.emplace_back(
        CreateWkbFromWkt("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"));  // id 0
    wkbs.emplace_back(CreateWkbFromWkt(
        "POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))"));  // id 1 overlaps with 0
    wkbs.emplace_back(CreateWkbFromWkt(
        "POLYGON((4 4, 4 5, 5 5, 5 4, 4 4))"));  // id 2 disjoint

    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data(), {});
    ASSERT_EQ(rtree.Count(), 3);

    // Upload and load a new instance for querying
    auto stats = rtree.UploadUnified({});
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_, fs_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);
    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.LoadUnified(cfg);

    auto run_query = [&](::milvus::proto::plan::GISFunctionFilterExpr_GISOp op,
                         const std::string& wkt) {
        auto ds = std::make_shared<milvus::Dataset>();
        ds->Set(milvus::index::OPERATOR_TYPE, op);
        ds->Set(milvus::index::MATCH_VALUE, CreateGeometryFromWkt(wkt));
        return rtree_load.Query(ds);
    };

    // Overlaps: query polygon overlapping both 0 and 1
    {
        auto bm = run_query(
            ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Overlaps,
            "POLYGON((0.5 0.5, 0.5 2.5, 2.5 2.5, 2.5 0.5, 0.5 0.5))");
        EXPECT_TRUE(bm[0]);
        EXPECT_TRUE(bm[1]);
        EXPECT_FALSE(bm[2]);
    }

    // Contains: big polygon contains small polygon
    {
        auto bm = run_query(
            ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Contains,
            "POLYGON(( -1 -1, -1 4, 4 4, 4 -1, -1 -1))");
        EXPECT_TRUE(bm[0]);
        EXPECT_TRUE(bm[1]);
        EXPECT_TRUE(bm[2]);
    }

    // Touches: polygon that only touches at the corner (2,2) with id1
    {
        auto bm = run_query(
            ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Touches,
            "POLYGON((2 2, 2 3, 3 3, 3 2, 2 2))");
        // This touches id1 at (2,2); depending on GEOS, touches excludes interior intersection
        // The id0 might also touch at (2,2). We only assert at least one touch.
        EXPECT_TRUE(bm[0] || bm[1]);
    }

    // Crosses: a segment crossing the first polygon
    {
        auto bm = run_query(
            ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Crosses,
            "LINESTRING( -1 1, 3 1 )");
        EXPECT_TRUE(bm[0]);
    }
}

TEST_F(RTreeIndexTest, GIS_Index_Exact_Filtering) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    // 1) Create schema: id (INT64, primary), vector, geometry
    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("id", DataType::INT64);
    auto dim = 16;
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->set_primary_field_id(pk_id);

    int N = 200;
    int num_iters = 1;
    auto full_ds = DataGen(schema, N * num_iters);
    auto sealed =
        CreateSealedWithFieldDataLoaded(schema, full_ds, false, {geo_id.get()});

    // Prepare controlled geometry WKBs mirroring the shapes used in growing
    std::vector<std::string> wkbs;
    wkbs.reserve(N * num_iters);
    auto ctx = GEOS_init_r();
    for (int i = 0; i < N * num_iters; ++i) {
        if (i % 4 == 0) {
            wkbs.emplace_back(
                milvus::Geometry(ctx, "POINT(0 0)").to_wkb_string());
        } else if (i % 4 == 1) {
            wkbs.emplace_back(
                milvus::Geometry(ctx, "POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))")
                    .to_wkb_string());
        } else if (i % 4 == 2) {
            wkbs.emplace_back(
                milvus::Geometry(ctx,
                                 "POLYGON((10 10,20 10,20 20,10 20,10 10))")
                    .to_wkb_string());
        } else {
            wkbs.emplace_back(
                milvus::Geometry(ctx, "LINESTRING(-1 0,1 0)").to_wkb_string());
        }
    }

    // Clean up GEOS context immediately after creating WKB data
    GEOS_finish_r(ctx);

    // now load the controlled geometry data into sealed
    auto geo_field_data =
        milvus::storage::CreateFieldData(milvus::storage::DataType::GEOMETRY,
                                         milvus::storage::DataType::NONE,
                                         false);
    geo_field_data->FillFieldData(wkbs.data(), wkbs.size());

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, geo_id.get(), {geo_field_data}, cm);
    sealed->LoadFieldData(load_info);

    // build geometry R-Tree index files and load into sealed
    // Write a single parquet for geometry to simulate build input
    // wkbs already prepared above
    auto remote_file = (temp_path_.get() / "rtree_e2e.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);

    // build index files by invoking RTreeIndex::Build
    milvus::storage::FileManagerContext fm_ctx(
        field_meta_, index_meta_, chunk_manager_, fs_);
    auto rtree_index =
        std::make_unique<milvus::index::RTreeIndex<std::string>>(fm_ctx);
    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};
    build_cfg["index_type"] = milvus::index::RTREE_INDEX_TYPE;

    rtree_index->Build(build_cfg);
    auto stats = rtree_index->UploadUnified({});

    // load geometry index into sealed segment
    milvus::segcore::LoadIndexInfo info{};
    info.collection_id = 1;
    info.partition_id = 1;
    info.segment_id = 1;
    info.field_id = geo_id.get();
    info.field_type = DataType::GEOMETRY;
    info.index_id = 1;
    info.index_build_id = 1;
    info.index_version = 1;
    info.schema = proto::schema::FieldSchema();
    info.schema.set_data_type(proto::schema::DataType::Geometry);
    info.index_params["index_type"] = milvus::index::RTREE_INDEX_TYPE;

    nlohmann::json cfg_load;
    cfg_load["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx_load;
    rtree_index->LoadUnified(cfg_load);

    info.cache_index =
        CreateTestCacheIndex("rtree_index_key", std::move(rtree_index));
    sealed->LoadIndex(info);

    // 3) Build a GIS filter expression and run exact filtering via segcore
    auto test_op = [&](const std::string& wkt,
                       proto::plan::GISFunctionFilterExpr_GISOp op,
                       std::function<bool(int)> expected) {
        auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY), op, wkt);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           gis_expr);
        BitsetType bits =
            ExecuteQueryExpr(plan, sealed.get(), N * num_iters, MAX_TIMESTAMP);
        ASSERT_EQ(bits.size(), N * num_iters);
        for (int i = 0; i < N * num_iters; ++i) {
            EXPECT_EQ(bool(bits[i]), expected(i)) << "i=" << i;
        }
    };

    // exact within: polygon around origin should include indices 0,1,3
    test_op("POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))",
            proto::plan::GISFunctionFilterExpr_GISOp_Within,
            [](int i) { return (i % 4 == 0) || (i % 4 == 1) || (i % 4 == 3); });

    // exact intersects: point (0,0) should intersect point, polygon containing it, and line through it
    test_op("POINT(0 0)",
            proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            [](int i) { return (i % 4 == 0) || (i % 4 == 1) || (i % 4 == 3); });

    // exact equals: only the point equals
    test_op("POINT(0 0)",
            proto::plan::GISFunctionFilterExpr_GISOp_Equals,
            [](int i) { return (i % 4 == 0); });

    // Explicit cleanup for this test to avoid conflicts
    sealed.reset();  // Release the sealed segment first

    // Clean up any remaining index files
    CleanupIndexFiles(stats->GetIndexFiles(), "GIS filtering test");
}

namespace {
// RAII guard so enableGISSplitFusion is always restored to false, even if
// ExecuteQueryExpr throws mid-run (a bare set/restore would leak the global
// flag into later tests).
struct GisSplitFusionFlagGuard {
    explicit GisSplitFusionFlagGuard(bool enable) {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_gis_split_fusion(enable);
    }
    ~GisSplitFusionFlagGuard() {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_gis_split_fusion(false);
    }
};

// RAII guard for the expr batch size, restored on scope exit, so the indexed
// equivalence check runs across MULTIPLE Eval batches (exercising the split
// nodes' per-batch coarse slicing + dual-cursor advance on the indexed path).
struct ExprBatchSizeGuardLocal {
    int64_t saved;
    explicit ExprBatchSizeGuardLocal(int64_t batch_size)
        : saved(milvus::EXEC_EVAL_EXPR_BATCH_SIZE.load()) {
        milvus::EXEC_EVAL_EXPR_BATCH_SIZE.store(batch_size);
    }
    ~ExprBatchSizeGuardLocal() {
        milvus::EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
    }
};
}  // namespace

// Equivalence test for the GIS coarse/refine split + same-column fusion on the
// INDEXED path: with a geometry R-Tree index loaded, the Coarse node's
// RunRTreeQuery is exercised. enableGISSplitFusion ON must match OFF. The check
// is run under a small batch size so the indexed coarse-bitmap slicing crosses
// batch boundaries (indexed x multi-batch coverage).
TEST_F(RTreeIndexTest, GIS_SplitFusion_Equivalence_Indexed) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->set_primary_field_id(pk_id);

    const int N = 200;
    auto full_ds = DataGen(schema, N);
    auto sealed =
        CreateSealedWithFieldDataLoaded(schema, full_ds, false, {geo_id.get()});

    // Controlled geometry data (mirrors GIS_Index_Exact_Filtering).
    std::vector<std::string> wkbs;
    wkbs.reserve(N);
    auto ctx = GEOS_init_r();
    for (int i = 0; i < N; ++i) {
        const char* wkt =
            (i % 4 == 0)   ? "POINT(0 0)"
            : (i % 4 == 1) ? "POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))"
            : (i % 4 == 2) ? "POLYGON((10 10,20 10,20 20,10 20,10 10))"
                           : "LINESTRING(-1 0,1 0)";
        wkbs.emplace_back(milvus::Geometry(ctx, wkt).to_wkb_string());
    }
    GEOS_finish_r(ctx);

    auto geo_field_data =
        milvus::storage::CreateFieldData(milvus::storage::DataType::GEOMETRY,
                                         milvus::storage::DataType::NONE,
                                         false);
    geo_field_data->FillFieldData(wkbs.data(), wkbs.size());
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, geo_id.get(), {geo_field_data}, cm);
    sealed->LoadFieldData(load_info);

    // Build + load the geometry R-Tree index. Use a distinct field/index id so
    // the index build temp dir (derived from collection_partition_segment_field)
    // does not collide with GIS_Index_Exact_Filtering, which reuses the fixture's
    // field_meta_ {1,1,1,100} and leaves that temp dir non-empty.
    milvus::storage::FieldDataMeta fusion_field_meta{1, 1, 1, 200};
    fusion_field_meta.field_schema.set_data_type(
        ::milvus::proto::schema::DataType::Geometry);
    milvus::storage::IndexMeta fusion_index_meta{1, 200, 1, 1};
    auto remote_file = (temp_path_.get() / "rtree_fusion.parquet").string();
    WriteGeometryInsertFile(
        chunk_manager_, fusion_field_meta, remote_file, wkbs);
    milvus::storage::FileManagerContext fm_ctx(
        fusion_field_meta, fusion_index_meta, chunk_manager_, fs_);
    auto rtree_index =
        std::make_unique<milvus::index::RTreeIndex<std::string>>(fm_ctx);
    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};
    build_cfg["index_type"] = milvus::index::RTREE_INDEX_TYPE;
    rtree_index->Build(build_cfg);
    auto stats = rtree_index->UploadUnified({});

    milvus::segcore::LoadIndexInfo info{};
    info.collection_id = 1;
    info.partition_id = 1;
    info.segment_id = 1;
    info.field_id = geo_id.get();
    info.field_type = DataType::GEOMETRY;
    info.index_id = 1;
    info.index_build_id = 1;
    info.index_version = 1;
    info.schema = proto::schema::FieldSchema();
    info.schema.set_data_type(proto::schema::DataType::Geometry);
    info.index_params["index_type"] = milvus::index::RTREE_INDEX_TYPE;
    nlohmann::json cfg_load;
    cfg_load["index_files"] = stats->GetIndexFiles();
    rtree_index->LoadUnified(cfg_load);
    info.cache_index =
        CreateTestCacheIndex("rtree_fusion_key", std::move(rtree_index));
    sealed->LoadIndex(info);

    ASSERT_TRUE(sealed->HasIndex(geo_id));

    // Build conjunction filters that combine a scalar predicate with same-column
    // geometry predicates (so SplitFuseGISConjunct fires and uses the index).
    auto col_geo = milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY);
    auto col_id = milvus::expr::ColumnInfo(pk_id, DataType::INT64);
    proto::plan::GenericValue zero;
    zero.set_int64_val(0);
    milvus::expr::TypedExprPtr scalar =
        std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            col_id, proto::plan::OpType::GreaterEqual, zero);

    auto gis = [&](proto::plan::GISFunctionFilterExpr_GISOp op,
                   const std::string& wkt) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            col_geo, op, wkt);
    };
    auto And = [](milvus::expr::TypedExprPtr a,
                  milvus::expr::TypedExprPtr b) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::And, a, b);
    };
    auto Or = [](milvus::expr::TypedExprPtr a,
                 milvus::expr::TypedExprPtr b) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::Or, a, b);
    };
    const auto kInter = proto::plan::GISFunctionFilterExpr_GISOp_Intersects;
    const auto kWithin = proto::plan::GISFunctionFilterExpr_GISOp_Within;
    const auto kDWithin = proto::plan::GISFunctionFilterExpr_GISOp_DWithin;

    std::vector<milvus::expr::TypedExprPtr> filters = {
        // scalar AND single GIS (indexed)
        And(scalar, gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))")),
        // scalar AND (GIS OR GIS) -- Shape B
        And(scalar,
            Or(gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))"),
               gis(kInter, "POLYGON((10 10,20 10,20 20,10 20,10 10))"))),
        // same-field AND group (intersects + within)
        And(gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))"),
            gis(kWithin,
                "POLYGON((-100 -100,100 -100,100 100,-100 100,-100 -100))")),
        // direct AND-leaf + Shape-B subgroup on the SAME field: geo is both a
        // direct conjunction leaf (within) and inside an OR subgroup, so the
        // rewrite emits two independent coarse/refine pairs for it -- the
        // dual-pair path, on the indexed side.
        And(gis(kWithin,
                "POLYGON((-100 -100,100 -100,100 100,-100 100,-100 -100))"),
            Or(gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))"),
               gis(kInter, "POLYGON((10 10,20 10,20 20,10 20,10 10))"))),
        // DWithin mixed with a groupable GIS leaf on the SAME field, on the
        // INDEXED side -- the config where a wrongly-grouped DWithin actually
        // corrupts the coarse phase (RunRTreeQuery would query the R-Tree
        // with the raw point instead of the distance-expanded bbox from
        // create_bounding_box_for_dwithin, and the group's Pred drops the
        // distance so refine degrades to 0.0). DWithin must stay on the
        // baseline path per the as_groupable_gis whitelist. Query point (3,3)
        // with a 1,000,000 m geodesic radius reaches the origin cluster
        // (point / small polygon / linestring, each a few hundred km away)
        // but not the (10,10)-(20,20) polygon (~1,100 km): a distance-0
        // degradation would select zero rows here, so ON-vs-OFF diverges
        // loudly if DWithin ever enters a fusion group.
        And(std::make_shared<milvus::expr::GISFunctionFilterExpr>(
                col_geo, kDWithin, "POINT(3 3)", /*distance=*/1000000.0),
            gis(kInter, "POLYGON((-2 -2,2 -2,2 2,-2 2,-2 -2))")),
    };

    auto run = [&](const milvus::expr::TypedExprPtr& f,
                   bool enable) -> BitsetType {
        // RAII: flag restored even if ExecuteQueryExpr throws.
        GisSplitFusionFlagGuard guard(enable);
        auto node =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, f);
        return ExecuteQueryExpr(node, sealed.get(), N, MAX_TIMESTAMP);
    };

    // Force multiple Eval batches (N=200 -> ~4 batches) so the indexed coarse
    // slicing is exercised across batch boundaries, not just a single batch.
    ExprBatchSizeGuardLocal batch_guard(64);

    for (const auto& f : filters) {
        BitsetType baseline = run(f, false);
        BitsetType fused = run(f, true);
        ASSERT_EQ(baseline.size(), fused.size());
        ASSERT_EQ(baseline.size(), static_cast<size_t>(N));
        for (int i = 0; i < N; ++i) {
            ASSERT_EQ(bool(baseline[i]), bool(fused[i])) << "row " << i;
        }
    }

    sealed.reset();
    CleanupIndexFiles(stats->GetIndexFiles(), "GIS split-fusion equivalence");
}

// Regression for PR #50951 review (RTreeIndexWrapper:212): an unparseable-WKB
// row is indexed with a placeholder MBR at the origin (never dropped), so an
// origin-covering query selects it as a coarse candidate. Exact refinement must
// then tolerate the corrupt WKB instead of throwing. With the geometry cache
// OFF (the default), refinement re-parses the raw WKB, so before the fix the
// throwing Geometry(ctx, wkb) ctor failed the ENTIRE query; now
// Geometry::TryParseFromWkb skips the row. Drives EvalForIndexSegment, which is
// exactly what stayed green before (the empty/unparseable index tests only
// check Count(), never a query over such a row).
TEST_F(RTreeIndexTest, GIS_Index_Refine_ToleratesUnparseableCandidate) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->set_primary_field_id(pk_id);

    const int N = 40;
    const int kBad = 7;  // the one unparseable row
    auto full_ds = DataGen(schema, N);
    auto sealed =
        CreateSealedWithFieldDataLoaded(schema, full_ds, false, {geo_id.get()});

    // Every row is POINT(0 0) (so a valid row intersects the origin), except
    // kBad which is a truncated -- unparseable -- WKB. The placeholder MBR the
    // index assigns kBad is also the origin, so the origin query below pulls it
    // into the candidate set and forces refinement to re-parse its raw bytes.
    std::vector<std::string> wkbs;
    wkbs.reserve(N);
    auto ctx = GEOS_init_r();
    std::string origin_wkb =
        milvus::Geometry(ctx, "POINT(0 0)").to_wkb_string();
    GEOS_finish_r(ctx);
    for (int i = 0; i < N; ++i) {
        if (i == kBad) {
            std::string bad = origin_wkb;
            bad.resize(bad.size() / 2);  // truncate -> unparseable
            wkbs.emplace_back(std::move(bad));
        } else {
            wkbs.emplace_back(origin_wkb);
        }
    }

    auto geo_field_data =
        milvus::storage::CreateFieldData(milvus::storage::DataType::GEOMETRY,
                                         milvus::storage::DataType::NONE,
                                         false);
    geo_field_data->FillFieldData(wkbs.data(), wkbs.size());
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, geo_id.get(), {geo_field_data}, cm);
    sealed->LoadFieldData(load_info);

    // Distinct field/index ids so the index build temp dir (derived from
    // collection_partition_segment_field) does not collide with the leftovers
    // of GIS_Index_Exact_Filtering, which reuses the fixture's {1,1,1,100}.
    milvus::storage::FieldDataMeta bad_field_meta{1, 1, 1, 300};
    bad_field_meta.field_schema.set_data_type(
        ::milvus::proto::schema::DataType::Geometry);
    milvus::storage::IndexMeta bad_index_meta{1, 300, 1, 1};
    auto remote_file = (temp_path_.get() / "rtree_bad_refine.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, bad_field_meta, remote_file, wkbs);
    milvus::storage::FileManagerContext fm_ctx(
        bad_field_meta, bad_index_meta, chunk_manager_, fs_);
    auto rtree_index =
        std::make_unique<milvus::index::RTreeIndex<std::string>>(fm_ctx);
    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};
    build_cfg["index_type"] = milvus::index::RTREE_INDEX_TYPE;
    rtree_index->Build(build_cfg);
    auto stats = rtree_index->UploadUnified({});

    milvus::segcore::LoadIndexInfo info{};
    info.collection_id = 1;
    info.partition_id = 1;
    info.segment_id = 1;
    info.field_id = geo_id.get();
    info.field_type = DataType::GEOMETRY;
    info.index_id = 1;
    info.index_build_id = 1;
    info.index_version = 1;
    info.schema = proto::schema::FieldSchema();
    info.schema.set_data_type(proto::schema::DataType::Geometry);
    info.index_params["index_type"] = milvus::index::RTREE_INDEX_TYPE;
    nlohmann::json cfg_load;
    cfg_load["index_files"] = stats->GetIndexFiles();
    rtree_index->LoadUnified(cfg_load);
    info.cache_index =
        CreateTestCacheIndex("rtree_bad_refine_key", std::move(rtree_index));
    sealed->LoadIndex(info);

    // Origin-covering intersects query: must NOT throw, must select every valid
    // origin point and skip the unparseable row.
    auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(0 0)");
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, gis_expr);
    BitsetType bits;
    ASSERT_NO_THROW(
        { bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(bits[i]), i != kBad) << "row " << i;
    }

    sealed.reset();
    CleanupIndexFiles(stats->GetIndexFiles(), "GIS bad-refine test");
}

// Regression for the legacy-short-index self-heal: old builders advanced the
// absolute row offset even when GEOS parsing failed, so a transient parse OOM
// could drop a valid geometry at an interior offset while later rows kept their
// original offsets. Count() then under-reports the row space but does not reveal
// where the hole is. The fallback must refine the whole segment, not only pad a
// presumed missing suffix. Absolute null offsets must still preserve SQL
// three-valued logic under NOT ST_*.
TEST_F(RTreeIndexTest,
       GIS_LegacyShortIndexRecoversInteriorHoleAndPreservesTailNull) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    constexpr int N = 6;
    auto seg = MakeLegacyShortIndexSealed(N, 500, "rtree_legacy_short_key");
    auto& sealed = seg.sealed;
    auto geo_id = seg.geo_id;

    // The short coarse bitmap has no bit for row 1. A tail-only resize leaves
    // that interior bit false and silently loses a valid match; the full-scan
    // fallback must recover it through exact refinement.
    auto origin_intersects =
        std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true),
            proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
            "POINT(0 0)");
    auto intersects_plan = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, origin_intersects);
    auto intersects_bits =
        ExecuteQueryExpr(intersects_plan, sealed.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(intersects_bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        EXPECT_TRUE(intersects_bits[i]) << "valid row " << i;
    }
    EXPECT_FALSE(intersects_bits[N - 1]) << "NULL tail row must not match";

    auto intersects = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(100 100)");
    auto not_intersects = std::make_shared<milvus::expr::LogicalUnaryExpr>(
        milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, intersects);
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       not_intersects);
    auto bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP);

    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        EXPECT_TRUE(bits[i]) << "non-null row " << i;
    }
    EXPECT_FALSE(bits[N - 1]) << "NULL tail row must remain unknown under NOT";

    sealed.reset();
    CleanupIndexFiles(seg.index_files, "GIS legacy-short-index test");
}

// Regression for PR #50951 review (GISFunctionFilterExpr.cpp
// process_sealed_data): the legacy short-index self-heal promotes EVERY row to
// a candidate, and with the geometry cache off (the default) refinement used
// to fetch all of them with ONE bulk_subscript -- a full-column WKB copy per
// query. Refinement now reads the candidates in batch_size_-row groups. Pin the
// batch size far below N so the group loop runs several full groups plus a
// partial tail, and check the answer is identical to the single-shot read:
// every valid row found, the NULL tail row not.
TEST_F(RTreeIndexTest, GIS_LegacyShortIndex_ChunkedRefinementMatchesFullRead) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    constexpr int N = 11;  // 11 candidates / batch 4 -> groups 4,4,3
    auto seg = MakeLegacyShortIndexSealed(N, 501, "rtree_legacy_chunked_key");
    auto& sealed = seg.sealed;
    auto geo_id = seg.geo_id;

    auto run = [&](const char* wkt, bool negate) -> BitsetType {
        milvus::expr::TypedExprPtr e =
            std::make_shared<milvus::expr::GISFunctionFilterExpr>(
                milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true),
                proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
                wkt);
        if (negate) {
            e = std::make_shared<milvus::expr::LogicalUnaryExpr>(
                milvus::expr::LogicalUnaryExpr::OpType::LogicalNot, e);
        }
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, e);
        return ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP);
    };

    // Reference: default batch size (>= N) -> a single bulk_subscript group.
    BitsetType single_hit = run("POINT(0 0)", false);
    BitsetType single_not = run("POINT(100 100)", true);
    ASSERT_EQ(single_hit.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        ASSERT_TRUE(single_hit[i]) << "valid row " << i;
        ASSERT_TRUE(single_not[i]) << "non-null row " << i;
    }
    ASSERT_FALSE(single_hit[N - 1]);
    ASSERT_FALSE(single_not[N - 1]);

    // Chunked: batch 4 -> three bulk_subscript groups over the 11 candidates.
    // (Also shrinks the Eval batch, so per-batch slicing of the cached refined
    // bitmap crosses group boundaries too.)
    ExprBatchSizeGuardLocal batch_guard(4);
    BitsetType chunked_hit = run("POINT(0 0)", false);
    BitsetType chunked_not = run("POINT(100 100)", true);
    ASSERT_EQ(chunked_hit.size(), static_cast<size_t>(N));
    ASSERT_EQ(chunked_not.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(chunked_hit[i]), bool(single_hit[i])) << "row " << i;
        EXPECT_EQ(bool(chunked_not[i]), bool(single_not[i])) << "row " << i;
    }

    sealed.reset();
    CleanupIndexFiles(seg.index_files, "GIS legacy chunked-refinement test");
}

// Regression for PR #50951 review (GISConjunctExpr.cpp RunRTreeQuery): the
// split/fusion coarse path used to pad a SHORT legacy R-Tree bitmap only at
// the tail (resize(active_count_, true)). The missing entry of a legacy index
// is an INTERIOR hole (row 1 here), so its bit stayed false, Refine's
// `survivors &= coarse_slice` dropped the row, and `A AND B` on the same
// column silently lost a match that either predicate alone (per-predicate
// path, which self-heals to a full scan) would return. Both paths now share
// PromoteShortGISCoarseBitmap: fusion ON must equal fusion OFF, and both must
// return every valid row and reject the NULL tail.
TEST_F(RTreeIndexTest, GIS_SplitFusion_LegacyShortIndexRecoversInteriorHole) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    constexpr int N = 6;
    auto seg = MakeLegacyShortIndexSealed(N, 502, "rtree_legacy_fusion_key");
    auto& sealed = seg.sealed;
    auto geo_id = seg.geo_id;

    auto col_geo =
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY, {}, true);
    auto gis = [&](proto::plan::GISFunctionFilterExpr_GISOp op,
                   const std::string& wkt) -> milvus::expr::TypedExprPtr {
        return std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            col_geo, op, wkt);
    };
    // Same-field AND group: intersects(origin) AND within(big box). Both are
    // satisfied by every valid row, so any false in the answer below is a row
    // the coarse phase wrongly pruned.
    milvus::expr::TypedExprPtr filter =
        std::make_shared<milvus::expr::LogicalBinaryExpr>(
            milvus::expr::LogicalBinaryExpr::OpType::And,
            gis(proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
                "POINT(0 0)"),
            gis(proto::plan::GISFunctionFilterExpr_GISOp_Within,
                "POLYGON((-100 -100,100 -100,100 100,-100 100,-100 -100))"));

    auto run = [&](bool enable) -> BitsetType {
        GisSplitFusionFlagGuard guard(enable);
        auto node =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, filter);
        return ExecuteQueryExpr(node, sealed.get(), N, MAX_TIMESTAMP);
    };
    // Small batch so the coarse slice is consumed across several Eval batches.
    ExprBatchSizeGuardLocal batch_guard(2);

    BitsetType baseline = run(false);
    BitsetType fused = run(true);
    ASSERT_EQ(baseline.size(), static_cast<size_t>(N));
    ASSERT_EQ(fused.size(), static_cast<size_t>(N));
    for (int i = 0; i < N - 1; ++i) {
        EXPECT_TRUE(baseline[i]) << "baseline valid row " << i;
        EXPECT_TRUE(fused[i])
            << "fused valid row " << i << " (interior hole is row 1)";
    }
    EXPECT_FALSE(baseline[N - 1]) << "NULL tail row must not match";
    EXPECT_FALSE(fused[N - 1]) << "NULL tail row must not match";

    sealed.reset();
    CleanupIndexFiles(seg.index_files, "GIS fusion legacy-short-index test");
}

namespace {
// RAII toggle for the static geometry-cache switch: restores the previous
// value even when a gtest ASSERT returns out of the test body early, so a
// failing test cannot leak the flag into later tests.
struct GeometryCacheFlagGuard {
    explicit GeometryCacheFlagGuard(bool enable)
        : previous_(milvus::segcore::SegcoreConfig::default_config()
                        .get_enable_geometry_cache()) {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_geometry_cache(enable);
    }
    ~GeometryCacheFlagGuard() {
        milvus::segcore::SegcoreConfig::default_config()
            .set_enable_geometry_cache(previous_);
    }
    bool previous_;
};

// Build the WKB column used by the corrupt-row tolerance tests: every row is
// POINT(0 0) except `bad_row`, whose WKB is truncated (unparseable).
std::vector<std::string>
MakeOriginWkbsWithOneCorruptRow(int n, int bad_row) {
    std::vector<std::string> wkbs;
    wkbs.reserve(n);
    auto ctx = GEOS_init_r();
    std::string origin_wkb =
        milvus::Geometry(ctx, "POINT(0 0)").to_wkb_string();
    GEOS_finish_r(ctx);
    for (int i = 0; i < n; ++i) {
        if (i == bad_row) {
            std::string bad = origin_wkb;
            bad.resize(bad.size() / 2);  // truncate -> unparseable
            wkbs.emplace_back(std::move(bad));
        } else {
            wkbs.emplace_back(origin_wkb);
        }
    }
    return wkbs;
}
}  // namespace

// Regression for PR #50951 review (GISFunctionFilterExpr no-cache brute-force
// branch): with NO index and the geometry cache OFF (the default
// configuration), a GIS predicate over a segment containing a corrupt WKB row
// used the throwing Geometry(ctx, wkb) constructor on a per-batch
// GEOS_init_r() context -- one corrupt row failed the whole query AND leaked
// the context (the throw skipped GEOS_finish_r). Now the branch parses with
// TryParseFromWkb on a thread-local context: the query succeeds and the
// corrupt row simply evaluates to false.
TEST_F(RTreeIndexTest, GIS_BruteForce_ToleratesCorruptRow_CacheOff) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    GeometryCacheFlagGuard cache_off(false);

    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->set_primary_field_id(pk_id);

    const int N = 40;
    const int kBad = 7;
    auto full_ds = DataGen(schema, N);
    auto sealed =
        CreateSealedWithFieldDataLoaded(schema, full_ds, false, {geo_id.get()});

    auto wkbs = MakeOriginWkbsWithOneCorruptRow(N, kBad);
    auto geo_field_data =
        milvus::storage::CreateFieldData(milvus::storage::DataType::GEOMETRY,
                                         milvus::storage::DataType::NONE,
                                         false);
    geo_field_data->FillFieldData(wkbs.data(), wkbs.size());
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, geo_id.get(), {geo_field_data}, cm);
    sealed->LoadFieldData(load_info);

    // No index loaded -> ExprExecPath::RawData -> the brute-force macro branch.
    auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(0 0)");
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, gis_expr);
    BitsetType bits;
    ASSERT_NO_THROW(
        { bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(bits[i]), i != kBad) << "row " << i;
    }
}

// Regression for PR #50951 review (GeometryCache.h AppendDataAt, the critical
// finding): with the geometry cache ENABLED, loading a segment containing one
// corrupt WKB row used the throwing Geometry ctor inside
// SimpleGeometryCache::AppendDataAt, so LoadFieldData -> LoadGeometryCache
// failed the ENTIRE segment load -- exactly the row shape the placeholder-MBR
// write paths deliberately keep. Now the corrupt row is cached as an invalid
// entry; the load succeeds and the cache branch of the filter macros skips the
// row (res=false) instead of tripping its former non-null assert.
TEST_F(RTreeIndexTest, GIS_CacheOn_CorruptRow_LoadsAndQueries) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;

    GeometryCacheFlagGuard cache_on(true);

    auto schema = std::make_shared<Schema>();
    auto pk_id = schema->AddDebugField("id", DataType::INT64);
    schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto geo_id = schema->AddDebugField("geo", DataType::GEOMETRY);
    schema->set_primary_field_id(pk_id);

    const int N = 40;
    const int kBad = 7;
    auto full_ds = DataGen(schema, N);
    auto sealed =
        CreateSealedWithFieldDataLoaded(schema, full_ds, false, {geo_id.get()});

    auto wkbs = MakeOriginWkbsWithOneCorruptRow(N, kBad);
    auto geo_field_data =
        milvus::storage::CreateFieldData(milvus::storage::DataType::GEOMETRY,
                                         milvus::storage::DataType::NONE,
                                         false);
    geo_field_data->FillFieldData(wkbs.data(), wkbs.size());
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, geo_id.get(), {geo_field_data}, cm);

    int64_t seg_id = -1;
    // The critical assertion: the load itself must tolerate the corrupt row.
    ASSERT_NO_THROW({ sealed->LoadFieldData(load_info); });
    seg_id = sealed->get_segment_id();
    auto published_cache = sealed->GetGeometryCache(geo_id);
    ASSERT_NE(published_cache, nullptr);
    // Sealed caches live in the immutable published runtime state, not in the
    // process-global manager. That is what makes a failed/cancelled reopen
    // discard the staged replacement together with its unpublished column.
    EXPECT_EQ(milvus::exec::SimpleGeometryCacheManager::Instance().GetCache(
                  sealed->segment_instance_uid(), seg_id, geo_id),
              nullptr);

    // Query through the cache branch of the filter macros (cache is enabled
    // and populated by the load above).
    auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
        milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY),
        proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
        "POINT(0 0)");
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, gis_expr);
    BitsetType bits;
    ASSERT_NO_THROW(
        { bits = ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    ASSERT_EQ(bits.size(), static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(bool(bits[i]), i != kBad) << "row " << i;
    }

    // Model the critical failure boundary of reopen: field loading and cache
    // construction mutate only a cloned runtime. If a later step throws or is
    // cancelled before Publish(), discarding that clone must leave the old
    // column/cache pair visible and must not leak the replacement globally.
    auto* sealed_impl =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(sealed.get());
    ASSERT_NE(sealed_impl, nullptr);
    auto staged_runtime = sealed_impl->TestCloneMutableRuntimeResourceState();
    auto unpublished_cache =
        std::make_shared<milvus::exec::SimpleGeometryCache>();
    staged_runtime->geometry_caches[geo_id] = unpublished_cache;
    EXPECT_EQ(sealed->GetGeometryCache(geo_id), published_cache);
    EXPECT_NE(sealed->GetGeometryCache(geo_id), unpublished_cache);
    staged_runtime.reset();
    EXPECT_EQ(sealed->GetGeometryCache(geo_id), published_cache);

    // Field retirement is another publication boundary: the cache must leave
    // the runtime snapshot together with the raw column.
    sealed->DropFieldData(geo_id);
    EXPECT_EQ(sealed->GetGeometryCache(geo_id), nullptr);
}

// Regression for PR #50951 review (RTreeIndex::AddGeometry nullability): a row
// whose valid_data says VALID but whose WKB payload is empty must be indexed
// as a non-null placeholder row -- exactly how the sealed
// bulk_load_from_field_data path (is_valid first, then payload) classifies it
// -- not pushed into null_offset_. Before the fix AddGeometry inferred
// nullness from wkb_data.empty(), so ST_ISNOTNULL / Count() disagreed between
// growing and sealed for such a row.
TEST_F(RTreeIndexTest, AddGeometryClassifiesNullByValidityNotPayload) {
    field_meta_.field_schema.set_nullable(true);
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    rtree.AddGeometry(CreatePointWKB(1.0, 1.0), 0, true);
    // Valid row, empty payload -> placeholder MBR, non-null.
    rtree.AddGeometry(std::string(), 1, true);
    // Genuinely null row -> null_offset_.
    rtree.AddGeometry(std::string(), 2, false);

    EXPECT_EQ(rtree.Count(), 3);

    auto is_not_null = rtree.IsNotNull();
    ASSERT_EQ(is_not_null.size(), 3u);
    EXPECT_TRUE(is_not_null[0]);
    EXPECT_TRUE(is_not_null[1]) << "valid empty-payload row must be non-null";
    EXPECT_FALSE(is_not_null[2]);

    auto is_null = rtree.IsNull();
    ASSERT_EQ(is_null.size(), 3u);
    EXPECT_FALSE(is_null[0]);
    EXPECT_FALSE(is_null[1]);
    EXPECT_TRUE(is_null[2]);
}

TEST_F(RTreeIndexTest, QueryBadAllocIsClassifiedAsMemAllocateFailed) {
    // Distinct field/index ids: the index build temp dir is derived from
    // collection_partition_segment_field and lives under a PROCESS-global local
    // storage root, not under this fixture's per-test temp_path_. Reusing the
    // fixture's field_meta_ {1,1,1,100} therefore inherits the leftovers of the
    // earlier GIS_Index_Exact_Filtering, and RTreeIndex::Build rejects a
    // non-empty temp dir -- the same collision GIS_SplitFusion_Equivalence_
    // Indexed and GIS_Index_Refine_ToleratesUnparseableCandidate avoid.
    milvus::storage::FieldDataMeta badalloc_field_meta{1, 1, 1, 400};
    badalloc_field_meta.field_schema.set_data_type(
        ::milvus::proto::schema::DataType::Geometry);
    milvus::storage::IndexMeta badalloc_index_meta{1, 400, 1, 1};
    milvus::storage::FileManagerContext ctx_build(
        badalloc_field_meta, badalloc_index_meta, chunk_manager_, fs_);
    TestableRTreeIndex rtree(ctx_build);
    std::vector<std::string> wkbs = {CreatePointWKB(1.0, 1.0)};
    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data(), {});

    auto dataset = std::make_shared<milvus::Dataset>();
    dataset->Set(milvus::index::OPERATOR_TYPE,
                 ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects);
    dataset->Set(milvus::index::MATCH_VALUE,
                 CreateGeometryFromWkt("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"));

    rtree.ThrowOnNextQueryForTesting();
    try {
        (void)rtree.Query(dataset);
        FAIL() << "expected injected query allocation failure";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::MemAllocateFailed);
    }

    // The hook is one-shot and the failed query did not damage the index.
    auto result = rtree.Query(dataset);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_TRUE(result[0]);
}

// Exercises the growing-segment path where a single writer keeps inserting
// geometries (RTreeIndex::AddGeometry) while reader threads concurrently call
// Count() and QueryCandidates(). Before the locking fix these read total row
// counts / null_offset_ / wrapper_ and the boost rtree size without holding
// any lock, racing the incremental inserts. Only TSAN can assert the accesses
// are now properly synchronized; ASAN (what CI runs) catches this class of
// regression only once it corrupts memory -- e.g. reading wrapper_ or the
// vectors while another thread reallocates them. Unsanitized, the test still
// must not crash and must converge to the expected final count.
//
// The writer runs on its own thread behind a deadline. It used to run inline
// on the test thread, which meant that if the readers ever managed to keep the
// index lock continuously busy the test simply never returned: it took ~96
// minutes in the plain C++ UT run and blew the coverage shard's 30 minute
// budget outright, where it was reported as a shard crash rather than as this
// test. A starved writer must fail here, quickly and by name.
TEST_F(RTreeIndexTest, GrowingConcurrentAddAndQuery) {
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    // Seed one geometry so wrapper_ is published before readers start querying
    // (QueryCandidates asserts a non-null wrapper).
    rtree.AddGeometry(CreatePointWKB(0.0, 0.0), 0, true);

    constexpr int kRows = 1000;
    // Generous: the writer's own work is milliseconds, so this only bounds how
    // long a lock-policy regression takes to surface.
    constexpr auto kWriterDeadline = std::chrono::seconds(120);
    std::atomic<bool> stop{false};
    std::atomic<int> reader_iters{0};
    std::atomic<int> writer_progress{0};

    auto reader = [&]() {
        auto ctx = GEOS_init_r();
        // The inner scope is load-bearing: ~Geometry calls
        // GEOSGeom_destroy_r(ctx_, ...), and locals are destroyed AFTER the
        // last statement of the enclosing block. With query_geom declared
        // beside ctx, the trailing GEOS_finish_r would free the context first
        // and the destructor would run against it -- a use-after-free on every
        // reader thread, in the very test meant to prove memory safety.
        {
            // A box covering the inserted points [0, kRows] x [0, kRows].
            milvus::Geometry query_geom(
                ctx,
                "POLYGON ((-1 -1, 100000 -1, 100000 100000, -1 100000, -1 "
                "-1))");
            while (!stop.load(std::memory_order_relaxed)) {
                volatile int64_t c = rtree.Count();
                (void)c;
                std::vector<int64_t> candidates;
                rtree.QueryCandidates(
                    ::milvus::proto::plan::
                        GISFunctionFilterExpr_GISOp_Intersects,
                    query_geom,
                    candidates);
                reader_iters.fetch_add(1, std::memory_order_relaxed);
            }
        }
        GEOS_finish_r(ctx);
    };

    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back(reader);
    }

    // Single writer, mirroring the per-segment serialized insert pipeline.
    std::thread writer([&]() {
        for (int i = 1; i <= kRows; ++i) {
            if (i % 7 == 0) {
                // Interleave null geometries (exercises the null_offset_ path).
                rtree.AddGeometry(std::string(), i, false);
            } else {
                rtree.AddGeometry(CreatePointWKB(static_cast<double>(i),
                                                 static_cast<double>(i)),
                                  i,
                                  true);
            }
            writer_progress.fetch_add(1, std::memory_order_relaxed);
        }
    });

    const auto deadline = std::chrono::steady_clock::now() + kWriterDeadline;
    while (writer_progress.load(std::memory_order_relaxed) < kRows &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const int progressed = writer_progress.load(std::memory_order_relaxed);

    stop.store(true, std::memory_order_relaxed);
    writer.join();
    for (auto& th : readers) {
        th.join();
    }

    ASSERT_EQ(progressed, kRows)
        << "the insert thread was starved by concurrent readers: only "
        << progressed << " of " << kRows
        << " rows were inserted within the deadline";
    EXPECT_GT(reader_iters.load(), 0);
    // Final count = seeded row 0 plus kRows incremental rows.
    EXPECT_EQ(rtree.Count(), static_cast<int64_t>(kRows + 1));
}

// Multiple concurrent writers building the same growing index. This exercises
// IndexingRecord::AppendingIndex's documented "concurrent, reentrant" contract
// as defense-in-depth: production currently serializes inserts per growing
// segment (one flowgraph consumer per vchannel), so this shape is not driven
// by production today -- the test pins the class-level contract so a future
// caller change fails here instead of in release. Several threads race on the
// first-time wrapper_ initialization and then keep inserting. Under TSAN this
// asserts wrapper_/total_num_rows_/null_offset_ are never touched
// unsynchronized; under ASAN (what CI runs) it catches only the memory errors
// an unsynchronized access produces, such as a double-published wrapper_ or a
// use-after-free from a concurrent vector reallocation. The final count assert
// pins that the lazy init stayed idempotent.
TEST_F(RTreeIndexTest, GrowingConcurrentMultiWriter) {
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    constexpr int kWriters = 6;
    constexpr int kPerWriter = 1000;
    std::atomic<bool> go{false};
    std::atomic<bool> stop_readers{false};

    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w]() {
            // Spin so every writer hits the first AddGeometry at ~the same time
            // and they race on the lazy wrapper_ initialization.
            while (!go.load(std::memory_order_relaxed)) {
            }
            for (int j = 0; j < kPerWriter; ++j) {
                int64_t off = static_cast<int64_t>(w) * kPerWriter + j;
                if (j % 5 == 0) {
                    // null geometry
                    rtree.AddGeometry(std::string(), off, false);
                } else {
                    rtree.AddGeometry(CreatePointWKB(static_cast<double>(off),
                                                     static_cast<double>(off)),
                                      off,
                                      true);
                }
            }
        });
    }

    std::vector<std::thread> readers;
    for (int r = 0; r < 2; ++r) {
        readers.emplace_back([&]() {
            while (!stop_readers.load(std::memory_order_relaxed)) {
                volatile int64_t c = rtree.Count();  // safe before/after init
                (void)c;
            }
        });
    }

    go.store(true, std::memory_order_relaxed);
    for (auto& t : writers) {
        t.join();
    }
    stop_readers.store(true, std::memory_order_relaxed);
    for (auto& t : readers) {
        t.join();
    }

    // Every row (null + non-null, disjoint offsets) must be accounted for once.
    EXPECT_EQ(rtree.Count(), static_cast<int64_t>(kWriters * kPerWriter));
}

// Regression for the IsNull()/IsNotNull() heap out-of-bounds write on the
// concurrent multi-writer growing index. Writers assign offsets round-robin so
// null_offset_ is appended in NON-monotonic order (i.e. unsorted), and each
// writer may publish a high offset while lower offsets are still in flight, so
// null_offset_ transiently holds values >= Count(). Readers hammer IsNull()/
// IsNotNull() throughout ingestion: with the old std::lower_bound shortcut over
// unsorted data those offsets escaped the bound and wrote past the bitset (a
// silent OOB in release builds; caught here under ASAN). The final bitsets must
// also match the exact null / non-null partition.
TEST_F(RTreeIndexTest, GrowingConcurrentMultiWriterIsNullBounds) {
    field_meta_.field_schema.set_nullable(true);
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    constexpr int kWriters = 6;
    constexpr int kTotal = 6000;  // divisible by kWriters
    // Row is null iff (offset % 3 == 0). Deterministic, independent of thread
    // scheduling, so the final counts are exact.
    auto is_null_row = [](int64_t off) { return off % 3 == 0; };

    std::atomic<bool> go{false};
    std::atomic<bool> stop_readers{false};
    std::atomic<int64_t> reader_iters{0};

    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w]() {
            while (!go.load(std::memory_order_relaxed)) {
            }
            // Round-robin offsets: writer w owns w, w+kWriters, w+2*kWriters...
            // so concurrent writers append null_offset_ out of order.
            for (int64_t off = w; off < kTotal; off += kWriters) {
                if (is_null_row(off)) {
                    rtree.AddGeometry(std::string(), off, false);
                } else {
                    rtree.AddGeometry(CreatePointWKB(static_cast<double>(off),
                                                     static_cast<double>(off)),
                                      off,
                                      true);
                }
            }
        });
    }

    // Readers concurrently drive the previously-unguarded null bitmap paths.
    // The point is to run IsNull()/IsNotNull() against the growing index while
    // null_offset_ is unsorted and mid-flight: under ASAN the old lower_bound
    // shortcut faulted here. We intentionally do NOT cross-check the two
    // results against each other, because IsNull() and IsNotNull() each take
    // an independent Count() snapshot and a concurrent writer can grow the row
    // count between the two calls (so their sizes legitimately differ by the
    // rows added in between). Per-snapshot correctness is asserted after join.
    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&]() {
            while (!stop_readers.load(std::memory_order_relaxed)) {
                auto is_null = rtree.IsNull();
                auto is_not_null = rtree.IsNotNull();
                // Each result is self-consistent: no set bit lies outside its
                // own length (the invariant the OOB fix restores).
                EXPECT_LE(is_null.count(), is_null.size());
                EXPECT_LE(is_not_null.count(), is_not_null.size());
                reader_iters.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    go.store(true, std::memory_order_relaxed);
    for (auto& t : writers) {
        t.join();
    }
    stop_readers.store(true, std::memory_order_relaxed);
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_GT(reader_iters.load(), 0);
    EXPECT_EQ(rtree.Count(), static_cast<int64_t>(kTotal));

    int64_t expected_nulls = 0;
    for (int64_t off = 0; off < kTotal; ++off) {
        if (is_null_row(off)) {
            ++expected_nulls;
        }
    }
    auto final_null = rtree.IsNull();
    auto final_not_null = rtree.IsNotNull();
    EXPECT_EQ(final_null.count(), expected_nulls);
    EXPECT_EQ(final_not_null.count(), kTotal - expected_nulls);
    // Every offset must land in exactly one of the two bitsets.
    for (int64_t off = 0; off < kTotal; ++off) {
        EXPECT_EQ(final_null[off], is_null_row(off)) << "offset " << off;
        EXPECT_EQ(final_not_null[off], !is_null_row(off)) << "offset " << off;
    }
}

// Count() feeds the bitmap size in Query(), so on a growing index it must
// report the row space rather than how many rows happen to be indexed.
// AddGeometry() is offset-addressed and keeps total_num_rows_ at
// max(row_offset) + 1, so a sparse or out-of-order offset set makes the two
// diverge: before the fix Count() returned wrapper->count() + nulls, and every
// candidate at or above that value was silently clipped by the bound in
// Query(), turning live rows into false negatives.
TEST_F(RTreeIndexTest, GrowingSparseOffsetsQueryDropsNoCandidate) {
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    // Three rows at sparse offsets, inserted out of order. Only 3 entries are
    // indexed, but the row space runs to 101.
    const std::vector<int64_t> offsets = {100, 0, 50};
    for (auto off : offsets) {
        rtree.AddGeometry(CreatePointWKB(1.0, 1.0), off, true);
    }

    EXPECT_EQ(rtree.Count(), 101)
        << "Count() must span the row space, not the indexed-entry count";

    auto ds = std::make_shared<milvus::Dataset>();
    ds->Set(milvus::index::OPERATOR_TYPE,
            ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects);
    ds->Set(milvus::index::MATCH_VALUE,
            CreateGeometryFromWkt("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"));
    auto res = rtree.Query(ds);

    ASSERT_EQ(res.size(), 101u);
    for (auto off : offsets) {
        EXPECT_TRUE(res[off])
            << "candidate at offset " << off << " was clipped away";
    }
    EXPECT_EQ(res.count(), offsets.size());
}

// The null path maintains total_num_rows_ too, so a growing index holding
// nothing but sparse nulls must still report the full row space.
TEST_F(RTreeIndexTest, GrowingSparseNullOffsetsCountSpansRowSpace) {
    field_meta_.field_schema.set_nullable(true);
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    rtree.AddGeometry(std::string(), 30, false);
    rtree.AddGeometry(std::string(), 5, false);

    EXPECT_EQ(rtree.Count(), 31);

    auto is_null = rtree.IsNull();
    ASSERT_EQ(is_null.size(), 31u);
    EXPECT_TRUE(is_null[30]);
    EXPECT_TRUE(is_null[5]);
    EXPECT_FALSE(is_null[0]);
}

// Candidate completeness of Query() WHILE writes are in flight -- the window
// the two GrowingConcurrentMultiWriter* tests leave uncovered (their readers
// only call Count()/IsNull()/IsNotNull(), and their offset sets are dense, so
// Count() never diverges from the indexed-entry count mid-flight).
//
// The anchor row is published first at a high offset, so from the very first
// insert the row space is kAnchor+1 while only ONE entry is in the rtree: with
// the pre-fix Count() (wrapper->count() + nulls) every concurrent reader would
// size its bitmap at 1 and the bound in Query() would clip the anchor away --
// a false negative on a committed row, reproduced deterministically instead of
// depending on thread interleaving.
//
// Only EVEN offsets carry a matching point; odd offsets are placed far away.
// That makes the completeness assertion two-sided and order-independent: the
// anchor must always be present (no live candidate clipped) and no odd offset
// may ever appear (no fabricated / mis-bound candidate), whatever the writers
// have published at the moment the reader takes its snapshot.
TEST_F(RTreeIndexTest, GrowingConcurrentQueryKeepsPublishedCandidates) {
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    constexpr int64_t kAnchor = 2000;  // even -> matches the query
    constexpr int kWriters = 4;
    // Even offsets sit inside the query polygon, odd offsets far outside it.
    auto matches = [](int64_t off) { return off % 2 == 0; };
    auto wkb_for = [&](int64_t off) {
        return matches(off) ? CreatePointWKB(1.0, 1.0)
                            : CreatePointWKB(1000.0, 1000.0);
    };
    auto make_query = []() {
        auto ds = std::make_shared<milvus::Dataset>();
        ds->Set(milvus::index::OPERATOR_TYPE,
                ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects);
        ds->Set(milvus::index::MATCH_VALUE,
                CreateGeometryFromWkt("POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))"));
        return ds;
    };

    // Publish the anchor before anyone reads: it is committed for the whole
    // run, so no snapshot is ever allowed to miss it.
    rtree.AddGeometry(wkb_for(kAnchor), kAnchor, true);

    std::atomic<bool> go{false};
    std::atomic<bool> stop_readers{false};
    std::atomic<int64_t> reader_iters{0};

    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w]() {
            while (!go.load(std::memory_order_relaxed)) {
            }
            // Round-robin so offsets reach the index out of order and the
            // sparse anchor stays above everything still in flight.
            for (int64_t off = w; off < kAnchor; off += kWriters) {
                rtree.AddGeometry(wkb_for(off), off, true);
            }
        });
    }

    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&]() {
            while (!stop_readers.load(std::memory_order_relaxed)) {
                auto res = rtree.Query(make_query());
                if (res.size() < static_cast<size_t>(kAnchor + 1)) {
                    ADD_FAILURE()
                        << "Count() under-reported the row space mid-flight: "
                        << res.size() << " < " << (kAnchor + 1);
                    break;
                }
                EXPECT_TRUE(res[kAnchor])
                    << "committed anchor candidate was clipped away";
                for (size_t off = 0; off < res.size(); ++off) {
                    if (res[off] && !matches(static_cast<int64_t>(off))) {
                        ADD_FAILURE() << "non-matching offset " << off
                                      << " appeared as a candidate";
                        break;
                    }
                }
                reader_iters.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    go.store(true, std::memory_order_relaxed);
    for (auto& t : writers) {
        t.join();
    }
    stop_readers.store(true, std::memory_order_relaxed);
    for (auto& t : readers) {
        t.join();
    }

    EXPECT_GT(reader_iters.load(), 0);

    // After ingestion the answer is exact: every even offset in [0, kAnchor].
    EXPECT_EQ(rtree.Count(), kAnchor + 1);
    auto final_res = rtree.Query(make_query());
    ASSERT_EQ(final_res.size(), static_cast<size_t>(kAnchor + 1));
    for (int64_t off = 0; off <= kAnchor; ++off) {
        EXPECT_EQ(final_res[off], matches(off)) << "offset " << off;
    }
    EXPECT_EQ(final_res.count(), static_cast<size_t>(kAnchor / 2 + 1));
}
