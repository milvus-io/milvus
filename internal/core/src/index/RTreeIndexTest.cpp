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

#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <atomic>
#include <functional>
#include <thread>
#include <vector>
#include <string>
#include <fstream>

#include "RTreeIndex.h"
#include "storage/Util.h"
#include "storage/FileManager.h"
#include "common/Types.h"
#include "test_utils/TmpPath.h"
#include "pb/schema.pb.h"
#include "pb/plan.pb.h"
#include "common/Geometry.h"
#include "common/EasyAssert.h"
#include "IndexFactory.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/DiskFileManagerImpl.h"
#include "test_utils/DataGen.h"
#include "query/ExecPlanNodeVisitor.h"
#include "common/Consts.h"
#include "test_utils/storage_test_utils.h"
#include "Utils.h"
#include "storage/ThreadPools.h"
#include "test_utils/cachinglayer_test_utils.h"

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

static milvus::Geometry
CreateGeometryFromWkt(const std::string& wkt) {
    auto ctx = GEOS_init_r();
    auto geom = milvus::Geometry(ctx, wkt.c_str());
    GEOS_finish_r(ctx);
    return geom;
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
        index_meta_ = milvus::storage::IndexMeta{.segment_id = 1,
                                                 .field_id = 100,
                                                 .build_id = 1,
                                                 .index_version = 1};
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

    milvus::storage::StorageConfig storage_config_;
    milvus::storage::ChunkManagerPtr chunk_manager_;
    milvus::storage::FieldDataMeta field_meta_;
    milvus::storage::IndexMeta index_meta_;
    milvus::test::TmpPath temp_path_;
    milvus_storage::ArrowFileSystemPtr fs_;
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

    // expect: 3 geometries (0, 2, 4) are valid and parsable, 1st geometry is marked null and skipped, 3rd geometry is bad WKB and skipped
    ASSERT_EQ(rtree.Count(), 4);

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
    ASSERT_EQ(rtree_load.Count(), 4);
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
    auto vec_id = schema->AddDebugField(
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

// Exercises the growing-segment path where a single writer keeps inserting
// geometries (RTreeIndex::AddGeometry) while reader threads concurrently call
// Count() and QueryCandidates(). Before the locking fix these read total row
// counts / null_offset_ / wrapper_ and the boost rtree size without holding
// any lock, racing the incremental inserts. Run under ASAN/TSAN this asserts
// the accesses are now properly synchronized; it must also not crash and must
// converge to the expected final count.
TEST_F(RTreeIndexTest, GrowingConcurrentAddAndQuery) {
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_, fs_);
    milvus::index::RTreeIndex<std::string> rtree(ctx_build);

    // Seed one geometry so wrapper_ is published before readers start querying
    // (QueryCandidates asserts a non-null wrapper).
    rtree.AddGeometry(CreatePointWKB(0.0, 0.0), 0);

    constexpr int kRows = 4000;
    std::atomic<bool> stop{false};
    std::atomic<int> reader_iters{0};

    auto reader = [&]() {
        auto ctx = GEOS_init_r();
        // A box covering the inserted points [0, kRows] x [0, kRows].
        milvus::Geometry query_geom(
            ctx,
            "POLYGON ((-1 -1, 100000 -1, 100000 100000, -1 100000, -1 -1))");
        while (!stop.load(std::memory_order_relaxed)) {
            volatile int64_t c = rtree.Count();
            (void)c;
            std::vector<int64_t> candidates;
            rtree.QueryCandidates(
                ::milvus::proto::plan::GISFunctionFilterExpr_GISOp_Intersects,
                query_geom,
                candidates);
            reader_iters.fetch_add(1, std::memory_order_relaxed);
        }
        GEOS_finish_r(ctx);
    };

    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back(reader);
    }

    // Single writer, mirroring the per-segment serialized insert pipeline.
    for (int i = 1; i <= kRows; ++i) {
        if (i % 7 == 0) {
            // Interleave null geometries (exercises the null_offset_ path).
            rtree.AddGeometry(std::string(), i);
        } else {
            rtree.AddGeometry(
                CreatePointWKB(static_cast<double>(i), static_cast<double>(i)),
                i);
        }
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& th : readers) {
        th.join();
    }

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
// first-time wrapper_ initialization and then keep inserting. Run under
// ASAN/TSAN this asserts the lazy init is idempotent and
// wrapper_/total_num_rows_/null_offset_ are never touched unsynchronized.
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
                    rtree.AddGeometry(std::string(), off);  // null geometry
                } else {
                    rtree.AddGeometry(CreatePointWKB(static_cast<double>(off),
                                                     static_cast<double>(off)),
                                      off);
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
                    rtree.AddGeometry(std::string(), off);
                } else {
                    rtree.AddGeometry(CreatePointWKB(static_cast<double>(off),
                                                     static_cast<double>(off)),
                                      off);
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
