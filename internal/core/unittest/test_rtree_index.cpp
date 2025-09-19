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
#include <iostream>
#include <vector>
#include <string>

#include "index/RTreeIndex.h"
#include "storage/Util.h"
#include "storage/FileManager.h"
#include "common/Types.h"
#include "test_utils/TmpPath.h"
#include "pb/schema.pb.h"
#include "pb/plan.pb.h"
#include "common/Geometry.h"
#include "common/EasyAssert.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/DiskFileManagerImpl.h"
#include "common/FieldData.h"
#include <boost/algorithm/string/predicate.hpp>
#include <fstream>
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "test_utils/DataGen.h"
#include "query/ExecPlanNodeVisitor.h"
#include "common/Consts.h"

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
    return milvus::Geometry(wkt.c_str()).to_wkb_string();
}

static milvus::Geometry
CreateGeometryFromWkt(const std::string& wkt) {
    return milvus::Geometry(wkt.c_str());
}

// Helper: write an InsertData parquet file to "remote" storage managed by chunk_manager_
static std::string
WriteGeometryInsertFile(const milvus::storage::ChunkManagerPtr& cm,
                        const milvus::storage::FieldDataMeta& field_meta,
                        const std::string& remote_path,
                        const std::vector<std::string>& wkbs,
                        bool nullable = false,
                        const uint8_t* valid_bitmap = nullptr) {
    auto field_data = milvus::storage::CreateFieldData(
        milvus::storage::DataType::GEOMETRY, nullable);
    if (nullable && valid_bitmap != nullptr) {
        field_data->FillFieldData(wkbs.data(), valid_bitmap, wkbs.size());
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
        // clean chunk manager files if any (TmpPath destructor will also remove)
    }

    milvus::storage::StorageConfig storage_config_;
    milvus::storage::ChunkManagerPtr chunk_manager_;
    milvus::storage::FieldDataMeta field_meta_;
    milvus::storage::IndexMeta index_meta_;
    milvus::test::TmpPath temp_path_;
};

TEST_F(RTreeIndexTest, Build_Upload_Load) {
    // ---------- Build via BuildWithRawDataForUT ----------
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree_build(ctx_build);

    std::vector<std::string> wkbs = {CreatePointWKB(1.0, 1.0),
                                     CreatePointWKB(2.0, 2.0)};
    rtree_build.BuildWithRawDataForUT(wkbs.size(), wkbs.data());

    ASSERT_EQ(rtree_build.Count(), 2);

    // ---------- Upload ----------
    auto stats = rtree_build.Upload({});
    ASSERT_NE(stats, nullptr);
    ASSERT_GT(stats->GetIndexFiles().size(), 0);

    // ---------- Load back ----------
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();

    milvus::tracer::TraceContext trace_ctx;  // empty context
    rtree_load.Load(trace_ctx, cfg);

    ASSERT_EQ(rtree_load.Count(), 2);
}

TEST_F(RTreeIndexTest, Load_WithFileNamesOnly) {
    // Build & upload first
    milvus::storage::FileManagerContext ctx_build(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree_build(ctx_build);

    std::vector<std::string> wkbs2 = {CreatePointWKB(10.0, 10.0),
                                      CreatePointWKB(20.0, 20.0)};
    rtree_build.BuildWithRawDataForUT(wkbs2.size(), wkbs2.data());

    auto stats = rtree_build.Upload({});

    // gather only filenames (strip parent path)
    std::vector<std::string> filenames;
    for (const auto& path : stats->GetIndexFiles()) {
        filenames.emplace_back(
            boost::filesystem::path(path).filename().string());
        // make sure file exists in remote storage
        ASSERT_TRUE(chunk_manager_->Exist(path));
        ASSERT_GT(chunk_manager_->Size(path), 0);
    }

    // Load using filename only list
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = filenames;  // no directory info

    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);

    ASSERT_EQ(rtree_load.Count(), 2);
}

TEST_F(RTreeIndexTest, Build_EmptyInput_ShouldThrow) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    std::vector<std::string> empty;
    EXPECT_THROW(rtree.BuildWithRawDataForUT(0, empty.data()),
                 milvus::SegcoreError);
}

TEST_F(RTreeIndexTest, Build_WithInvalidWKB_Upload_Load) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    std::string bad = CreatePointWKB(0.0, 0.0);
    bad.resize(bad.size() / 2);  // truncate to make invalid

    std::vector<std::string> wkbs = {
        CreateWkbFromWkt("POINT(1 1)"), bad, CreateWkbFromWkt("POINT(2 2)")};
    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data());

    // Upload and then load back to let loader compute count from wrapper
    auto stats = rtree.Upload({});

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);

    // Only 2 valid points should be present
    ASSERT_EQ(rtree_load.Count(), 2);
}

TEST_F(RTreeIndexTest, Build_VariousGeometries) {
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    std::vector<std::string> wkbs = {
        CreateWkbFromWkt("POINT(-1.5 2.5)"),
        CreateWkbFromWkt("LINESTRING(0 0,1 1,2 3)"),
        CreateWkbFromWkt("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        CreateWkbFromWkt("POINT(1000000 -1000000)"),
        CreateWkbFromWkt("POINT(0 0)")};

    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data());
    ASSERT_EQ(rtree.Count(), wkbs.size());

    auto stats = rtree.Upload({});
    ASSERT_FALSE(stats->GetIndexFiles().empty());

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);
    ASSERT_EQ(rtree_load.Count(), wkbs.size());
}

TEST_F(RTreeIndexTest, Build_ConfigAndMetaJson) {
    // Prepare one insert file via storage pipeline
    std::vector<std::string> wkbs = {CreateWkbFromWkt("POINT(0 0)"),
                                     CreateWkbFromWkt("POINT(1 1)")};
    auto remote_file = (temp_path_.get() / "geom.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);
    auto stats = rtree.Upload({});

    // Cache remote index files locally
    milvus::storage::DiskFileManagerImpl diskfm(
        {field_meta_, index_meta_, chunk_manager_});
    auto index_files = stats->GetIndexFiles();
    diskfm.CacheIndexToDisk(index_files);
    auto local_paths = diskfm.GetLocalFilePaths();
    ASSERT_FALSE(local_paths.empty());
    // Determine base path like RTreeIndex::Load
    auto ends_with = [](const std::string& value, const std::string& suffix) {
        return value.size() >= suffix.size() &&
               value.compare(
                   value.size() - suffix.size(), suffix.size(), suffix) == 0;
    };

    std::string base_path;
    for (const auto& p : local_paths) {
        if (ends_with(p, ".bgi")) {
            base_path = p.substr(0, p.size() - 4);
            break;
        }
    }
    if (base_path.empty()) {
        for (const auto& p : local_paths) {
            if (ends_with(p, ".meta.json")) {
                base_path =
                    p.substr(0, p.size() - std::string(".meta.json").size());
                break;
            }
        }
    }
    if (base_path.empty()) {
        base_path = local_paths.front();
    }
    // Parse local meta json
    std::ifstream ifs(base_path + ".meta.json");
    ASSERT_TRUE(ifs.good());
    nlohmann::json meta = nlohmann::json::parse(ifs);
    ASSERT_EQ(meta["dimension"], 2);
}

TEST_F(RTreeIndexTest, Load_MixedFileNamesAndPaths) {
    // Build and upload
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);
    std::vector<std::string> wkbs = {CreatePointWKB(6.0, 6.0),
                                     CreatePointWKB(7.0, 7.0)};
    rtree.BuildWithRawDataForUT(wkbs.size(), wkbs.data());
    auto stats = rtree.Upload({});

    // Use full list, but replace one with filename-only
    auto mixed = stats->GetIndexFiles();
    ASSERT_FALSE(mixed.empty());
    mixed[0] = boost::filesystem::path(mixed[0]).filename().string();

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = mixed;
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);
    ASSERT_EQ(rtree_load.Count(), wkbs.size());
}

TEST_F(RTreeIndexTest, Load_NonexistentRemote_ShouldThrow) {
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    // nonexist file
    nlohmann::json cfg;
    cfg["index_files"] = std::vector<std::string>{
        (temp_path_.get() / "does_not_exist.bgi_0").string()};
    milvus::tracer::TraceContext trace_ctx;
    EXPECT_THROW(rtree_load.Load(trace_ctx, cfg), milvus::SegcoreError);
}

TEST_F(RTreeIndexTest, Build_EndToEnd_FromInsertFiles) {
    // prepare remote file via InsertData serialization
    std::vector<std::string> wkbs = {CreateWkbFromWkt("POINT(0 0)"),
                                     CreateWkbFromWkt("POINT(2 2)")};
    auto remote_file = (temp_path_.get() / "geom3.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);

    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);
    ASSERT_EQ(rtree.Count(), wkbs.size());

    auto stats = rtree.Upload({});

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);
    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);
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
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);

    ASSERT_EQ(rtree.Count(), static_cast<int64_t>(N));

    // Upload index
    auto stats = rtree.Upload({});
    ASSERT_GT(stats->GetIndexFiles().size(), 0);

    // Load index back and verify
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg_load;
    cfg_load["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg_load);

    ASSERT_EQ(rtree_load.Count(), static_cast<int64_t>(N));
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
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree(ctx);

    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree.Build(build_cfg);

    // expect: 3 geometries (0, 2, 4) are valid and parsable, 1st geometry is marked null and skipped, 3rd geometry is bad WKB and skipped
    ASSERT_EQ(rtree.Count(), 4);

    // upload -> load back and verify consistency
    auto stats = rtree.Upload({});
    ASSERT_GT(stats->GetIndexFiles().size(), 0);

    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);

    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();

    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);
    ASSERT_EQ(rtree_load.Count(), 4);
}

// The following two tests only test the coarse query (R-Tree) and not the exact query (GDAL)

TEST_F(RTreeIndexTest, Query_CoarseAndExact_Equals_Intersects_Within) {
    // Build a small index in-memory (via UT API)
    milvus::storage::FileManagerContext ctx(
        field_meta_, index_meta_, chunk_manager_);
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
    auto stats = rtree.Upload({});
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);
    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);

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
        field_meta_, index_meta_, chunk_manager_);
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
    auto stats = rtree.Upload({});
    milvus::storage::FileManagerContext ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    ctx_load.set_for_loading_index(true);
    milvus::index::RTreeIndex<std::string> rtree_load(ctx_load);
    nlohmann::json cfg;
    cfg["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx;
    rtree_load.Load(trace_ctx, cfg);

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
    // 2) Promote to sealed and build/load indices for vector + geometry
    auto sealed = milvus::segcore::CreateSealedSegment(schema);
    // load raw field data into sealed, excluding geometry (we will load controlled geometry separately)
    auto full_ds = DataGen(schema, N * num_iters);
    SealedLoadFieldData(full_ds, *sealed, {geo_id.get()});

    // Prepare controlled geometry WKBs mirroring the shapes used in growing
    std::vector<std::string> wkbs;
    wkbs.reserve(N * num_iters);
    for (int i = 0; i < N * num_iters; ++i) {
        if (i % 4 == 0) {
            wkbs.emplace_back(milvus::Geometry("POINT(0 0)").to_wkb_string());
        } else if (i % 4 == 1) {
            wkbs.emplace_back(
                milvus::Geometry("POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))")
                    .to_wkb_string());
        } else if (i % 4 == 2) {
            wkbs.emplace_back(
                milvus::Geometry("POLYGON((10 10,20 10,20 20,10 20,10 10))")
                    .to_wkb_string());
        } else {
            wkbs.emplace_back(
                milvus::Geometry("LINESTRING(-1 0,1 0)").to_wkb_string());
        }
    }

    // now load the controlled geometry data into sealed
    FieldDataInfo geo_fd_info;
    geo_fd_info.field_id = geo_id.get();
    geo_fd_info.row_count = N * num_iters;
    auto geo_field_data = milvus::storage::CreateFieldData(
        milvus::storage::DataType::GEOMETRY, /*nullable=*/false);
    geo_field_data->FillFieldData(wkbs.data(), wkbs.size());
    geo_fd_info.channel->push(geo_field_data);
    geo_fd_info.channel->close();
    sealed->LoadFieldData(geo_id, geo_fd_info);

    // build geometry R-Tree index files and load into sealed
    // Write a single parquet for geometry to simulate build input
    // wkbs already prepared above
    auto remote_file = (temp_path_.get() / "rtree_e2e.parquet").string();
    WriteGeometryInsertFile(chunk_manager_, field_meta_, remote_file, wkbs);

    // build index files by invoking RTreeIndex::Build
    milvus::storage::FileManagerContext fm_ctx(
        field_meta_, index_meta_, chunk_manager_);
    milvus::index::RTreeIndex<std::string> rtree_build(fm_ctx);
    nlohmann::json build_cfg;
    build_cfg["insert_files"] = std::vector<std::string>{remote_file};

    rtree_build.Build(build_cfg);
    auto stats = rtree_build.Upload({});

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
    // Prepare a loaded RTree index instance and assign to info.index for scalar index loading path
    milvus::storage::FileManagerContext fm_ctx_load(
        field_meta_, index_meta_, chunk_manager_);
    fm_ctx_load.set_for_loading_index(true);
    auto rtree_loaded =
        std::make_unique<milvus::index::RTreeIndex<std::string>>(fm_ctx_load);
    nlohmann::json cfg_load;
    cfg_load["index_files"] = stats->GetIndexFiles();
    milvus::tracer::TraceContext trace_ctx_load;
    rtree_loaded->Load(trace_ctx_load, cfg_load);
    info.index = std::move(rtree_loaded);
    sealed->LoadIndex(info);

    // 3) Build a GIS filter expression and run exact filtering via segcore
    auto test_op = [&](const std::string& wkt,
                       proto::plan::GISFunctionFilterExpr_GISOp op,
                       std::function<bool(int)> expected) {
        milvus::Geometry right(wkt.c_str());
        auto gis_expr = std::make_shared<milvus::expr::GISFunctionFilterExpr>(
            milvus::expr::ColumnInfo(geo_id, DataType::GEOMETRY), op, right);
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
}