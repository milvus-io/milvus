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

#include "common/EasyAssert.h"
#include "log/Log.h"
#include "ogr_geometry.h"
#include "pb/plan.pb.h"
#include <filesystem>
#include <fstream>
#include <mutex>
#include <nlohmann/json.hpp>
#include "common/FieldDataInterface.h"
#include "RTreeIndexWrapper.h"
#include "RTreeIndexSerialization.h"

namespace milvus::index {

RTreeIndexWrapper::RTreeIndexWrapper(std::string& path, bool is_build_mode)
    : index_path_(path), is_build_mode_(is_build_mode) {
    if (is_build_mode_) {
        std::filesystem::path dir_path =
            std::filesystem::path(path).parent_path();
        if (!dir_path.empty()) {
            std::filesystem::create_directories(dir_path);
        }
        // Start with an empty rtree for dynamic insertions
        rtree_ = RTree();
    }
}

RTreeIndexWrapper::~RTreeIndexWrapper() = default;

void
RTreeIndexWrapper::add_geometry(const uint8_t* wkb_data,
                                size_t len,
                                int64_t row_offset) {
    // Acquire write lock to protect rtree_
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);

    AssertInfo(is_build_mode_, "Cannot add geometry in load mode");

    // Parse WKB data to OGR geometry
    OGRGeometry* geom = nullptr;
    OGRErr err =
        OGRGeometryFactory::createFromWkb(wkb_data, nullptr, &geom, len);

    if (err != OGRERR_NONE || geom == nullptr) {
        LOG_ERROR("Failed to parse WKB data for row {}", row_offset);
        return;
    }

    // Get bounding box
    double minX, minY, maxX, maxY;
    get_bounding_box(geom, minX, minY, maxX, maxY);

    // Create Boost box and insert
    Box box(Point(minX, minY), Point(maxX, maxY));
    Value val(box, row_offset);
    values_.push_back(val);
    rtree_.insert(val);

    // Clean up
    OGRGeometryFactory::destroyGeometry(geom);
}

// No IDataStream; bulk-load implemented directly for Boost R-tree

void
RTreeIndexWrapper::bulk_load_from_field_data(
    const std::vector<std::shared_ptr<::milvus::FieldDataBase>>& field_datas,
    bool nullable) {
    // Acquire write lock to protect rtree_ creation and modification
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);

    AssertInfo(is_build_mode_, "Cannot bulk load in load mode");

    std::vector<Value> local_values;
    local_values.reserve(1024);
    int64_t absolute_offset = 0;
    for (const auto& fd : field_datas) {
        const auto n = fd->get_num_rows();
        for (int64_t i = 0; i < n; ++i, ++absolute_offset) {
            const bool is_nullable_effective = nullable || fd->IsNullable();
            if (is_nullable_effective && !fd->is_valid(i)) {
                continue;
            }
            const auto* wkb_str =
                static_cast<const std::string*>(fd->RawValue(i));
            if (wkb_str == nullptr || wkb_str->empty()) {
                continue;
            }
            OGRGeometry* geom = nullptr;
            auto err = OGRGeometryFactory::createFromWkb(
                reinterpret_cast<const uint8_t*>(wkb_str->data()),
                nullptr,
                &geom,
                wkb_str->size());
            if (err != OGRERR_NONE || geom == nullptr) {
                continue;
            }
            OGREnvelope env;
            geom->getEnvelope(&env);
            OGRGeometryFactory::destroyGeometry(geom);
            Box box(Point(env.MinX, env.MinY), Point(env.MaxX, env.MaxY));
            local_values.emplace_back(box, absolute_offset);
        }
    }
    values_.swap(local_values);
    rtree_ = RTree(values_.begin(), values_.end());
    LOG_INFO("R-Tree bulk load (Boost) completed with {} entries",
             values_.size());
}

void
RTreeIndexWrapper::finish() {
    // Acquire write lock to protect rtree_ modification and cleanup
    // Guard against repeated invocations which could otherwise attempt to
    // release resources multiple times (e.g. BuildWithRawDataForUT() calls
    // finish(), and Upload() may call it again).
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);
    if (finished_) {
        LOG_DEBUG("RTreeIndexWrapper::finish() called more than once, skip.");
        return;
    }

    AssertInfo(is_build_mode_, "Cannot finish in load mode");

    // Persist to disk: write meta and binary data file
    try {
        // Write binary rtree data
        RTreeSerializer::saveBinary(rtree_, index_path_ + ".bgi");

        // Write meta json
        nlohmann::json meta;
        // index/leaf capacities are not used in Boost implementation
        meta["dimension"] = dimension_;
        meta["count"] = static_cast<uint64_t>(values_.size());

        std::ofstream ofs(index_path_ + ".meta.json", std::ios::trunc);
        ofs << meta.dump();
        ofs.close();
        LOG_INFO("R-Tree meta written: {}.meta.json", index_path_);
    } catch (const std::exception& e) {
        LOG_WARN("Failed to write R-Tree files: {}", e.what());
    }

    finished_ = true;

    LOG_INFO("R-Tree index (Boost) finished building and saved to {}",
             index_path_);
}

void
RTreeIndexWrapper::load() {
    // Acquire write lock to protect rtree_ initialization during loading
    std::unique_lock<std::shared_mutex> guard(rtree_mutex_);

    AssertInfo(!is_build_mode_, "Cannot load in build mode");

    try {
        // Read meta (optional)
        try {
            std::ifstream ifs(index_path_ + ".meta.json");
            if (ifs.good()) {
                auto meta = nlohmann::json::parse(ifs);
                // index/leaf capacities are ignored for Boost implementation
                if (meta.contains("dimension"))
                    dimension_ = meta["dimension"].get<uint32_t>();
            }
        } catch (const std::exception& e) {
            LOG_WARN("Failed to read meta json: {}", e.what());
        }

        // Read binary data
        RTreeSerializer::loadBinary(rtree_, index_path_ + ".bgi");

        LOG_INFO("R-Tree index (Boost) loaded from {}", index_path_);
    } catch (const std::exception& e) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("Failed to load R-Tree index from {}: {}",
                              index_path_,
                              e.what()));
    }
}

void
RTreeIndexWrapper::query_candidates(proto::plan::GISFunctionFilterExpr_GISOp op,
                                    const OGRGeometry* query_geom,
                                    std::vector<int64_t>& candidate_offsets) {
    candidate_offsets.clear();

    // Get bounding box of query geometry
    double minX, minY, maxX, maxY;
    get_bounding_box(query_geom, minX, minY, maxX, maxY);

    // Create query box
    Box query_box(Point(minX, minY), Point(maxX, maxY));

    // Perform coarse intersection query
    std::vector<Value> results;
    {
        std::shared_lock<std::shared_mutex> guard(rtree_mutex_);
        rtree_.query(boost::geometry::index::intersects(query_box),
                     std::back_inserter(results));
    }
    candidate_offsets.reserve(results.size());
    for (const auto& v : results) {
        candidate_offsets.push_back(v.second);
    }

    LOG_DEBUG("R-Tree query returned {} candidates for operation {}",
              candidate_offsets.size(),
              static_cast<int>(op));
}

void
RTreeIndexWrapper::get_bounding_box(const OGRGeometry* geom,
                                    double& minX,
                                    double& minY,
                                    double& maxX,
                                    double& maxY) {
    AssertInfo(geom != nullptr, "Geometry is null");

    OGREnvelope env;
    geom->getEnvelope(&env);

    minX = env.MinX;
    minY = env.MinY;
    maxX = env.MaxX;
    maxY = env.MaxY;
}

int64_t
RTreeIndexWrapper::count() const {
    return static_cast<int64_t>(rtree_.size());
}

// index/leaf capacity setters removed; not applicable for Boost rtree
}  // namespace milvus::index