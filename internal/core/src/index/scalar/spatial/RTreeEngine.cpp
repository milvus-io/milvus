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

#include "index/scalar/spatial/RTreeEngine.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <tuple>
#include <utility>

#include <boost/geometry/index/predicates.hpp>
#include <boost/geometry/index/detail/rtree/utilities/statistics.hpp>
#include <boost/geometry/index/detail/rtree/utilities/view.hpp>

#include "common/EasyAssert.h"
#include "index/scalar/spatial/RTreeSerialization.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

class GeometryGuard {
 public:
    GeometryGuard(GEOSContextHandle_t context, GEOSGeometry* geometry)
        : context_(context), geometry_(geometry) {
    }

    GeometryGuard(const GeometryGuard&) = delete;
    GeometryGuard&
    operator=(const GeometryGuard&) = delete;

    ~GeometryGuard() {
        if (geometry_ != nullptr) {
            GEOSGeom_destroy_r(context_, geometry_);
        }
    }

    GEOSGeometry*
    Get() const {
        return geometry_;
    }

 private:
    GEOSContextHandle_t context_;
    GEOSGeometry* geometry_;
};

std::optional<rtree_detail::Box>
BoundingBox(const GEOSGeometry* geometry, GEOSContextHandle_t context) {
    if (geometry == nullptr || context == nullptr ||
        GEOSisEmpty_r(context, geometry) != 0) {
        return std::nullopt;
    }

    double min_x = 0;
    double min_y = 0;
    double max_x = 0;
    double max_y = 0;
    if (GEOSGeom_getXMin_r(context, geometry, &min_x) != 1 ||
        GEOSGeom_getYMin_r(context, geometry, &min_y) != 1 ||
        GEOSGeom_getXMax_r(context, geometry, &max_x) != 1 ||
        GEOSGeom_getYMax_r(context, geometry, &max_y) != 1 ||
        !std::isfinite(min_x) || !std::isfinite(min_y) ||
        !std::isfinite(max_x) || !std::isfinite(max_y) || min_x > max_x ||
        min_y > max_y) {
        return std::nullopt;
    }
    return rtree_detail::Box(rtree_detail::Point(min_x, min_y),
                             rtree_detail::Point(max_x, max_y));
}

}  // namespace

RTreeBuildEngine::RTreeBuildEngine(std::string index_path)
    : index_path_(std::move(index_path)), geos_context_(GEOS_init_r()) {
    if (geos_context_ == nullptr) {
        ThrowInfo(UnexpectedError,
                  "failed to initialize GEOS for R-Tree build");
    }
    wkb_reader_ = GEOSWKBReader_create_r(geos_context_);
    if (wkb_reader_ == nullptr) {
        GEOS_finish_r(geos_context_);
        geos_context_ = nullptr;
        ThrowInfo(UnexpectedError,
                  "failed to create GEOS WKB reader for R-Tree build");
    }
}

RTreeBuildEngine::~RTreeBuildEngine() {
    if (wkb_reader_ != nullptr) {
        GEOSWKBReader_destroy_r(geos_context_, wkb_reader_);
    }
    if (geos_context_ != nullptr) {
        GEOS_finish_r(geos_context_);
    }
}

void
RTreeBuildEngine::AddGeometry(const uint8_t* wkb,
                              size_t len,
                              int64_t row_offset) {
    AssertInfo(!finished_, "cannot add geometry after R-Tree Finish");
    AssertInfo(row_offset >= 0,
               "R-Tree row coordinate must not be negative: {}",
               row_offset);
    AssertInfo(len == 0 || wkb != nullptr,
               "R-Tree received null WKB with non-zero length");
    if (len == 0) {
        return;
    }

    GeometryGuard geometry(
        geos_context_,
        GEOSWKBReader_read_r(geos_context_, wkb_reader_, wkb, len));
    if (geometry.Get() == nullptr) {
        return;
    }
    auto box = BoundingBox(geometry.Get(), geos_context_);
    if (!box.has_value()) {
        return;
    }
    values_.emplace_back(std::move(*box), row_offset);
}

void
RTreeBuildEngine::BulkLoad(std::vector<rtree_detail::Value> values) {
    AssertInfo(!finished_, "cannot bulk-load geometry after R-Tree Finish");
    for (const auto& value : values) {
        AssertInfo(value.second >= 0,
                   "R-Tree row coordinate must not be negative: {}",
                   value.second);
    }
    values_ = std::move(values);
}

void
RTreeBuildEngine::Finish() {
    if (finished_) {
        return;
    }
    if (index_path_.empty()) {
        ThrowInfo(FileCreateFailed, "R-Tree index path is empty");
    }
    const auto parent = std::filesystem::path(index_path_).parent_path();
    std::error_code error;
    if (!parent.empty()) {
        std::filesystem::create_directories(parent, error);
        if (error) {
            ThrowInfo(FileCreateFailed,
                      "failed to create R-Tree staging directory {}: {}",
                      parent.string(),
                      error.message());
        }
    }

    rtree_ = rtree_detail::RTree(values_.begin(), values_.end());
    RTreeSerializer::saveBinary(rtree_, index_path_ + ".bgi");

    nlohmann::json metadata;
    metadata["dimension"] = dimension_;
    metadata["count"] = static_cast<uint64_t>(rtree_.size());
    const auto metadata_path = index_path_ + ".meta.json";
    std::ofstream output(metadata_path, std::ios::trunc);
    if (!output.is_open()) {
        ThrowInfo(FileOpenFailed,
                  "failed to open R-Tree metadata for writing: {}",
                  metadata_path);
    }
    output << metadata.dump();
    output.flush();
    if (!output.good()) {
        ThrowInfo(FileWriteFailed,
                  "failed to write R-Tree metadata {}",
                  metadata_path);
    }
    output.close();
    if (output.fail()) {
        ThrowInfo(FileWriteFailed,
                  "failed to close R-Tree metadata {}",
                  metadata_path);
    }

    std::vector<rtree_detail::Value>().swap(values_);
    finished_ = true;
}

std::vector<rtree_detail::Value>
RTreeBuildEngine::TakeValues() && {
    AssertInfo(!finished_, "cannot consume values after R-Tree Finish");
    return std::move(values_);
}

int64_t
RTreeBuildEngine::Count() const {
    const auto count = finished_ ? rtree_.size() : values_.size();
    AssertInfo(
        count <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "R-Tree entry count exceeds int64 domain");
    return static_cast<int64_t>(count);
}

const std::string&
RTreeBuildEngine::IndexPath() const {
    return index_path_;
}

RTreeQueryEngine::RTreeQueryEngine(std::string index_path)
    : index_path_(std::move(index_path)) {
}

RTreeQueryEngine::RTreeQueryEngine(std::vector<rtree_detail::Value> values)
    : rtree_(values.begin(), values.end()) {
    UpdateHeapBytes();
}

std::shared_ptr<const RTreeQueryEngine>
RTreeQueryEngine::Create(std::vector<rtree_detail::Value> values) {
    return std::shared_ptr<const RTreeQueryEngine>(
        new RTreeQueryEngine(std::move(values)));
}

RTreeQueryEngine::~RTreeQueryEngine() = default;

void
RTreeQueryEngine::Load() {
    const auto metadata_path = index_path_ + ".meta.json";
    std::ifstream metadata(metadata_path);
    if (metadata.good()) {
        try {
            const auto value = nlohmann::json::parse(metadata);
            if (value.contains("dimension")) {
                dimension_ = value.at("dimension").get<uint32_t>();
            }
        } catch (const nlohmann::json::exception&) {
            // Optional legacy metadata is advisory for this fixed 2D tree.
            dimension_ = 2;
        }
    }
    RTreeSerializer::loadBinary(rtree_, index_path_ + ".bgi");

    UpdateHeapBytes();
    std::string().swap(index_path_);
}

void
RTreeQueryEngine::UpdateHeapBytes() {
    using View = boost::geometry::index::detail::rtree::utilities::view<
        rtree_detail::RTree>;
    using Node = typename View::members_holder::node;
    const auto statistics =
        boost::geometry::index::detail::rtree::utilities::statistics(rtree_);
    const auto internal_nodes = std::get<1>(statistics);
    const auto leaf_nodes = std::get<2>(statistics);
    if (internal_nodes > std::numeric_limits<size_t>::max() - leaf_nodes) {
        heap_bytes_ = std::numeric_limits<int64_t>::max();
    } else {
        const auto node_count = internal_nodes + leaf_nodes;
        constexpr auto kObjectBytes = sizeof(RTreeQueryEngine);
        if (node_count >
            static_cast<size_t>(
                (std::numeric_limits<int64_t>::max() - kObjectBytes) /
                sizeof(Node))) {
            heap_bytes_ = std::numeric_limits<int64_t>::max();
        } else {
            heap_bytes_ =
                static_cast<int64_t>(kObjectBytes + node_count * sizeof(Node));
        }
    }
}

bool
RTreeQueryEngine::QueryCandidates(
    const GEOSGeometry* query_geom,
    GEOSContextHandle_t ctx,
    std::vector<int64_t>& candidate_offsets) const {
    candidate_offsets.clear();
    return ForEachCandidate(query_geom, ctx, [&](int64_t offset) {
        candidate_offsets.push_back(offset);
    });
}

bool
RTreeQueryEngine::ForEachCandidate(
    const GEOSGeometry* query_geom,
    GEOSContextHandle_t ctx,
    const std::function<void(int64_t)>& visitor) const {
    AssertInfo(static_cast<bool>(visitor),
               "R-Tree candidate visitor must not be empty");
    auto box = BoundingBox(query_geom, ctx);
    if (!box.has_value()) {
        return false;
    }

    const auto predicate = rtree_detail::bgi::intersects(*box);
    for (auto it = rtree_.qbegin(predicate); it != rtree_.qend(); ++it) {
        visitor(it->second);
    }
    return true;
}

int64_t
RTreeQueryEngine::Count() const {
    AssertInfo(rtree_.size() <=
                   static_cast<size_t>(std::numeric_limits<int64_t>::max()),
               "R-Tree entry count exceeds int64 domain");
    return static_cast<int64_t>(rtree_.size());
}

void
RTreeQueryEngine::ValidateCoordinates(
    int64_t total_num_rows, const std::vector<size_t>& null_offsets) const {
    if (total_num_rows < 0) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree coordinate count must not be negative");
    }
    for (const auto& value : rtree_) {
        if (value.second < 0 || value.second >= total_num_rows) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree row coordinate {} is outside [0, {})",
                      value.second,
                      total_num_rows);
        }
        if (std::binary_search(null_offsets.begin(),
                               null_offsets.end(),
                               static_cast<size_t>(value.second))) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree row coordinate {} is both indexed and null",
                      value.second);
        }
    }
}

int64_t
RTreeQueryEngine::ByteSize() const {
    return heap_bytes_;
}

void
RTreeQueryEngine::AppendValues(
    std::vector<rtree_detail::Value>& output) const {
    if (rtree_.size() > std::numeric_limits<size_t>::max() - output.size()) {
        ThrowInfo(UnexpectedError,
                  "R-Tree compaction value count overflows size_t");
    }
    output.reserve(output.size() + rtree_.size());
    output.insert(output.end(), rtree_.begin(), rtree_.end());
}

}  // namespace milvus::index
