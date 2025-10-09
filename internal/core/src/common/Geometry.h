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
#pragma once

#include <geos_c.h>
#include <memory>
#include <cmath>
#include <string>
#include "common/EasyAssert.h"

namespace milvus {

class Geometry {
 public:
    // Default constructor creates invalid geometry
    Geometry() : geometry_(nullptr), ctx_(nullptr) {
    }

    ~Geometry() {
        if (geometry_ != nullptr) {
            GEOSGeom_destroy_r(ctx_, geometry_);
        }
    }

    // Constructor from WKB data
    explicit Geometry(GEOSContextHandle_t ctx, const void* wkb, size_t size)
        : ctx_(ctx) {
        GEOSWKBReader* reader = GEOSWKBReader_create_r(ctx);
        AssertInfo(reader != nullptr, "Failed to create GEOS WKB reader");

        GEOSGeometry* geom = GEOSWKBReader_read_r(
            ctx, reader, static_cast<const unsigned char*>(wkb), size);
        GEOSWKBReader_destroy_r(ctx, reader);

        AssertInfo(geom != nullptr,
                   "Failed to construct geometry from WKB data");
        geometry_ = geom;
    }

    // Constructor from WKT string
    explicit Geometry(GEOSContextHandle_t ctx, const char* wkt) : ctx_(ctx) {
        GEOSWKTReader* reader = GEOSWKTReader_create_r(ctx);
        AssertInfo(reader != nullptr, "Failed to create GEOS WKT reader");

        GEOSGeometry* geom = GEOSWKTReader_read_r(ctx, reader, wkt);
        GEOSWKTReader_destroy_r(ctx, reader);

        AssertInfo(geom != nullptr,
                   "Failed to construct geometry from WKT data");
        geometry_ = geom;
    }

    // Copy assignment
    Geometry&
    operator=(const Geometry& other) {
        if (this != &other) {
            geometry_ = other.geometry_;
            ctx_ = other.ctx_;
        }
        return *this;
    }

    // Copy constructor with context (for cloning)
    Geometry(const Geometry& other) : ctx_(other.ctx_) {
        if (other.IsValid()) {
            GEOSGeometry* cloned =
                GEOSGeom_clone_r(other.ctx_, other.geometry_);
            AssertInfo(cloned != nullptr, "Failed to clone geometry");
            geometry_ = cloned;
        } else {
            geometry_ = nullptr;
        }
    }

    bool
    IsValid() const {
        return geometry_ != nullptr;
    }

    // Get raw GEOS geometry pointer (for cache management)
    GEOSGeometry*
    GetRawGeometry() const {
        return geometry_;
    }

    GEOSGeometry*
    GetGeometry() const {
        return geometry_;
    }

    // Spatial relation operations using GEOS API
    bool
    equals(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSEquals_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    bool
    touches(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSTouches_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    bool
    overlaps(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSOverlaps_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    bool
    crosses(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSCrosses_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    bool
    contains(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSContains_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    bool
    intersects(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSIntersects_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    bool
    within(const Geometry& other) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }
        char result = GEOSWithin_r(ctx_, geometry_, other.geometry_);
        return result == 1;
    }

    // Distance within check using GEOS distance calculation
    bool
    dwithin(const Geometry& other, double distance) const {
        if (!IsValid() || !other.IsValid()) {
            return false;
        }

        // Get geometry types
        int thisType = GEOSGeomTypeId_r(ctx_, geometry_);
        int otherType = GEOSGeomTypeId_r(ctx_, other.geometry_);

        // Ensure other geometry is a point
        AssertInfo(otherType == GEOS_POINT, "other geometry is not a point");

        // For point-to-point, use Haversine formula for accuracy
        if (thisType == GEOS_POINT) {
            double thisX, thisY, otherX, otherY;
            if (GEOSGeomGetX_r(ctx_, geometry_, &thisX) == 1 &&
                GEOSGeomGetY_r(ctx_, geometry_, &thisY) == 1 &&
                GEOSGeomGetX_r(ctx_, other.geometry_, &otherX) == 1 &&
                GEOSGeomGetY_r(ctx_, other.geometry_, &otherY) == 1) {
                double actual_distance =
                    haversine_distance_meters(thisY, thisX, otherY, otherX);
                return actual_distance <= distance;
            }
        }

        // For other geometry types, use GEOS distance (in degrees)
        double geos_distance;
        if (GEOSDistance_r(ctx_, geometry_, other.geometry_, &geos_distance) ==
            1) {
            // Get query point coordinates for conversion reference
            double query_lat, query_lon;
            if (GEOSGeomGetX_r(ctx_, other.geometry_, &query_lon) == 1 &&
                GEOSGeomGetY_r(ctx_, other.geometry_, &query_lat) == 1) {
                double distance_in_meters =
                    degrees_to_meters_at_location(geos_distance, query_lat);
                return distance_in_meters <= distance;
            }
        }

        return false;
    }

 private:
    // Convert degrees distance to meters using approximate location
    static double
    degrees_to_meters_at_location(double degrees_distance, double center_lat) {
        const double metersPerDegreeLat = 111320.0;

        // For small distances, approximate using latitude-adjusted conversion
        double latRad = center_lat * 3.14159265358979323846 / 180.0;
        double avgMetersPerDegree =
            metersPerDegreeLat *
            std::sqrt((1.0 + std::cos(latRad) * std::cos(latRad)) / 2.0);

        return degrees_distance * avgMetersPerDegree;
    }

    // Haversine formula to calculate great-circle distance between two points on Earth
    static double
    haversine_distance_meters(double lat1,
                              double lon1,
                              double lat2,
                              double lon2) {
        const double R = 6371000.0;  // Earth's radius in meters
        const double PI = 3.14159265358979323846;

        // Convert degrees to radians
        double lat1_rad = lat1 * PI / 180.0;
        double lon1_rad = lon1 * PI / 180.0;
        double lat2_rad = lat2 * PI / 180.0;
        double lon2_rad = lon2 * PI / 180.0;

        // Haversine formula
        double dlat = lat2_rad - lat1_rad;
        double dlon = lon2_rad - lon1_rad;

        double a = std::sin(dlat / 2.0) * std::sin(dlat / 2.0) +
                   std::cos(lat1_rad) * std::cos(lat2_rad) *
                       std::sin(dlon / 2.0) * std::sin(dlon / 2.0);
        double c = 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));

        return R * c;  // Distance in meters
    }

 public:
    // Export to WKT string
    std::string
    to_wkt_string() const {
        if (!IsValid()) {
            return "";
        }

        GEOSWKTWriter* writer = GEOSWKTWriter_create_r(ctx_);
        AssertInfo(writer != nullptr, "Failed to create GEOS WKT writer");

        char* wkt = GEOSWKTWriter_write_r(ctx_, writer, geometry_);
        GEOSWKTWriter_destroy_r(ctx_, writer);

        if (!wkt) {
            return "";
        }

        std::string result(wkt);
        GEOSFree_r(ctx_, wkt);
        return result;
    }

    // Export to WKB string (for test)
    std::string
    to_wkb_string() const {
        if (!IsValid()) {
            return "";
        }

        GEOSWKBWriter* writer = GEOSWKBWriter_create_r(ctx_);
        AssertInfo(writer != nullptr, "Failed to create GEOS WKB writer");

        size_t size;
        unsigned char* wkb =
            GEOSWKBWriter_write_r(ctx_, writer, geometry_, &size);
        GEOSWKBWriter_destroy_r(ctx_, writer);

        if (!wkb) {
            ThrowInfo(UnexpectedError, "Failed to create GEOS WKB writer");
        }

        std::string result(reinterpret_cast<const char*>(wkb), size);
        GEOSFree_r(ctx_, wkb);
        return result;
    }

 private:
    GEOSGeometry* geometry_;  // Raw pointer, managed by cache
    GEOSContextHandle_t ctx_;
};

}  // namespace milvus