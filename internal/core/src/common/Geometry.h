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

#include "ogr_geometry.h"
#include <memory>
#include <cmath>
#include "common/EasyAssert.h"

namespace milvus {

struct OGRGeometryDeleter {
    void
    operator()(OGRGeometry* ptr) const noexcept {
        if (ptr) {
            OGRGeometryFactory::destroyGeometry(ptr);
        }
    }
};

class Geometry {
 public:
    Geometry() = default;

    // all ctr assume that wkb data is valid
    explicit Geometry(const void* wkb, size_t size) {
        OGRGeometry* geometry = nullptr;
        OGRGeometryFactory::createFromWkb(wkb, nullptr, &geometry, size);

        AssertInfo(geometry != nullptr,
                   "failed to construct geometry from wkb data");
        geometry_.reset(geometry);
    }

    explicit Geometry(const char* wkt) {
        OGRGeometry* geometry = nullptr;
        OGRGeometryFactory::createFromWkt(wkt, nullptr, &geometry);
        AssertInfo(geometry != nullptr,
                   "failed to construct geometry from wkt data");
        geometry_.reset(geometry);
    }

    Geometry(const Geometry& other) {
        if (other.IsValid()) {
            this->geometry_.reset(other.geometry_->clone());
        }
    }

    Geometry(Geometry&& other) noexcept
        : geometry_(std::move(other.geometry_)) {
    }

    Geometry&
    operator=(const Geometry& other) {
        if (this != &other && other.IsValid()) {
            this->geometry_.reset(other.geometry_->clone());
        }
        return *this;
    }

    Geometry&
    operator=(Geometry&& other) noexcept {
        if (this != &other) {
            geometry_ = std::move(other.geometry_);
        }
        return *this;
    }

    ~Geometry() = default;

    bool
    IsValid() const {
        return geometry_ != nullptr;
    }

    OGRGeometry*
    GetGeometry() const {
        return geometry_.get();
    }

    //spatial relation
    bool
    equals(const Geometry& other) const {
        return geometry_->Equals(other.geometry_.get());
    }

    bool
    touches(const Geometry& other) const {
        return geometry_->Touches(other.geometry_.get());
    }

    bool
    overlaps(const Geometry& other) const {
        return geometry_->Overlaps(other.geometry_.get());
    }

    bool
    crosses(const Geometry& other) const {
        return geometry_->Crosses(other.geometry_.get());
    }

    bool
    contains(const Geometry& other) const {
        return geometry_->Contains(other.geometry_.get());
    }

    bool
    intersects(const Geometry& other) const {
        return geometry_->Intersects(other.geometry_.get());
    }

    bool
    within(const Geometry& other) const {
        return geometry_->Within(other.geometry_.get());
    }

    // RangeWithin implementation supporting all geometry types
    // Note: other geometry is always a POINT (query point)
    bool
    dwithin(const Geometry& other, double distance) const {
        AssertInfo(other.geometry_->getGeometryType() == wkbPoint,
                   "other geometry is not a point");
        // Step 1: Extract query point coordinates (other is always a point)
        OGRPoint* query_point = static_cast<OGRPoint*>(other.geometry_.get());
        double query_lon = query_point->getX();  // longitude
        double query_lat = query_point->getY();  // latitude

        // Step 2: Special case for point-to-point using Haversine formula (most accurate)
        if (geometry_->getGeometryType() == wkbPoint) {
            OGRPoint* data_point = static_cast<OGRPoint*>(geometry_.get());
            double data_lon = data_point->getX();  // longitude
            double data_lat = data_point->getY();  // latitude

            double actual_distance = haversine_distance_meters(
                data_lat, data_lon, query_lat, query_lon);
            return actual_distance <= distance;
        }

        // Step 3: For other geometry types, use OGR Distance (returns degrees)
        double degrees_distance = geometry_->Distance(other.geometry_.get());

        // Step 4: Convert degrees distance to meters using query point location
        // Use the query point's latitude as reference for unit conversion
        double distance_in_meters =
            degrees_to_meters_at_location(degrees_distance, query_lat);

        // Step 5: Compare with given distance in meters
        return distance_in_meters <= distance;
    }

 private:
    // Convert degrees distance to meters using approximate location
    // This approximates the degrees distance returned by OGR to meters
    static double
    degrees_to_meters_at_location(double degrees_distance, double center_lat) {
        const double metersPerDegreeLat = 111320.0;

        // For small distances, approximate using latitude-adjusted conversion
        // This is a rough approximation since degrees_distance could be in any direction
        double latRad = center_lat * 3.14159265358979323846 / 180.0;
        double avgMetersPerDegree =
            metersPerDegreeLat *
            std::sqrt((1.0 + std::cos(latRad) * std::cos(latRad)) / 2.0);

        return degrees_distance * avgMetersPerDegree;
    }

    // Haversine formula to calculate great-circle distance between two points on Earth
    // Input: latitude and longitude in degrees
    // Output: distance in meters
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
    std::string
    to_wkt_string() const {
        return geometry_->exportToWkt();
    }

    // used for test
    std::string
    to_wkb_string() const {
        std::unique_ptr<unsigned char[]> wkb(
            new unsigned char[geometry_->WkbSize()]);
        geometry_->exportToWkb(wkbNDR, wkb.get());
        return std::string(reinterpret_cast<const char*>(wkb.get()),
                           geometry_->WkbSize());
    }

 private:
    std::unique_ptr<OGRGeometry, OGRGeometryDeleter> geometry_;
};

}  // namespace milvus