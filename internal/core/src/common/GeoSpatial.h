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

#include <gdal.h>
#include <ogr_geometry.h>
#include <string_view>
#include "common/EasyAssert.h"

namespace milvus {

class GeoSpatial {
 public:
    GeoSpatial() = default;

    // all ctr assume that wkb data is valid
    explicit GeoSpatial(const void* wkb, size_t size) {
        OGRGeometryFactory::createFromWkb(wkb, nullptr, &geometry_, size);
        AssertInfo(geometry_ != nullptr,
                   "failed to construct geometry from wkb data");
        to_wkb_internal();
    }

    GeoSpatial(const GeoSpatial& other) {
        if (other.IsValid()) {
            this->geometry_ = other.geometry_->clone();
            this->to_wkb_internal();
        }
    }

    GeoSpatial(GeoSpatial&& other) noexcept
        : wkb_data_(std::move(other.wkb_data_)),
          size_(std::move(other.size_)),
          geometry_(std::move(other.geometry_)) {
    }

    GeoSpatial&
    operator=(const GeoSpatial& other) {
        if (this != &other && other.IsValid()) {
            this->geometry_ = other.geometry_->clone();
            this->to_wkb_internal();
        }
        return *this;
    }

    GeoSpatial&
    operator=(GeoSpatial&& other) noexcept {
        if (this != &other) {
            wkb_data_ = std::move(other.wkb_data_);
            size_ = std::move(other.size_);
            geometry_ = std::move(other.geometry_);
        }
        return *this;
    }

    operator std::string() const {
        //tmp string created by copy ctr
        return std::string(reinterpret_cast<const char*>(wkb_data_));
    }

    operator std::string_view() const {
        return std::string_view(reinterpret_cast<const char*>(wkb_data_),
                                size_);
    }

    ~GeoSpatial() {
        if (geometry_) {
            OGRGeometryFactory::destroyGeometry(geometry_);
        }
        if (wkb_data_) {  // the data
            delete[] wkb_data_;
        }
    }

    bool
    IsValid() const {
        return geometry_ != nullptr;
    }

    OGRGeometry*
    GetGeometry() const {
        return geometry_;
    }

    const unsigned char*
    data() const {
        return wkb_data_;
    }

    size_t
    size() const {
        return size_;
    }

    //spatial relation
    bool
    equals(const GeoSpatial& other) const {
        return geometry_->Equals(other.geometry_);
    }

    bool
    touches(const GeoSpatial& other) const {
        return geometry_->Touches(other.geometry_);
    }

    bool
    overlaps(const GeoSpatial& other) const {
        return geometry_->Overlaps(other.geometry_);
    }

    bool
    crosses(const GeoSpatial& other) const {
        return geometry_->Crosses(other.geometry_);
    }

    bool
    contains(const GeoSpatial& other) const {
        return geometry_->Contains(other.geometry_);
    }

    bool
    intersects(const GeoSpatial& other) const {
        return geometry_->Intersects(other.geometry_);
    }

    bool
    within(const GeoSpatial& other) const {
        return geometry_->Within(other.geometry_);
    }

 private:
    inline void
    to_wkb_internal() {
        if (geometry_ && size_ == 0) {
            size_ = geometry_->WkbSize();
            wkb_data_ = new unsigned char[size_];
            // little-endian order to save wkb
            geometry_->exportToWkb(wkbNDR, wkb_data_);
        }
    }

    //read write ptr, use to save a OGRGeometry object when need to create new storage file
    unsigned char* wkb_data_{nullptr};
    size_t size_{0};
    OGRGeometry* geometry_{nullptr};
};

}  // namespace milvus