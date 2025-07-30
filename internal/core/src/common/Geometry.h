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
#include <string_view>
#include "common/EasyAssert.h"

namespace milvus {

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
        to_wkb_internal();
    }

    explicit Geometry(const char* wkt) {
        OGRGeometry* geometry = nullptr;
        OGRGeometryFactory::createFromWkt(wkt, nullptr, &geometry);
        AssertInfo(geometry != nullptr,
                   "failed to construct geometry from wkt data");
        geometry_.reset(geometry);
        to_wkb_internal();
    }

    Geometry(const Geometry& other) {
        if (other.IsValid()) {
            this->geometry_.reset(other.geometry_->clone());
            this->to_wkb_internal();
        }
    }

    Geometry(Geometry&& other) noexcept
        : wkb_data_(std::move(other.wkb_data_)),
          size_(other.size_),
          geometry_(std::move(other.geometry_)) {
    }

    Geometry&
    operator=(const Geometry& other) {
        if (this != &other && other.IsValid()) {
            this->geometry_.reset(other.geometry_->clone());
            this->to_wkb_internal();
        }
        return *this;
    }

    Geometry&
    operator=(Geometry&& other) noexcept {
        if (this != &other) {
            wkb_data_ = std::move(other.wkb_data_);
            size_ = std::move(other.size_);
            geometry_ = std::move(other.geometry_);
        }
        return *this;
    }

    operator std::string() const {
        //tmp string created by copy ctr
        return std::string(reinterpret_cast<const char*>(wkb_data_.get()),
                           size_);
    }

    operator std::string_view() const {
        return std::string_view(reinterpret_cast<const char*>(wkb_data_.get()),
                                size_);
    }

    ~Geometry() {
        if (geometry_) {
            OGRGeometryFactory::destroyGeometry(geometry_.release());
        }
    }

    bool
    IsValid() const {
        return geometry_ != nullptr;
    }

    OGRGeometry*
    GetGeometry() const {
        return geometry_.get();
    }

    const unsigned char*
    data() const {
        return wkb_data_.get();
    }

    size_t
    size() const {
        return size_;
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

    std::string
    to_wkt_string() const {
        return geometry_->exportToWkt();
    }

    std::string
    to_wkb_string() const {
        return std::string(reinterpret_cast<const char*>(wkb_data_.get()),
                           size_);
    }

 private:
    inline void
    to_wkb_internal() {
        if (geometry_) {
            size_ = geometry_->WkbSize();
            wkb_data_ = std::make_unique<unsigned char[]>(size_);
            // little-endian order to save wkb
            geometry_->exportToWkb(wkbNDR, wkb_data_.get());
        }
    }

    std::unique_ptr<unsigned char[]> wkb_data_;
    size_t size_{0};
    std::unique_ptr<OGRGeometry> geometry_;
};

}  // namespace milvus