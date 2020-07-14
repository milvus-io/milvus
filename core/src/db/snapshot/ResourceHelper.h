// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <string>

#include "db/SnapshotVisitor.h"
#include "utils/Status.h"

namespace milvus::engine::snapshot {

static const char* COLLECTION_PREFIX = "C_";
static const char* PARTITION_PREFIX = "P_";
static const char* SEGMENT_PREFIX = "S_";
static const char* SEGMENT_FILE_PREFIX = "F_";

template <class ResourceT>
inline Status
GetResPath(std::string& path, const std::string& root, const typename ResourceT::Ptr& res_ptr) {
    if (res_ptr == nullptr) {
        return Status(DB_ERROR, "NULL resource");
    }
    return Status::OK();
}

template <>
inline Status
GetResPath<Collection>(std::string& path, const std::string& root, const Collection::Ptr& res_ptr) {
    if (res_ptr == nullptr) {
        return Status(DB_ERROR, "NULL collection");
    }
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetID();

    path = ss.str();
    return Status::OK();
}

template <>
inline Status
GetResPath<Partition>(std::string& path, const std::string& root, const Partition::Ptr& res_ptr) {
    if (res_ptr == nullptr) {
        return Status(DB_ERROR, "NULL partition");
    }
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetID();

    path = ss.str();
    return Status::OK();
}

template <>
inline Status
GetResPath<Segment>(std::string& path, const std::string& root, const Segment::Ptr& res_ptr) {
    if (res_ptr == nullptr) {
        return Status(DB_ERROR, "NULL segment");
    }
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetPartitionId() << "/";
    ss << SEGMENT_PREFIX << res_ptr->GetID();

    path = ss.str();
    return Status::OK();
}

template <>
inline Status
GetResPath<SegmentFile>(std::string& path, const std::string& root, const SegmentFile::Ptr& res_ptr) {
    if (res_ptr == nullptr) {
        return Status(DB_ERROR, "NULL segment file");
    }
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetPartitionId() << "/";
    ss << SEGMENT_PREFIX << res_ptr->GetSegmentId() << "/";
    ss << SEGMENT_FILE_PREFIX << res_ptr->GetID();

    path = ss.str();
    return Status::OK();
}

inline Status
GetResPath(std::string& path, const std::string& root, const SegmentVisitorPtr& visitor) {
    if (visitor == nullptr) {
        return Status(DB_ERROR, "NULL visitor");
    }
    return GetResPath<Segment>(path, root, visitor->GetSegment());
}

inline Status
GetResPath(std::string& path, const std::string& root, const SegmentFieldElementVisitorPtr& visitor) {
    if (visitor == nullptr) {
        return Status(DB_ERROR, "NULL visitor");
    }
    return GetResPath<SegmentFile>(path, root, visitor->GetFile());
}

}  // namespace milvus::engine::snapshot
