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

#include <memory>
#include <string>

#include "db/snapshot/Resources.h"
#include "utils/Status.h"

namespace milvus::engine::snapshot {

static const char* COLLECTION_PREFIX = "C_";
static const char* PARTITION_PREFIX = "P_";
static const char* SEGMENT_PREFIX = "S_";
static const char* SEGMENT_FILE_PREFIX = "F_";
static const char* MAP_SUFFIX = ".map";

template <class ResourceT>
inline std::string
GetResPath(const std::string& root, const typename ResourceT::Ptr& res_ptr) {
    return std::string();
}

template <>
inline std::string
GetResPath<Collection>(const std::string& root, const Collection::Ptr& res_ptr) {
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetID();

    return ss.str();
}

template <>
inline std::string
GetResPath<CollectionCommit>(const std::string& root, const CollectionCommit::Ptr& res_ptr) {
    auto& ids = res_ptr->GetFlushIds();
    if (ids.size() == 0) {
        return std::string();
    }

    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId();
    ss << "/";
    ss << *(ids.begin()) << MAP_SUFFIX;

    return ss.str();
}

template <>
inline std::string
GetResPath<PartitionCommit>(const std::string& root, const PartitionCommit::Ptr& res_ptr) {
    auto& ids = res_ptr->GetFlushIds();
    if (ids.size() == 0) {
        return std::string();
    }

    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetPartitionId() << "/";
    ss << *(ids.begin()) << MAP_SUFFIX;

    return ss.str();
}

template <>
inline std::string
GetResPath<Partition>(const std::string& root, const Partition::Ptr& res_ptr) {
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetID();

    return ss.str();
}

template <>
inline std::string
GetResPath<Segment>(const std::string& root, const Segment::Ptr& res_ptr) {
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetPartitionId() << "/";
    ss << SEGMENT_PREFIX << res_ptr->GetID();

    return ss.str();
}

template <>
inline std::string
GetResPath<SegmentFile>(const std::string& root, const SegmentFile::Ptr& res_ptr) {
    std::stringstream ss;
    ss << root << "/";
    ss << COLLECTION_PREFIX << res_ptr->GetCollectionId() << "/";
    ss << PARTITION_PREFIX << res_ptr->GetPartitionId() << "/";
    ss << SEGMENT_PREFIX << res_ptr->GetSegmentId() << "/";
    ss << SEGMENT_FILE_PREFIX << res_ptr->GetID();

    return ss.str();
}

///////////////////////////////////////////////////////////////////////////////////////
/// Default resource creator
template <typename T>
inline typename T::Ptr
CreateResPtr() {
    return nullptr;
}

template <>
inline Collection::Ptr
CreateResPtr<Collection>() {
    return std::make_shared<Collection>("");
}

template <>
inline CollectionCommit::Ptr
CreateResPtr<CollectionCommit>() {
    return std::make_shared<CollectionCommit>(0, 0);
}

template <>
inline Partition::Ptr
CreateResPtr<Partition>() {
    return std::make_shared<Partition>("", 0);
}

template <>
inline PartitionCommit::Ptr
CreateResPtr<PartitionCommit>() {
    return std::make_shared<PartitionCommit>(0, 0);
}

template <>
inline Segment::Ptr
CreateResPtr<Segment>() {
    return std::make_shared<Segment>(0, 0);
}

template <>
inline SegmentCommit::Ptr
CreateResPtr<SegmentCommit>() {
    return std::make_shared<SegmentCommit>(0, 0, 0);
}

template <>
inline SegmentFile::Ptr
CreateResPtr<SegmentFile>() {
    return std::make_shared<SegmentFile>(0, 0, 0, 0, FieldElementType::FET_NONE);
}

template <>
inline SchemaCommit::Ptr
CreateResPtr<SchemaCommit>() {
    return std::make_shared<SchemaCommit>(0);
}

template <>
inline Field::Ptr
CreateResPtr<Field>() {
    return std::make_shared<Field>("", 0, DataType::NONE);
}

template <>
inline FieldCommit::Ptr
CreateResPtr<FieldCommit>() {
    return std::make_shared<FieldCommit>(0, 0);
}

template <>
inline FieldElement::Ptr
CreateResPtr<FieldElement>() {
    return std::make_shared<FieldElement>(0, 0, "", FieldElementType::FET_NONE);
}

}  // namespace milvus::engine::snapshot
