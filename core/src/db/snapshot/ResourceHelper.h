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
#include <vector>

#include "db/snapshot/Resources.h"
#include "utils/Status.h"

namespace milvus::engine::snapshot {

template <class ResourceT>
inline Status
GetResFiles(std::vector<std::string>& file_list, typename ResourceT::Ptr& res_ptr) {
    return Status::OK();
}

template <>
inline Status
GetResFiles<Collection>(std::vector<std::string>& file_list, Collection::Ptr& res_ptr) {
    std::stringstream ss;
    ss << res_ptr->GetID();

    file_list.push_back(ss.str());
    return Status::OK();
}

template <>
inline Status
GetResFiles<Partition>(std::vector<std::string>& file_list, Partition::Ptr& res_ptr) {
    std::stringstream ss;
    ss << res_ptr->GetCollectionId() << "/";
    ss << res_ptr->GetID();

    file_list.push_back(ss.str());
    return Status::OK();
}

template <>
inline Status
GetResFiles<Segment>(std::vector<std::string>& file_list, Segment::Ptr& res_ptr) {
    std::stringstream ss;
    ss << res_ptr->GetCollectionId() << "/";
    ss << res_ptr->GetPartitionId() << "/";
    ss << res_ptr->GetID();

    file_list.push_back(ss.str());
    return Status::OK();
}

template <>
inline Status
GetResFiles<SegmentFile>(std::vector<std::string>& file_list, SegmentFile::Ptr& res_ptr) {
    std::stringstream ss;
    ss << res_ptr->GetCollectionId() << "/";
    ss << res_ptr->GetPartitionId() << "/";
    ss << res_ptr->GetSegmentId() << "/";
    ss << res_ptr->GetID();

    file_list.push_back(ss.str());
    return Status::OK();
}

}  // namespace milvus::engine::snapshot
