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

#include "db/snapshot/ResourceHelper.h"
#include "db/snapshot/Resources.h"

namespace milvus::engine::snapshot {

template<class ResourceT>
Status
GetResFiles(std::vector<std::string>& file_list, class ResourceT::Ptr& res_ptr) {
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
template<>
Status
GetResFiles<Collection>(std::vector<std::string>& file_list, Collection::Ptr& res_ptr) {
    file_list.push_back(res_ptr->GetName());
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
template<>
Status
GetResFiles<Partition>(std::vector<std::string>& file_list, Partition::Ptr& res_ptr) {
    file_list.push_back(res_ptr->GetName());
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
template<>
Status
GetResFiles<Segment>(std::vector<std::string>& file_list, Segment::Ptr& res_ptr) {
    file_list.push_back(std::to_string(res_ptr->GetID()));
    return Status::OK();
}

////////////////////////////////////////////////////////////////////////////////
template<>
Status
GetResFiles<SegmentFile>(std::vector<std::string>& file_list, SegmentFile::Ptr& res_ptr) {
    file_list.push_back(std::to_string(res_ptr->GetID()));
    return Status::OK();
}

}  // namespace milvus::engine::snapshot
