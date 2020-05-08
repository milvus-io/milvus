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

#include "db/merge/MergeManagerImpl.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

MergeManagerImpl::MergeManagerImpl(const meta::MetaPtr& meta_ptr, const DBOptions& options)
: meta_ptr_(meta_ptr), options_(options) {

}

Status MergeManagerImpl::MergeFiles(const std::string& collection_id) {
    meta::FilesHolder files_holder;
    auto status = meta_ptr_->FilesToMerge(collection_id, files_holder);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to get merge files for collection: " << collection_id;
        return status;
    }

    if (files_holder.HoldFiles().size() < 2) {
        return Status::OK();
    }

    return Status::OK();
}

}  // namespace engine
}  // namespace milvus
