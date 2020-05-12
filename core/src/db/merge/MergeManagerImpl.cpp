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
#include "db/merge/MergeLayeredStrategy.h"
#include "db/merge/MergeSimpleStrategy.h"
#include "db/merge/MergeStrategy.h"
#include "db/merge/MergeTask.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace engine {

MergeManagerImpl::MergeManagerImpl(const meta::MetaPtr& meta_ptr, const DBOptions& options, MergeStrategyType type)
    : meta_ptr_(meta_ptr), options_(options) {
    UseStrategy(type);
}

Status
MergeManagerImpl::UseStrategy(MergeStrategyType type) {
    switch (type) {
        case MergeStrategyType::SIMPLE: {
            strategy_ = std::make_shared<MergeSimpleStrategy>();
            break;
        }
        case MergeStrategyType::LAYERED: {
            strategy_ = std::make_shared<MergeLayeredStrategy>();
            break;
        }
        default: {
            std::string msg = "Unsupported merge strategy type: " + std::to_string((int32_t)type);
            LOG_ENGINE_ERROR_ << msg;
            throw Exception(DB_ERROR, msg);
        }
    }

    return Status::OK();
}

Status
MergeManagerImpl::MergeFiles(const std::string& collection_id) {
    if (strategy_ == nullptr) {
        std::string msg = "No merge strategy specified";
        LOG_ENGINE_ERROR_ << msg;
        return Status(DB_ERROR, msg);
    }

    meta::FilesHolder files_holder;
    auto status = meta_ptr_->FilesToMerge(collection_id, files_holder);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to get merge files for collection: " << collection_id;
        return status;
    }

    if (files_holder.HoldFiles().size() < 2) {
        return Status::OK();
    }

    MergeFilesGroups files_groups;
    status = strategy_->RegroupFiles(files_holder, files_groups);
    if (!status.ok()) {
        LOG_ENGINE_ERROR_ << "Failed to regroup files for: " << collection_id
                          << ", continue to merge all files into one";

        MergeTask task(meta_ptr_, options_, files_holder.HoldFiles());
        return task.Execute();
    }

    for (auto& group : files_groups) {
        MergeTask task(meta_ptr_, options_, group);
        status = task.Execute();

        files_holder.UnmarkFiles(group);
    }

    return status;
}

}  // namespace engine
}  // namespace milvus
