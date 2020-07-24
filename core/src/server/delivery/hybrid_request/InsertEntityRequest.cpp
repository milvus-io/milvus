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

#include "server/delivery/hybrid_request/InsertEntityRequest.h"
#include "db/Utils.h"
#include "db/snapshot/Context.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu-local.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef ENABLE_CPU_PROFILING
#include <gperftools/profiler.h>
#endif

namespace milvus {
namespace server {

InsertEntityRequest::InsertEntityRequest(const std::shared_ptr<milvus::server::Context>& context,
                                         const std::string& collection_name, const std::string& partition_name,
                                         const int32_t& row_count,
                                         std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data)
    : BaseRequest(context, BaseRequest::kInsertEntity),
      collection_name_(collection_name),
      partition_name_(partition_name),
      row_count_(row_count),
      chunk_data_(chunk_data) {
}

BaseRequestPtr
InsertEntityRequest::Create(const std::shared_ptr<milvus::server::Context>& context, const std::string& collection_name,
                            const std::string& partition_name, const int32_t& row_count,
                            std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data) {
    return std::shared_ptr<BaseRequest>(
        new InsertEntityRequest(context, collection_name, partition_name, row_count, chunk_data));
}

Status
InsertEntityRequest::OnExecute() {
    LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "insert", 0) << "Execute insert request.";
    try {
        std::string hdr = "InsertEntityRequest(table=" + collection_name_ + ", partition_name=" + partition_name_ + ")";
        TimeRecorder rc(hdr);

        // step 1: check arguments
        auto status = ValidateCollectionName(collection_name_);
        if (!status.ok()) {
            return status;
        }

        if (chunk_data_.empty()) {
            return Status{SERVER_INVALID_ARGUMENT,
                          "The vector field is empty, Make sure you have entered vector records"};
        }

        // step 2: check table existence
        engine::snapshot::CollectionPtr collection;
        engine::snapshot::CollectionMappings mappings;
        status = DBWrapper::SSDB()->DescribeCollection(collection_name_, collection, mappings);
        if (collection == nullptr) {
            if (status.code() == DB_NOT_FOUND) {
                return Status(SERVER_COLLECTION_NOT_EXIST, CollectionNotExistMsg(collection_name_));
            } else {
                return status;
            }
        }

        engine::DataChunkPtr data_chunk = std::make_shared<engine::DataChunk>();
        data_chunk->count_ = row_count_;
        data_chunk->fixed_fields_.swap(chunk_data_);
        status = DBWrapper::SSDB()->InsertEntities(collection_name_, partition_name_, data_chunk);
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%ld] %s", "InsertEntities", 0, status.message().c_str());
            return status;
        }
        chunk_data_[engine::DEFAULT_UID_NAME] = data_chunk->fixed_fields_[engine::DEFAULT_UID_NAME];

        rc.RecordSection("add vectors to engine");
        rc.ElapseFromBegin("total cost");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
