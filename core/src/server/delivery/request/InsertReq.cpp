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

#include "server/delivery/request/InsertReq.h"
#include "db/Utils.h"
#include "db/snapshot/Context.h"
#include "server/DBWrapper.h"
#include "server/ValidationUtil.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <fiu/fiu-local.h>
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

InsertReq::InsertReq(const ContextPtr& context, const std::string& collection_name, const std::string& partition_name,
                     const int64_t& row_count, std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data)
    : BaseReq(context, ReqType::kInsert),
      collection_name_(collection_name),
      partition_name_(partition_name),
      row_count_(row_count),
      chunk_data_(chunk_data) {
}

BaseReqPtr
InsertReq::Create(const ContextPtr& context, const std::string& collection_name, const std::string& partition_name,
                  const int64_t& row_count, std::unordered_map<std::string, std::vector<uint8_t>>& chunk_data) {
    return std::shared_ptr<BaseReq>(new InsertReq(context, collection_name, partition_name, row_count, chunk_data));
}

Status
InsertReq::OnExecute() {
    LOG_SERVER_INFO_ << LogOut("[%s][%ld] ", "insert", 0) << "Execute InsertReq.";
    try {
        std::string hdr = "InsertReq(table=" + collection_name_ + ", partition_name=" + partition_name_ + ")";
        TimeRecorder rc(hdr);

        if (chunk_data_.empty()) {
            return Status{SERVER_INVALID_ARGUMENT,
                          "The vector field is empty, Make sure you have entered vector records"};
        }

        // step 1: check collection existence
        bool exist = false;
        STATUS_CHECK(DBWrapper::DB()->HasCollection(collection_name_, exist));
        if (!exist) {
            return Status(SERVER_COLLECTION_NOT_EXIST, "Collection not exist: " + collection_name_);
        }

        // step 2: construct insert data
        engine::DataChunkPtr data_chunk = std::make_shared<engine::DataChunk>();
        data_chunk->count_ = row_count_;
        for (auto& pair : chunk_data_) {
            engine::BinaryDataPtr bin = std::make_shared<engine::BinaryData>();
            bin->data_.swap(pair.second);
            data_chunk->fixed_fields_.insert(std::make_pair(pair.first, bin));
        }

        // step 3: check insert data limitation
        auto status = ValidateInsertDataSize(data_chunk);
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%d] Invalid vector data: %s", "insert", 0, status.message().c_str());
            return status;
        }

        // step 4: insert data into db
        status = DBWrapper::DB()->Insert(collection_name_, partition_name_, data_chunk);
        if (!status.ok()) {
            LOG_SERVER_ERROR_ << LogOut("[%s][%ld] %s", "Insert", 0, status.message().c_str());
            return status;
        }

        // step 5: return entity id to client
        auto iter = data_chunk->fixed_fields_.find(engine::FIELD_UID);
        if (iter == data_chunk->fixed_fields_.end() || iter->second == nullptr) {
            return Status(SERVER_UNEXPECTED_ERROR, "Insert action return empty id array");
        }
        chunk_data_[engine::FIELD_UID] = iter->second->data_;

        rc.ElapseFromBegin("done");
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
